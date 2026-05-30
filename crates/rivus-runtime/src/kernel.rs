//! Vectorized predicate kernels for the numeric lanes.
//!
//! The reference interpreter (`eval.rs`) walks the `Expr` tree *and resolves
//! each field by name* on every row — `O(rows × fields)` name lookups. For the
//! overwhelmingly common shape (a conjunction of `field <cmp> number`) we
//! instead **compile once**: resolve each field to a column index and a numeric
//! rhs, then evaluate with tight, per-column typed loops the compiler can
//! auto-vectorize. Anything that doesn't fit (OR, string compares, deep/dynamic
//! access) returns `None` and the caller falls back to the interpreter, so
//! results are identical (gated by `tests/optimizer_equiv.rs` + stress tests).

use rivus_core::{Chunk, Column, Value};
use rivus_ir::{Access, CmpOp, Expr};
use std::cmp::Ordering;

/// A compiled `column <op> rhs` comparison on a numeric lane.
pub struct NumCmp {
    col: usize,
    op: CmpOp,
    rhs: f64,
}

/// Try to compile a conjunction of predicates into numeric comparisons bound to
/// column indices for `schema`. Returns `None` if any predicate isn't a pure
/// numeric `field <cmp> literal` (e.g. OR, string compare, missing/str column).
pub fn compile(preds: &[&Expr], chunk: &Chunk) -> Option<Vec<NumCmp>> {
    let mut out = Vec::new();
    for p in preds {
        flatten_conj(p, chunk, &mut out)?;
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn flatten_conj(e: &Expr, chunk: &Chunk, out: &mut Vec<NumCmp>) -> Option<()> {
    match e {
        Expr::And(a, b) => {
            flatten_conj(a, chunk, out)?;
            flatten_conj(b, chunk, out)
        }
        Expr::Compare { left, op, right } => {
            out.push(compile_cmp(left, *op, right, chunk)?);
            Some(())
        }
        _ => None, // OR / bare field / literal → not a numeric conjunction
    }
}

fn compile_cmp(left: &Expr, op: CmpOp, right: &Expr, chunk: &Chunk) -> Option<NumCmp> {
    // Accept `field <op> literal` or `literal <op> field` (operator flipped).
    if let (Some(col), Some(rhs)) = (num_col(left, chunk), lit_num(right)) {
        return Some(NumCmp { col, op, rhs });
    }
    if let (Some(rhs), Some(col)) = (lit_num(left), num_col(right, chunk)) {
        return Some(NumCmp {
            col,
            op: flip(op),
            rhs,
        });
    }
    None
}

fn num_col(e: &Expr, chunk: &Chunk) -> Option<usize> {
    match e {
        Expr::Field {
            name,
            access: Access::Fast,
        } => {
            let idx = chunk.schema.index_of(name)?;
            match chunk.columns[idx] {
                Column::I64(_) | Column::F64(_) | Column::Bool(_) => Some(idx),
                Column::Str(_) => None,
            }
        }
        _ => None,
    }
}

fn lit_num(e: &Expr) -> Option<f64> {
    match e {
        Expr::Literal(v) => match v {
            Value::I64(_) | Value::F64(_) | Value::Bool(_) => v.as_f64(),
            _ => None,
        },
        _ => None,
    }
}

fn flip(op: CmpOp) -> CmpOp {
    match op {
        CmpOp::Lt => CmpOp::Gt,
        CmpOp::Le => CmpOp::Ge,
        CmpOp::Gt => CmpOp::Lt,
        CmpOp::Ge => CmpOp::Le,
        CmpOp::Eq => CmpOp::Eq,
        CmpOp::Ne => CmpOp::Ne,
    }
}

/// Evaluate the compiled conjunction over a chunk, returning surviving row
/// indices. Each predicate is applied with a per-column typed loop.
pub fn run(preds: &[NumCmp], chunk: &Chunk) -> Vec<usize> {
    debug_assert!(!preds.is_empty());
    // Seed the candidate set from the first predicate, then narrow.
    let mut keep = apply(&preds[0], chunk, None);
    for p in &preds[1..] {
        keep = apply(p, chunk, Some(&keep));
    }
    keep
}

/// Apply one comparison. If `over` is `Some`, only those rows are tested
/// (narrowing); otherwise the whole column is scanned.
fn apply(p: &NumCmp, chunk: &Chunk, over: Option<&[usize]>) -> Vec<usize> {
    macro_rules! scan {
        ($get:expr) => {{
            match over {
                None => {
                    let n = chunk.len;
                    let mut keep = Vec::with_capacity(n);
                    for i in 0..n {
                        if cmp($get(i), p.op, p.rhs) {
                            keep.push(i);
                        }
                    }
                    keep
                }
                Some(idx) => idx
                    .iter()
                    .copied()
                    .filter(|&i| cmp($get(i), p.op, p.rhs))
                    .collect(),
            }
        }};
    }
    match &chunk.columns[p.col] {
        Column::I64(v) => scan!(|i: usize| v[i] as f64),
        Column::F64(v) => scan!(|i: usize| v[i]),
        Column::Bool(v) => scan!(|i: usize| if v[i] { 1.0 } else { 0.0 }),
        Column::Str(_) => Vec::new(), // compiled out by `num_col`
    }
}

#[inline]
fn cmp(v: f64, op: CmpOp, rhs: f64) -> bool {
    matches!(
        (v.partial_cmp(&rhs), op),
        (Some(Ordering::Equal), CmpOp::Eq | CmpOp::Le | CmpOp::Ge)
            | (Some(Ordering::Less), CmpOp::Lt | CmpOp::Le | CmpOp::Ne)
            | (Some(Ordering::Greater), CmpOp::Gt | CmpOp::Ge | CmpOp::Ne)
    )
}
