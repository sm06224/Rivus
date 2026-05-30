//! Row-wise expression evaluation for the MVP.
//!
//! This is the deliberately simple, always-correct interpreter. The optimizer
//! / JIT story (docs 08 + 09) replaces this hot path with vectorized, then
//! compiled, predicate kernels — but they must produce identical results to
//! this reference evaluator.
//!
//! Predicate evaluation has borrowed fast paths for the common `Field CMP
//! Literal` shape: a string comparison reads the arena column as `&str` and a
//! numeric comparison reads the lane directly, so neither allocates a `Value`
//! (in particular, no `String` per row for string-keyed filters). Anything that
//! doesn't fit the fast paths falls back to the owned-`Value` interpreter, so
//! results are identical.

use rivus_core::{Chunk, Column, Value};
use rivus_ir::{Access, CmpOp, Expr};
use std::cmp::Ordering;

/// Evaluate a predicate expression for a single row.
pub fn eval_predicate(expr: &Expr, chunk: &Chunk, row: usize) -> bool {
    match expr {
        Expr::Compare { left, op, right } => compare_fast(left, *op, right, chunk, row),
        Expr::And(a, b) => eval_predicate(a, chunk, row) && eval_predicate(b, chunk, row),
        Expr::Or(a, b) => eval_predicate(a, chunk, row) || eval_predicate(b, chunk, row),
        other => matches!(eval(other, chunk, row), Value::Bool(true)),
    }
}

pub fn eval(expr: &Expr, chunk: &Chunk, row: usize) -> Value {
    match expr {
        Expr::Literal(v) => v.clone(),
        Expr::Field { name, access } => eval_field(name, *access, chunk, row),
        Expr::Compare { left, op, right } => {
            Value::Bool(compare_fast(left, *op, right, chunk, row))
        }
        Expr::And(a, b) => {
            Value::Bool(eval_predicate(a, chunk, row) && eval_predicate(b, chunk, row))
        }
        Expr::Or(a, b) => {
            Value::Bool(eval_predicate(a, chunk, row) || eval_predicate(b, chunk, row))
        }
    }
}

fn eval_field(name: &str, _access: Access, chunk: &Chunk, row: usize) -> Value {
    // MVP: Fast / Deep / Dynamic all resolve via the flat schema. The slow-path
    // access strategies (recursive `$_..`, dynamic `item(..)`) are recorded in
    // the IR so the optimizer can specialize them once nested chunks land.
    match chunk.column(name) {
        Some(col) => col.value_at(row),
        None => Value::Null,
    }
}

/// Compare two sub-expressions for a row, taking borrowed fast paths first.
fn compare_fast(left: &Expr, op: CmpOp, right: &Expr, chunk: &Chunk, row: usize) -> bool {
    // String fast path: no `String` allocation per side per row.
    if let (Some(a), Some(b)) = (as_str(left, chunk, row), as_str(right, chunk, row)) {
        return cmp_ord(a.partial_cmp(b), op);
    }
    // Numeric fast path (int/float/bool lanes), no allocation.
    if let (Some(a), Some(b)) = (as_num(left, chunk, row), as_num(right, chunk, row)) {
        return cmp_ord(a.partial_cmp(&b), op);
    }
    // General fallback for mixed / null operands: owned-Value comparison.
    let l = eval(left, chunk, row);
    let r = eval(right, chunk, row);
    compare(&l, op, &r)
}

/// Borrow a `&str` for a Field backed by a string column, or a string literal.
fn as_str<'a>(e: &'a Expr, chunk: &'a Chunk, row: usize) -> Option<&'a str> {
    match e {
        Expr::Literal(Value::Str(s)) => Some(s),
        Expr::Field { name, .. } => match chunk.column(name)? {
            Column::Str(s) => Some(s.get(row)),
            _ => None,
        },
        _ => None,
    }
}

/// Read a numeric value for a Field backed by a numeric/bool lane, or a numeric
/// literal — without materializing a `Value`.
fn as_num(e: &Expr, chunk: &Chunk, row: usize) -> Option<f64> {
    match e {
        Expr::Literal(v) => v.as_f64(),
        Expr::Field { name, .. } => match chunk.column(name)? {
            Column::I64(v) => Some(v[row] as f64),
            Column::F64(v) => Some(v[row]),
            Column::Bool(v) => Some(if v[row] { 1.0 } else { 0.0 }),
            Column::Str(_) => None,
        },
        _ => None,
    }
}

fn cmp_ord(ord: Option<Ordering>, op: CmpOp) -> bool {
    matches!(
        (ord, op),
        (Some(Ordering::Equal), CmpOp::Eq | CmpOp::Le | CmpOp::Ge)
            | (Some(Ordering::Less), CmpOp::Lt | CmpOp::Le | CmpOp::Ne)
            | (Some(Ordering::Greater), CmpOp::Gt | CmpOp::Ge | CmpOp::Ne)
    )
}

fn compare(l: &Value, op: CmpOp, r: &Value) -> bool {
    let ord = match (l, r) {
        (Value::Str(a), Value::Str(b)) => a.partial_cmp(b),
        _ => match (l.as_f64(), r.as_f64()) {
            (Some(a), Some(b)) => a.partial_cmp(&b),
            _ => {
                // Fall back to equality semantics for mixed/null operands.
                return match op {
                    CmpOp::Eq => l == r,
                    CmpOp::Ne => l != r,
                    _ => false,
                };
            }
        },
    };
    cmp_ord(ord, op)
}
