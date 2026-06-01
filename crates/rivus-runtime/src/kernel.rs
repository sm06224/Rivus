//! Vectorized predicate kernels for the numeric lanes.
//!
//! The reference interpreter (`eval.rs`) walks the `Expr` tree *and resolves
//! each field by name* on every row — `O(rows × fields)` name lookups. For the
//! overwhelmingly common shape (a conjunction of `field <cmp> number`) we
//! instead **compile once**: resolve each field to a column index and a numeric
//! rhs, then evaluate with tight, per-column typed loops over the contiguous
//! backing slice. Anything that doesn't fit (OR, string compares, deep/dynamic
//! access) returns `None` and the caller falls back to the interpreter, so
//! results are identical (gated by `tests/optimizer_equiv.rs` + stress tests).
//!
//! ## Evaluation strategy (Epic #38, lever 1 — #39)
//!
//! Each predicate writes a **byte mask** (`1`/`0` per row) over the whole
//! column, and the conjunction ANDs masks together; a single final pass turns
//! the surviving mask into row indices. The per-row compare is **branch-free**
//! (`(v <cmp> rhs) as u8`) over a contiguous `&[i64]`/`&[f64]`, which LLVM
//! auto-vectorizes into packed SIMD compares — with **zero `unsafe` and zero
//! third-party deps**.
//!
//! A hand-written AVX2 kernel was prototyped and **measured**: it gave no
//! speedup over this auto-vectorized form because the compare is
//! memory-bandwidth-bound (~40 MB read for 5 M `f64`), and the run-time cost is
//! dominated by *index collection*, not the compare. Per "faster is never
//! asserted without a measured number", the `unsafe` intrinsic path was dropped;
//! the real lever is the gather (a columnar selection vector — Epic #38 lever 2,
//! #40). See `docs/BENCHMARKS.md`.

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
                Column::I64(_) | Column::F64(_) | Column::Bool(_) | Column::Dec(_) => Some(idx),
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
/// indices. Builds a byte mask per predicate, ANDs them, then collects indices.
pub fn run(preds: &[NumCmp], chunk: &Chunk) -> Vec<usize> {
    debug_assert!(!preds.is_empty());
    let n = chunk.len;
    // `mask[i] == 1` ⇔ row i still survives. Seed with the first predicate, then
    // AND each subsequent predicate's compare into it (over all rows — the
    // per-row compare is cheaper than a gather, and stays auto-vectorizable).
    let mut mask = vec![0u8; n];
    write_mask(&preds[0], chunk, &mut mask);
    for p in &preds[1..] {
        and_mask(p, chunk, &mut mask);
    }
    // Single pass: surviving mask → row indices.
    let mut keep = Vec::with_capacity(n);
    for (i, &m) in mask.iter().enumerate() {
        if m != 0 {
            keep.push(i);
        }
    }
    keep
}

/// Write `mask[i] = (col[i] <op> rhs)` for every row (seeding the conjunction).
fn write_mask(p: &NumCmp, chunk: &Chunk, mask: &mut [u8]) {
    match &chunk.columns[p.col] {
        Column::I64(v) => cmp_i64_into(v, p.op, p.rhs, mask),
        Column::F64(v) => cmp_f64_into(v, p.op, p.rhs, mask),
        Column::Bool(v) => {
            for (m, &b) in mask.iter_mut().zip(v.iter()) {
                *m = cmp_scalar(if b { 1.0 } else { 0.0 }, p.op, p.rhs) as u8;
            }
        }
        // Decimal: exact i128 compare against the literal scaled to the column's
        // scale (shared with the interpreter so the two stay byte-identical;
        // avoids the lossy `u as f64 / 10^scale` once |u| > 2^53). The scaling is
        // hoisted out of the row loop. #44 / doc 21.
        Column::Dec(d) => match crate::eval::dec_scaled_rhs(p.rhs, d.scale) {
            Some(r) => {
                for (m, &u) in mask.iter_mut().zip(d.unscaled.iter()) {
                    *m = crate::eval::dec_cmp_i128(u, p.op, r) as u8;
                }
            }
            None => {
                for (m, &u) in mask.iter_mut().zip(d.unscaled.iter()) {
                    *m = crate::eval::dec_cmp_f64_fallback(u, d.scale, p.op, p.rhs) as u8;
                }
            }
        },
        Column::Str(_) => mask.fill(0), // compiled out by `num_col`
    }
}

/// AND `(col[i] <op> rhs)` into an existing mask (narrowing the conjunction).
fn and_mask(p: &NumCmp, chunk: &Chunk, mask: &mut [u8]) {
    match &chunk.columns[p.col] {
        Column::I64(v) => {
            for (m, &x) in mask.iter_mut().zip(v.iter()) {
                *m &= cmp_scalar(x as f64, p.op, p.rhs) as u8;
            }
        }
        Column::F64(v) => {
            for (m, &x) in mask.iter_mut().zip(v.iter()) {
                *m &= cmp_scalar(x, p.op, p.rhs) as u8;
            }
        }
        Column::Bool(v) => {
            for (m, &b) in mask.iter_mut().zip(v.iter()) {
                *m &= cmp_scalar(if b { 1.0 } else { 0.0 }, p.op, p.rhs) as u8;
            }
        }
        Column::Dec(d) => match crate::eval::dec_scaled_rhs(p.rhs, d.scale) {
            Some(r) => {
                for (m, &u) in mask.iter_mut().zip(d.unscaled.iter()) {
                    *m &= crate::eval::dec_cmp_i128(u, p.op, r) as u8;
                }
            }
            None => {
                for (m, &u) in mask.iter_mut().zip(d.unscaled.iter()) {
                    *m &= crate::eval::dec_cmp_f64_fallback(u, d.scale, p.op, p.rhs) as u8;
                }
            }
        },
        Column::Str(_) => mask.fill(0),
    }
}

/// `i64` lane: a branch-free scalar loop over the contiguous slice. Writing
/// `(cmp) as u8` (no `push`, no branch) is what LLVM auto-vectorizes; a measured
/// experiment with hand-written AVX2 here gave no speedup (the compare is
/// memory-bandwidth-bound), so we keep the portable, zero-`unsafe` form — see
/// `docs/BENCHMARKS.md`, "SIMD predicate kernel (measured negative result)".
fn cmp_i64_into(v: &[i64], op: CmpOp, rhs: f64, mask: &mut [u8]) {
    for (m, &x) in mask.iter_mut().zip(v.iter()) {
        *m = cmp_scalar(x as f64, op, rhs) as u8;
    }
}

/// `f64` lane: the same branch-free, auto-vectorizable compare into a byte mask.
fn cmp_f64_into(v: &[f64], op: CmpOp, rhs: f64, mask: &mut [u8]) {
    for (m, &x) in mask.iter_mut().zip(v.iter()) {
        *m = cmp_scalar(x, op, rhs) as u8;
    }
}

#[inline]
fn cmp_scalar(v: f64, op: CmpOp, rhs: f64) -> bool {
    matches!(
        (v.partial_cmp(&rhs), op),
        (Some(Ordering::Equal), CmpOp::Eq | CmpOp::Le | CmpOp::Ge)
            | (Some(Ordering::Less), CmpOp::Lt | CmpOp::Le | CmpOp::Ne)
            | (Some(Ordering::Greater), CmpOp::Gt | CmpOp::Ge | CmpOp::Ne)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rivus_core::{Chunk, Column, DataType, Field, Schema};
    use std::sync::Arc;

    // SplitMix64 — same family as gendata; no `rand` dep, fully deterministic.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
    }

    fn one(col: usize, op: CmpOp, rhs: f64) -> NumCmp {
        NumCmp { col, op, rhs }
    }

    /// Scalar oracle over an f64 column: the surviving indices per `cmp_scalar`.
    fn oracle(v: &[f64], op: CmpOp, rhs: f64) -> Vec<usize> {
        (0..v.len())
            .filter(|&i| cmp_scalar(v[i], op, rhs))
            .collect()
    }

    fn f64_chunk(v: Vec<f64>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::F64)]));
        Chunk::new(1, schema, vec![Column::F64(v)])
    }

    /// The mask kernel must return exactly the scalar oracle's survivors —
    /// across seeded values that include boundary, sign, and NaN cases, and
    /// lengths that exercise the loop tail. This is the equivalence gate that
    /// pins the auto-vectorized compare to `partial_cmp` semantics (incl. NaN).
    #[test]
    fn kernel_matches_scalar_oracle() {
        let ops = [
            CmpOp::Lt,
            CmpOp::Le,
            CmpOp::Gt,
            CmpOp::Ge,
            CmpOp::Eq,
            CmpOp::Ne,
        ];
        // Lengths around multiples of 4 to hit the AVX2 tail (0..3 leftover).
        for &n in &[0usize, 1, 3, 4, 5, 7, 8, 31, 33, 1000, 4096, 4099] {
            let mut rng = Rng(0xC0FF_EE00 ^ n as u64);
            let mut v = Vec::with_capacity(n);
            for _ in 0..n {
                // A mix: small integers (exact as f64), the threshold itself,
                // negatives, and the occasional NaN to probe ordered compares.
                let r = rng.next();
                let x = match r % 8 {
                    0 => 50.0,                       // == rhs boundary
                    1 => f64::NAN,                   // unordered
                    2 => -(((r >> 8) % 100) as f64), // negatives
                    _ => ((r >> 8) % 100) as f64,    // 0..99
                };
                v.push(x);
            }
            let rhs = 50.0;
            for &op in &ops {
                let chunk = f64_chunk(v.clone());
                let plan = vec![one(0, op, rhs)];
                let got = run(&plan, &chunk);
                let want = oracle(&v, op, rhs);
                assert_eq!(got, want, "kernel != oracle (n={n}, op={op:?})");
            }
        }
    }

    /// A multi-predicate conjunction (the AND-mask path) over i64 + f64 + bool
    /// lanes matches a hand-rolled scalar oracle.
    #[test]
    fn conjunction_over_mixed_lanes_matches_oracle() {
        let n = 500usize;
        let mut rng = Rng(7);
        let (mut ages, mut scores, mut actives) = (Vec::new(), Vec::new(), Vec::new());
        for _ in 0..n {
            ages.push((rng.next() % 90) as i64);
            scores.push(((rng.next() % 10_000) as f64) / 100.0);
            actives.push(rng.next() % 2 == 1);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("age", DataType::I64),
            Field::new("score", DataType::F64),
            Field::new("active", DataType::Bool),
        ]));
        let chunk = Chunk::new(
            1,
            schema,
            vec![
                Column::I64(ages.clone()),
                Column::F64(scores.clone()),
                Column::Bool(actives.clone()),
            ],
        );
        // age >= 30 AND score < 50.0 AND active == 1
        let plan = vec![
            one(0, CmpOp::Ge, 30.0),
            one(1, CmpOp::Lt, 50.0),
            one(2, CmpOp::Eq, 1.0),
        ];
        let got = run(&plan, &chunk);
        let want: Vec<usize> = (0..n)
            .filter(|&i| {
                ages[i] >= 30 && scores[i] < 50.0 && (if actives[i] { 1.0 } else { 0.0 } == 1.0)
            })
            .collect();
        assert_eq!(got, want);
    }
}
