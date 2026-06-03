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
//! asserted without a measured number", the `unsafe` intrinsic path was dropped.
//!
//! That measured bottleneck — index collection — is what [`compact_mask`]
//! targets (Epic #38 lever 2, #40): the mask → selection-vector build is now
//! **branch-free**, so a random ~50 %-selectivity mask pays no branch
//! mispredictions (measured 7.3× over the branchy `filter().collect()` at 50 %,
//! constant regardless of selectivity). See `docs/BENCHMARKS.md`.

use rivus_core::{Chunk, Column, Decimal, Value};
use rivus_ir::{Access, CmpOp, Expr};
use std::cmp::Ordering;

/// A compiled `column <op> rhs` comparison on a numeric lane.
pub struct NumCmp {
    col: usize,
    op: CmpOp,
    rhs: f64,
    /// The literal as an exact decimal, when it was written as one (or an
    /// integer). Used for the decimal lane so the compare never rounds the
    /// literal; the i64/f64/bool lanes keep using `rhs`.
    rhs_dec: Option<Decimal>,
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
        return Some(NumCmp {
            col,
            op,
            rhs,
            rhs_dec: lit_dec(right),
        });
    }
    if let (Some(rhs), Some(col)) = (lit_num(left), num_col(right, chunk)) {
        return Some(NumCmp {
            col,
            op: flip(op),
            rhs,
            rhs_dec: lit_dec(left),
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
                // Datetime stays off the f64 kernel (ns ticks > 2^53 lose
                // precision as f64); it routes to the interpreter's exact i64
                // `dt_cmp` instead, so kernel and interpreter agree. Design 23 / #53.
                // Datetime/duration stay off the f64 kernel (ns ticks > 2^53 lose
                // precision as f64); they route to the interpreter's exact i64
                // path instead, so kernel and interpreter agree. Design 23 / #53/#57.
                Column::DateTime(_) | Column::Duration(_) | Column::Date(_) => None,
                Column::Str(_) => None,
            }
        }
        _ => None,
    }
}

fn lit_num(e: &Expr) -> Option<f64> {
    match e {
        Expr::Literal(v) => match v {
            // `Dec` covers written decimal literals (their f64 view drives the
            // i64/f64 lanes); the exact value rides along in `rhs_dec`.
            Value::I64(_) | Value::F64(_) | Value::Bool(_) | Value::Dec(_) => v.as_f64(),
            _ => None,
        },
        _ => None,
    }
}

/// The literal as an exact decimal (written decimals and integers), for the
/// decimal lane's no-rounding compare. `None` for an f64 literal (then the
/// decimal arm degrades to the f64 view).
fn lit_dec(e: &Expr) -> Option<Decimal> {
    match e {
        Expr::Literal(Value::Dec(d)) => Some(*d),
        Expr::Literal(Value::I64(n)) => Some(Decimal::new(*n as i128, 0)),
        Expr::Literal(Value::Bool(b)) => Some(Decimal::new(*b as i128, 0)),
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
    // Single pass: surviving mask → the selection vector of row indices.
    compact_mask(&mask)
}

/// Build the **selection vector** (surviving row indices, ascending) from a
/// byte mask. Branch-free (#40): each step writes the current index and advances
/// the write cursor by the mask bit, so a random ~50 %-selectivity mask costs no
/// branch mispredictions — the measured bottleneck was this index collection,
/// not the compare (kernel.rs header / BENCHMARKS). Identical output to the
/// branchy `filter(..).collect()`: the same indices in the same order.
#[inline]
fn compact_mask(mask: &[u8]) -> Vec<usize> {
    let n = mask.len();
    let mut keep: Vec<usize> = Vec::with_capacity(n);
    // SAFETY: `w` advances by at most 1 per iteration and starts at 0, so before
    // the write at iteration `i` we have `w ≤ i < n ≤ capacity` — every write is
    // in-bounds. The final `w` (≤ n) is the count of set bytes, a valid length.
    let ptr = keep.as_mut_ptr();
    let mut w = 0usize;
    for (i, &m) in mask.iter().enumerate() {
        unsafe {
            *ptr.add(w) = i;
        }
        w += (m != 0) as usize;
    }
    unsafe {
        keep.set_len(w);
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
        // Decimal: compare against the *exact* decimal literal at the larger of
        // the two scales — never rounding the literal (accounting contract;
        // shared with the interpreter so the two stay byte-identical). #44 / doc 21.
        Column::Dec(d) => dec_write(d, p, mask),
        // Datetime/duration are never compiled into the kernel (`num_col` returns
        // `None`) — they route to the interpreter's exact i64 path. Unreachable;
        // here only for match exhaustiveness. #53/#57.
        Column::DateTime(_) | Column::Duration(_) | Column::Date(_) | Column::Str(_) => {
            mask.fill(0)
        }
    }
}

/// Seed/AND helper for the decimal lane (factored so `write_mask` and `and_mask`
/// share the exact-literal compare). `seed` writes the mask; otherwise ANDs.
///
/// The exact compare is `Decimal(u, d.scale) <op> lit` at the larger of the two
/// scales — the same rule the interpreter runs through `Decimal::partial_cmp`,
/// so the two are byte-identical. The per-row work (lifting `u` and the literal
/// to the common scale) is **hoisted**: the literal lifts once, and each cell
/// only multiplies by `factor` (which is `1` whenever the literal's scale ≤ the
/// column's — the common case). Overflow on a cell falls back to the exact
/// per-cell compare, identical to the interpreter.
fn dec_mask(d: &rivus_core::DecColumn, p: &NumCmp, mask: &mut [u8], seed: bool) {
    let put = |m: &mut u8, b: bool| {
        let b = b as u8;
        if seed {
            *m = b;
        } else {
            *m &= b;
        }
    };
    match p.rhs_dec {
        Some(lit) => {
            let common = d.scale.max(lit.scale);
            match (
                lit.rescale(common),
                crate::eval::pow10_i128(common - d.scale),
            ) {
                (Some(lit_c), Some(factor)) => {
                    let r = lit_c.unscaled;
                    if factor == 1 {
                        // Common case (literal scale ≤ column scale): the cell is
                        // already at the common scale — a single i128 compare.
                        for (m, &u) in mask.iter_mut().zip(d.unscaled.iter()) {
                            put(m, crate::eval::dec_cmp_i128(u, p.op, r));
                        }
                    } else {
                        // Literal is finer than the column: lift each cell up.
                        for (m, &u) in mask.iter_mut().zip(d.unscaled.iter()) {
                            let b = match u.checked_mul(factor) {
                                Some(uc) => crate::eval::dec_cmp_i128(uc, p.op, r),
                                // Cell-level i128 overflow → exact per-cell compare
                                // (matches the interpreter's f64-view fallback).
                                None => crate::eval::dec_cmp(u, d.scale, p.op, &lit),
                            };
                            put(m, b);
                        }
                    }
                }
                // The literal itself can't be lifted to the common scale: exact
                // per-cell compare for the whole column (rare; huge scale gap).
                _ => {
                    for (m, &u) in mask.iter_mut().zip(d.unscaled.iter()) {
                        put(m, crate::eval::dec_cmp(u, d.scale, p.op, &lit));
                    }
                }
            }
        }
        // No exact literal (an f64 literal): degrade to the f64 view, identically
        // to the interpreter's numeric path.
        None => {
            let pow = 10f64.powi(d.scale as i32);
            for (m, &u) in mask.iter_mut().zip(d.unscaled.iter()) {
                put(m, cmp_scalar(u as f64 / pow, p.op, p.rhs));
            }
        }
    }
}

#[inline]
fn dec_write(d: &rivus_core::DecColumn, p: &NumCmp, mask: &mut [u8]) {
    dec_mask(d, p, mask, true);
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
        Column::Dec(d) => dec_mask(d, p, mask, false),
        // Unreachable: `num_col` excludes datetime/duration (exact i64 path). #53/#57.
        Column::DateTime(_) | Column::Duration(_) | Column::Date(_) | Column::Str(_) => {
            mask.fill(0)
        }
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
        NumCmp {
            col,
            op,
            rhs,
            rhs_dec: None,
        }
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

    /// `compact_mask` must produce exactly the branchy `filter(..).collect()`
    /// reference — the same surviving indices in ascending order — for every
    /// selectivity and length (incl. empty, all-set, all-clear, loop tails). #40.
    #[test]
    fn compact_mask_matches_branchy() {
        let branchy =
            |mask: &[u8]| -> Vec<usize> { (0..mask.len()).filter(|&i| mask[i] != 0).collect() };
        let mut rng = Rng(0xABCD_1234);
        for &n in &[0usize, 1, 2, 3, 7, 8, 31, 64, 1000, 4096, 4097] {
            for &density in &[0u64, 1, 13, 50, 87, 100] {
                let mask: Vec<u8> = (0..n)
                    .map(|_| ((rng.next() % 100) < density) as u8)
                    .collect();
                assert_eq!(
                    compact_mask(&mask),
                    branchy(&mask),
                    "compact_mask != branchy (n={n}, density={density})"
                );
                // Also exercise non-1 truthy byte values (mask uses 0/1, but the
                // contract is "non-zero survives").
                let mask2: Vec<u8> = mask.iter().map(|&m| m.wrapping_mul(255)).collect();
                assert_eq!(compact_mask(&mask2), branchy(&mask2));
            }
        }
    }

    /// Micro-benchmark (ignored; run with
    /// `cargo test -p rivus-runtime --release --lib bench_compact_mask -- --ignored --nocapture`):
    /// selection-vector build, branchy `filter().collect()` vs branch-free. #40.
    #[test]
    #[ignore]
    fn bench_compact_mask() {
        use std::time::Instant;
        let n = 1_000_000usize;
        let reps = 300usize;
        println!("\n[#40 compact-mask] n={n} reps={reps}");
        for density in [1u64, 25, 50, 75, 99] {
            let mut rng = Rng(0x5EED ^ density);
            let mask: Vec<u8> = (0..n)
                .map(|_| ((rng.next() % 100) < density) as u8)
                .collect();

            let t = Instant::now();
            let mut sink = 0usize;
            for _ in 0..reps {
                let v: Vec<usize> = (0..n).filter(|&i| mask[i] != 0).collect();
                sink ^= v.len();
            }
            let branchy = t.elapsed();

            let t = Instant::now();
            for _ in 0..reps {
                let v = compact_mask(&mask);
                sink ^= v.len();
            }
            let bfree = t.elapsed();
            std::hint::black_box(sink);

            println!(
                "  sel {density:>2}% | branchy {:>8.2?}  branch-free {:>8.2?}  ({:.2}x)",
                branchy,
                bfree,
                branchy.as_secs_f64() / bfree.as_secs_f64()
            );
        }
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
