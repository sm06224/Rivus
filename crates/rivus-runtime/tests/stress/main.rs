//! Correctness-at-scale stress tests.
//!
//! These assert that the engine stays *correct* under the same three regimes
//! the benchmarks measure for *speed*: large clean data, error-heavy input, and
//! mixed-type columns. They run as part of `cargo test` (smaller row counts
//! than the benches so CI stays fast) and are the regression guard for every
//! optimization that follows.

// Re-exported (`pub(crate)`) so the split submodules can pull these in with a
// single `use super::*;` (design 26 §26.8.1, mechanical move-only split).
pub(crate) use rivus_runtime::gendata::{self, Rng};
pub(crate) use rivus_runtime::{run, RunOptions};

struct TempCsv(std::path::PathBuf);

impl Drop for TempCsv {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn run_src(src: &str, chunk_size: usize) -> rivus_runtime::RunResult {
    let graph = rivus_parser::parse(src).expect("parse");
    run(
        &graph,
        RunOptions {
            chunk_size,
            ..Default::default()
        },
    )
    .expect("run")
}

/// Independent oracle: count clean rows with age >= threshold by regenerating
/// the exact same PRNG sequence used by `gendata::clean`.
fn expected_clean_ge(rows: usize, seed: u64, threshold: u64) -> u64 {
    let mut rng = Rng::new(seed);
    let mut n = 0;
    for _ in 0..rows {
        let age = rng.below(90);
        let _score = rng.below(10_000);
        let _country = rng.below(5);
        let _active = rng.below(2);
        if age >= threshold {
            n += 1;
        }
    }
    n
}

/// Collect an integer column across all chunks of the output labeled `label`.
fn collect_i64(res: &rivus_runtime::RunResult, label: &str, col: &str) -> Vec<i64> {
    let mut out = Vec::new();
    let o = res
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some(label))
        .expect("labeled output");
    for c in &o.chunks {
        if let Some(ci) = c.schema.index_of(col) {
            for r in 0..c.len {
                out.push(c.value(r, ci).as_f64().unwrap() as i64);
            }
        }
    }
    out
}

fn collect_strings(res: &rivus_runtime::RunResult, label: &str, col: &str) -> Vec<String> {
    let mut out = Vec::new();
    let o = res
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some(label))
        .expect("labeled output");
    for c in &o.chunks {
        if let Some(ci) = c.schema.index_of(col) {
            for r in 0..c.len {
                out.push(c.value(r, ci).to_string());
            }
        }
    }
    out
}

// --- Split test modules (design 26 §26.8.1; move-only). New null-model
// tests land in `stress/null.rs` per the staged plan. ---
mod bug_specs;
mod byte_identity;
mod decimal;
mod filesystem;
mod groupby;
mod io_formats;
mod joins;
mod null;
mod real_etl;
mod syntax;
mod temporal;
mod transforms;
