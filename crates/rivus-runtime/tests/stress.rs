//! Correctness-at-scale stress tests.
//!
//! These assert that the engine stays *correct* under the same three regimes
//! the benchmarks measure for *speed*: large clean data, error-heavy input, and
//! mixed-type columns. They run as part of `cargo test` (smaller row counts
//! than the benches so CI stays fast) and are the regression guard for every
//! optimization that follows.

use rivus_runtime::gendata::{self, Rng};
use rivus_runtime::{run, RunOptions};

struct TempCsv(std::path::PathBuf);
impl Drop for TempCsv {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn run_src(src: &str, chunk_size: usize) -> rivus_runtime::RunResult {
    let graph = rivus_parser::parse(src).expect("parse");
    run(&graph, RunOptions { chunk_size }).expect("run")
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

#[test]
fn large_clean_filter_is_exact() {
    let rows = 50_000;
    let seed = 42;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_clean", &data));
    let p = f.0.display();

    // Run across several chunk sizes: the result must be identical regardless
    // of chunk granularity (chunk-size independence).
    let expected = expected_clean_ge(rows, seed, 45);
    for cs in [1, 7, 1024, 8192, rows] {
        let src = format!("F:\n open {p}\n |? age >= 45\n;");
        let res = run_src(&src, cs);
        assert_eq!(res.total_rows_out(), expected, "chunk_size={cs}");
        assert!(res.errors.is_empty(), "clean data should not error");
    }
}

#[test]
fn error_heavy_skips_and_continues() {
    let rows = 40_000;
    let data = gendata::error_heavy(rows, 0.5, 7);
    let f = TempCsv(gendata::write_temp("stress_err", &data));
    let p = f.0.display();

    // Roughly half the rows are malformed; the run must still succeed, surface a
    // recoverable error about skipped rows, and never go fatal.
    let src = format!("F:\n open {p}\n |? age >= 0\n;");
    let res = run_src(&src, 4096);

    assert!(
        res.errors.iter().any(|e| e.message.contains("malformed")),
        "expected a recoverable malformed-row error"
    );
    assert!(
        !res.errors.iter().any(rivus_core::ErrorEvent::is_fatal),
        "error-heavy input must not be fatal (continue-first)"
    );
    let out = res.total_rows_out();
    assert!(out > 0 && out < rows as u64, "kept {out} of {rows}");
}

#[test]
fn mixed_types_degrades_to_string_lane() {
    let rows = 30_000;
    // Pure-int column: inference picks i64, predicate is numeric.
    let pure = gendata::mixed_types(rows, 0.0, 1);
    let fp = TempCsv(gendata::write_temp("stress_pure", &pure));
    let res_pure = run_src(
        &format!("F:\n open {}\n |? value >= 50\n;", fp.0.display()),
        4096,
    );
    assert!(res_pure.errors.is_empty());

    // Mixed column: some cells are non-numeric, so inference falls back to Str
    // and the comparison runs on the string lane — it must still run, not crash.
    let mixed = gendata::mixed_types(rows, 0.3, 1);
    let fm = TempCsv(gendata::write_temp("stress_mixed", &mixed));
    let res_mixed = run_src(
        &format!("F:\n open {}\n |? value >= 50\n;", fm.0.display()),
        4096,
    );
    // Both runs complete; the mixed run produces a (string-comparison) result
    // without going fatal.
    assert!(!res_mixed
        .errors
        .iter()
        .any(rivus_core::ErrorEvent::is_fatal));
}

#[test]
fn string_filter_matches_oracle() {
    // Filter on a string column (country == "JP") must match an independent
    // count, exercising the borrowed-&str predicate fast path across chunk
    // sizes. Also checks `!=` for the complementary count.
    let rows = 40_000;
    let seed = 123;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_strfilter", &data));
    let p = f.0.display();

    // Oracle: replay the generator's PRNG to count JP rows.
    let mut rng = Rng::new(seed);
    let countries = ["JP", "US", "DE", "FR", "BR"];
    let mut jp = 0u64;
    for _ in 0..rows {
        let _age = rng.below(90);
        let _score = rng.below(10_000);
        let c = countries[rng.below(5) as usize];
        let _active = rng.below(2);
        if c == "JP" {
            jp += 1;
        }
    }

    for cs in [1, 1000, 8192] {
        let eq = run_src(&format!("F:\n open {p}\n |? country == \"JP\"\n;"), cs);
        assert_eq!(eq.total_rows_out(), jp, "== chunk_size={cs}");
        let ne = run_src(&format!("F:\n open {p}\n |? country != \"JP\"\n;"), cs);
        assert_eq!(ne.total_rows_out(), rows as u64 - jp, "!= chunk_size={cs}");
    }
}

#[test]
fn binary_source_matches_oracle() {
    // Fixed-width binary records (C struct dump): i32 id, i32 age, f64 score,
    // u8 active. Decoding must produce the same filter result as an oracle that
    // replays the generator's PRNG, across chunk sizes.
    let rows = 50_000;
    let seed = 7;
    let bytes = gendata::bin_clean(rows, seed);
    let f = TempCsv(gendata::write_temp_bytes("stress_bin", &bytes));
    let p = f.0.display();

    let mut rng = Rng::new(seed);
    let mut ge = 0u64;
    for _ in 0..rows {
        let age = rng.below(90);
        let _score = rng.below(10_000);
        let _active = rng.below(2);
        if age >= 45 {
            ge += 1;
        }
    }

    for cs in [1, 1000, 8192] {
        let src =
            format!("F:\n readbin {p} (id:i32 age:i32 score:f64 active:u8)\n |? age >= 45\n;");
        let res = run_src(&src, cs);
        assert_eq!(res.total_rows_out(), ge, "binary filter chunk_size={cs}");
        assert!(res.errors.is_empty(), "clean binary should not error");
    }
}

#[test]
fn fanout_merge_conserves_rows() {
    let rows = 20_000;
    let data = gendata::clean(rows, 99);
    let f = TempCsv(gendata::write_temp("stress_fanout", &data));
    let p = f.0.display();

    // Partition by age into 3 disjoint, exhaustive branches, then merge: the
    // merged row count must equal the clean input row count exactly.
    let src = format!(
        "Src:\n open {p}\n \
          -> A: |? age >= 60 ;\n \
          -> B: |? age >= 30 ;\n \
          -> C: |? age <  30 ;\n;\n\
         M:\n A + B + C\n;"
    );
    let res = run_src(&src, 4096);
    let merged = res
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some("M"))
        .expect("M output");
    let merged_rows: usize = merged.chunks.iter().map(|c| c.len).sum();
    // A(age>=60) + B(age>=30) + C(age<30) overlaps on [60,90): A⊂B. So the
    // conservation check is: B ∪ C == all rows, and A is a subset of B.
    // Here we assert the total equals |B|+|C|+|A| = rows + |A|.
    let a = run_src(&format!("F:\n open {p}\n |? age >= 60\n;"), 4096).total_rows_out() as usize;
    assert_eq!(merged_rows, rows + a, "fan-out/merge row conservation");
}
