//! Observability / telemetry correctness (Epic #30, Pillar A — issue #31).
//!
//! These assert that *measurement* is correct and, crucially, that it never
//! changes the result. Pillar A is pure accounting, so every test here also
//! checks the data is exactly what an unmeasured run would produce.

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
    // The reader-side prefilter is produced by the optimizer's filter_pushdown,
    // so run the optimized graph (as the CLI does).
    let (graph, _report) = rivus_optimizer::optimize(graph);
    // Force serial so the prefilter count is reported by a single reader (the
    // parallel path sums per-worker counts, also valid but harder to assert).
    std::env::set_var("RIVUS_NO_PARALLEL", "1");
    let r = run(
        &graph,
        RunOptions {
            chunk_size,
            ..Default::default()
        },
    )
    .expect("run");
    std::env::remove_var("RIVUS_NO_PARALLEL");
    r
}

/// A1: the pushed-down prefilter reports how many rows it skipped building, the
/// count equals (total − passing) computed independently, it is chunk-size
/// independent, and the *result* is unchanged (exactly the passing rows).
#[test]
fn prefilter_skip_count_is_exact_and_result_invariant() {
    let rows = 9_000usize;
    let seed = 41;
    let csv = TempCsv(gendata::write_temp(
        "obs_prefilter",
        &gendata::clean(rows, seed),
    ));
    let p = csv.0.display();

    // Oracle: replay clean()'s PRNG to count rows with age >= 50.
    let mut rng = Rng::new(seed);
    let mut passing = 0u64;
    for _ in 0..rows {
        let age = rng.below(90);
        let _score = rng.below(10_000);
        let _country = rng.below(5);
        let _active = rng.below(2);
        if age >= 50 {
            passing += 1;
        }
    }
    let skipped_oracle = rows as u64 - passing;
    assert!(passing > 0 && skipped_oracle > 0, "test needs a real split");

    for cs in [1usize, 1000, 8192] {
        // `age >= 50` is a numeric atom → filter_pushdown compiles it into the
        // reader's prefilter, so the skip is counted.
        let res = run_src(
            &format!("F:\n open {p}\n |? age >= 50\n |> name age\n;"),
            cs,
        );
        // Result invariance: exactly the passing rows survive.
        assert_eq!(res.total_rows_out(), passing, "rows out @cs={cs}");

        // The reader's prefilter-skip telemetry is present and exact.
        let msg = res
            .errors
            .iter()
            .find(|e| e.message.contains("prefilter skipped"))
            .unwrap_or_else(|| panic!("no prefilter telemetry @cs={cs}: {:?}", res.errors));
        let n: u64 = msg
            .message
            .split_whitespace()
            .find_map(|t| t.parse().ok())
            .expect("a count in the message");
        assert_eq!(n, skipped_oracle, "prefilter skip count @cs={cs}");
    }
}

/// Without a pushed-down prefilter (a non-numeric / no filter), no prefilter
/// telemetry is emitted — the counter only reflects genuine reader-side skips.
#[test]
fn no_prefilter_means_no_skip_telemetry() {
    let rows = 2_000usize;
    let csv = TempCsv(gendata::write_temp(
        "obs_noprefilter",
        &gendata::clean(rows, 7),
    ));
    let p = csv.0.display();
    let res = run_src(&format!("F:\n open {p}\n |> name age\n;"), 4096);
    assert_eq!(res.total_rows_out(), rows as u64, "all rows pass through");
    assert!(
        !res.errors
            .iter()
            .any(|e| e.message.contains("prefilter skipped")),
        "no prefilter → no skip telemetry: {:?}",
        res.errors
    );
}
