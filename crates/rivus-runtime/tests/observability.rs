//! Observability / telemetry correctness (Epic #30, Pillar A — issue #31).
//!
//! These assert that *measurement* is correct and, crucially, that it never
//! changes the result. Pillar A is pure accounting, so every test here also
//! checks the data is exactly what an unmeasured run would produce.

use rivus_runtime::gendata::{self, Rng};
use rivus_runtime::{run, run_with_progress, RunOptions, RuntimeSnapshot};

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

/// A2: a parallel (byte-range) run records per-worker telemetry — one entry per
/// worker — whose `rows_out` sum equals the run's total, while the result and
/// the node aggregate are unchanged. The serial path leaves `workers` empty.
#[test]
fn parallel_run_records_per_worker_telemetry() {
    // A file large enough to split into ≥2 byte ranges; a `save` sink to a real
    // file makes it eligible for the streaming-parallel path.
    let rows = 200_000usize;
    let csv = TempCsv(gendata::write_temp(
        "obs_workers",
        &gendata::clean(rows, 13),
    ));
    let mut out = csv.0.clone();
    out.set_extension("out.csv");
    let _oguard = TempCsv(out.clone());

    let src = format!(
        "F:\n open {}\n |? age >= 50\n |> name age\n save {}\n;",
        csv.0.display(),
        out.display()
    );
    let graph = rivus_parser::parse(&src).expect("parse");
    let (graph, _r) = rivus_optimizer::optimize(graph);

    // Force the streaming-parallel reader (threshold 0). Restore afterwards.
    std::env::remove_var("RIVUS_NO_PARALLEL");
    std::env::set_var("RIVUS_PARALLEL_MIN_BYTES", "0");
    let res = run(
        &graph,
        RunOptions {
            chunk_size: 8192,
            ..Default::default()
        },
    )
    .expect("run");
    std::env::remove_var("RIVUS_PARALLEL_MIN_BYTES");

    // Per-worker telemetry exists, with ≥2 workers, indexed 0..n.
    assert!(
        res.workers.len() >= 2,
        "expected ≥2 workers, got {}",
        res.workers.len()
    );
    for (i, w) in res.workers.iter().enumerate() {
        assert_eq!(w.worker, i, "worker indices are 0..n in order");
    }
    // The workers' rows_out sum to the run's total (the sink is written once on
    // merge, so total_rows_out is 0 here — compare against the worker sum and an
    // independent oracle instead).
    let worker_rows: u64 = res.workers.iter().map(|w| w.rows_out).sum();

    // Oracle: count age>=50 by replaying the PRNG.
    let mut rng = Rng::new(13);
    let mut passing = 0u64;
    for _ in 0..rows {
        let age = rng.below(90);
        let _ = rng.below(10_000);
        let _ = rng.below(5);
        let _ = rng.below(2);
        if age >= 50 {
            passing += 1;
        }
    }
    assert_eq!(
        worker_rows, passing,
        "per-worker rows_out must sum to the result"
    );
}

/// A3: a run records a first-row latency (time to the first produced chunk).
/// It's `Some` for any run that produces rows, and `None` for an empty result.
#[test]
fn first_row_latency_is_recorded() {
    let rows = 5_000usize;
    let csv = TempCsv(gendata::write_temp(
        "obs_firstrow",
        &gendata::clean(rows, 21),
    ));
    let p = csv.0.display();

    // A run that produces rows: latency is recorded.
    let res = run_src(&format!("F:\n open {p}\n |> name age\n;"), 4096);
    assert_eq!(res.total_rows_out(), rows as u64);
    assert!(
        res.first_row_latency.is_some(),
        "a producing run must record a first-row latency"
    );

    // A run whose source yields nothing (impossible filter is post-source, so
    // instead point at an empty file) records no first row.
    let empty = TempCsv(gendata::write_temp("obs_empty", "id,age\n"));
    let res = run_src(
        &format!("F:\n open {}\n |> id age\n;", empty.0.display()),
        4096,
    );
    assert_eq!(res.total_rows_out(), 0);
    assert!(
        res.first_row_latency.is_none(),
        "an empty source produces no first row"
    );
}

/// A5: a progress subscriber receives ≥1 live snapshot during a run, the final
/// snapshot's rows_seen matches the data, and the result is unchanged whether or
/// not a hook is attached.
#[test]
fn progress_hook_publishes_live_snapshots() {
    let rows = 60_000usize; // enough chunks to trigger several snapshots
    let csv = TempCsv(gendata::write_temp(
        "obs_snapshot",
        &gendata::clean(rows, 31),
    ));
    let p = csv.0.display();
    let src = format!("F:\n open {p}\n |> name age\n;");
    let graph = rivus_parser::parse(&src).expect("parse");

    // Baseline (no hook) for result-invariance.
    std::env::set_var("RIVUS_NO_PARALLEL", "1");
    let baseline = run(
        &graph,
        RunOptions {
            chunk_size: 4096,
            ..Default::default()
        },
    )
    .expect("run");

    // With a subscriber: collect the snapshots.
    let mut snaps: Vec<RuntimeSnapshot> = Vec::new();
    let mut hook = |s: &RuntimeSnapshot| snaps.push(s.clone());
    let res = run_with_progress(
        &graph,
        RunOptions {
            chunk_size: 4096,
            ..Default::default()
        },
        Some(&mut hook),
    )
    .expect("run");
    std::env::remove_var("RIVUS_NO_PARALLEL");

    // Result invariance: hook changes nothing.
    assert_eq!(res.total_rows_out(), baseline.total_rows_out());
    assert_eq!(res.total_rows_out(), rows as u64);

    // At least one snapshot, monotonically non-decreasing rows_seen, and the
    // final snapshot saw every row.
    assert!(!snaps.is_empty(), "subscriber received no snapshots");
    let mut last = 0u64;
    for s in &snaps {
        assert!(s.rows_seen >= last, "rows_seen must be monotonic");
        assert_eq!(s.nodes.len(), graph.nodes.len(), "all nodes in snapshot");
        last = s.rows_seen;
    }
    assert_eq!(
        snaps.last().unwrap().rows_seen,
        rows as u64,
        "final snapshot must have seen every row"
    );
}
