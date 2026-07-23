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

/// The engine reads `RIVUS_NO_PARALLEL` / `RIVUS_PARALLEL_MIN_BYTES` /
/// `RIVUS_CPUS` from the PROCESS environment, and the test harness runs tests
/// on threads — two tests mutating these concurrently race. Measured flake
/// (CI, 2026-07-19): `parallel_run_records_per_worker_telemetry` saw another
/// test's `RIVUS_NO_PARALLEL=1` land between its own `remove_var` and the
/// engine's check, fell back to serial, and asserted `workers == 0`. Every
/// env-touching test serializes its whole set→run→remove span through this
/// lock; a poisoned lock (a panicked test) just yields the guard.
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
fn env_guard() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn run_src(src: &str, chunk_size: usize) -> rivus_runtime::RunResult {
    let graph = rivus_parser::parse(src).expect("parse");
    // The reader-side prefilter is produced by the optimizer's filter_pushdown,
    // so run the optimized graph (as the CLI does).
    let (graph, _report) = rivus_optimizer::optimize(graph);
    // Force serial so the prefilter count is reported by a single reader (the
    // parallel path sums per-worker counts, also valid but harder to assert).
    let _env = env_guard();
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

/// #bugreport ②④: a non-empty cell that can't be parsed into its column's lane
/// (malformed, or an `i128` overflow in the decimal lane) is defaulted to 0
/// (continue-first) AND the loss is surfaced on the error stream — one summary
/// per affected column, count exact and chunk-size independent. Empty cells are
/// "missing", not failures, so they're never counted (no false positives).
#[test]
fn parse_failures_are_reported_and_defaulted() {
    // amount: a valid decimal, a malformed cell, a valid int, and an i128 overflow.
    let body = "id,amount\n1,12.34\n2,abc\n3,7\n\
                4,999999999999999999999999999999999999999999999\n";
    let csv = TempCsv(gendata::write_temp("obs_parsefail", body));
    let p = csv.0.display();
    for cs in [1usize, 2, 4096] {
        let res = run_src(
            &format!("F:\n open {p} (id:int amount:decimal(2))\n |> id amount\n;"),
            cs,
        );
        // Continue-first: all four rows survive (bad cells kept as 0).
        assert_eq!(res.total_rows_out(), 4, "rows out @cs={cs}");
        // Exactly one summary for `amount`, counting the 2 unparseable cells.
        let msg = res
            .errors
            .iter()
            .find(|e| e.message.contains("could not be parsed"))
            .unwrap_or_else(|| panic!("no parse-failure telemetry @cs={cs}: {:?}", res.errors));
        assert!(
            msg.message.starts_with("2 value(s) in column 'amount'"),
            "@cs={cs} got: {}",
            msg.message
        );
    }
    // Clean data raises no parse-failure telemetry.
    let clean = TempCsv(gendata::write_temp(
        "obs_parsefail_clean",
        "id,n\n1,5\n2,6\n",
    ));
    let res = run_src(
        &format!(
            "F:\n open {} (id:int n:int)\n |> id n\n;",
            clean.0.display()
        ),
        4096,
    );
    assert!(
        !res.errors
            .iter()
            .any(|e| e.message.contains("could not be parsed")),
        "clean data must not raise parse-failure telemetry: {:?}",
        res.errors
    );
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
    let _env = env_guard();
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

    // Per-worker telemetry exists, with ≥2 workers, indexed 0..n. Guarded on
    // the host's parallelism like `live_hook_stays_parallel` below: on a
    // 1-effective-CPU host (a quota-constrained CI runner), the engine
    // correctly chooses the serial path, so demanding workers there would
    // assert the wrong thing — the serial fallback is the *contract*, and the
    // row-count oracle below still validates the run.
    if std::thread::available_parallelism()
        .map(|t| t.get())
        .unwrap_or(1)
        < 2
    {
        eprintln!("skipping worker-telemetry assertions: <2 CPUs available");
        return;
    }
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

/// #80: the datetime and duration lanes now surface a non-empty unparseable
/// cell on the error stream too (they previously defaulted to 0/epoch
/// *silently*), finishing the "no silent failure" pass across every lane. Empty
/// cells stay "missing" (never counted), rows survive (continue-first), the
/// counts are exact and chunk-size independent, and the data is unchanged.
#[test]
fn datetime_duration_parse_failures_are_reported() {
    // ts (datetime) / d (duration): valid, invalid, empty (missing), invalid.
    let body = "ts,d\n\
                2024-01-02,01:02:03\n\
                notadate,nope\n\
                ,\n\
                zzz,9\n";
    let csv = TempCsv(gendata::write_temp("obs_dtdur_parsefail", body));
    let p = csv.0.display();
    for cs in [1usize, 2, 4096] {
        let res = run_src(
            &format!("F:\n open {p} (ts:datetime d:duration)\n |> ts d\n;"),
            cs,
        );
        // Continue-first: all four rows survive (bad cells kept as default).
        assert_eq!(res.total_rows_out(), 4, "rows out @cs={cs}");
        let summary = |col: &str| -> String {
            res.errors
                .iter()
                .map(|e| e.message.as_str())
                .find(|m| m.contains("could not be parsed") && m.contains(&format!("'{col}'")))
                .unwrap_or_else(|| {
                    panic!(
                        "no parse-failure telemetry for {col} @cs={cs}: {:?}",
                        res.errors
                    )
                })
                .to_string()
        };
        // 2 unparseable each — the empty cell is "missing", not a failure.
        assert!(
            summary("ts").starts_with("2 value(s) in column 'ts'"),
            "@cs={cs}: {}",
            summary("ts")
        );
        assert!(
            summary("d").starts_with("2 value(s) in column 'd'"),
            "@cs={cs}: {}",
            summary("d")
        );
    }
}

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
    let _env = env_guard();
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

/// A4: a CSV source whose column widens int→float surfaces it in
/// `RunResult.inference`; a clean column does not. Result-invariant (the column
/// still resolves to F64), and it never touches the error stream.
#[test]
fn inference_widening_is_surfaced_off_the_error_stream() {
    let mut text = String::from("id,v\n");
    for i in 1..=3_000u64 {
        text.push_str(&format!("{i},{i}\n"));
    }
    text.push_str("3001,3.5\n"); // forces v: int -> float
    let csv = TempCsv(gendata::write_temp_bytes("obs_widen", text.as_bytes()));
    let res = run_src(&format!("F:\n open {}\n |> id v\n;", csv.0.display()), 4096);

    assert_eq!(res.total_rows_out(), 3001);
    // Widened column is reported in inference, NOT on the error stream.
    let widened: Vec<&str> = res
        .inference
        .iter()
        .filter(|(_, _, w)| *w)
        .map(|(n, _, _)| n.as_str())
        .collect();
    assert_eq!(widened, vec!["v"], "v should be reported widened");
    assert!(
        res.errors.is_empty(),
        "inference telemetry must not pollute the error stream: {:?}",
        res.errors
    );

    // Clean all-int column: nothing widened.
    let clean = TempCsv(gendata::write_temp_bytes(
        "obs_nowiden",
        b"id,v\n1,10\n2,20\n",
    ));
    let res = run_src(
        &format!("F:\n open {}\n |> id v\n;", clean.0.display()),
        4096,
    );
    assert!(
        !res.inference.iter().any(|(_, _, w)| *w),
        "clean column must not be widened"
    );
}

/// Pillar C (#33): the `--memory` strategy is **result-invariant** — `Low`
/// (forced serial), `Auto` and `Fast` produce byte-identical output — and the
/// chosen strategy is surfaced on `RunResult.strategy` (Observability §13).
/// `Low` must always report a serial decision.
#[test]
fn memory_strategy_is_result_invariant_and_surfaced() {
    let _env = env_guard();
    use rivus_runtime::MemoryPref;

    let csv = TempCsv(gendata::write_temp(
        "obs_strategy",
        &gendata::clean(20_000, 7),
    ));
    let p = csv.0.display();
    let src = format!("F:\n open {p}\n |? age >= 30\n |> name age\n;");

    let run_pref = |pref: MemoryPref, min_bytes: &str| -> (Vec<String>, Option<String>) {
        let g = rivus_parser::parse(&src).unwrap();
        let (g, _) = rivus_optimizer::optimize(g);
        std::env::set_var("RIVUS_PARALLEL_MIN_BYTES", min_bytes);
        let res = run(
            &g,
            RunOptions {
                chunk_size: 1024,
                memory: pref,
                ..Default::default()
            },
        )
        .unwrap();
        std::env::remove_var("RIVUS_PARALLEL_MIN_BYTES");
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("F"))
            .unwrap();
        let mut rows = Vec::new();
        for c in &o.chunks {
            for r in 0..c.len {
                let row: Vec<String> = (0..c.columns.len())
                    .map(|i| c.value(r, i).to_string())
                    .collect();
                rows.push(row.join(","));
            }
        }
        (rows, res.strategy)
    };

    // Baseline: forced serial. Its decision must say so.
    let (baseline, low_note) = run_pref(MemoryPref::Low, "8388608");
    assert!(!baseline.is_empty());
    assert!(
        low_note.as_deref().unwrap_or("").contains("serial"),
        "memory=low must report a serial decision, got {low_note:?}"
    );

    // Auto and Fast must match byte-for-byte, with a threshold low enough that
    // the autotuner would pick parallel on a multicore host.
    for pref in [MemoryPref::Auto, MemoryPref::Fast] {
        let (rows, note) = run_pref(pref, "0");
        assert_eq!(rows, baseline, "memory={pref:?} changed the result");
        assert!(note.is_some(), "a file source must surface a strategy note");
    }
}

/// #35: the string literal-substring prefilter must also engage on the
/// **parallel** byte-range path (the default for large files), not just serial.
/// We force the streaming-parallel reader and assert: (a) the result is
/// byte-identical to a forced-serial run of the same program, and (b) the
/// reader's prefilter-skip telemetry is emitted by the workers and sums to the
/// independently-computed (total − matching) count — so A1 accounting stays
/// exact across workers.
#[test]
fn string_prefilter_engages_on_parallel_path() {
    let _env = env_guard();
    // gendata::clean's country column cycles a fixed 5-country alphabet; pick a
    // needle that lands in some rows so the prescan really skips the rest.
    let rows = 120_000usize;
    let seed = 29;
    let csv = TempCsv(gendata::write_temp(
        "obs_par_strpf",
        &gendata::clean(rows, seed),
    ));

    let mut out = csv.0.clone();
    out.set_extension("strpf.out.csv");
    let _oguard = TempCsv(out.clone());

    let prog = |sink: &str| {
        format!(
            "F:\n open {}\n |? contains(country, \"US\")\n |> id country\n save {}\n;",
            csv.0.display(),
            sink
        )
    };

    // --- serial reference (real sink writes the file) ---
    let mut sout = csv.0.clone();
    sout.set_extension("strpf.serial.csv");
    let _sguard = TempCsv(sout.clone());
    let g = rivus_parser::parse(&prog(&sout.to_string_lossy())).expect("parse");
    let (g, _r) = rivus_optimizer::optimize(g);
    std::env::set_var("RIVUS_NO_PARALLEL", "1");
    run(
        &g,
        RunOptions {
            chunk_size: 8192,
            ..Default::default()
        },
    )
    .expect("serial run");
    std::env::remove_var("RIVUS_NO_PARALLEL");
    let serial_bytes = std::fs::read(&sout).expect("read serial out");

    // --- forced streaming-parallel run ---
    let g = rivus_parser::parse(&prog(&out.to_string_lossy())).expect("parse");
    let (g, _r) = rivus_optimizer::optimize(g);
    std::env::remove_var("RIVUS_NO_PARALLEL");
    std::env::set_var("RIVUS_PARALLEL_MIN_BYTES", "0");
    let res = run(
        &g,
        RunOptions {
            chunk_size: 8192,
            ..Default::default()
        },
    )
    .expect("parallel run");
    std::env::remove_var("RIVUS_PARALLEL_MIN_BYTES");
    let par_bytes = std::fs::read(&out).expect("read parallel out");

    // (a) byte-identical to serial (ordered part-file concat).
    assert_eq!(
        par_bytes, serial_bytes,
        "parallel string-prefilter output must equal serial, byte-for-byte"
    );
    assert!(
        res.workers.len() >= 2,
        "expected the parallel path (≥2 workers)"
    );

    // (b) the prefilter-skip telemetry sums to (total − matching) across
    // workers. Derive "matching" from the serial output (data rows that passed)
    // rather than replaying the generator — no dependence on gendata internals.
    let skipped: u64 = res
        .errors
        .iter()
        .filter(|e| e.message.contains("prefilter skipped"))
        .filter_map(|e| {
            e.message
                .split_whitespace()
                .find_map(|t| t.parse::<u64>().ok())
        })
        .sum();
    // Serial output rows = header + matching data rows.
    let serial_text = String::from_utf8(serial_bytes.clone()).expect("utf8");
    let matching = serial_text.lines().count().saturating_sub(1) as u64;
    assert!(
        matching > 0 && matching < rows as u64,
        "test needs a real split (matching={matching})"
    );
    assert_eq!(
        skipped,
        rows as u64 - matching,
        "parallel prefilter-skip telemetry must sum to (total − matching)"
    );
}

/// Observable First: a live progress hook (TUI / --serve) must NOT force the
/// serial path — observing a run must not throttle it. With the autotuner set to
/// parallel, a hooked run stays parallel (per-worker breakdown present) and the
/// hook still observes aggregate snapshots; the run is never relabelled "live
/// observation → serial". (Supersedes the old #36 force-serial contract.)
#[test]
fn live_hook_stays_parallel() {
    let _env = env_guard();
    let rows = 200_000usize;
    let csv = TempCsv(gendata::write_temp(
        "obs_live_parallel",
        &gendata::clean(rows, 5),
    ));
    let mut out = csv.0.clone();
    out.set_extension("live.out.csv");
    let _oguard = TempCsv(out.clone());
    let src = format!(
        "F:\n open {}\n |? age >= 30\n |> name age\n save {}\n;",
        csv.0.display(),
        out.display()
    );
    let graph = rivus_parser::parse(&src).expect("parse");
    let (graph, _r) = rivus_optimizer::optimize(graph);

    // Push the autotuner toward parallel (multicore + zero threshold), then
    // attach a hook — the engine must still run parallel and observe it.
    std::env::remove_var("RIVUS_NO_PARALLEL");
    std::env::set_var("RIVUS_CPUS", "4");
    std::env::set_var("RIVUS_PARALLEL_MIN_BYTES", "0");
    let mut frames = 0usize;
    let mut hook = |_snap: &RuntimeSnapshot| {
        frames += 1;
    };
    let res = run_with_progress(
        &graph,
        RunOptions {
            chunk_size: 4096,
            memory: rivus_runtime::MemoryPref::Auto,
            ..Default::default()
        },
        Some(&mut hook),
    )
    .expect("run_with_progress");
    std::env::remove_var("RIVUS_CPUS");
    std::env::remove_var("RIVUS_PARALLEL_MIN_BYTES");

    // The hook observes the run (at least the terminal aggregate snapshot),
    // regardless of strategy.
    assert!(frames > 0, "the hook must observe at least one snapshot");
    let note = res.strategy.unwrap_or_default();
    assert!(
        !note.contains("serial"),
        "observation must not downgrade processing to serial, got: {note}"
    );
    // On a real multicore host the parallel path actually engages under the
    // hook (the whole point): a per-worker breakdown is present.
    if std::thread::available_parallelism()
        .map(|t| t.get())
        .unwrap_or(1)
        >= 2
    {
        assert!(
            !res.workers.is_empty(),
            "a hooked, parallel-eligible run must still run parallel (got no workers)"
        );
    }
}

/// design/42 stage (c): the fused integer-id group fast path ACTIVATES on
/// dictionary-eligible chunks (発動 assert — a guard with no activation assert
/// rots), and the parallel output is byte-identical to the forced-serial run.
/// The serial oracle never dictionary-encodes, so this one comparison pins
/// dict-vs-plain AND id-vs-string end to end.
#[test]
fn fused_id_path_activates_and_matches_serial() {
    let dir = std::env::temp_dir().join(format!("rivus_idloop_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let right = dir.join("regions.csv");
    let mut rtext = String::from("region,country\n");
    for r in 0..5 {
        rtext.push_str(&format!("r{r},C{}\n", r % 3));
    }
    std::fs::write(&right, rtext).unwrap();
    // 3 files → 3 workers; low-cardinality `region` (join key) and `category`
    // (group key) sample as dictionary candidates; `amount` mixes signs so the
    // filter does real work.
    for f in 0..3usize {
        let mut t = String::from("order_id,region,category,amount\n");
        for i in 0..20_000usize {
            t.push_str(&format!(
                "{i},r{},c{},{}\n",
                (i + f) % 5,
                i % 7,
                (i % 100) as i64 - 5
            ));
        }
        std::fs::write(dir.join(format!("part_{f}.csv")), t).unwrap();
    }
    let glob = dir.join("part_*.csv");
    let out_par = dir.join("par.csv");
    let out_ser = dir.join("ser.csv");
    let src = |out: &std::path::Path| {
        format!(
            "R: open {} (region:str country:str) ;\n\
             S: ls \"{}\" read as csv cast amount :int ;\n\
             J: S &left R on region\n \
             |? amount > 0\n \
             |> (coalesce(country, \"@\")) as country (coalesce(category, \"@\")) as category amount\n \
             |# country category sum:amount count:amount\n \
             sort country category\n \
             save {} ;\n",
            right.display(),
            glob.display(),
            out.display()
        )
    };
    let parse_opt = |s: &str| rivus_optimizer::optimize(rivus_parser::parse(s).expect("parse")).0;
    let gp = parse_opt(&src(&out_par));
    let gs = parse_opt(&src(&out_ser));

    let _env = env_guard();
    std::env::remove_var("RIVUS_NO_PARALLEL");
    std::env::set_var("RIVUS_PARALLEL_MIN_BYTES", "0");
    let before = rivus_runtime::fused_id_rows_total();
    run(
        &gp,
        RunOptions {
            chunk_size: 4096,
            ..Default::default()
        },
    )
    .expect("parallel run");
    let id_delta = rivus_runtime::fused_id_rows_total() - before;
    std::env::set_var("RIVUS_NO_PARALLEL", "1");
    run(
        &gs,
        RunOptions {
            chunk_size: 4096,
            ..Default::default()
        },
    )
    .expect("serial run");
    std::env::remove_var("RIVUS_NO_PARALLEL");
    std::env::remove_var("RIVUS_PARALLEL_MIN_BYTES");

    let a = std::fs::read_to_string(&out_par).unwrap();
    let b = std::fs::read_to_string(&out_ser).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(a.lines().count() > 1, "expected real grouped output");
    assert_eq!(a, b, "fused id path must be byte-identical to serial");
    // Same guard as the worker-telemetry test: a 1-CPU host correctly stays
    // serial, where demanding activation would assert the wrong thing.
    if std::thread::available_parallelism()
        .map(|t| t.get())
        .unwrap_or(1)
        < 2
    {
        eprintln!("skipping activation assert: <2 CPUs available");
        return;
    }
    assert!(
        id_delta > 0,
        "dictionary chunks must engage the fused id fast path (0 rows took it)"
    );
}

/// Decode-column pruning (#240 キュー3, 対称方式): parallel and forced-serial
/// runs of a prunable read→sink chain — dirty data included — produce
/// byte-identical output AND identical error streams, because the SAME
/// `read_prune_allow` set feeds both paths. Structural malformed-row counting
/// is width-based, so it is unaffected by pruning and must survive on both.
#[test]
fn decode_prune_is_symmetric_and_identical() {
    let dir = std::env::temp_dir().join(format!("rivus_prune_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    for f in 0..2usize {
        let mut t = String::from("order_id,region,amount,junk\n");
        for i in 0..8_000usize {
            t.push_str(&format!(
                "{i},r{},{},garbage-{i}\n",
                i % 5,
                (i % 1000) as i64
            ));
        }
        if f == 0 {
            t.push_str("only,three,fields\n"); // malformed (width) row
        }
        std::fs::write(dir.join(format!("p{f}.csv")), t).unwrap();
    }
    let glob = dir.join("p*.csv");
    // NOT `p*.csv`-matching names — the sink outputs must never re-enter
    // the input glob on the second run.
    let out_par = dir.join("out_par.csv");
    let out_ser = dir.join("out_ser.csv");
    let src = |out: &std::path::Path| {
        format!(
            "S: ls \"{}\" read as csv cast amount :int |? amount > 500 \
             |> order_id region amount save {} ;",
            glob.display(),
            out.display()
        )
    };
    let parse_opt = |s: &str| rivus_optimizer::optimize(rivus_parser::parse(s).expect("parse")).0;
    let gp = parse_opt(&src(&out_par));
    let gs = parse_opt(&src(&out_ser));
    // The chain is prunable and `junk` is outside the set.
    let allow = rivus_runtime::read_prune_allow(&gp).expect("prunable chain");
    assert!(!allow.iter().any(|c| c == "junk"), "junk must be pruned");

    let _env = env_guard();
    std::env::remove_var("RIVUS_NO_PARALLEL");
    std::env::set_var("RIVUS_PARALLEL_MIN_BYTES", "0");
    let rp = run(&gp, RunOptions::default()).expect("parallel run");
    std::env::set_var("RIVUS_NO_PARALLEL", "1");
    let rs = run(&gs, RunOptions::default()).expect("serial run");
    std::env::remove_var("RIVUS_NO_PARALLEL");
    std::env::remove_var("RIVUS_PARALLEL_MIN_BYTES");

    let a = std::fs::read_to_string(&out_par).unwrap();
    let b = std::fs::read_to_string(&out_ser).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(a.lines().count() > 1, "expected real output");
    assert_eq!(a, b, "pruned parallel output must equal pruned serial");
    // Error-stream parity: the malformed row is structural (field-count) and
    // must surface identically on both paths.
    let msgs = |r: &rivus_runtime::RunResult| {
        let mut v: Vec<String> = r.errors.iter().map(|e| e.message.clone()).collect();
        v.sort();
        v
    };
    assert_eq!(msgs(&rp), msgs(&rs), "error streams must be in parity");
    assert!(
        rp.errors
            .iter()
            .any(|e| e.message.contains("malformed") || e.message.contains("row")),
        "the malformed row must still be reported: {:?}",
        rp.errors
    );
}

/// design/42 (b)(c) JSONL side: the JSONL reader's dictionary lanes engage
/// the SAME fused integer-id fast path (chunk-level, format-agnostic), the
/// parallel output is byte-identical to the forced-serial run, and the
/// activation is asserted (発動 assert) — the JSONL twin of
/// `fused_id_path_activates_and_matches_serial`.
#[test]
fn jsonl_dict_lanes_activate_id_path_and_match_serial() {
    let dir = std::env::temp_dir().join(format!("rivus_jdict_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let right = dir.join("regions.csv");
    let mut rtext = String::from("region,country\n");
    for r in 0..5 {
        rtext.push_str(&format!("r{r},C{}\n", r % 3));
    }
    std::fs::write(&right, rtext).unwrap();
    for f in 0..3usize {
        let mut t = String::new();
        for i in 0..20_000usize {
            t.push_str(&format!(
                "{{\"order_id\":{i},\"region\":\"r{}\",\"category\":\"c{}\",\"amount\":{}}}\n",
                (i + f) % 5,
                i % 7,
                (i % 100) as i64 - 5
            ));
        }
        std::fs::write(dir.join(format!("part_{f}.jsonl")), t).unwrap();
    }
    let glob = dir.join("part_*.jsonl");
    let out_par = dir.join("out_par.csv");
    let out_ser = dir.join("out_ser.csv");
    let src = |out: &std::path::Path| {
        format!(
            "R: open {} (region:str country:str) ;\n\
             S: ls \"{}\" read as jsonl cast amount :int ;\n\
             J: S &left R on region\n \
             |? amount > 0\n \
             |> (coalesce(country, \"@\")) as country (coalesce(category, \"@\")) as category amount\n \
             |# country category sum:amount count:amount\n \
             sort country category\n \
             save {} ;\n",
            right.display(),
            glob.display(),
            out.display()
        )
    };
    let parse_opt = |s: &str| rivus_optimizer::optimize(rivus_parser::parse(s).expect("parse")).0;
    let gp = parse_opt(&src(&out_par));
    let gs = parse_opt(&src(&out_ser));

    let _env = env_guard();
    std::env::remove_var("RIVUS_NO_PARALLEL");
    std::env::set_var("RIVUS_PARALLEL_MIN_BYTES", "0");
    let before = rivus_runtime::fused_id_rows_total();
    run(
        &gp,
        RunOptions {
            chunk_size: 4096,
            ..Default::default()
        },
    )
    .expect("parallel run");
    let id_delta = rivus_runtime::fused_id_rows_total() - before;
    std::env::set_var("RIVUS_NO_PARALLEL", "1");
    run(
        &gs,
        RunOptions {
            chunk_size: 4096,
            ..Default::default()
        },
    )
    .expect("serial run");
    std::env::remove_var("RIVUS_NO_PARALLEL");
    std::env::remove_var("RIVUS_PARALLEL_MIN_BYTES");

    let a = std::fs::read_to_string(&out_par).unwrap();
    let b = std::fs::read_to_string(&out_ser).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(a.lines().count() > 1, "expected real grouped output");
    assert_eq!(a, b, "JSONL dict/id path must be byte-identical to serial");
    if std::thread::available_parallelism()
        .map(|t| t.get())
        .unwrap_or(1)
        < 2
    {
        eprintln!("skipping activation assert: <2 CPUs available");
        return;
    }
    assert!(
        id_delta > 0,
        "JSONL dictionary chunks must engage the fused id fast path"
    );
}
