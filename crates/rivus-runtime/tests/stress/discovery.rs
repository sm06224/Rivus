//! Discovery-as-flow — `ls "glob"` → file rows (§28.3, slice 3a).
//!
//! Pins: recursive `**` glob matching, deterministic uri-ascending order,
//! chunk-size independence, the ordinary `path` / `name` / `size` columns (no
//! accessor — bare fields), predicate filtering on them, and continue-first
//! 0-match.

use super::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// A temp directory tree, cleaned up on drop.
struct TempTree(PathBuf);
impl Drop for TempTree {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Build `base/{logs/2025/c.csv, logs/2026/a.csv, logs/2026/b.csv,
/// logs/2026/skip.txt}` and return the tree (and its base path string).
fn mk_tree() -> (TempTree, String) {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir().join(format!("rivus_ls_{}_{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    for (rel, body) in [
        ("logs/2026/a.csv", "x\n1\n"),
        ("logs/2026/b.csv", "x\n2\n"),
        ("logs/2025/c.csv", "x\n3\n"),
        ("logs/2026/skip.txt", "x\n"),
    ] {
        let p = base.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, body).unwrap();
    }
    let s = base.display().to_string();
    (TempTree(base), s)
}

#[test]
fn ls_recursive_glob_is_deterministic_and_emits_file_columns() {
    let (_t, base) = mk_tree();
    // `path` (Resource → renders as uri) and `name` are ordinary columns.
    let res = run_src(&format!("L:\n ls \"{base}/**/*.csv\"\n |> path name\n;"), 4);
    let paths = collect_strings(&res, "L", "path");
    let names = collect_strings(&res, "L", "name");
    // Only the three .csv files, in uri-ascending order (2025 before 2026); the
    // .txt is excluded by the `*.csv` segment.
    assert_eq!(
        paths,
        vec![
            format!("{base}/logs/2025/c.csv"),
            format!("{base}/logs/2026/a.csv"),
            format!("{base}/logs/2026/b.csv"),
        ],
        "ls must match *.csv recursively, uri-ascending"
    );
    assert_eq!(names, vec!["c.csv", "a.csv", "b.csv"], "name = basename");
}

#[test]
fn ls_is_chunk_size_independent() {
    let (_t, base) = mk_tree();
    let order = |cs: usize| {
        let res = run_src(&format!("L:\n ls \"{base}/**/*.csv\"\n |> path\n;"), cs);
        collect_strings(&res, "L", "path")
    };
    let one = order(1);
    assert_eq!(one.len(), 3);
    assert_eq!(one, order(2), "ls order must not depend on chunk size");
    assert_eq!(one, order(1000), "ls order must not depend on chunk size");
}

#[test]
fn ls_predicate_filters_on_bare_field() {
    let (_t, base) = mk_tree();
    // Filter on the ordinary `name` column (a bare field — works in flow mode).
    let res = run_src(
        &format!("L:\n ls \"{base}/**/*.csv\"\n |? name == \"a.csv\"\n |> path\n;"),
        4,
    );
    let paths = collect_strings(&res, "L", "path");
    assert_eq!(paths, vec![format!("{base}/logs/2026/a.csv")]);
}

#[test]
fn ls_size_column_is_populated() {
    let (_t, base) = mk_tree();
    // `size` is a real i64 column (out-of-contract per §0.14, but usable). Each
    // csv here is "x\n1\n" / "x\n2\n" / "x\n3\n" = 4 bytes; filtering by size keeps
    // them all, and the values are > 0.
    let res = run_src(
        &format!("L:\n ls \"{base}/**/*.csv\"\n |? size > 0\n |> name size\n;"),
        4,
    );
    let sizes = collect_i64(&res, "L", "size");
    assert_eq!(sizes.len(), 3, "all three files have size > 0");
    assert!(
        sizes.iter().all(|&s| s == 4),
        "each csv is 4 bytes, got {sizes:?}"
    );
}

#[test]
fn ls_zero_matches_warns_and_is_empty() {
    let (_t, base) = mk_tree();
    let res = run_src(&format!("L:\n ls \"{base}/**/*.parquet\"\n |> path\n;"), 4);
    // An empty stream produces no rows (the labeled output may be absent or have
    // only empty chunks) — either way, no path is emitted.
    let rows: usize = res
        .outputs
        .iter()
        .filter(|o| o.label.as_deref() == Some("L"))
        .flat_map(|o| o.chunks.iter())
        .map(|c| c.len)
        .sum();
    assert_eq!(rows, 0, "no match → empty stream");
    assert!(
        res.errors
            .iter()
            .any(|e| e.message.contains("no files match")),
        "0 matches must surface a warning (continue-first), got {:?}",
        res.errors
    );
}

#[test]
fn watch_blocking_op_is_refused_pre_run_with_guidance() {
    // §28.12.0 (ratified #149 ①): a blocking operator downstream of the
    // unbounded `watch` would emit only on a finish that never comes — refused
    // pre-run with guidance, identically in every build (the plan-shape check
    // runs before the feature gate; never-silent, no hang).
    for flow in [
        "W:\n watch \"in/*.csv\"\n read as csv\n sort id\n;",
        "W:\n watch \"in/*.csv\"\n read as csv\n |# country sum:age\n;",
    ] {
        let g = rivus_parser::parse(flow).expect("parse is always-std");
        let err = run(&g, RunOptions::default()).expect_err("must refuse pre-run");
        let msg = err.to_string();
        assert!(
            msg.contains("unbounded") && msg.contains("take N"),
            "guidance must name the cause and a way out: {msg}"
        );
    }
}

#[cfg(not(feature = "unbounded"))]
#[test]
fn watch_without_the_feature_is_refused_pre_run() {
    // §28.12 (ratified #149 ⑤): the default (zero-dep) build cannot evaluate
    // `watch` — explicit pre-run refusal with rebuild guidance (the
    // `regex`/`gzip` shape), never a silent wrong answer. Parse/to_source
    // stay always-std (exercised by the parser round-trip tests).
    let g = rivus_parser::parse("W:\n watch \"in/*.csv\"\n read as csv\n take 1\n;")
        .expect("parse is always-std");
    let err = run(&g, RunOptions::default()).expect_err("must refuse pre-run");
    let msg = err.to_string();
    assert!(
        msg.contains("--features unbounded"),
        "refusal must guide the rebuild: {msg}"
    );
}

#[test]
fn watch_plan_is_not_touched_by_the_optimizer() {
    // §28.12 (ratified #149 ③): the boundedness-derived determinism tag — the
    // optimizer skips an unbounded plan entirely (skeleton posture) and says
    // so in the report (observable-first).
    let g = rivus_parser::parse("W:\n watch \"in/*.csv\"\n |? name == \"a.csv\"\n |> path\n;")
        .expect("parse");
    let before = g.to_source();
    let (opt, report) = rivus_optimizer::optimize(g);
    assert_eq!(opt.to_source(), before, "unbounded plan must be untouched");
    assert!(
        report.applied.iter().any(|l| l.contains("skipped")),
        "the skip must be reported, got {:?}",
        report.applied
    );
}

#[test]
fn bounded_take_never_early_stops_the_source() {
    // §28.12 pin (ratified #149 ④ review note): the bounded serial loop is
    // UNTOUCHED by the unbounded saturation plumbing — a bounded source with a
    // filled `take` downstream still drains to exhaustion, exactly as before.
    let text: String = std::iter::once("id\n".to_string())
        .chain((0..100).map(|i| format!("{i}\n")))
        .collect();
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_take_pin",
        text.as_bytes(),
    ));
    let out = std::env::temp_dir().join(format!("rivus_take_pin_{}.csv", std::process::id()));
    let _outg = TempCsv(out.clone());
    let g = rivus_parser::parse(&format!(
        "B:\n open {} (id:int)\n take 2\n save {}\n;",
        f.0.display(),
        out.display()
    ))
    .expect("parse");
    let res = run(
        &g,
        RunOptions {
            chunk_size: 10,
            memory: rivus_runtime::MemoryPref::Low,
            ..Default::default()
        },
    )
    .expect("run");
    assert_eq!(
        res.telemetry[0].rows_out, 100,
        "bounded source must drain fully (no early stop on saturation)"
    );
    assert_eq!(
        std::fs::read_to_string(&out).unwrap(),
        "id\n0\n1\n",
        "take semantics unchanged"
    );
}

#[cfg(feature = "unbounded")]
#[test]
fn watch_subscribes_streams_and_terminates_on_saturation() {
    // §28.12 e2e (feature `unbounded`): ⑥ capability reject → ②/④ subscribe,
    // stream OS change events through the bounded queue, ③ saturation (`take`)
    // stops the endless source. One test fn so the env-var section can never
    // race the streaming section.
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let base = std::env::temp_dir().join(format!("rivus_watch_{}", std::process::id()));
    let dir = base.join("in");
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::create_dir_all(base.join("allowzone_xyzzy")).unwrap();

    // -- ⑥ capability boundary: a root outside the granted prefix set is
    // rejected as an event (Recoverable, continue-first — the run completes,
    // nothing hangs), the event names only the target, never the allowlist.
    std::env::set_var(
        "RIVUS_CAP_WATCH_PATHS",
        base.join("allowzone_xyzzy").display().to_string(),
    );
    let flow = format!("W:\n watch \"{}/*.csv\"\n take 1\n;", dir.display());
    let g = rivus_parser::parse(&flow).expect("parse");
    let res = run(
        &g,
        RunOptions {
            chunk_size: 1,
            memory: rivus_runtime::MemoryPref::Low,
            ..Default::default()
        },
    )
    .expect("run completes");
    std::env::remove_var("RIVUS_CAP_WATCH_PATHS");
    assert_eq!(
        res.final_mode,
        rivus_core::Mode::Normal,
        "a capability reject is an event, not a fatal"
    );
    assert_eq!(res.total_rows_out(), 0);
    let reject = res
        .errors
        .iter()
        .find(|e| e.message.contains("watch capability"))
        .expect("reject event must surface");
    assert!(
        reject.message.contains(&dir.display().to_string()),
        "the event names the rejected target: {}",
        reject.message
    );
    assert!(
        !reject.message.contains("allowzone_xyzzy"),
        "the allowlist is a boundary, not echoed: {}",
        reject.message
    );

    // -- subscribe + stream + saturation-terminate. The writer keeps modifying
    // files (events may precede the subscription; later ones land) until the
    // run returns; `take 2` then saturates and the engine stops the source.
    let stop = Arc::new(AtomicBool::new(false));
    let wstop = stop.clone();
    let wdir = dir.clone();
    let writer = std::thread::spawn(move || {
        let mut i = 0u32;
        while !wstop.load(Ordering::Relaxed) {
            let _ = std::fs::write(wdir.join(format!("f{}.csv", i % 3)), format!("id\n{i}\n"));
            i += 1;
            std::thread::sleep(std::time::Duration::from_millis(40));
        }
    });
    let flow = format!(
        "W:\n watch \"{}/*.csv\"\n take 2\n read as csv\n |> id\n;",
        dir.display()
    );
    let g = rivus_parser::parse(&flow).expect("parse");
    let (txr, rxr) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let r = run(
            &g,
            RunOptions {
                chunk_size: 1,
                memory: rivus_runtime::MemoryPref::Low,
                ..Default::default()
            },
        );
        let _ = txr.send(r);
    });
    let res = rxr
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("watch flow must terminate once `take` saturates")
        .expect("run");
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    assert_eq!(res.final_mode, rivus_core::Mode::Normal);
    assert_eq!(
        res.total_rows_out(),
        2,
        "2 handles pass `take` → 2 single-row files read"
    );
    let _ = std::fs::remove_dir_all(&base);
}
