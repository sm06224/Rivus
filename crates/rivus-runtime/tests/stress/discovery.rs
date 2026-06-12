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
