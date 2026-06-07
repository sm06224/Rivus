//! Discovery-as-flow — `ls "glob"` → `Stream<Resource>` (§28.3, slice 3a).
//!
//! Pins: recursive `**` glob matching, deterministic uri-ascending order,
//! chunk-size independence, the `path.uri` / `.name` / `.scheme` accessor over
//! the Resource column, predicate filtering on it, and continue-first 0-match.

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
fn ls_recursive_glob_is_deterministic_and_accessor_works() {
    let (_t, base) = mk_tree();
    let res = run_src(
        &format!(
            "L:\n ls \"{base}/**/*.csv\"\n |> (path.uri) as uri (path.name) as name (path.scheme) as scheme\n;"
        ),
        4,
    );
    let uris = collect_strings(&res, "L", "uri");
    let names = collect_strings(&res, "L", "name");
    let schemes = collect_strings(&res, "L", "scheme");
    // Only the three .csv files, in uri-ascending order (2025 before 2026); the
    // .txt is excluded by the `*.csv` segment.
    assert_eq!(
        uris,
        vec![
            format!("{base}/logs/2025/c.csv"),
            format!("{base}/logs/2026/a.csv"),
            format!("{base}/logs/2026/b.csv"),
        ],
        "ls must match *.csv recursively, uri-ascending"
    );
    assert_eq!(
        names,
        vec!["c.csv", "a.csv", "b.csv"],
        "path.name = basename"
    );
    assert!(
        schemes.iter().all(|s| s == "file"),
        "local paths → file scheme"
    );
}

#[test]
fn ls_is_chunk_size_independent() {
    let (_t, base) = mk_tree();
    let order = |cs: usize| {
        let res = run_src(
            &format!("L:\n ls \"{base}/**/*.csv\"\n |> (path.uri) as uri\n;"),
            cs,
        );
        collect_strings(&res, "L", "uri")
    };
    let one = order(1);
    assert_eq!(one.len(), 3);
    assert_eq!(one, order(2), "ls order must not depend on chunk size");
    assert_eq!(one, order(1000), "ls order must not depend on chunk size");
}

#[test]
fn ls_predicate_filters_on_resource_field() {
    let (_t, base) = mk_tree();
    // Filter on the Resource column's `name` field (parenthesized so `path.name`
    // lexes as the field accessor, not a single flow-mode identifier), then
    // materialize the uri.
    let res = run_src(
        &format!(
            "L:\n ls \"{base}/**/*.csv\"\n |? (path.name == \"a.csv\")\n |> (path.uri) as uri\n;"
        ),
        4,
    );
    let uris = collect_strings(&res, "L", "uri");
    assert_eq!(uris, vec![format!("{base}/logs/2026/a.csv")]);
}

#[test]
fn ls_zero_matches_warns_and_is_empty() {
    let (_t, base) = mk_tree();
    let res = run_src(
        &format!("L:\n ls \"{base}/**/*.parquet\"\n |> (path.uri) as uri\n;"),
        4,
    );
    // An empty stream produces no rows (the labeled output may be absent or have
    // only empty chunks) — either way, no uri is emitted.
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
