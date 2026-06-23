//! `read` — multi-file union-by-name (§28.3, slice 3c).
//!
//! Pins: union-by-name (ordered first-seen columns, missing → null), numeric
//! widening (int+float → no truncation), per-row provenance, deterministic
//! uri-ascending order, chunk-size independence, and never-silent quarantine of
//! an unopenable file.

use super::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

struct TempTree(PathBuf);
impl Drop for TempTree {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A temp dir with the given `(relative_path, body)` files; returns it + base.
fn mk(files: &[(&str, &str)]) -> (TempTree, String) {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir().join(format!("rivus_read_{}_{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    for (rel, body) in files {
        let p = base.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, body).unwrap();
    }
    (TempTree(base.clone()), base.display().to_string())
}

#[test]
fn sort_and_distinct_by_nested_key_resolve_the_path() {
    // §32 s4b: `sort` / `distinct` keys resolve a nested path to the underlying
    // value (numeric `user.age`), not the bare struct's text form. Sort orders
    // numerically; distinct folds equal nested-key values.
    let (_t, base) = mk(&[(
        "d.jsonl",
        "{\"user\":{\"age\":30},\"id\":1}\n\
         {\"user\":{\"age\":15},\"id\":2}\n\
         {\"user\":{\"age\":40},\"id\":3}\n\
         {\"user\":{\"age\":15},\"id\":4}\n",
    )]);
    // Sort by the nested key ascending: ages 15,15,30,40 → ids 2,4,1,3 (ties keep
    // source order).
    let sorted = run_src(
        &format!("S:\n open {base}/d.jsonl\n sort user.age\n |> id\n;"),
        2,
    );
    assert_eq!(
        collect_strings(&sorted, "S", "id"),
        vec!["2", "4", "1", "3"],
        "sort must resolve the nested key numerically"
    );
    // Distinct by the nested key: keep the first row per distinct age (30,15,40).
    let distinct = run_src(
        &format!("D:\n open {base}/d.jsonl\n distinct user.age\n |> id\n;"),
        2,
    );
    assert_eq!(
        collect_strings(&distinct, "D", "id"),
        vec!["1", "2", "3"],
        "distinct must fold equal nested-key values"
    );
}

#[test]
fn nested_path_resolves_with_null_propagation_and_counted_misses() {
    // §32 s4 invalid-path policy (§32.8③): a nested path resolves struct fields
    // (`user.age`) and list indices (`tags[0]`) against the typed nested lanes,
    // and distinguishes two null outcomes:
    //   * **Null propagation (§26, NOT counted):** a null base (`user` absent on
    //     a row) makes `user.age` null silently — like SQL `NULL.field = NULL`.
    //   * **Structural miss (counted, never-silent):** a field absent from the
    //     struct's schema (`user.weight`), an out-of-range list index
    //     (`tags[0]` on `[]`), or a type mismatch is a typed null AND a counted
    //     failure on the error stream.
    let (_t, base) = mk(&[(
        "d.jsonl",
        "{\"id\":1,\"user\":{\"age\":30},\"tags\":[10,20]}\n\
         {\"id\":2,\"user\":{\"age\":15},\"tags\":[]}\n\
         {\"id\":3,\"tags\":[99]}\n",
    )]);
    let res = run_src(
        &format!(
            "P:\n open {base}/d.jsonl\n |> id (user.age) as age (user.weight) as w (tags[0]) as first\n;"
        ),
        4,
    );
    let id = collect_strings(&res, "P", "id");
    let age = collect_strings(&res, "P", "age");
    let w = collect_strings(&res, "P", "w");
    let first = collect_strings(&res, "P", "first");
    assert_eq!(id, vec!["1", "2", "3"]);
    // `user.age`: rows 1-2 resolve; row 3 has no `user` → null *propagation*.
    assert_eq!(age, vec!["30", "15", ""], "struct field + null propagation");
    // `user.weight`: `weight` is absent from the `user` schema → structural miss
    // on rows 1-2 (counted); row 3's null base propagates null (not counted).
    assert_eq!(w, vec!["", "", ""], "missing struct field → typed null");
    // `tags[0]`: row 2's `tags` is `[]` → out-of-range structural miss (counted).
    assert_eq!(
        first,
        vec!["10", "", "99"],
        "list index + out-of-range null"
    );

    // Structural misses are surfaced per column; pure null propagation is not.
    let surfaced: String = res.errors.iter().map(|e| e.message.clone()).collect();
    assert!(
        surfaced.contains("'w'") && surfaced.contains("'first'"),
        "structural misses must be surfaced per column, got: {surfaced}"
    );
    assert!(
        !surfaced.contains("'age'"),
        "null propagation must NOT be counted as a failure, got: {surfaced}"
    );
}

#[test]
fn read_union_by_name_widening_and_provenance() {
    // a/b share `amount` (int then float → widen to float, no truncation); c has
    // a different column `region` → union pads with null. Provenance is per row.
    let (_t, base) = mk(&[
        ("a.csv", "id,amount\n1,10\n2,20\n"),
        ("b.csv", "id,amount\n3,1.5\n"),
        ("c.csv", "id,region\n9,jp\n"),
    ]);
    let res = run_src(
        &format!(
            "R:\n ls \"{base}/*.csv\"\n read as csv with source\n |> id amount region (source.uri) as src\n;"
        ),
        4,
    );
    let id = collect_strings(&res, "R", "id");
    let amount = collect_strings(&res, "R", "amount");
    let region = collect_strings(&res, "R", "region");
    let src = collect_strings(&res, "R", "src");
    // Deterministic uri order: a (1,2), then b (3), then c (9).
    assert_eq!(id, vec!["1", "2", "3", "9"], "uri-ascending file order");
    // `amount` widened to float — 1.5 kept exactly, ints not truncated; c → null.
    assert_eq!(
        amount,
        vec!["10", "20", "1.5", ""],
        "int+float widen, no trunc"
    );
    // union-by-name: `region` only on c; a/b → null.
    assert_eq!(region, vec!["", "", "", "jp"], "missing column → null");
    // Per-row provenance: each row carries its source file.
    assert_eq!(
        src,
        vec![
            format!("{base}/a.csv"),
            format!("{base}/a.csv"),
            format!("{base}/b.csv"),
            format!("{base}/c.csv"),
        ],
        "source.uri is per-file"
    );
}

#[test]
fn read_is_chunk_size_independent() {
    let (_t, base) = mk(&[
        ("a.csv", "id,v\n1,10\n2,20\n3,30\n"),
        ("b.csv", "id,v\n4,40\n5,50\n"),
    ]);
    let rows = |cs: usize| {
        let res = run_src(
            &format!("R:\n ls \"{base}/*.csv\"\n read as csv\n |> id v\n;"),
            cs,
        );
        let id = collect_strings(&res, "R", "id");
        let v = collect_strings(&res, "R", "v");
        id.into_iter().zip(v).collect::<Vec<_>>()
    };
    let one = rows(1);
    assert_eq!(one.len(), 5);
    assert_eq!(
        one,
        rows(2),
        "read order/content must not depend on chunk size"
    );
    assert_eq!(
        one,
        rows(1000),
        "read order/content must not depend on chunk size"
    );
}

#[test]
fn read_quarantines_unopenable_file_never_silent() {
    // A manifest listing one real file and one missing file: the missing one is
    // surfaced on the error stream (recoverable) and skipped; the real one reads.
    let (_t, base) = mk(&[("ok.csv", "id\n1\n2\n")]);
    let manifest = format!("filepath\n{base}/ok.csv\n{base}/missing.csv\n");
    let (_m, mpath) = mk(&[("manifest.csv", &manifest)]);
    let res = run_src(
        &format!(
            "R:\n open {mpath}/manifest.csv\n |> (resource(filepath)) as path\n read as csv\n |> id\n;"
        ),
        4,
    );
    let id = collect_strings(&res, "R", "id");
    assert_eq!(id, vec!["1", "2"], "the openable file still reads");
    assert!(
        res.errors
            .iter()
            .any(|e| e.message.contains("skipped") && e.message.contains("missing.csv")),
        "the unopenable file must be surfaced (never-silent), got {:?}",
        res.errors
    );
    // Quarantine is recoverable, not fatal — the run completes Normal.
    assert!(
        !res.errors.iter().any(rivus_core::ErrorEvent::is_fatal),
        "quarantine must not be fatal"
    );
}

#[test]
fn read_with_filename_materializes_per_file_column() {
    let (_t, base) = mk(&[("a.csv", "id\n1\n"), ("b.csv", "id\n2\n")]);
    let res = run_src(
        &format!("R:\n ls \"{base}/*.csv\"\n read as csv with filename\n |> id filename\n;"),
        4,
    );
    let id = collect_strings(&res, "R", "id");
    let filename = collect_strings(&res, "R", "filename");
    assert_eq!(id, vec!["1", "2"]);
    assert_eq!(
        filename,
        vec![format!("{base}/a.csv"), format!("{base}/b.csv")],
        "with filename materializes the per-file path"
    );
}
