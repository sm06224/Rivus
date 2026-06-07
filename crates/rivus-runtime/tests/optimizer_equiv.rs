//! The optimizer must preserve semantics: for the same program, the optimized
//! graph must produce the same outputs as the unoptimized one. This is the
//! correctness gate that lets optimization PRs claim only speed, never behavior.

use rivus_runtime::gendata;
use rivus_runtime::{run, RunOptions, RunResult};
use std::collections::BTreeMap;

struct TempCsv(std::path::PathBuf);
impl Drop for TempCsv {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Canonicalize a run's outputs into `label -> sorted row strings`, independent
/// of node ids, chunk boundaries and branch ordering.
fn fingerprint(res: &RunResult) -> BTreeMap<String, Vec<String>> {
    let mut map = BTreeMap::new();
    for out in &res.outputs {
        let label = out
            .label
            .clone()
            .unwrap_or_else(|| format!("#{}", out.node_id));
        let mut rows: Vec<String> = Vec::new();
        for chunk in &out.chunks {
            for r in 0..chunk.len {
                let cells: Vec<String> = (0..chunk.columns.len())
                    .map(|c| chunk.value(r, c).to_string())
                    .collect();
                rows.push(cells.join("\u{1f}"));
            }
        }
        rows.sort();
        map.insert(label, rows);
    }
    map
}

fn run_both(src: &str) -> (RunResult, RunResult) {
    let g = rivus_parser::parse(src).expect("parse");
    let raw = run(
        &g,
        RunOptions {
            chunk_size: 4096,
            ..Default::default()
        },
    )
    .expect("raw run");
    let (opt_g, report) = rivus_optimizer::optimize(g);
    assert!(!report.is_empty(), "expected the optimizer to fire");
    let opt = run(
        &opt_g,
        RunOptions {
            chunk_size: 4096,
            ..Default::default()
        },
    )
    .expect("opt run");
    (raw, opt)
}

#[test]
fn dedup_sources_preserves_results() {
    let rows = 20_000;
    let data = gendata::clean(rows, 5);
    let f = TempCsv(gendata::write_temp("opt_equiv", &data));
    let p = f.0.display();

    let src = format!(
        "A:\n open {p}\n |? age >= 30\n |> name age\n;\n\
         B:\n open {p}\n |# country\n;\n\
         C:\n open {p}\n |? age < 30\n;"
    );

    let (raw, opt) = run_both(&src);
    assert_eq!(
        fingerprint(&raw),
        fingerprint(&opt),
        "optimized outputs must match unoptimized outputs exactly"
    );
}

#[test]
fn fusion_and_pushdown_preserve_results() {
    // Single scope filter->project triggers fusion + projection pushdown (the
    // source builds only {age, name}). Output must be identical to unoptimized.
    let rows = 20_000;
    let data = gendata::clean(rows, 11);
    let f = TempCsv(gendata::write_temp("opt_fp", &data));
    let p = f.0.display();

    let src = format!("F:\n open {p}\n |? age >= 40\n |? age < 80\n |> name age\n;");
    let (raw, opt) = run_both(&src);
    assert_eq!(fingerprint(&raw), fingerprint(&opt));
    // And the projected schema is exactly [name, age].
    let out = opt
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some("F"))
        .unwrap();
    let names = out.chunks[0].schema.field_names();
    assert_eq!(names, vec!["name", "age"]);
}

/// Run a program twice (optimized / unoptimized) and return both fingerprints,
/// *without* requiring the optimizer to fire (unlike `run_both`). Used to assert
/// equivalence for programs the optimizer may legitimately leave untouched.
fn fingerprints_both(src: &str) -> (BTreeMap<String, Vec<String>>, BTreeMap<String, Vec<String>>) {
    let g = rivus_parser::parse(src).expect("parse");
    let raw = run(
        &g,
        RunOptions {
            chunk_size: 4096,
            ..Default::default()
        },
    )
    .expect("raw run");
    let (opt_g, _report) = rivus_optimizer::optimize(g);
    let opt = run(
        &opt_g,
        RunOptions {
            chunk_size: 4096,
            ..Default::default()
        },
    )
    .expect("opt run");
    (fingerprint(&raw), fingerprint(&opt))
}

#[test]
fn optimizer_preserves_computed_columns() {
    // Whatever the optimizer does (or doesn't do) to a flow with computed
    // string-function columns, the result must be identical to unoptimized.
    // Guards the new replace/split_part/concat projections — and computed
    // columns generally — against any current or future rewrite.
    let data = "id,em,n\n1,abc,10\n2,xyz,20\n3,pq,30\n".as_bytes();
    let f = TempCsv(gendata::write_temp_bytes("opt_comp", data));
    let p = f.0.display();

    for src in [
        format!("F:\n open {p}\n |> (upper(em)) as u\n;"),
        format!("F:\n open {p}\n |? n >= 20\n |> (concat(em, \"!\")) as e id\n;"),
        format!("F:\n open {p}\n |> (replace(em, \"b\", \"-\")) as r\n;"),
        format!("F:\n open {p}\n |> (split_part(em, \"b\", 1)) as head\n;"),
    ] {
        let (raw, opt) = fingerprints_both(&src);
        assert_eq!(raw, opt, "optimized != unoptimized for: {src}");
    }
}

#[test]
fn string_prefilter_is_a_superset_and_preserves_results() {
    // The string prefilter is a raw-line substring pre-scan: a row whose
    // substring appears in the WRONG column must still be excluded by the
    // authoritative FilterProject. Optimized must equal unoptimized for every
    // string predicate shape that pushes a needle down.
    let data = concat!(
        "id,country,name\n",
        "1,US,JPman\n", // "JP" only in name → must NOT match contains(country,"JP")
        "2,JP,bob\n",   // genuine match
        "3,US,carol\n",
        "4,US,upJPper\n", // "JP" mid-name → must NOT match
        "5,JP,JPse\n",    // genuine match (and "JP" in name too)
    )
    .as_bytes();
    let f = TempCsv(gendata::write_temp_bytes("opt_strpf", data));
    let p = f.0.display();

    for src in [
        format!("F:\n open {p}\n |? contains(country, \"JP\")\n |> id country name\n;"),
        format!("F:\n open {p}\n |? country == \"JP\"\n |> id country\n;"),
        format!("F:\n open {p}\n |? starts_with(country, \"J\")\n |> id\n;"),
        format!("F:\n open {p}\n |? like(country, \"JP%\")\n |> id\n;"),
    ] {
        let (raw, opt) = fingerprints_both(&src);
        assert_eq!(raw, opt, "string prefilter changed results for: {src}");
    }
}

#[test]
fn string_prefilter_handles_quote_escaped_needles() {
    // Issue #37: the raw-line pre-scan sees quote-ESCAPED bytes (`"` → `""`),
    // but the logical field value is decoded. A needle containing `"` (or a
    // newline) must therefore NOT be pushed down, or a row whose decoded value
    // matches would be dropped at the reader — a false negative. The optimizer
    // now declines such needles; the result must stay byte-identical to the
    // unoptimized run (FilterProject is still authoritative).
    //
    // Row 1's `text` field is quoted with an escaped quote: raw bytes are
    // `a""b` while the decoded value is `a"b`, which matches `contains(text,
    // "a\"b")`. A naive raw-line `contains("a\"b")` would miss it.
    let data = concat!(
        "id,text\n",
        "1,\"x a\"\"b y\"\n", // decoded: x a"b y   → matches contains(text,"a\"b")
        "2,plain\n",          // no match
        "3,\"a\"\"b\"\n",     // decoded: a"b       → matches
    )
    .as_bytes();
    let f = TempCsv(gendata::write_temp_bytes("opt_strpf_quote", data));
    let p = f.0.display();

    // Needle `a"b` contains a quote → must not be pushed; result still correct.
    let src = format!("F:\n open {p}\n |? contains(text, \"a\\\"b\")\n |> id text\n;");
    let (raw, opt) = fingerprints_both(&src);
    assert_eq!(raw, opt, "quote-escaped needle changed results: {src}");
    // Sanity: the predicate really matches the two escaped rows (not zero/all).
    let matched = opt.get("F").map(|v| v.len()).unwrap_or(0);
    assert_eq!(matched, 2, "expected exactly rows 1 and 3 to match");
}

/// The decimal lane must survive optimization byte-identically. A filter +
/// projection on a `decimal(2)` column fires the optimizer (fusing the two into
/// the vectorized FilterProject kernel) while the raw graph evaluates them as
/// separate nodes — so this gates the kernel vs interpreter Dec paths agreeing,
/// and the column staying exact (text → i128, never via f64).
#[test]
fn decimal_column_optimizes_equivalently() {
    // Mixed scales in the text (1, 2 and 3 fractional digits) all read at
    // scale(2): 0.1→0.10, 12.345→12.34 (round half-even), 100→100.00.
    let mut text = String::from("id,amount\n");
    for i in 0..2_000u64 {
        // Deterministic decimals that straddle the >= 50.00 boundary.
        let cents = (i * 7) % 10_000; // 0.00 .. 99.99
        text.push_str(&format!("{i},{}.{:02}\n", cents / 100, cents % 100));
    }
    let f = TempCsv(gendata::write_temp_bytes("equiv_decimal", text.as_bytes()));
    let p = f.0.display();
    let src =
        format!("D:\n open {p} (id amount:decimal(2))\n |? amount >= 50.00\n |> id amount\n;");
    let (raw, opt) = run_both(&src);
    assert_eq!(
        fingerprint(&raw),
        fingerprint(&opt),
        "decimal filter+project diverged under optimization"
    );
    // The kept column must actually be the exact decimal lane at scale 2.
    let o = opt
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some("D"))
        .unwrap();
    let c = &o.chunks[0];
    let ai = c.schema.index_of("amount").unwrap();
    assert_eq!(
        c.schema.fields[ai].dtype,
        rivus_core::DataType::Decimal { scale: 2 }
    );
    // Every emitted value renders with exactly two fractional digits.
    for r in 0..c.len {
        let s = c.value(r, ai).to_string();
        assert_eq!(
            s.split('.').nth(1).map(|f| f.len()),
            Some(2),
            "scale lost: {s}"
        );
    }
}

/// #44: decimal filter comparisons are exact (i128), not via the lossy f64 view,
/// and the kernel and interpreter agree. Uses unscaled values straddling 2^53,
/// where `u as f64` collapses adjacent integers — the f64 path would mis-decide.
#[test]
fn decimal_filter_is_exact_i128() {
    // 2^53 = 9007199254740992; 9007199254740993 is NOT representable in f64.
    let text = "id,big\n\
        1,9007199254740992\n\
        2,9007199254740993\n\
        3,9007199254740994\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "equiv_decimal_exact",
        text.as_bytes(),
    ));
    let p = f.0.display();

    // Simple predicate → vectorized kernel; OR predicate → interpreter. Both must
    // keep exactly id 2 and 3 (big > 2^53), which the f64 view gets wrong.
    let kernel_src =
        format!("D:\n open {p} (id big:decimal(0))\n |? big > 9007199254740992\n |> id\n;");
    let interp_src = format!(
        "D:\n open {p} (id big:decimal(0))\n |? big > 9007199254740992 or id < 0\n |> id\n;"
    );
    let kernel = fingerprints_both(&kernel_src).1;
    let interp = fingerprints_both(&interp_src).1;
    assert_eq!(
        kernel, interp,
        "kernel and interpreter disagree on exact decimal"
    );
    let kept = kernel.get("D").cloned().unwrap_or_default();
    assert_eq!(
        kept,
        vec!["2".to_string(), "3".to_string()],
        "exact i128 compare wrong: {kept:?}"
    );
}

/// #44: scale-2 boundary equality/inequality compares exactly (e.g. `== 0.30`,
/// `> 19.99`), and stays byte-identical raw vs optimized.
#[test]
fn decimal_filter_boundaries_exact() {
    let text = "id,amount\n\
        1,0.30\n\
        2,0.29\n\
        3,19.99\n\
        4,20.00\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "equiv_decimal_bound",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let eq = fingerprints_both(&format!(
        "D:\n open {p} (id amount:decimal(2))\n |? amount == 0.30\n |> id\n;"
    ));
    assert_eq!(eq.0, eq.1);
    assert_eq!(
        eq.1.get("D").cloned().unwrap_or_default(),
        vec!["1".to_string()]
    );
    let gt = fingerprints_both(&format!(
        "D:\n open {p} (id amount:decimal(2))\n |? amount > 19.99\n |> id\n;"
    ));
    assert_eq!(gt.0, gt.1);
    assert_eq!(
        gt.1.get("D").cloned().unwrap_or_default(),
        vec!["4".to_string()]
    );
}

/// #44 (accounting contract): a decimal comparison must NEVER silently round the
/// literal. A sub-scale literal keeps full precision — `> 19.995` keeps 20.00
/// (it must not quantize 19.995 → 20.00 and drop the boundary), `== 0.305`
/// matches nothing at scale 2, `> 0.299` keeps 0.30. Verified on both the kernel
/// (simple predicate) and the interpreter (OR predicate) so they agree exactly.
#[test]
fn decimal_filter_no_silent_rounding() {
    let text = "id,amount\n1,0.29\n2,0.30\n3,19.99\n4,20.00\n5,0.31\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "equiv_decimal_noround",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let keep = |pred: &str| -> Vec<String> {
        // Kernel path (simple predicate) and interpreter path (OR with a
        // never-true disjunct) must return the identical surviving ids.
        let k = fingerprints_both(&format!(
            "D:\n open {p} (id amount:decimal(2))\n |? {pred}\n |> id\n;"
        ))
        .1;
        let i = fingerprints_both(&format!(
            "D:\n open {p} (id amount:decimal(2))\n |? {pred} or id < 0\n |> id\n;"
        ))
        .1;
        assert_eq!(k, i, "kernel vs interpreter disagree on `{pred}`");
        k.get("D").cloned().unwrap_or_default()
    };
    // 19.995 is NOT rounded to 20.00: 20.00 survives, 19.99 does not.
    assert_eq!(keep("amount > 19.995"), vec!["4".to_string()]);
    // 0.305 (scale 3) equals no scale-2 value — no silent rounding to 0.30/0.31.
    assert!(keep("amount == 0.305").is_empty());
    // 0.299 keeps 0.30 (and above), drops 0.29.
    assert_eq!(
        keep("amount > 0.299"),
        vec![
            "2".to_string(),
            "3".to_string(),
            "4".to_string(),
            "5".to_string()
        ]
    );
}

#[test]
fn discovery_prefilter_preserves_results() {
    // Slice 3b: the `ls` name-prefilter pushdown is a superset prune with the
    // downstream filter authoritative, so optimized output == unoptimized.
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    struct Dir(std::path::PathBuf);
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir().join(format!("opt_ls_{}_{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    for rel in ["2026/app.csv", "2026/other.csv", "2025/app.csv"] {
        let p = base.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, "x\n").unwrap();
    }
    let _d = Dir(base.clone());
    let b = base.display();
    // Each name predicate must be result-invariant under the pushdown.
    for pred in [
        "contains(name, \"app\")",
        "name == \"app.csv\"",
        "starts_with(name, \"app\")",
    ] {
        let (raw, opt) = fingerprints_both(&format!(
            "L:\n ls \"{b}/**/*.csv\"\n |? {pred}\n |> path\n;"
        ));
        assert_eq!(raw, opt, "discovery pushdown changed results for `{pred}`");
        assert_eq!(
            opt.get("L").map(|v| v.len()).unwrap_or(0),
            2,
            "expected the two app.csv files for `{pred}`"
        );
    }
    // The pushdown actually fires (report records it) for a name predicate.
    let g = rivus_parser::parse(&format!(
        "L:\n ls \"{b}/**/*.csv\"\n |? contains(name, \"app\")\n |> path\n;"
    ))
    .unwrap();
    let (_g, report) = rivus_optimizer::optimize(g);
    assert!(
        report
            .applied
            .iter()
            .any(|l| l.contains("discovery_prefilter")),
        "name-predicate pushdown should fire, got {:?}",
        report.applied
    );
}
