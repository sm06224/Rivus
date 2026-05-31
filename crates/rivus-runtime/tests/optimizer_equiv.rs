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
