//! Partitioned / dynamic output routing (design §28.7, ratified #143):
//! `save "out/{country}.csv"` / `save "out/" by k [as flat]`.
//!
//! The file set and every path are a **pure, injective function** of the
//! partition-key values: null renders as the DuckDB-compatible
//! `__HIVE_DEFAULT_PARTITION__` sentinel, and key values are percent-escaped
//! (including `%` itself, so `a/b` can never collide with a literal `a%2Fb`).
//! Rows are written in stream order within each partition, so each file is
//! byte-identical across serial / parallel / chunk-size (the parallel path
//! collects and routes the merged stream through this same module).
//!
//! Per the #143 ruling there is **no preventive cardinality cap**: a
//! partitioned save is an explicit opt-in and is written out in full; only a
//! real resource failure surfaces (per partition, continue-first — never a
//! silent fallback to a different layout).

use rivus_core::{Chunk, Value};
use rivus_ir::Expr;
use rivus_ir::{parse_route_template, RouteSeg, SinkCodec};
use std::collections::HashMap;

/// The DuckDB/Hive-compatible partition directory name for a null key.
pub const NULL_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// Escape one key value for use as a path component. Deterministic and
/// injective: `%` itself, path separators, ASCII control bytes and the
/// Windows-unsafe set are `%XX`-escaped (uppercase hex); everything else —
/// including non-ASCII UTF-8 (Japanese keys, §27.6) — passes through.
pub fn escape_component(s: &str) -> String {
    // A component that is exactly `.` or `..` would walk the directory tree —
    // a data-driven escape from the declared output root (review #145). Fully
    // escape it (never-silent: the bytes still say what the key was).
    if s == "." || s == ".." {
        return s.chars().map(|_| "%2E").collect();
    }
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_ascii() {
            let b = ch as u8;
            let danger = matches!(
                b,
                b'%' | b'/' | b'\\' | b':' | b'*' | b'?' | b'"' | b'<' | b'>' | b'|'
            ) || b < 0x20
                || b == 0x7f;
            if danger {
                out.push_str(&format!("%{b:02X}"));
            } else {
                out.push(ch);
            }
        } else {
            out.push(ch);
        }
    }
    out
}

/// The part-file extension for a codec (`part.csv` / `part.tsv` / `part.jsonl`
/// / `part.json` — fixed, deterministic; #143 ④).
fn ext_for(codec: SinkCodec) -> &'static str {
    match codec {
        SinkCodec::Csv { delim: b'\t' } => "tsv",
        SinkCodec::Csv { .. } => "csv",
        SinkCodec::Jsonl => "jsonl",
        SinkCodec::Json => "json",
    }
}

/// One key cell rendered for path use: sentinel for null, escaped otherwise.
fn key_component(chunk: &Chunk, col: Option<usize>, row: usize) -> String {
    match col {
        Some(c) => {
            let column = &chunk.columns[c];
            if column.is_null(row) {
                NULL_PARTITION.to_string()
            } else {
                escape_component(&column.value_at(row).to_string())
            }
        }
        // A key column missing from the live schema folds to the null
        // partition (the serial operator surfaces it once; never a panic).
        None => NULL_PARTITION.to_string(),
    }
}

/// Group `chunks` by rendered output path, preserving stream order within each
/// partition and first-seen partition order. Pure function of the key values.
pub fn group_by_path(
    chunks: &[Chunk],
    template: &str,
    by: &[String],
    flat: bool,
    codec: SinkCodec,
    exprs: &[Expr],
    fails: &mut u64,
) -> Vec<(String, Vec<Chunk>)> {
    // Validated at declaration time; an un-parseable template here would be a
    // parser bug — fall back to a single literal segment (never a panic).
    let segs = parse_route_template(template)
        .unwrap_or_else(|_| vec![RouteSeg::Lit(template.to_string())]);
    let templated = segs
        .iter()
        .any(|s| matches!(s, RouteSeg::Key(_) | RouteSeg::Raw(_)));
    // Align each Raw seg with its parsed expression (template order).
    let mut next_expr = exprs.iter();
    let seg_exprs: Vec<Option<&Expr>> = segs
        .iter()
        .map(|g| match g {
            RouteSeg::Raw(_) => next_expr.next(),
            _ => None,
        })
        .collect();
    let ext = ext_for(codec);
    let base = template.trim_end_matches('/');

    let mut order: Vec<String> = Vec::new();
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut groups: Vec<Vec<Chunk>> = Vec::new();

    for chunk in chunks {
        // Resolve key columns once per chunk (schema is stable within one).
        let cols: Vec<(String, Option<usize>)> = by
            .iter()
            .map(|k| (k.clone(), chunk.schema.index_of(k)))
            .collect();
        let mut rows_for: HashMap<String, Vec<usize>> = HashMap::new();
        let mut path_order: Vec<String> = Vec::new();
        for row in 0..chunk.len {
            let path = if templated {
                let mut p = String::new();
                for (si, seg) in segs.iter().enumerate() {
                    match seg {
                        RouteSeg::Lit(l) => p.push_str(l),
                        RouteSeg::Key(k) => {
                            let col = chunk.schema.index_of(k);
                            p.push_str(&key_component(chunk, col, row));
                        }
                        // Computed key (s4c): evaluated per row; an eval
                        // failure → counted + the null partition (same
                        // continue-first shape as a cast that won't parse).
                        RouteSeg::Raw(_) => match seg_exprs[si] {
                            Some(e) => {
                                let v = crate::eval::eval_acc(e, chunk, row, fails);
                                if matches!(v, Value::Null) {
                                    p.push_str(NULL_PARTITION);
                                } else {
                                    p.push_str(&escape_component(&v.to_string()));
                                }
                            }
                            None => p.push_str(NULL_PARTITION),
                        },
                    }
                }
                p
            } else if flat {
                let vals: Vec<String> = cols
                    .iter()
                    .map(|(_, c)| key_component(chunk, *c, row))
                    .collect();
                format!("{base}/{}.{ext}", vals.join("_"))
            } else {
                // Hive layout (DuckDB-compatible `k=v/` directories).
                let dirs: Vec<String> = cols
                    .iter()
                    .map(|(k, c)| format!("{k}={}", key_component(chunk, *c, row)))
                    .collect();
                format!("{base}/{}/part.{ext}", dirs.join("/"))
            };
            rows_for.entry(path.clone()).or_insert_with(|| {
                path_order.push(path.clone());
                Vec::new()
            });
            rows_for.get_mut(&path).unwrap().push(row);
        }
        for path in path_order {
            let rows = &rows_for[&path];
            let sub = chunk.gather(rows);
            let gi = *index.entry(path.clone()).or_insert_with(|| {
                order.push(path.clone());
                groups.push(Vec::new());
                groups.len() - 1
            });
            groups[gi].push(sub);
        }
    }
    order.into_iter().zip(groups).collect()
}

/// Write every partition (creating parent directories), attempting **all** of
/// them even when one fails (continue-first; never a silent fallback). Returns
/// the per-partition failures for the caller to surface.
pub fn write_routed(
    template: &str,
    by: &[String],
    flat: bool,
    codec: SinkCodec,
    exprs: &[Expr],
    chunks: &[Chunk],
    fails: &mut u64,
) -> Vec<(String, std::io::Error)> {
    let mut failures = Vec::new();
    for (path, parts) in group_by_path(chunks, template, by, flat, codec, exprs, fails) {
        let res = std::path::Path::new(&path)
            .parent()
            .map_or(Ok(()), std::fs::create_dir_all)
            .and_then(|()| match codec {
                SinkCodec::Csv { delim } => crate::operators::write_csv_file(&path, &parts, delim),
                SinkCodec::Jsonl => crate::operators::write_jsonl_file(&path, &parts),
                SinkCodec::Json => crate::operators::write_json_file(&path, &parts),
            });
        if let Err(e) = res {
            failures.push((path, e));
        }
    }
    failures
}
