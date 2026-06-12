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

use rivus_core::{Chunk, Schema, Value};
use rivus_ir::Expr;
use rivus_ir::{parse_route_template, RouteSeg, SinkCodec};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};

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

/// Default open-file budget for the streaming [`RouteWriter`]. Bounded so a
/// high-cardinality partitioned save never exhausts file descriptors; the
/// least-recently-used writer is flushed and closed when the budget is hit, and
/// reopened (append) when its partition next receives a row. Comfortably under
/// a typical 1024 fd ulimit.
const ROUTE_FD_BUDGET: usize = 512;

/// Persistent per-partition state, kept for **every** path ever opened (small:
/// the path string plus two integers), so an evicted-then-reopened file appends
/// without re-emitting its header and a JSON array closes exactly once.
#[derive(Default)]
struct PartMeta {
    /// CSV header / JSON `[` already written (so a reopen appends).
    header_done: bool,
    /// Objects written so far (JSON array comma placement, across evictions).
    json_items: u64,
}

/// An open partition writer plus its recency stamp (for LRU eviction).
struct OpenFile {
    w: BufWriter<File>,
    last_used: u64,
}

/// **Streaming** partitioned writer (design §28.7 / #143 ③ engineering
/// follow-up): routes rows to per-partition files **as chunks arrive**, holding
/// at most [`ROUTE_FD_BUDGET`] files open at once (LRU eviction) instead of
/// buffering the whole stream. The bytes per file are identical to the buffered
/// [`write_routed`] — the same row formatters (`write_cell` / `json_object_row`)
/// and the same within-partition stream order — so byte-identity (serial ==
/// parallel == chunk-size) is preserved by construction; an eviction only
/// flushes and reopens (append), never re-headers or reorders.
pub struct RouteWriter {
    codec: SinkCodec,
    cap: usize,
    clock: u64,
    pool: HashMap<String, OpenFile>,
    meta: HashMap<String, PartMeta>,
    /// Per-partition write failures (continue-first: one bad path never stops
    /// the others), surfaced by the caller.
    failures: Vec<(String, std::io::Error)>,
}

impl RouteWriter {
    pub fn new(codec: SinkCodec) -> Self {
        // `RIVUS_ROUTE_FD_BUDGET` overrides the open-file budget (tests force a
        // tiny budget to exercise evict/reopen; ops can raise it under a higher
        // ulimit). Invalid / unset → the default.
        let cap = std::env::var("RIVUS_ROUTE_FD_BUDGET")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(ROUTE_FD_BUDGET);
        Self::with_cap(codec, cap)
    }

    /// Construct with an explicit open-file budget (≥ 1) — lets tests force
    /// eviction with a tiny budget to pin evict/reopen byte-identity.
    pub fn with_cap(codec: SinkCodec, cap: usize) -> Self {
        RouteWriter {
            codec,
            cap: cap.max(1),
            clock: 0,
            pool: HashMap::new(),
            meta: HashMap::new(),
            failures: Vec::new(),
        }
    }

    /// Route one input chunk's rows (already grouped by path) to disk. `groups`
    /// comes straight from [`group_by_path`] on a single chunk, so each entry's
    /// sub-chunks are exactly the rows destined for `path`.
    pub fn write_groups(&mut self, groups: Vec<(String, Vec<Chunk>)>) {
        for (path, subs) in groups {
            if let Err(e) = self.append(&path, &subs) {
                self.failures.push((path, e));
            }
        }
    }

    /// Append the rows of `subs` (all destined for `path`) to that partition's
    /// file, opening / re-opening it as needed.
    fn append(&mut self, path: &str, subs: &[Chunk]) -> std::io::Result<()> {
        let nrows: usize = subs.iter().map(|c| c.len).sum();
        if nrows == 0 {
            return Ok(());
        }
        // Format the fragment first (no `self.pool` borrow), tracking the
        // JSON-array item counter so commas stay correct across calls/evictions.
        let mut items = self.meta.get(path).map_or(0, |m| m.json_items);
        let mut frag = String::new();
        match self.codec {
            SinkCodec::Csv { delim } => {
                let sep = delim as char;
                for chunk in subs {
                    for row in 0..chunk.len {
                        for c in 0..chunk.columns.len() {
                            if c > 0 {
                                frag.push(sep);
                            }
                            crate::operators::write_cell(&mut frag, &chunk.columns[c], row, delim);
                        }
                        frag.push('\n');
                    }
                }
            }
            SinkCodec::Jsonl => {
                for chunk in subs {
                    let names = chunk.schema.field_names();
                    for row in 0..chunk.len {
                        crate::operators::json_object_row(&mut frag, chunk, &names, row);
                        frag.push('\n');
                    }
                }
            }
            SinkCodec::Json => {
                for chunk in subs {
                    let names = chunk.schema.field_names();
                    for row in 0..chunk.len {
                        if items > 0 {
                            frag.push(',');
                        }
                        crate::operators::json_object_row(&mut frag, chunk, &names, row);
                        items += 1;
                    }
                }
            }
        }
        let schema = subs[0].schema.clone();
        let w = self.ensure_open(path, &schema)?;
        w.write_all(frag.as_bytes())?;
        if matches!(self.codec, SinkCodec::Json) {
            self.meta.entry(path.to_string()).or_default().json_items = items;
        }
        Ok(())
    }

    /// Get (or open) the writer for `path`, writing the header / array-open on
    /// the first open and evicting the LRU file if the budget is exceeded.
    fn ensure_open(
        &mut self,
        path: &str,
        schema: &Schema,
    ) -> std::io::Result<&mut BufWriter<File>> {
        self.clock += 1;
        let now = self.clock;
        if let Some(of) = self.pool.get_mut(path) {
            of.last_used = now;
            return Ok(&mut self.pool.get_mut(path).unwrap().w);
        }
        let first_open = !self.meta.get(path).is_some_and(|m| m.header_done);
        if let Some(parent) = std::path::Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let file = if first_open {
            File::create(path)?
        } else {
            OpenOptions::new().append(true).open(path)?
        };
        let mut w = BufWriter::with_capacity(64 * 1024, file);
        if first_open {
            match self.codec {
                SinkCodec::Csv { delim } => {
                    let sep = (delim as char).to_string();
                    writeln!(w, "{}", schema.field_names().join(&sep))?;
                }
                SinkCodec::Jsonl => {}
                SinkCodec::Json => write!(w, "[")?,
            }
            self.meta.entry(path.to_string()).or_default().header_done = true;
        }
        // Evict the LRU open file (flush first) before inserting the new one.
        if self.pool.len() >= self.cap {
            if let Some(victim) = self
                .pool
                .iter()
                .min_by_key(|(_, of)| of.last_used)
                .map(|(p, _)| p.clone())
            {
                if let Some(mut of) = self.pool.remove(&victim) {
                    of.w.flush()?;
                }
            }
        }
        self.pool
            .insert(path.to_string(), OpenFile { w, last_used: now });
        Ok(&mut self.pool.get_mut(path).unwrap().w)
    }

    /// Close every partition: write the JSON array's `]` (each path opened, even
    /// if since evicted), flush all open writers, and return the accumulated
    /// per-partition failures. Paths are visited in sorted order for
    /// deterministic error reporting.
    pub fn finish(mut self) -> Vec<(String, std::io::Error)> {
        if matches!(self.codec, SinkCodec::Json) {
            let mut paths: Vec<String> = self.meta.keys().cloned().collect();
            paths.sort();
            for path in paths {
                // Reopen (append) and close the array; a never-opened path can't
                // occur (we only record meta on a real write).
                let r = self
                    .ensure_open(&path, &Schema::new(Vec::new()))
                    .and_then(|w| {
                        writeln!(w, "]")?;
                        w.flush()
                    });
                if let Err(e) = r {
                    self.failures.push((path, e));
                }
            }
        }
        // Flush whatever is still open (CSV/JSONL, or JSON just closed above).
        let mut open: Vec<(String, OpenFile)> = self.pool.drain().collect();
        open.sort_by(|a, b| a.0.cmp(&b.0));
        for (path, mut of) in open {
            if let Err(e) = of.w.flush() {
                self.failures.push((path, e));
            }
        }
        self.failures
    }
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
