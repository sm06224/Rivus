//! Operator implementations.
//!
//! Every flow node compiles to one boxed [`Operator`]. The engine drives them
//! with a chunk-granular, single-threaded push schedule (see `engine.rs`).
//! Fan-out (`->` branch) is handled by the engine via multiple outgoing edges,
//! so there is no dedicated branch operator.

use crate::csv;
use crate::eval;
use crate::jsonl;
use crate::kernel;
use rivus_core::{
    Chunk, Column, ColumnData, DataType, DateTime, DtColumn, ErrorEvent, ErrorScope, Field, Schema,
    Severity, StrColumn, TimeUnit, Validity, Value,
};
use rivus_ir::{
    AggFunc, BinType, CmpOp, Codec, Disposition, Endian, Expr, FillMethod, JoinKind, NodeId, Op,
    SinkCodec,
};
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;

/// An incremental sink writer: opens the file (or stdout for `-`) on the first
/// chunk and appends as chunks arrive, so a sink never buffers the whole output
/// in memory. Shared by the streaming CSV and JSONL sinks.
struct StreamWriter {
    path: String,
    inner: Option<BufWriter<Box<dyn Write>>>,
    wrote_header: bool,
    failed: bool,
}

impl StreamWriter {
    fn new(path: String) -> Self {
        StreamWriter {
            path,
            inner: None,
            wrote_header: false,
            failed: false,
        }
    }

    fn writer(&mut self) -> std::io::Result<&mut BufWriter<Box<dyn Write>>> {
        if self.inner.is_none() {
            let w: Box<dyn Write> = if self.path == "-" {
                Box::new(std::io::stdout())
            } else {
                Box::new(File::create(&self.path)?)
            };
            self.inner = Some(BufWriter::with_capacity(256 * 1024, w));
        }
        Ok(self.inner.as_mut().unwrap())
    }

    /// Flush on completion; if no chunk ever arrived, still create an empty file
    /// (matching the old whole-buffer sinks) — but never touch stdout.
    fn finish(&mut self) -> std::io::Result<()> {
        if let Some(w) = self.inner.as_mut() {
            w.flush()?;
        } else if self.path != "-" {
            File::create(&self.path)?;
        }
        Ok(())
    }
}

/// Per-call execution context handed to operators.
pub struct OpCtx<'a> {
    pub label: String,
    pub errors: &'a mut Vec<ErrorEvent>,
    pub next_chunk_id: &'a mut u64,
}

impl OpCtx<'_> {
    pub fn fresh_id(&mut self) -> u64 {
        let id = *self.next_chunk_id;
        *self.next_chunk_id += 1;
        id
    }

    pub fn raise(&mut self, ev: ErrorEvent) {
        self.errors.push(ev);
    }
}

pub trait Operator {
    fn is_source(&self) -> bool {
        false
    }
    /// Sources produce the next chunk, or `None` when exhausted.
    fn pull(&mut self, _ctx: &mut OpCtx) -> Option<Chunk> {
        None
    }
    /// Transform one input chunk arriving from upstream node `from`.
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk>;
    /// Flush buffered state once all inputs are exhausted.
    fn finish(&mut self, _ctx: &mut OpCtx) -> Vec<Chunk> {
        Vec::new()
    }
    /// Per-column type-inference outcome `(name, type, widened)` for a source
    /// that inferred its schema, surfaced as telemetry (A4). Empty for non-source
    /// operators and for declared/sample-inferred schemas. Read after the run.
    fn inference(&self) -> Vec<(String, DataType, bool)> {
        Vec::new()
    }
}

/// Write a text sink: the `-` sentinel writes stdout, otherwise a file.
fn write_output(path: &str, data: &str) -> std::io::Result<()> {
    if path == "-" {
        use std::io::Write;
        std::io::stdout().write_all(data.as_bytes())
    } else {
        std::fs::write(path, data)
    }
}

/// A source that yields pre-parsed chunks (used by the parallel executor: the
/// file is parsed once, then partitions are fed to per-worker sub-DAGs).
pub fn mem_source(chunks: Vec<Chunk>) -> Box<dyn Operator> {
    Box::new(MemSource {
        chunks: chunks.into(),
    })
}

/// An identity operator that forwards its input, so the engine captures it as a
/// leaf output (used to collect a file sink's rows for a single post-merge write
/// during parallel execution).
pub fn collector() -> Box<dyn Operator> {
    Box::new(Merge)
}

/// A streaming JSONL source over one newline-aligned byte range `[start, end)`,
/// used by the parallel executor (#49). The global schema/types are pre-inferred
/// (see [`jsonl::plan_parallel`]); on open error it yields nothing (continue-first).
#[allow(clippy::too_many_arguments)]
pub fn jsonl_range_source(
    path: &str,
    names: Vec<String>,
    dtypes: Vec<rivus_core::DataType>,
    schema: Arc<Schema>,
    start: u64,
    end: u64,
    chunk_size: usize,
    provenance: rivus_ir::Provenance,
) -> Box<dyn Operator> {
    match jsonl::JsonlChunker::for_range(path, names, dtypes, start, end, chunk_size) {
        Ok(ch) => Box::new(SourceJsonl::from_chunker(schema, ch).with_provenance(provenance, path)),
        Err(_) => Box::new(MemSource {
            chunks: std::collections::VecDeque::new(),
        }),
    }
}

/// A streaming CSV source over one byte range `[start, end)` of a file, used by
/// the parallel streaming executor. The global schema/types are pre-inferred
/// (see [`csv::plan_parallel`]); on open error it yields nothing (continue-first
/// — the worker simply contributes no rows).
#[allow(clippy::too_many_arguments)]
pub fn csv_range_source(
    path: &str,
    dtypes: Vec<rivus_core::DataType>,
    dt_specs: Vec<Option<Arc<csv::DtSpec>>>,
    keep: Vec<usize>,
    ncols: usize,
    schema: Arc<Schema>,
    start: u64,
    end: u64,
    chunk_size: usize,
    prefilter: Vec<(usize, CmpOp, f64)>,
    str_prefilter: Vec<String>,
    delim: u8,
    provenance: rivus_ir::Provenance,
) -> Box<dyn Operator> {
    match csv::CsvChunker::for_range(
        path,
        dtypes,
        dt_specs,
        keep,
        ncols,
        start,
        end,
        chunk_size,
        prefilter,
        str_prefilter,
        delim,
    ) {
        Ok(ch) => Box::new(SourceCsv::from_stream(schema, ch).with_provenance(provenance, path)),
        Err(_) => Box::new(MemSource {
            chunks: std::collections::VecDeque::new(),
        }),
    }
}

struct MemSource {
    chunks: std::collections::VecDeque<Chunk>,
}

impl Operator for MemSource {
    fn is_source(&self) -> bool {
        true
    }
    fn pull(&mut self, _ctx: &mut OpCtx) -> Option<Chunk> {
        self.chunks.pop_front()
    }
    fn process(&mut self, _from: NodeId, _chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        Vec::new()
    }
}

/// Build the operator for a node from its IR op. `preview` lets a CSV source
/// sample-infer its schema (instant start) for sink-less preview runs.
pub fn build(op: &Op, inputs: &[NodeId], chunk_size: usize, preview: bool) -> Box<dyn Operator> {
    match op {
        // One source node; the codec picks which reader to build. The path comes
        // from discovery (v1 single fixed resource); transport is path-derived.
        Op::Source {
            discovery,
            codec,
            provenance,
            ..
        } => {
            let path = discovery.path();
            match codec {
                Codec::Csv {
                    header,
                    declared,
                    dt_formats,
                    delim,
                    projection,
                    prefilter,
                    str_prefilter,
                } => Box::new(
                    SourceCsv::new(
                        path.to_string(),
                        projection.clone(),
                        chunk_size,
                        preview,
                        prefilter.clone(),
                        str_prefilter.clone(),
                        *header,
                        declared.clone(),
                        dt_formats.clone(),
                        *delim,
                    )
                    .with_provenance(*provenance, path),
                ),
                Codec::Binary {
                    fields,
                    endian,
                    c_align,
                } => Box::new(
                    SourceBinary::new(
                        path.to_string(),
                        fields.clone(),
                        *endian,
                        *c_align,
                        chunk_size,
                    )
                    .with_provenance(*provenance, path),
                ),
                Codec::Jsonl => Box::new(
                    SourceJsonl::new(path.to_string(), chunk_size)
                        .with_provenance(*provenance, path),
                ),
                // `ls` discovery: enumerate the glob (`path` is the pattern) into a
                // Resource stream; no codec decode, no provenance. The unbounded
                // `watch` discovery (§28.12) is dispatched apart: its evaluator is
                // feature-gated and `run_with_progress` refuses a feature-less
                // plan pre-run, so the stub here is defense-in-depth (a caller
                // that skips the engine still gets a loud Fatal, never a silent
                // `ls`-like one-shot scan).
                Codec::Discover { name_prefilter } => {
                    if discovery.is_unbounded() {
                        Box::new(SourceUnboundedStub)
                    } else {
                        Box::new(SourceDiscover::new(
                            path.to_string(),
                            chunk_size,
                            name_prefilter.clone(),
                        ))
                    }
                }
            }
        }
        Op::Read { fmt, provenance } => Box::new(Read::new(*fmt, *provenance, chunk_size)),
        Op::StreamRef { name } => Box::new(StreamRef { name: name.clone() }),
        Op::Filter { pred } => Box::new(Filter {
            pred: pred.clone(),
            cast_fails: 0,
        }),
        Op::Validate { pred, disposition } => Box::new(Validate {
            pred: pred.clone(),
            disposition: *disposition,
            fails: 0,
            sample: None,
            cast_fails: 0,
        }),
        Op::Take { n } => Box::new(Take { remaining: *n }),
        Op::Sort { keys } => Box::new(Sort::new(keys.clone())),
        Op::Distinct { keys } => Box::new(Distinct::new(keys.clone())),
        Op::Describe => Box::new(Describe::default()),
        Op::DropNa { cols } => Box::new(DropNa { cols: cols.clone() }),
        Op::Fill { col, method } => match method {
            FillMethod::Value(value) => Box::new(Fill {
                col: col.clone(),
                value: value.clone(),
            }),
            FillMethod::Ffill => Box::new(FillDirectional::ffill(col.clone())),
            FillMethod::Bfill => Box::new(FillDirectional::bfill(col.clone())),
            FillMethod::Mean => Box::new(FillStat::new(col.clone(), false)),
            FillMethod::Median => Box::new(FillStat::new(col.clone(), true)),
        },
        Op::Rename { pairs } => Box::new(Rename {
            pairs: pairs.clone(),
        }),
        Op::Drop { cols } => Box::new(Drop { cols: cols.clone() }),
        Op::Cast { casts } => Box::new(Cast {
            casts: casts.clone(),
            fails: Default::default(),
        }),
        Op::Reorder { cols } => Box::new(Reorder { cols: cols.clone() }),
        // `views` are metadata only (§29.3, s2): sub-view references are lowered
        // to `Expr::SubView` at parse time, so the operator needs just `items`.
        Op::ProjectExpr { items, .. } => Box::new(ProjectExpr {
            items: items.clone(),
            fails: Default::default(),
        }),
        Op::Project { fields } => Box::new(Project {
            fields: fields.clone(),
        }),
        Op::FilterProject { preds, fields } => Box::new(FilterProject {
            preds: preds.clone(),
            fields: fields.clone(),
            cast_fails: 0,
        }),
        Op::GroupBy { keys, aggs } => Box::new(GroupBy::new(keys.clone(), aggs.clone())),
        Op::Merge => Box::new(Merge),
        Op::Branch => Box::new(Merge), // identity forwarder; fan-out is structural
        Op::Join {
            left_keys,
            right_keys,
            kind,
        } => Box::new(Join::new(
            left_keys.clone(),
            right_keys.clone(),
            *kind,
            inputs.first().copied().unwrap_or(usize::MAX),
        )),
        Op::SinkPrint => Box::new(SinkPrint),
        // The unified sink (§28.7): Route::Fixed + Transport::Local today, so
        // the codec alone picks the writer (same operators as before the
        // unification — behaviour-identical).
        Op::Sink { route, codec, .. } => match route {
            rivus_ir::Route::Fixed(path) => {
                let path = path.clone();
                match codec {
                    SinkCodec::Csv { delim } => Box::new(SinkCsv::new(path, *delim)),
                    SinkCodec::Jsonl => Box::new(SinkJsonl::new(path)),
                    SinkCodec::Json => Box::new(SinkJson::new(path)),
                }
            }
            // Partitioned route (§28.7 / #143): collect, then write every
            // partition on finish through `crate::route` (same bytes as the
            // parallel single-write merge).
            rivus_ir::Route::Template {
                template,
                by,
                flat,
                exprs,
            } => Box::new(SinkRoute {
                template: template.clone(),
                by: by.clone(),
                flat: *flat,
                exprs: exprs.clone(),
                codec: *codec,
                writer: None,
                eval_fails: 0,
                warned_missing: false,
            }),
        },
    }
}

/// A streaming CSV sink to `path` (used by the parallel executor to write a
/// worker's byte-range partition to a part file).
pub fn csv_sink(path: String, delim: u8) -> Box<dyn Operator> {
    Box::new(SinkCsv::new(path, delim))
}

/// A streaming JSONL sink to `path` (parallel worker part file).
pub fn jsonl_sink(path: String) -> Box<dyn Operator> {
    Box::new(SinkJsonl::new(path))
}

// --- Split operator modules (design 26 §26.8.1; move-only). ---
mod aggregate;
mod join;
mod read;
mod sink;
mod source;
mod transform;

// Flat shared namespace: each submodule pulls these via `use super::*`, and the
// dispatch/tests below see the submodules' items via these globs.
use aggregate::*;
use join::*;
use read::*;
use sink::*;
use source::*;
use transform::*;

// Re-exports for `engine.rs` (which refers to these as `operators::X`).
pub(crate) use aggregate::{group_parallel_safe, new_group, GroupBy};
pub(crate) use sink::{
    json_object_row, write_cell, write_csv_file, write_json_file, write_jsonl_file,
};
pub(crate) use source::{bin_layout, bin_range_source, bin_schema};

#[cfg(test)]
mod agg_merge_tests {
    use super::*;
    use rivus_core::Decimal;

    // Accumulate `vals` into one AggAcc (the serial single-pass reference).
    fn single(func: AggFunc, vals: &[Value]) -> AggAcc {
        let mut a = AggAcc::new(func);
        for v in vals {
            a.observe(v);
        }
        a
    }

    // Accumulate `vals` split into `parts` partitions, each into its own AggAcc,
    // then merge them in source order (mirrors per-worker partials → merge).
    fn partitioned(func: AggFunc, vals: &[Value], parts: usize) -> AggAcc {
        let chunks: Vec<&[Value]> = vals.chunks(vals.len().div_ceil(parts.max(1))).collect();
        let mut accs: Vec<AggAcc> = chunks
            .iter()
            .map(|c| {
                let mut a = AggAcc::new(func);
                for v in *c {
                    a.observe(v);
                }
                a
            })
            .collect();
        let mut merged = accs.remove(0);
        for a in &accs {
            merged.merge(a);
        }
        merged
    }

    #[test]
    fn decimal_sum_merge_equals_single_pass() {
        // Decimals whose f64 sum would drift; merged i128 sum must be byte-exact.
        let vals: Vec<Value> = (0..1000)
            .map(|i| Value::Dec(Decimal::new((i % 97) + 1, 2)))
            .collect();
        for parts in [1, 2, 3, 7, 16] {
            let s = single(AggFunc::Sum, &vals);
            let m = partitioned(AggFunc::Sum, &vals, parts);
            assert_eq!(
                m.dec_value().unwrap().to_string(),
                s.dec_value().unwrap().to_string(),
                "decimal sum merge != single-pass @parts={parts}"
            );
            // And exact vs an independent i128 oracle.
            let oracle: i128 = (0..1000).map(|i| (i % 97) + 1).sum();
            assert_eq!(m.dec_value().unwrap(), Decimal::new(oracle, 2));
        }
    }

    #[test]
    fn datetime_minmax_is_exact_i64_and_type_preserving() {
        // Nanosecond ticks past 2^53, adjacent (1 ns apart): `tick as f64` would
        // collapse them, so an f64 min/max would be wrong and would drop the
        // DateTime type. The i64 lane must be exact and keep `DateTime`. #53.
        let base = 1_700_000_000_000_000_000_i64; // ≈ 2023 in ns, ≫ 2^53
        assert!(
            base as f64 == (base + 1) as f64,
            "precondition: f64 loses 1ns"
        );
        let vals: Vec<Value> = [base + 2, base + 9, base, base + 5, base + 1]
            .into_iter()
            .map(|t| Value::DateTime(DateTime::new(t, TimeUnit::Nano)))
            .collect();

        for parts in [1usize, 2, 3, 5] {
            let mn = partitioned(AggFunc::Min, &vals, parts);
            let mx = partitioned(AggFunc::Max, &vals, parts);
            // Exact i64 extremes, type preserved (DateTime, Nano), parallel-safe.
            assert_eq!(mn.dt_value(), Some(DateTime::new(base, TimeUnit::Nano)));
            assert_eq!(mx.dt_value(), Some(DateTime::new(base + 9, TimeUnit::Nano)));
            // The exact min/max are distinct (the f64 lane could not tell them
            // from one another up here): single-pass agrees with the merge.
            let s_mn = single(AggFunc::Min, &vals);
            assert_eq!(
                mn.dt_value(),
                s_mn.dt_value(),
                "min merge != single @{parts}"
            );
        }
    }

    #[test]
    fn duration_aggregates_are_exact_and_parallel_safe() {
        // Durations are exact i64, so sum/avg/min/max are associative → the
        // partitioned merge equals the single pass, and the result keeps the
        // Duration type. Nanosecond ticks past 2^53 (f64 would be wrong). #57.
        let base = 1_700_000_000_000_000_000_i64;
        assert!(
            base as f64 == (base + 1) as f64,
            "precondition: f64 loses 1ns"
        );
        let vals: Vec<Value> = [base, base + 2, base + 4, base + 6]
            .into_iter()
            .map(|t| Value::Duration(rivus_core::Duration::new(t, TimeUnit::Nano)))
            .collect();
        // Independent i128 oracle.
        let oracle_sum: i128 = vals
            .iter()
            .map(|v| match v {
                Value::Duration(d) => d.ticks as i128,
                _ => 0,
            })
            .sum();
        for parts in [1usize, 2, 3, 4] {
            for (func, want) in [
                (AggFunc::Sum, oracle_sum),
                (AggFunc::Avg, oracle_sum / 4), // (0+2+4+6)/4 = 3 exactly, no rounding
                (AggFunc::Min, base as i128),
                (AggFunc::Max, (base + 6) as i128),
            ] {
                let m = partitioned(func, &vals, parts);
                let s = single(func, &vals);
                assert_eq!(
                    m.dur_value(),
                    s.dur_value(),
                    "{func:?} merge != single @{parts}"
                );
                assert_eq!(
                    m.dur_value(),
                    Some(rivus_core::Duration::new(want as i64, TimeUnit::Nano)),
                    "{func:?} not exact @{parts}"
                );
            }
        }
    }

    #[test]
    fn decimal_avg_merge_equals_single_pass() {
        let vals: Vec<Value> = (0..500)
            .map(|i| Value::Dec(Decimal::new((i * 7 % 1000) + 1, 2)))
            .collect();
        for parts in [1, 2, 5, 13] {
            let s = single(AggFunc::Avg, &vals);
            let m = partitioned(AggFunc::Avg, &vals, parts);
            assert_eq!(
                m.dec_value().unwrap().to_string(),
                s.dec_value().unwrap().to_string(),
                "decimal avg merge != single-pass @parts={parts}"
            );
        }
    }

    #[test]
    fn safe_aggregates_merge_equals_single_pass() {
        let vals: Vec<Value> = (0..300i64)
            .map(|i| match i % 5 {
                0 => Value::I64(i),
                1 => Value::F64(i as f64 * 1.5),
                2 => Value::Str(format!("v{}", i % 11)),
                _ => Value::Dec(Decimal::new(i as i128, 3)),
            })
            .collect();
        for parts in [1, 2, 4, 9] {
            // min/max (f64, associative), count_distinct, first, last, percentile.
            for func in [
                AggFunc::Min,
                AggFunc::Max,
                AggFunc::CountDistinct,
                AggFunc::First,
                AggFunc::Last,
                AggFunc::Pct(50),
                AggFunc::Pct(90),
            ] {
                let s = single(func, &vals);
                let m = partitioned(func, &vals, parts);
                let (sv, mv) = match func {
                    AggFunc::CountDistinct => (
                        s.distinct_count().to_string(),
                        m.distinct_count().to_string(),
                    ),
                    AggFunc::First => (s.first_str().to_string(), m.first_str().to_string()),
                    AggFunc::Last => (s.last_str().to_string(), m.last_str().to_string()),
                    _ => (
                        s.num_value().to_bits().to_string(),
                        m.num_value().to_bits().to_string(),
                    ),
                };
                assert_eq!(sv, mv, "{func:?} merge != single-pass @parts={parts}");
            }
        }
    }
}
