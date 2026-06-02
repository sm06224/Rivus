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
    Chunk, Column, DataType, DateTime, DtColumn, ErrorEvent, ErrorScope, Field, Schema, Severity,
    StrColumn, TimeUnit, Value,
};
use rivus_ir::{AggFunc, BinType, CmpOp, Endian, Expr, FillMethod, JoinKind, NodeId, Op};
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

/// Read a text source: the `-` sentinel reads stdin, otherwise a file.
fn read_input(path: &str) -> std::io::Result<String> {
    if path == "-" {
        use std::io::Read;
        let mut s = String::new();
        std::io::stdin().read_to_string(&mut s)?;
        Ok(s)
    } else {
        std::fs::read_to_string(path)
    }
}

/// A compressed source path (`.gz`/`.zst`/`.zstd`) needs the single-pass
/// decompressing reader (features `gzip` / `zstd`).
fn is_compressed_path(path: &str) -> bool {
    if path == "-" {
        return false;
    }
    let l = path.to_ascii_lowercase();
    l.ends_with(".gz") || l.ends_with(".zst") || l.ends_with(".zstd")
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
pub fn jsonl_range_source(
    path: &str,
    names: Vec<String>,
    dtypes: Vec<rivus_core::DataType>,
    schema: Arc<Schema>,
    start: u64,
    end: u64,
    chunk_size: usize,
) -> Box<dyn Operator> {
    match jsonl::JsonlChunker::for_range(path, names, dtypes, start, end, chunk_size) {
        Ok(ch) => Box::new(SourceJsonl::from_chunker(schema, ch)),
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
        Ok(ch) => Box::new(SourceCsv::from_stream(schema, ch)),
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
        Op::OpenCsv {
            path,
            projection,
            prefilter,
            str_prefilter,
            header,
            declared,
            dt_formats,
            delim,
        } => Box::new(SourceCsv::new(
            path.clone(),
            projection.clone(),
            chunk_size,
            preview,
            prefilter.clone(),
            str_prefilter.clone(),
            *header,
            declared.clone(),
            dt_formats.clone(),
            *delim,
        )),
        Op::OpenBinary {
            path,
            fields,
            endian,
            c_align,
        } => Box::new(SourceBinary::new(
            path.clone(),
            fields.clone(),
            *endian,
            *c_align,
            chunk_size,
        )),
        Op::OpenJsonl { path } => Box::new(SourceJsonl::new(path.clone(), chunk_size)),
        Op::StreamRef { name } => Box::new(StreamRef { name: name.clone() }),
        Op::Filter { pred } => Box::new(Filter { pred: pred.clone() }),
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
        }),
        Op::Reorder { cols } => Box::new(Reorder { cols: cols.clone() }),
        Op::ProjectExpr { items } => Box::new(ProjectExpr {
            items: items.clone(),
        }),
        Op::Project { fields } => Box::new(Project {
            fields: fields.clone(),
        }),
        Op::FilterProject { preds, fields } => Box::new(FilterProject {
            preds: preds.clone(),
            fields: fields.clone(),
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
        Op::SinkCsv { path, delim } => Box::new(SinkCsv::new(path.clone(), *delim)),
        Op::SinkJsonl { path } => Box::new(SinkJsonl::new(path.clone())),
        Op::SinkJson { path } => Box::new(SinkJson::new(path.clone())),
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

// ---------------------------------------------------------------- source (csv)

/// CSV source. A real file streams (bounded memory, [`csv::CsvChunker`]); the
/// `-` stdin sentinel can't be re-read for two-pass inference, so it falls back
/// to the buffered whole-input parse (stdin is rarely the 15 GB case).
struct SourceCsv {
    path: String,
    projection: Option<Vec<String>>,
    chunk_size: usize,
    loaded: bool,
    preview: bool,
    /// Numeric `(column, op, rhs)` predicates pushed down by the optimizer; the
    /// reader uses them to skip *building* rows that definitely fail (the
    /// downstream FilterProject remains authoritative).
    prefilter: Vec<(String, CmpOp, f64)>,
    /// Required literal substrings pushed down by the optimizer; the reader skips
    /// any raw line lacking one before splitting it (a superset pre-scan).
    str_prefilter: Vec<String>,
    header: bool,
    declared: Option<Vec<(String, Option<DataType>)>>,
    /// Explicit `:datetime("fmt")` parse formats, keyed by column name (design 23).
    dt_formats: Vec<(String, String)>,
    /// Field delimiter byte (`b','` CSV, `b'\t'` TSV).
    delim: u8,
    schema: Arc<Schema>,
    /// Streaming reader for a real file; `None` for stdin / after a load error.
    stream: Option<csv::CsvChunker>,
    /// Streaming reader for a compressed file (`--features gzip`/`zstd`).
    #[cfg(any(feature = "gzip", feature = "zstd"))]
    cz_stream: Option<csv::CompressedCsvReader>,
    /// Buffered fallback (stdin): pre-parsed columns sliced by `pull`.
    columns: Vec<Column>,
    cursor: usize,
    total: usize,
}

impl SourceCsv {
    #[allow(clippy::too_many_arguments)]
    fn new(
        path: String,
        projection: Option<Vec<String>>,
        chunk_size: usize,
        preview: bool,
        prefilter: Vec<(String, CmpOp, f64)>,
        str_prefilter: Vec<String>,
        header: bool,
        declared: Option<Vec<(String, Option<DataType>)>>,
        dt_formats: Vec<(String, String)>,
        delim: u8,
    ) -> Self {
        SourceCsv {
            path,
            projection,
            chunk_size: chunk_size.max(1),
            loaded: false,
            preview,
            prefilter,
            str_prefilter,
            header,
            declared,
            dt_formats,
            delim,
            schema: Schema::empty(),
            stream: None,
            #[cfg(any(feature = "gzip", feature = "zstd"))]
            cz_stream: None,
            columns: Vec::new(),
            cursor: 0,
            total: 0,
        }
    }

    /// A source wrapping an already-built streaming reader (a parallel worker's
    /// byte range), with a schema inferred globally beforehand.
    fn from_stream(schema: Arc<Schema>, chunker: csv::CsvChunker) -> Self {
        SourceCsv {
            path: String::new(),
            projection: None,
            chunk_size: 0,
            loaded: true,
            preview: false,
            prefilter: Vec::new(),
            str_prefilter: Vec::new(),
            header: true,
            declared: None,
            dt_formats: Vec::new(),
            delim: b',',
            schema,
            stream: Some(chunker),
            #[cfg(any(feature = "gzip", feature = "zstd"))]
            cz_stream: None,
            columns: Vec::new(),
            cursor: 0,
            total: 0,
        }
    }

    fn load(&mut self, ctx: &mut OpCtx) {
        self.loaded = true;
        if self.path == "-" {
            self.load_stdin(ctx);
        } else if is_compressed_path(&self.path) {
            self.load_compressed(ctx);
        } else {
            match csv::CsvChunker::open(
                &self.path,
                self.projection.as_deref(),
                self.chunk_size,
                self.preview,
                &self.prefilter,
                &self.str_prefilter,
                self.header,
                self.declared.as_deref(),
                &self.dt_formats,
                self.delim,
            ) {
                Ok((schema, chunker)) => {
                    if chunker.bad_rows > 0 {
                        ctx.raise(
                            ErrorEvent::new(
                                Severity::Recoverable,
                                ErrorScope::Item,
                                format!("{} malformed row(s) skipped", chunker.bad_rows),
                            )
                            .at_node(ctx.label.clone()),
                        );
                    }
                    self.schema = Arc::new(schema);
                    self.stream = Some(chunker);
                }
                Err(e) => ctx.raise(
                    ErrorEvent::new(Severity::Fatal, ErrorScope::Graph, e)
                        .at_node(ctx.label.clone()),
                ),
            }
        }
    }

    /// Open a compressed source via the single-pass decompressing reader
    /// (features `gzip`/`zstd`). An extension whose feature is off — or a default
    /// build with neither — raises a fatal, actionable error.
    #[cfg(any(feature = "gzip", feature = "zstd"))]
    fn load_compressed(&mut self, ctx: &mut OpCtx) {
        match csv::CompressedCsvReader::open(
            &self.path,
            self.projection.as_deref(),
            self.chunk_size,
            self.header,
            self.declared.as_deref(),
            &self.dt_formats,
            self.delim,
        ) {
            Ok((schema, reader)) => {
                if reader.bad_rows > 0 {
                    ctx.raise(
                        ErrorEvent::new(
                            Severity::Recoverable,
                            ErrorScope::Item,
                            format!("{} malformed row(s) skipped", reader.bad_rows),
                        )
                        .at_node(ctx.label.clone()),
                    );
                }
                self.schema = Arc::new(schema);
                self.cz_stream = Some(reader);
            }
            Err(e) => ctx.raise(
                ErrorEvent::new(Severity::Fatal, ErrorScope::Graph, e).at_node(ctx.label.clone()),
            ),
        }
    }

    #[cfg(not(any(feature = "gzip", feature = "zstd")))]
    fn load_compressed(&mut self, ctx: &mut OpCtx) {
        let l = self.path.to_ascii_lowercase();
        let feat = if l.ends_with(".gz") { "gzip" } else { "zstd" };
        ctx.raise(
            ErrorEvent::new(
                Severity::Fatal,
                ErrorScope::Graph,
                format!(
                    "'{}' is compressed; rebuild with `--features {feat}` to read it",
                    self.path
                ),
            )
            .at_node(ctx.label.clone()),
        );
    }

    fn load_stdin(&mut self, ctx: &mut OpCtx) {
        let text = match read_input(&self.path) {
            Ok(t) => t,
            Err(e) => {
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Fatal,
                        ErrorScope::Graph,
                        format!("cannot open '{}': {e}", self.path),
                    )
                    .at_node(ctx.label.clone()),
                );
                return;
            }
        };
        match csv::parse_projected(&text, self.projection.as_deref(), self.delim) {
            Ok(data) => {
                if data.bad_rows > 0 {
                    ctx.raise(
                        ErrorEvent::new(
                            Severity::Recoverable,
                            ErrorScope::Item,
                            format!("{} malformed row(s) skipped", data.bad_rows),
                        )
                        .at_node(ctx.label.clone()),
                    );
                }
                self.total = data.columns.first().map(|c| c.len()).unwrap_or(0);
                self.schema = Arc::new(data.schema);
                self.columns = data.columns;
            }
            Err(e) => ctx.raise(
                ErrorEvent::new(Severity::Fatal, ErrorScope::Graph, e).at_node(ctx.label.clone()),
            ),
        }
    }
}

impl Operator for SourceCsv {
    fn is_source(&self) -> bool {
        true
    }

    fn inference(&self) -> Vec<(String, DataType, bool)> {
        self.stream
            .as_ref()
            .map(|c| c.inference().to_vec())
            .unwrap_or_default()
    }

    fn pull(&mut self, ctx: &mut OpCtx) -> Option<Chunk> {
        if !self.loaded {
            self.load(ctx);
        }
        if let Some(chunker) = self.stream.as_mut() {
            match chunker.next_columns() {
                Some(cols) => {
                    let id = ctx.fresh_id();
                    return Some(Chunk::new(id, self.schema.clone(), cols));
                }
                None => {
                    // Source exhausted: report how many rows the pushed-down
                    // prefilter skipped building (pure accounting — the result is
                    // unchanged, the downstream FilterProject would drop them).
                    let skipped = chunker.rows_prefiltered;
                    if skipped > 0 {
                        ctx.raise(
                            ErrorEvent::new(
                                Severity::Info,
                                ErrorScope::Item,
                                format!("prefilter skipped {skipped} row(s) at the reader"),
                            )
                            .at_node(ctx.label.clone()),
                        );
                    }
                    return None;
                }
            }
        }
        #[cfg(any(feature = "gzip", feature = "zstd"))]
        if let Some(cz) = self.cz_stream.as_mut() {
            let cols = cz.next_columns()?;
            let id = ctx.fresh_id();
            return Some(Chunk::new(id, self.schema.clone(), cols));
        }
        // Buffered (stdin) path.
        if self.cursor >= self.total {
            return None;
        }
        let end = (self.cursor + self.chunk_size).min(self.total);
        let idx: Vec<usize> = (self.cursor..end).collect();
        let columns = self.columns.iter().map(|c| c.gather(&idx)).collect();
        let id = ctx.fresh_id();
        self.cursor = end;
        Some(Chunk::new(id, self.schema.clone(), columns))
    }

    fn process(&mut self, _from: NodeId, _chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        Vec::new()
    }
}

// ------------------------------------------------------------- source (binary)

/// Reads fixed-width binary records (a C struct dump): fields are packed in
/// declaration order, little-endian. Each field decodes straight into its
/// columnar lane — no text parsing at all, so this is much faster than CSV.
struct SourceBinary {
    path: String,
    fields: Vec<(String, BinType)>,
    endian: Endian,
    c_align: bool,
    chunk_size: usize,
    schema: Arc<Schema>,
    chunker: Option<BinChunker>,
    loaded: bool,
}

impl SourceBinary {
    fn new(
        path: String,
        fields: Vec<(String, BinType)>,
        endian: Endian,
        c_align: bool,
        chunk_size: usize,
    ) -> Self {
        SourceBinary {
            path,
            fields,
            endian,
            c_align,
            chunk_size: chunk_size.max(1),
            schema: Schema::empty(),
            chunker: None,
            loaded: false,
        }
    }

    /// A source wrapping an already-built streaming binary reader (a parallel
    /// worker's record range), with a globally known schema.
    fn from_chunker(schema: Arc<Schema>, chunker: BinChunker) -> Self {
        SourceBinary {
            path: String::new(),
            fields: Vec::new(),
            endian: Endian::Little,
            c_align: false,
            chunk_size: 0,
            schema,
            chunker: Some(chunker),
            loaded: true,
        }
    }

    fn load(&mut self, ctx: &mut OpCtx) {
        self.loaded = true;
        match BinChunker::open(
            &self.path,
            self.fields.clone(),
            self.endian,
            self.c_align,
            self.chunk_size,
        ) {
            Ok((schema, ch)) => {
                if ch.trailing > 0 {
                    ctx.raise(
                        ErrorEvent::new(
                            Severity::Recoverable,
                            ErrorScope::Item,
                            format!("{} trailing byte(s) ignored (partial record)", ch.trailing),
                        )
                        .at_node(ctx.label.clone()),
                    );
                }
                self.schema = Arc::new(schema);
                self.chunker = Some(ch);
            }
            Err(e) => ctx.raise(
                ErrorEvent::new(Severity::Fatal, ErrorScope::Graph, e).at_node(ctx.label.clone()),
            ),
        }
    }
}

impl Operator for SourceBinary {
    fn is_source(&self) -> bool {
        true
    }

    fn pull(&mut self, ctx: &mut OpCtx) -> Option<Chunk> {
        if !self.loaded {
            self.load(ctx);
        }
        let ch = self.chunker.as_mut()?;
        let columns = ch.next_columns()?;
        let id = ctx.fresh_id();
        Some(Chunk::new(id, self.schema.clone(), columns))
    }

    fn process(&mut self, _from: NodeId, _chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        Vec::new()
    }
}

/// Field byte-offsets and the record stride for a fixed-width binary layout
/// (honoring C natural alignment when `c_align`). `None` for an empty layout.
pub(crate) fn bin_layout(
    fields: &[(String, BinType)],
    c_align: bool,
) -> Option<(Vec<usize>, usize)> {
    let mut offsets = Vec::with_capacity(fields.len());
    let mut acc = 0usize;
    let mut max_align = 1usize;
    for (_, t) in fields {
        if c_align {
            let a = t.align();
            max_align = max_align.max(a);
            acc = round_up(acc, a);
        }
        offsets.push(acc);
        acc += t.size();
    }
    let rec = if c_align {
        round_up(acc, max_align)
    } else {
        acc
    };
    (rec != 0).then_some((offsets, rec))
}

/// Schema for a fixed-width binary layout (one field per column, declared order).
pub(crate) fn bin_schema(fields: &[(String, BinType)]) -> Schema {
    Schema::new(
        fields
            .iter()
            .map(|(n, t)| Field::new(n.clone(), t.lane()))
            .collect(),
    )
}

/// Decode `n` fixed-width records packed in `buf` into one column per field.
fn decode_bin_batch(
    buf: &[u8],
    fields: &[(String, BinType)],
    offsets: &[usize],
    rec_size: usize,
    endian: Endian,
    n: usize,
) -> Vec<Column> {
    fields
        .iter()
        .enumerate()
        .map(|(fi, (_, t))| {
            let foff = offsets[fi];
            let sz = t.size();
            let cell = |r: usize| -> &[u8] { &buf[r * rec_size + foff..r * rec_size + foff + sz] };
            match t.lane() {
                DataType::Bool => Column::Bool((0..n).map(|r| cell(r)[0] != 0).collect()),
                DataType::F64 => {
                    Column::F64((0..n).map(|r| decode_f64(cell(r), *t, endian)).collect())
                }
                _ => Column::I64((0..n).map(|r| decode_int(cell(r), *t, endian)).collect()),
            }
        })
        .collect()
}

/// Streaming fixed-width binary reader (bounded memory): reads `chunk_size`
/// records per call, decoding straight into columns. Records are fixed width, so
/// a byte range is exactly `[start_rec, end_rec) * rec_size` — no boundary scan.
pub(crate) struct BinChunker {
    reader: std::io::BufReader<std::fs::File>,
    fields: Vec<(String, BinType)>,
    offsets: Vec<usize>,
    rec_size: usize,
    endian: Endian,
    chunk_size: usize,
    recs_left: usize,
    /// Trailing bytes after the last whole record (reported once by the source).
    pub trailing: usize,
}

impl BinChunker {
    pub(crate) fn open(
        path: &str,
        fields: Vec<(String, BinType)>,
        endian: Endian,
        c_align: bool,
        chunk_size: usize,
    ) -> Result<(Schema, BinChunker), String> {
        let (offsets, rec_size) = bin_layout(&fields, c_align).ok_or("empty binary layout")?;
        let len = std::fs::metadata(path)
            .map_err(|e| format!("cannot open '{path}': {e}"))?
            .len() as usize;
        let f = std::fs::File::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
        let schema = bin_schema(&fields);
        Ok((
            schema,
            BinChunker {
                reader: std::io::BufReader::with_capacity(256 * 1024, f),
                fields,
                offsets,
                rec_size,
                endian,
                chunk_size: chunk_size.max(1),
                recs_left: len / rec_size,
                trailing: len % rec_size,
            },
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn for_range(
        path: &str,
        fields: Vec<(String, BinType)>,
        offsets: Vec<usize>,
        rec_size: usize,
        endian: Endian,
        start_rec: usize,
        n_recs: usize,
        chunk_size: usize,
    ) -> Result<BinChunker, String> {
        let mut f = std::fs::File::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
        std::io::Seek::seek(
            &mut f,
            std::io::SeekFrom::Start((start_rec * rec_size) as u64),
        )
        .map_err(|e| e.to_string())?;
        Ok(BinChunker {
            reader: std::io::BufReader::with_capacity(256 * 1024, f),
            fields,
            offsets,
            rec_size,
            endian,
            chunk_size: chunk_size.max(1),
            recs_left: n_recs,
            trailing: 0,
        })
    }

    pub(crate) fn next_columns(&mut self) -> Option<Vec<Column>> {
        if self.recs_left == 0 {
            return None;
        }
        let take = self.chunk_size.min(self.recs_left);
        let mut buf = vec![0u8; take * self.rec_size];
        if std::io::Read::read_exact(&mut self.reader, &mut buf).is_err() {
            self.recs_left = 0;
            return None;
        }
        self.recs_left -= take;
        Some(decode_bin_batch(
            &buf,
            &self.fields,
            &self.offsets,
            self.rec_size,
            self.endian,
            take,
        ))
    }
}

/// A streaming binary source over one record-aligned byte range, for the
/// parallel executor (#49). On open error it yields nothing (continue-first).
#[allow(clippy::too_many_arguments)]
pub fn bin_range_source(
    path: &str,
    fields: Vec<(String, BinType)>,
    offsets: Vec<usize>,
    rec_size: usize,
    endian: Endian,
    schema: Arc<Schema>,
    start_rec: usize,
    n_recs: usize,
    chunk_size: usize,
) -> Box<dyn Operator> {
    match BinChunker::for_range(
        path, fields, offsets, rec_size, endian, start_rec, n_recs, chunk_size,
    ) {
        Ok(ch) => Box::new(SourceBinary::from_chunker(schema, ch)),
        Err(_) => Box::new(MemSource {
            chunks: std::collections::VecDeque::new(),
        }),
    }
}

/// Round `x` up to a multiple of `align` (a power of two ≥ 1).
fn round_up(x: usize, align: usize) -> usize {
    x.div_ceil(align) * align
}

macro_rules! from_bytes {
    ($ty:ty, $b:expr, $e:expr, $n:literal) => {{
        let arr: [u8; $n] = $b[..$n].try_into().unwrap();
        match $e {
            Endian::Little => <$ty>::from_le_bytes(arr),
            Endian::Big => <$ty>::from_be_bytes(arr),
        }
    }};
}

/// Decode an integer field of any supported width into `i64`, honoring endian.
fn decode_int(b: &[u8], t: BinType, e: Endian) -> i64 {
    match t {
        BinType::I8 => b[0] as i8 as i64,
        BinType::U8 | BinType::Bool => b[0] as i64,
        BinType::I16 => from_bytes!(i16, b, e, 2) as i64,
        BinType::U16 => from_bytes!(u16, b, e, 2) as i64,
        BinType::I32 => from_bytes!(i32, b, e, 4) as i64,
        BinType::U32 => from_bytes!(u32, b, e, 4) as i64,
        BinType::I64 => from_bytes!(i64, b, e, 8),
        // u64 above i64::MAX wraps; documented limitation until a u64 lane exists.
        BinType::U64 => from_bytes!(u64, b, e, 8) as i64,
        BinType::F32 | BinType::F64 => 0, // not an integer lane
    }
}

fn decode_f64(b: &[u8], t: BinType, e: Endian) -> f64 {
    match t {
        BinType::F32 => from_bytes!(f32, b, e, 4) as f64,
        BinType::F64 => from_bytes!(f64, b, e, 8),
        _ => 0.0,
    }
}

// -------------------------------------------------------------- source (jsonl)

/// Reads JSON Lines (one flat JSON object per line) into columns. See
/// `jsonl.rs` for the parser and its continue-first behavior.
struct SourceJsonl {
    path: String,
    chunk_size: usize,
    schema: Arc<Schema>,
    /// Line-oriented JSONL streams in bounded memory; a top-level array can't be
    /// streamed (an element may span lines) so it materializes via `jsonl::parse`.
    chunker: Option<jsonl::JsonlChunker>,
    columns: Vec<Column>,
    cursor: usize,
    total: usize,
    loaded: bool,
}

impl SourceJsonl {
    fn new(path: String, chunk_size: usize) -> Self {
        SourceJsonl {
            path,
            chunk_size: chunk_size.max(1),
            schema: Schema::empty(),
            chunker: None,
            columns: Vec::new(),
            cursor: 0,
            total: 0,
            loaded: false,
        }
    }

    /// A source wrapping an already-built streaming JSONL reader (a parallel
    /// worker's byte range), with a globally pre-inferred schema.
    fn from_chunker(schema: Arc<Schema>, chunker: jsonl::JsonlChunker) -> Self {
        SourceJsonl {
            path: String::new(),
            chunk_size: 0,
            schema,
            chunker: Some(chunker),
            columns: Vec::new(),
            cursor: 0,
            total: 0,
            loaded: true,
        }
    }

    fn load(&mut self, ctx: &mut OpCtx) {
        self.loaded = true;
        // Line-oriented JSONL → bounded streaming reader; top-level array → the
        // whole-file parse (can't be streamed).
        if !jsonl::is_json_array(&self.path) {
            match jsonl::JsonlChunker::open(&self.path, self.chunk_size) {
                Ok((schema, ch)) => {
                    if ch.bad_rows > 0 {
                        ctx.raise(
                            ErrorEvent::new(
                                Severity::Recoverable,
                                ErrorScope::Item,
                                format!("{} malformed JSONL line(s) skipped", ch.bad_rows),
                            )
                            .at_node(ctx.label.clone()),
                        );
                    }
                    self.schema = Arc::new(schema);
                    self.chunker = Some(ch);
                }
                Err(e) => ctx.raise(
                    ErrorEvent::new(Severity::Fatal, ErrorScope::Graph, e)
                        .at_node(ctx.label.clone()),
                ),
            }
            return;
        }
        let text = match read_input(&self.path) {
            Ok(t) => t,
            Err(e) => {
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Fatal,
                        ErrorScope::Graph,
                        format!("cannot open '{}': {e}", self.path),
                    )
                    .at_node(ctx.label.clone()),
                );
                return;
            }
        };
        match jsonl::parse(&text) {
            Ok(data) => {
                if data.bad_rows > 0 {
                    ctx.raise(
                        ErrorEvent::new(
                            Severity::Recoverable,
                            ErrorScope::Item,
                            format!("{} malformed JSONL line(s) skipped", data.bad_rows),
                        )
                        .at_node(ctx.label.clone()),
                    );
                }
                self.total = data.columns.first().map(|c| c.len()).unwrap_or(0);
                self.schema = Arc::new(data.schema);
                self.columns = data.columns;
            }
            Err(e) => ctx.raise(
                ErrorEvent::new(Severity::Fatal, ErrorScope::Graph, e).at_node(ctx.label.clone()),
            ),
        }
    }
}

impl Operator for SourceJsonl {
    fn is_source(&self) -> bool {
        true
    }

    fn pull(&mut self, ctx: &mut OpCtx) -> Option<Chunk> {
        if !self.loaded {
            self.load(ctx);
        }
        if let Some(ch) = &mut self.chunker {
            let columns = ch.next_columns()?;
            let id = ctx.fresh_id();
            return Some(Chunk::new(id, self.schema.clone(), columns));
        }
        if self.cursor >= self.total {
            return None;
        }
        let end = (self.cursor + self.chunk_size).min(self.total);
        let idx: Vec<usize> = (self.cursor..end).collect();
        let columns = self.columns.iter().map(|c| c.gather(&idx)).collect();
        let id = ctx.fresh_id();
        self.cursor = end;
        Some(Chunk::new(id, self.schema.clone(), columns))
    }

    fn process(&mut self, _from: NodeId, _chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        Vec::new()
    }
}

// ----------------------------------------------------------- stream ref (stub)

/// `stream X` replay. The MVP has no checkpoint store yet, so a replay with no
/// recorded history simply produces nothing and notes it on the error stream.
struct StreamRef {
    name: String,
}

impl Operator for StreamRef {
    fn is_source(&self) -> bool {
        true
    }
    fn pull(&mut self, ctx: &mut OpCtx) -> Option<Chunk> {
        ctx.raise(ErrorEvent::new(
            Severity::Info,
            ErrorScope::Graph,
            format!(
                "stream replay '{}' has no recorded history (MVP)",
                self.name
            ),
        ));
        None
    }
    fn process(&mut self, _from: NodeId, _chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        Vec::new()
    }
}

// -------------------------------------------------------------------- filter

struct Filter {
    pred: Expr,
}

impl Operator for Filter {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        // Vectorized numeric path when possible; else the row-wise interpreter.
        let keep = match kernel::compile(&[&self.pred], &chunk) {
            Some(plan) => kernel::run(&plan, &chunk),
            None => (0..chunk.len)
                .filter(|&row| eval::eval_predicate(&self.pred, &chunk, row))
                .collect(),
        };
        if keep.is_empty() {
            return Vec::new();
        }
        if keep.len() == chunk.len {
            return vec![chunk];
        }
        vec![chunk.gather(&keep)]
    }
}

// ---------------------------------------------------------------------- take

/// `take N` — forward at most `N` rows total, then drop everything else.
/// Stateful: `remaining` is the global budget, so results are independent of
/// `chunk_size` (a chunk straddling the limit is truncated to fit).
struct Take {
    remaining: usize,
}

impl Operator for Take {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.remaining == 0 {
            return Vec::new();
        }
        if chunk.len <= self.remaining {
            self.remaining -= chunk.len;
            return vec![chunk];
        }
        // Chunk overruns the budget: keep just the first `remaining` rows.
        let idx: Vec<usize> = (0..self.remaining).collect();
        self.remaining = 0;
        vec![chunk.gather(&idx)]
    }
}

// ---------------------------------------------------------------------- sort

/// `sort KEY [desc]` — a blocking sort. Buffers every chunk, then on finish
/// concatenates them (in arrival = source order), stably sorts by the key
/// column, and emits one ordered chunk. Stable + concatenate-then-sort makes
/// the output independent of `chunk_size`; ties keep source order for both
/// ascending and descending.
struct Sort {
    keys: Vec<(String, bool)>,
    buf: Vec<Chunk>,
    emitted: bool,
}

impl Sort {
    fn new(keys: Vec<(String, bool)>) -> Self {
        Sort {
            keys,
            buf: Vec::new(),
            emitted: false,
        }
    }
}

/// Compare two rows of one column for ordering (NaN treated as equal).
fn cmp_rows(col: &Column, a: usize, b: usize) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match col {
        Column::Bool(v) => v[a].cmp(&v[b]),
        Column::I64(v) => v[a].cmp(&v[b]),
        Column::F64(v) => v[a].partial_cmp(&v[b]).unwrap_or(Ordering::Equal),
        // One column shares a scale, so the unscaled i128 order is the exact
        // value order — no precision loss in the sort key (design doc 21).
        Column::Dec(d) => d.unscaled[a].cmp(&d.unscaled[b]),
        // One column shares a unit, so the integer tick order is the exact
        // chronological order.
        Column::DateTime(d) => d.ticks[a].cmp(&d.ticks[b]),
        // Duration shares a unit too → exact i64 magnitude order (#57).
        Column::Duration(d) => d.ticks[a].cmp(&d.ticks[b]),
        Column::Str(v) => v.get(a).cmp(v.get(b)),
    }
}

impl Operator for Sort {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        if !chunk.is_empty() {
            self.buf.push(chunk);
        }
        Vec::new() // blocking boundary: output on finish
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.emitted || self.buf.is_empty() {
            return Vec::new();
        }
        self.emitted = true;

        // Concatenate buffered chunks into one set of columns (source order).
        let mut iter = std::mem::take(&mut self.buf).into_iter();
        let first = iter.next().unwrap();
        let schema = first.schema.clone();
        let mut cols = first.columns;
        for c in iter {
            for (i, col) in c.columns.iter().enumerate() {
                cols[i].append(col);
            }
        }
        let total = cols.first().map(|c| c.len()).unwrap_or(0);

        // Resolve each sort key to (column index, descending). An unknown key
        // warns once and is skipped (continue-first); if none resolve the stream
        // is emitted in source order.
        let mut key_cols: Vec<(usize, bool)> = Vec::with_capacity(self.keys.len());
        for (k, desc) in &self.keys {
            match schema.index_of(k) {
                Some(ki) => key_cols.push((ki, *desc)),
                None => ctx.raise(
                    ErrorEvent::new(
                        Severity::Warn,
                        ErrorScope::Chunk,
                        format!("sort: unknown key '{k}' (ignored)"),
                    )
                    .at_node(ctx.label.clone()),
                ),
            }
        }

        let mut idx: Vec<usize> = (0..total).collect();
        if !key_cols.is_empty() {
            idx.sort_by(|&a, &b| {
                for &(ki, desc) in &key_cols {
                    let o = cmp_rows(&cols[ki], a, b);
                    let o = if desc { o.reverse() } else { o };
                    if o != std::cmp::Ordering::Equal {
                        return o;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        let sorted: Vec<Column> = cols.iter().map(|c| c.gather(&idx)).collect();
        vec![Chunk::new(ctx.fresh_id(), schema, sorted)]
    }
}

// ------------------------------------------------------------------ distinct

/// `distinct [keys...]` — keep the first occurrence of each distinct key,
/// dropping later duplicates. Streaming (emits surviving rows per chunk) but
/// stateful: a global seen-set spans chunks, so it runs serially. Output order
/// is first-occurrence order, independent of `chunk_size`.
struct Distinct {
    keys: Vec<String>,
    seen: std::collections::HashSet<String>,
}

impl Distinct {
    fn new(keys: Vec<String>) -> Self {
        Distinct {
            keys,
            seen: std::collections::HashSet::new(),
        }
    }
}

impl Operator for Distinct {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        // Columns that form the dedup key: the named ones, or every column.
        let idxs: Vec<usize> = if self.keys.is_empty() {
            (0..chunk.columns.len()).collect()
        } else {
            self.keys
                .iter()
                .filter_map(|k| chunk.schema.index_of(k))
                .collect()
        };

        let mut keep = Vec::new();
        let mut key = String::new();
        for row in 0..chunk.len {
            key.clear();
            for (j, &ci) in idxs.iter().enumerate() {
                if j > 0 {
                    key.push('\u{1f}'); // unit separator: unlikely in data
                }
                key.push_str(&chunk.value(row, ci).to_string());
            }
            if self.seen.insert(key.clone()) {
                keep.push(row);
            }
        }

        if keep.is_empty() {
            return Vec::new();
        }
        if keep.len() == chunk.len {
            return vec![chunk];
        }
        vec![chunk.gather(&keep)]
    }
}

// ------------------------------------------------------------------ describe

/// `describe` — a one-pass streaming summary: per input column, its type, row
/// count, and (for numeric columns) min / max / mean. Accumulates across chunks
/// and emits a single summary chunk on finish (one row per column). Stateful →
/// serial path. The summary is rendered as string cells for clean display.
#[derive(Default)]
struct Describe {
    names: Vec<String>,
    types: Vec<DataType>,
    count: u64,
    // Per-column numeric accumulators (used only for I64/F64 columns).
    n: Vec<u64>,
    sum: Vec<f64>,
    min: Vec<f64>,
    max: Vec<f64>,
    inited: bool,
    emitted: bool,
}

impl Describe {
    fn init(&mut self, chunk: &Chunk) {
        self.names = chunk
            .schema
            .field_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        self.types = chunk.columns.iter().map(|c| c.dtype()).collect();
        let k = self.names.len();
        self.n = vec![0; k];
        self.sum = vec![0.0; k];
        self.min = vec![f64::INFINITY; k];
        self.max = vec![f64::NEG_INFINITY; k];
        self.inited = true;
    }
}

impl Operator for Describe {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        if !self.inited {
            self.init(&chunk);
        }
        self.count += chunk.len as u64;
        for (ci, col) in chunk.columns.iter().enumerate() {
            let vals: &mut dyn Iterator<Item = f64> = match col {
                Column::I64(v) => &mut v.iter().map(|&x| x as f64),
                Column::F64(v) => &mut v.iter().copied(),
                _ => continue, // non-numeric: only type + count are reported
            };
            for x in vals {
                self.n[ci] += 1;
                self.sum[ci] += x;
                self.min[ci] = self.min[ci].min(x);
                self.max[ci] = self.max[ci].max(x);
            }
        }
        Vec::new() // summary emitted on finish
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.emitted || !self.inited {
            return Vec::new();
        }
        self.emitted = true;

        let fmt = |x: f64| {
            if x.fract() == 0.0 && x.abs() < 1e15 {
                format!("{x:.0}")
            } else {
                format!("{x}")
            }
        };
        let mut column = StrColumn::default();
        let mut typ = StrColumn::default();
        let mut count = Vec::new();
        let mut min = StrColumn::default();
        let mut max = StrColumn::default();
        let mut mean = StrColumn::default();
        for (i, name) in self.names.iter().enumerate() {
            column.push(name);
            typ.push(&self.types[i].to_string());
            count.push(self.count as i64);
            if self.n[i] > 0 {
                min.push(&fmt(self.min[i]));
                max.push(&fmt(self.max[i]));
                mean.push(&fmt(self.sum[i] / self.n[i] as f64));
            } else {
                min.push("");
                max.push("");
                mean.push("");
            }
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("column", DataType::Str),
            Field::new("type", DataType::Str),
            Field::new("count", DataType::I64),
            Field::new("min", DataType::Str),
            Field::new("max", DataType::Str),
            Field::new("mean", DataType::Str),
        ]));
        let columns = vec![
            Column::Str(column),
            Column::Str(typ),
            Column::I64(count),
            Column::Str(min),
            Column::Str(max),
            Column::Str(mean),
        ];
        vec![Chunk::new(ctx.fresh_id(), schema, columns)]
    }
}

// ------------------------------------------------------------ dropna / fill

/// `dropna [cols]` — drop rows whose value in any target column is missing
/// (renders empty: an empty string cell or null). With no columns, any column.
/// Streaming and stateless. (Numeric columns can't carry an "empty" cell — a
/// blank parses to 0 — so dropna is meaningful on text columns; declare a
/// column `:str` first if you need to detect its blanks.)
struct DropNa {
    cols: Vec<String>,
}

impl Operator for DropNa {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        let idxs: Vec<usize> = if self.cols.is_empty() {
            (0..chunk.columns.len()).collect()
        } else {
            self.cols
                .iter()
                .filter_map(|c| chunk.schema.index_of(c))
                .collect()
        };
        let keep: Vec<usize> = (0..chunk.len)
            .filter(|&r| {
                !idxs
                    .iter()
                    .any(|&ci| chunk.value(r, ci).to_string().is_empty())
            })
            .collect();
        if keep.is_empty() {
            return Vec::new();
        }
        if keep.len() == chunk.len {
            return vec![chunk];
        }
        vec![chunk.gather(&keep)]
    }
}

/// `fill col VALUE` — replace missing (empty) cells of a text column with
/// `VALUE`. Streaming, stateless. A non-text column is passed through unchanged
/// (its blanks already became 0 at parse time).
struct Fill {
    col: String,
    value: String,
}

impl Operator for Fill {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        let Some(ci) = chunk.schema.index_of(&self.col) else {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Warn,
                    ErrorScope::Chunk,
                    format!("fill: unknown column '{}'", self.col),
                )
                .at_node(ctx.label.clone()),
            );
            return vec![chunk];
        };
        let Column::Str(s) = &chunk.columns[ci] else {
            return vec![chunk]; // numeric column: no empty cells to fill
        };
        let mut filled = StrColumn::with_capacity(chunk.len, 0);
        for r in 0..chunk.len {
            let v = s.get(r);
            filled.push(if v.is_empty() { &self.value } else { v });
        }
        let mut columns = chunk.columns.clone();
        columns[ci] = Column::Str(filled);
        let mut out = Chunk::new(chunk.meta.id, chunk.schema.clone(), columns);
        out.meta = chunk.meta.clone();
        vec![out]
    }
}

/// Replace a text column's blank cells with the nearest non-empty value:
/// `ffill` carries the last seen value forward, `bfill` the next value back.
///
/// `ffill` is streaming — it carries one value across chunks and rewrites each
/// chunk in flight. `bfill` needs the *next* value, which may live in a later
/// chunk, so it buffers the stream and emits on `finish` (a pipeline-breaker
/// like `sort`). Both rewrite only a `Str` column; a numeric column is passed
/// through unchanged (its blanks already became `0` at parse time). Leading
/// blanks for `ffill` (and trailing blanks for `bfill`) have no neighbor to
/// borrow and stay empty.
struct FillDirectional {
    col: String,
    forward: bool,
    /// `ffill` state: the last non-empty value seen so far (carried across
    /// chunks). Unused for `bfill`.
    carry: Option<String>,
    /// `bfill` buffer: every chunk, replayed in a single backward pass on finish.
    buf: Vec<Chunk>,
    warned: bool,
}

impl FillDirectional {
    fn ffill(col: String) -> Self {
        FillDirectional {
            col,
            forward: true,
            carry: None,
            buf: Vec::new(),
            warned: false,
        }
    }
    fn bfill(col: String) -> Self {
        FillDirectional {
            col,
            forward: false,
            carry: None,
            buf: Vec::new(),
            warned: false,
        }
    }

    /// Warn once if the column is unknown or non-text; returns the column index
    /// when it's a fillable `Str` column.
    fn target(&mut self, chunk: &Chunk, ctx: &mut OpCtx) -> Option<usize> {
        let Some(ci) = chunk.schema.index_of(&self.col) else {
            if !self.warned {
                self.warned = true;
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Warn,
                        ErrorScope::Chunk,
                        format!("fill: unknown column '{}'", self.col),
                    )
                    .at_node(ctx.label.clone()),
                );
            }
            return None;
        };
        matches!(chunk.columns[ci], Column::Str(_)).then_some(ci)
    }
}

impl Operator for FillDirectional {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        if !self.forward {
            // bfill: buffer; the next non-empty value may be in a later chunk.
            self.buf.push(chunk);
            return Vec::new();
        }
        let Some(ci) = self.target(&chunk, ctx) else {
            return vec![chunk];
        };
        let Column::Str(s) = &chunk.columns[ci] else {
            return vec![chunk];
        };
        let mut filled = StrColumn::with_capacity(chunk.len, 0);
        for r in 0..chunk.len {
            let v = s.get(r);
            if v.is_empty() {
                match &self.carry {
                    Some(c) => filled.push(c),
                    None => filled.push(""),
                }
            } else {
                filled.push(v);
                self.carry = Some(v.to_string());
            }
        }
        let mut columns = chunk.columns.clone();
        columns[ci] = Column::Str(filled);
        let mut out = Chunk::new(chunk.meta.id, chunk.schema.clone(), columns);
        out.meta = chunk.meta.clone();
        vec![out]
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.forward || self.buf.is_empty() {
            return Vec::new();
        }
        let chunks = std::mem::take(&mut self.buf);
        // Resolve the column once against the first chunk (schema is stable).
        let ci = match self.target(&chunks[0], ctx) {
            Some(ci) => ci,
            None => return chunks, // unknown or non-text → pass through unchanged
        };
        // One backward pass across all rows, carrying the next non-empty value.
        let mut next: Option<String> = None;
        let mut out = chunks;
        for chunk in out.iter_mut().rev() {
            let Column::Str(s) = &chunk.columns[ci] else {
                continue;
            };
            let mut vals: Vec<String> = (0..chunk.len).map(|r| s.get(r).to_string()).collect();
            for v in vals.iter_mut().rev() {
                if v.is_empty() {
                    if let Some(n) = &next {
                        *v = n.clone();
                    }
                } else {
                    next = Some(v.clone());
                }
            }
            let mut filled = StrColumn::with_capacity(chunk.len, 0);
            for v in &vals {
                filled.push(v);
            }
            chunk.columns[ci] = Column::Str(filled);
        }
        out
    }
}

/// `fill col mean|median` — replace blank cells of a text column with a
/// whole-column statistic of its non-empty **numeric** cells. Buffers the entire
/// stream (a pipeline-breaker like `sort`): the statistic needs every value, so
/// it can only be known on `finish`. Works on a `Str` column (declare `:str` so
/// blanks survive parsing); a numeric column has no blank cells (they became `0`
/// at parse time) and is passed through unchanged. Cells that don't parse as a
/// number are ignored when computing the statistic but kept as-is in the output.
struct FillStat {
    col: String,
    median: bool,
    buf: Vec<Chunk>,
    warned: bool,
}

impl FillStat {
    fn new(col: String, median: bool) -> Self {
        FillStat {
            col,
            median,
            buf: Vec::new(),
            warned: false,
        }
    }

    /// Linear-interpolated median (p50) of a sorted-in-place value set; mirrors
    /// the percentile aggregate so `fill median` and `|# median:` agree.
    fn median_of(mut v: Vec<f64>) -> f64 {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        if v.is_empty() {
            return 0.0;
        }
        if v.len() == 1 {
            return v[0];
        }
        let rank = 0.5 * (v.len() - 1) as f64;
        let lo = rank.floor() as usize;
        let hi = rank.ceil() as usize;
        let frac = rank - lo as f64;
        v[lo] + (v[hi] - v[lo]) * frac
    }

    /// Format the fill value without a trailing `.0` when it is integral, so an
    /// integer-looking column stays integer-looking after the fill.
    fn format_stat(x: f64) -> String {
        if x.fract() == 0.0 && x.abs() < 1e15 {
            format!("{}", x as i64)
        } else {
            format!("{x}")
        }
    }
}

impl Operator for FillStat {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        self.buf.push(chunk);
        Vec::new() // blocking: needs the whole column to know the statistic
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.buf.is_empty() {
            return Vec::new();
        }
        let mut chunks = std::mem::take(&mut self.buf);
        let Some(ci) = chunks[0].schema.index_of(&self.col) else {
            if !self.warned {
                self.warned = true;
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Warn,
                        ErrorScope::Chunk,
                        format!("fill: unknown column '{}'", self.col),
                    )
                    .at_node(ctx.label.clone()),
                );
            }
            return chunks;
        };
        // Numeric column → no blanks to fill (parsed to 0 already); pass through.
        if !matches!(chunks[0].columns[ci], Column::Str(_)) {
            return chunks;
        }

        // Pass 1: collect every non-empty cell that parses as a number.
        let mut nums: Vec<f64> = Vec::new();
        let mut count = 0f64;
        let mut sum = 0f64;
        for c in &chunks {
            if let Column::Str(s) = &c.columns[ci] {
                for r in 0..c.len {
                    let cell = s.get(r).trim();
                    if cell.is_empty() {
                        continue;
                    }
                    if let Ok(x) = cell.parse::<f64>() {
                        sum += x;
                        count += 1.0;
                        if self.median {
                            nums.push(x);
                        }
                    }
                }
            }
        }
        // No numeric cell to learn from → leave blanks as-is (warn once).
        if count == 0.0 {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Warn,
                    ErrorScope::Chunk,
                    format!(
                        "fill {}: no numeric values to compute {}",
                        self.col,
                        if self.median { "median" } else { "mean" }
                    ),
                )
                .at_node(ctx.label.clone()),
            );
            return chunks;
        }
        let stat = if self.median {
            Self::median_of(nums)
        } else {
            sum / count
        };
        let fill = Self::format_stat(stat);

        // Pass 2: rewrite blank cells with the formatted statistic.
        for c in chunks.iter_mut() {
            let Column::Str(s) = &c.columns[ci] else {
                continue;
            };
            let mut filled = StrColumn::with_capacity(c.len, 0);
            for r in 0..c.len {
                let v = s.get(r);
                filled.push(if v.trim().is_empty() { &fill } else { v });
            }
            c.columns[ci] = Column::Str(filled);
        }
        chunks
    }
}

/// `rename OLD NEW [OLD NEW ...]` — rename columns in place. Position, type and
/// values are untouched; only the field name changes. Unknown `OLD` names raise
/// a one-line warning and are skipped. Stateless and streaming.
struct Rename {
    pairs: Vec<(String, String)>,
}

impl Operator for Rename {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        let mut fields = chunk.schema.fields.clone();
        for (from, to) in &self.pairs {
            match chunk.schema.index_of(from) {
                Some(i) => fields[i] = Field::new(to.clone(), fields[i].dtype),
                None => ctx.raise(
                    ErrorEvent::new(
                        Severity::Warn,
                        ErrorScope::Chunk,
                        format!("rename: unknown column '{from}'"),
                    )
                    .at_node(ctx.label.clone())
                    .at_chunk(chunk.meta.id),
                ),
            }
        }
        let schema = Arc::new(Schema::new(fields));
        let mut out = Chunk::new(chunk.meta.id, schema, chunk.columns.clone());
        out.meta = chunk.meta.clone();
        vec![out]
    }
}

/// `drop COL [COL ...]` — remove the named columns, keeping the rest in order.
/// Unknown names are ignored (dropping a non-existent column is a no-op).
/// Stateless and streaming.
struct Drop {
    cols: Vec<String>,
}

impl Operator for Drop {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        let keep: Vec<usize> = (0..chunk.schema.fields.len())
            .filter(|&i| !self.cols.iter().any(|c| c == &chunk.schema.fields[i].name))
            .collect();
        if keep.len() == chunk.schema.fields.len() {
            return vec![chunk]; // nothing matched → unchanged
        }
        let fields: Vec<Field> = keep
            .iter()
            .map(|&i| chunk.schema.fields[i].clone())
            .collect();
        let columns: Vec<Column> = keep.iter().map(|&i| chunk.columns[i].clone()).collect();
        let schema = Arc::new(Schema::new(fields));
        let mut out = Chunk::new(chunk.meta.id, schema, columns);
        out.meta = chunk.meta.clone();
        vec![out]
    }
}

/// `cast COL:type [COL:type ...]` — re-type named columns in place (position and
/// name kept; the column's values are re-coerced through the cast lane, exactly
/// like an inline `(col:type)` projection). Unknown names warn once and are
/// skipped. Stateless and streaming.
struct Cast {
    casts: Vec<(String, DataType)>,
}

impl Operator for Cast {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        let mut fields = chunk.schema.fields.clone();
        let mut columns = chunk.columns.clone();
        let mut changed = false;
        for (name, ty) in &self.casts {
            match chunk.schema.index_of(name) {
                Some(i) => {
                    columns[i] = eval::cast_column(columns[i].clone(), *ty);
                    fields[i] = Field::new(name.clone(), *ty);
                    changed = true;
                }
                None => ctx.raise(
                    ErrorEvent::new(
                        Severity::Warn,
                        ErrorScope::Chunk,
                        format!("cast: unknown column '{name}'"),
                    )
                    .at_node(ctx.label.clone())
                    .at_chunk(chunk.meta.id),
                ),
            }
        }
        if !changed {
            return vec![chunk];
        }
        let schema = Arc::new(Schema::new(fields));
        let mut out = Chunk::new(chunk.meta.id, schema, columns);
        out.meta = chunk.meta.clone();
        vec![out]
    }
}

/// `reorder COL [COL ...]` — move the named columns to the front in the given
/// order; the remaining columns follow in their original order. Unknown names
/// are ignored. Stateless, streaming, type/value preserving (a permutation).
struct Reorder {
    cols: Vec<String>,
}

impl Operator for Reorder {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        // Front: the named columns that exist, in request order (dedup so a
        // repeated name doesn't duplicate a column). Then every other column in
        // its original order.
        let mut order: Vec<usize> = Vec::with_capacity(chunk.schema.fields.len());
        for name in &self.cols {
            if let Some(i) = chunk.schema.index_of(name) {
                if !order.contains(&i) {
                    order.push(i);
                }
            }
        }
        for i in 0..chunk.schema.fields.len() {
            if !order.contains(&i) {
                order.push(i);
            }
        }
        // A no-op permutation (already in this order) passes through untouched.
        if order.iter().enumerate().all(|(pos, &i)| pos == i) {
            return vec![chunk];
        }
        let fields: Vec<Field> = order
            .iter()
            .map(|&i| chunk.schema.fields[i].clone())
            .collect();
        let columns: Vec<Column> = order.iter().map(|&i| chunk.columns[i].clone()).collect();
        let schema = Arc::new(Schema::new(fields));
        let mut out = Chunk::new(chunk.meta.id, schema, columns);
        out.meta = chunk.meta.clone();
        vec![out]
    }
}

// -------------------------------------------------------- computed projection

/// `|> field (expr) as alias …` — projection that can compute new columns.
/// Each item is evaluated columnar-style over the chunk (see `eval::eval_column`)
/// and emitted under its output name. Stateless and row-count preserving.
struct ProjectExpr {
    items: Vec<(Expr, String)>,
}

impl Operator for ProjectExpr {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        let mut fields = Vec::with_capacity(self.items.len());
        let mut cols = Vec::with_capacity(self.items.len());
        for (expr, alias) in &self.items {
            // Observe a bare reference to a missing column (continue-first).
            if let Expr::Field { name, .. } = expr {
                if chunk.column(name).is_none() {
                    ctx.raise(
                        ErrorEvent::new(
                            Severity::Warn,
                            ErrorScope::Chunk,
                            format!("project: unknown field '{name}'"),
                        )
                        .at_node(ctx.label.clone())
                        .at_chunk(chunk.meta.id),
                    );
                }
            }
            let col = eval::eval_column(expr, &chunk);
            fields.push(Field::new(alias.clone(), col.dtype()));
            cols.push(col);
        }
        let schema = Arc::new(Schema::new(fields));
        let mut out = Chunk::new(chunk.meta.id, schema, cols);
        out.meta = chunk.meta.clone(); // preserve mode / telemetry
        vec![out]
    }
}

// ------------------------------------------------------------------- project

struct Project {
    fields: Vec<String>,
}

impl Operator for Project {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        match chunk.project(&self.fields) {
            Some(c) => vec![c],
            None => {
                // Missing field: warn and pass through unchanged (continue-first).
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Warn,
                        ErrorScope::Chunk,
                        format!("project: unknown field in {:?}", self.fields),
                    )
                    .at_node(ctx.label.clone())
                    .at_chunk(chunk.meta.id),
                );
                vec![chunk]
            }
        }
    }
}

// ------------------------------------------------------- fused filter+project

/// Optimizer-produced fusion of consecutive filters and an optional trailing
/// projection. Evaluates all predicates (AND) in one row scan, then gathers
/// **only the projected columns** at the surviving indices — a single gather
/// instead of filter-then-project's two, and unused columns are never copied.
struct FilterProject {
    preds: Vec<Expr>,
    fields: Option<Vec<String>>,
}

impl Operator for FilterProject {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        // Vectorized numeric path when the whole conjunction compiles; else the
        // row-wise interpreter (must produce identical results).
        let pred_refs: Vec<&Expr> = self.preds.iter().collect();
        let keep = match kernel::compile(&pred_refs, &chunk) {
            Some(plan) => kernel::run(&plan, &chunk),
            None => (0..chunk.len)
                .filter(|&row| {
                    self.preds
                        .iter()
                        .all(|p| eval::eval_predicate(p, &chunk, row))
                })
                .collect(),
        };
        if keep.is_empty() {
            return Vec::new();
        }

        let Some(fields) = &self.fields else {
            // Pure fused filter (no projection).
            if keep.len() == chunk.len {
                return vec![chunk];
            }
            return vec![chunk.gather(&keep)];
        };

        // Gather only the projected columns at the surviving rows (one pass).
        let mut idx = Vec::with_capacity(fields.len());
        for f in fields {
            match chunk.schema.index_of(f) {
                Some(i) => idx.push(i),
                None => {
                    // Missing field: warn, fall back to keeping all columns.
                    ctx.raise(
                        ErrorEvent::new(
                            Severity::Warn,
                            ErrorScope::Chunk,
                            format!("fused project: unknown field in {fields:?}"),
                        )
                        .at_node(ctx.label.clone())
                        .at_chunk(chunk.meta.id),
                    );
                    return vec![chunk.gather(&keep)];
                }
            }
        }
        let columns: Vec<Column> = idx
            .iter()
            .map(|&i| chunk.columns[i].gather(&keep))
            .collect();
        let schema = Arc::new(Schema::new(
            idx.iter()
                .map(|&i| chunk.schema.fields[i].clone())
                .collect(),
        ));
        let mut out = Chunk::new(chunk.meta.id, schema, columns);
        out.meta = chunk.meta.clone(); // preserve provenance (id, mode, warnings)
        vec![out]
    }
}

// ------------------------------------------------------------------- group by

/// Running accumulator for one aggregate within one group. Carries the
/// aggregate's `func` so it only maintains the state that function needs
/// (numeric moments, a distinct set, or first/last cells).
#[derive(Clone)]
struct AggAcc {
    func: AggFunc,
    sum: f64,
    sum_sq: f64,
    min: f64,
    max: f64,
    n: i64,
    first: Option<String>,
    last: Option<String>,
    distinct: std::collections::HashSet<String>,
    /// Buffered numeric values, only for percentile aggregates (`Pct`). Bounded
    /// by group cardinality, so percentiles are pipeline-breakers like sort.
    values: Vec<f64>,
    /// Exact decimal accumulation (design 21 §21.5): set once a `Value::Dec` is
    /// observed (a decimal column shares one scale). `sum`/`min`/`max` are kept in
    /// `i128` so the result is exact and order-independent — the property that
    /// lets a decimal `sum`/`avg` parallelize byte-identically (#41). `overflow`
    /// degrades that aggregate to the f64 lane (continue-first; §21.7).
    dec_scale: Option<u8>,
    dec_sum: i128,
    dec_min: i128,
    dec_max: i128,
    dec_overflow: bool,
    /// Exact datetime lane (design 23 / #53): set once a `Value::DateTime` is
    /// observed (a column shares one unit). `min`/`max` are kept as exact `i64`
    /// ticks — never `tick as f64` — so they are correct at nanosecond
    /// resolution (ticks past 2^53) and the result keeps the `DateTime` type.
    dt_unit: Option<TimeUnit>,
    dt_min: i64,
    dt_max: i64,
}

/// Extra fractional digits an exact decimal `avg` carries beyond the input scale
/// (the exact `sum/count` quotient is rounded half-even to this scale; §21.5).
const DEC_AVG_EXTRA: u8 = 6;

/// Integer division `num / den` (with `den > 0`) rounded **half-to-even** — the
/// deterministic rounding the exact decimal `avg` shares with the reader, so the
/// quotient is identical regardless of how the (exact) `sum` and `count` were
/// accumulated (serial or parallel partition→merge). `|r|*2` can't overflow:
/// `|r| < den` and `den` is a row count.
fn div_round_half_even(num: i128, den: i128) -> i128 {
    debug_assert!(den > 0);
    let q = num / den;
    let r = num % den;
    let twice = r.abs() * 2;
    // Round up (toward num's sign) when past the half, or exactly at the half with
    // an odd quotient (half-to-even); otherwise keep the truncated quotient.
    if twice > den || (twice == den && q % 2 != 0) {
        q + num.signum()
    } else {
        q
    }
}

impl AggAcc {
    fn new(func: AggFunc) -> Self {
        AggAcc {
            func,
            sum: 0.0,
            sum_sq: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            n: 0,
            first: None,
            last: None,
            distinct: std::collections::HashSet::new(),
            values: Vec::new(),
            dec_scale: None,
            dec_sum: 0,
            dec_min: i128::MAX,
            dec_max: i128::MIN,
            dec_overflow: false,
            dt_unit: None,
            dt_min: i64::MAX,
            dt_max: i64::MIN,
        }
    }

    /// Observe one cell value for this aggregate. Numeric aggregates ignore
    /// non-numeric cells; first/last/count_distinct ignore empty cells.
    fn observe(&mut self, v: &Value) {
        match self.func {
            AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max | AggFunc::Std => {
                if let Some(x) = v.as_f64() {
                    self.sum += x;
                    self.sum_sq += x * x;
                    self.min = self.min.min(x);
                    self.max = self.max.max(x);
                    self.n += 1;
                    // Exact decimal lane: accumulate the unscaled i128 in parallel
                    // with the f64 moments (the f64 side still backs `std` and the
                    // overflow fallback). A column shares one scale.
                    if let Value::Dec(d) = v {
                        let s = *self.dec_scale.get_or_insert(d.scale);
                        // Same-column values share the scale; rescale defensively.
                        let u = if d.scale == s {
                            Some(d.unscaled)
                        } else {
                            d.rescale(s).map(|r| r.unscaled)
                        };
                        match u.and_then(|u| self.dec_sum.checked_add(u).map(|s| (u, s))) {
                            Some((u, sum)) => {
                                self.dec_sum = sum;
                                self.dec_min = self.dec_min.min(u);
                                self.dec_max = self.dec_max.max(u);
                            }
                            None => self.dec_overflow = true,
                        }
                    }
                    // Exact datetime lane: keep min/max as i64 ticks (design 23 /
                    // #53). A column shares one unit. min/max are associative →
                    // byte-identical in parallel; sum/avg stay on the f64 side
                    // (not meaningful instants; not parallel-safe — engine gates).
                    if let Value::DateTime(t) = v {
                        self.dt_unit.get_or_insert(t.unit);
                        self.dt_min = self.dt_min.min(t.ticks);
                        self.dt_max = self.dt_max.max(t.ticks);
                    }
                }
            }
            AggFunc::CountDistinct => {
                let s = v.to_string();
                if !s.is_empty() {
                    self.distinct.insert(s);
                }
            }
            AggFunc::First => {
                if self.first.is_none() {
                    let s = v.to_string();
                    if !s.is_empty() {
                        self.first = Some(s);
                    }
                }
            }
            AggFunc::Last => {
                let s = v.to_string();
                if !s.is_empty() {
                    self.last = Some(s);
                }
            }
            AggFunc::Pct(_) => {
                if let Some(x) = v.as_f64() {
                    self.values.push(x);
                }
            }
        }
    }

    /// Fold another partial accumulator (covering a *later* run of source rows)
    /// into this one — the deterministic merge that lets a group-by run on
    /// per-partition workers and recombine in **source order** (#41). `other`
    /// must be the same `func` and follow `self` in source order (so `first`
    /// keeps the earliest and `last` the latest). Exact lanes (i128 decimal sum,
    /// counts, min/max, buffered percentile values) merge byte-identically; the
    /// f64 moments are folded too but a *parallel* group-by is only enabled when
    /// no aggregate depends on f64 associativity (the engine gates that).
    fn merge(&mut self, other: &AggAcc) {
        self.sum += other.sum;
        self.sum_sq += other.sum_sq;
        self.n += other.n;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        // Exact decimal lane (associative i128); a column shares one scale.
        if let Some(os) = other.dec_scale {
            let scale = *self.dec_scale.get_or_insert(os);
            let ou = if os == scale {
                Some(other.dec_sum)
            } else {
                None
            };
            match ou.and_then(|ou| self.dec_sum.checked_add(ou)) {
                Some(s) => self.dec_sum = s,
                None => self.dec_overflow = true,
            }
            self.dec_min = self.dec_min.min(other.dec_min);
            self.dec_max = self.dec_max.max(other.dec_max);
        }
        self.dec_overflow |= other.dec_overflow;
        // Exact datetime lane (associative i64); a column shares one unit.
        if let Some(ou) = other.dt_unit {
            self.dt_unit.get_or_insert(ou);
            self.dt_min = self.dt_min.min(other.dt_min);
            self.dt_max = self.dt_max.max(other.dt_max);
        }
        for s in &other.distinct {
            self.distinct.insert(s.clone());
        }
        // Source order: `self` precedes `other`, so the earliest non-empty
        // `first` and the latest non-empty `last` win.
        if self.first.is_none() {
            self.first = other.first.clone();
        }
        if other.last.is_some() {
            self.last = other.last.clone();
        }
        self.values.extend_from_slice(&other.values);
    }

    /// Numeric aggregate value (sum/avg/min/max/std). `0.0` for an empty group.
    fn num_value(&self) -> f64 {
        match self.func {
            AggFunc::Sum => self.sum,
            AggFunc::Avg => {
                if self.n > 0 {
                    self.sum / self.n as f64
                } else {
                    0.0
                }
            }
            AggFunc::Min => {
                if self.n > 0 {
                    self.min
                } else {
                    0.0
                }
            }
            AggFunc::Max => {
                if self.n > 0 {
                    self.max
                } else {
                    0.0
                }
            }
            // ddof=1 sample std needs ≥2 values; otherwise it falls to `_ => 0.0`.
            AggFunc::Std if self.n > 1 => {
                // Sample standard deviation (ddof=1): √((Σx² − Σx·mean)/(n−1)).
                let mean = self.sum / self.n as f64;
                let var = (self.sum_sq - self.sum * mean) / (self.n as f64 - 1.0);
                var.max(0.0).sqrt()
            }
            AggFunc::Pct(p) => self.percentile(p),
            _ => 0.0,
        }
    }

    /// Linear-interpolated percentile of the buffered values (numpy/pandas
    /// default: rank = p/100·(n−1), interpolate between the two nearest order
    /// statistics). `0.0` for an empty group. Sorts a clone, so the accumulator
    /// stays reusable; the buffer is bounded by group cardinality.
    fn percentile(&self, p: u8) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }
        let mut v = self.values.clone();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        if v.len() == 1 {
            return v[0];
        }
        let rank = (p as f64 / 100.0) * (v.len() - 1) as f64;
        let lo = rank.floor() as usize;
        let hi = rank.ceil() as usize;
        let frac = rank - lo as f64;
        v[lo] + (v[hi] - v[lo]) * frac
    }

    /// Exact decimal result for `sum`/`min`/`max`/`avg` on a decimal column, or
    /// `None` when this aggregate isn't an exact-decimal one (then the caller uses
    /// the f64 `num_value`). `avg` rounds the exact `sum/count` quotient half-even
    /// to `scale + DEC_AVG_EXTRA`; an i128 overflow leaves it to the f64 fallback.
    fn dec_value(&self) -> Option<rivus_core::Decimal> {
        let scale = self.dec_scale?;
        if self.dec_overflow {
            return None;
        }
        match self.func {
            AggFunc::Sum => Some(rivus_core::Decimal::new(self.dec_sum, scale)),
            AggFunc::Min if self.n > 0 => Some(rivus_core::Decimal::new(self.dec_min, scale)),
            AggFunc::Max if self.n > 0 => Some(rivus_core::Decimal::new(self.dec_max, scale)),
            AggFunc::Avg if self.n > 0 => {
                let out_scale = scale.saturating_add(DEC_AVG_EXTRA);
                let mut factor: i128 = 1;
                for _ in 0..(out_scale - scale) {
                    factor = factor.checked_mul(10)?;
                }
                let num = self.dec_sum.checked_mul(factor)?;
                Some(rivus_core::Decimal::new(
                    div_round_half_even(num, self.n as i128),
                    out_scale,
                ))
            }
            _ => None,
        }
    }

    /// Exact datetime result for `min`/`max` on a datetime column, or `None`
    /// when this aggregate isn't an exact-datetime `min`/`max` (then the caller
    /// uses the f64 `num_value`). Keeps the `i64` ticks and the column's unit, so
    /// the result is exact at any resolution and stays the `DateTime` type. #53.
    fn dt_value(&self) -> Option<DateTime> {
        let unit = self.dt_unit?;
        match self.func {
            AggFunc::Min if self.n > 0 => Some(DateTime::new(self.dt_min, unit)),
            AggFunc::Max if self.n > 0 => Some(DateTime::new(self.dt_max, unit)),
            _ => None,
        }
    }

    fn distinct_count(&self) -> i64 {
        self.distinct.len() as i64
    }
    fn first_str(&self) -> &str {
        self.first.as_deref().unwrap_or("")
    }
    fn last_str(&self) -> &str {
        self.last.as_deref().unwrap_or("")
    }
}

struct GroupState {
    /// The group's key values, one per group key (in key order). Stored so the
    /// output can emit one column per key (the map key is a packed composite).
    key_parts: Vec<String>,
    count: i64,
    accs: Vec<AggAcc>,
}

pub(crate) struct GroupBy {
    keys: Vec<String>,
    aggs: Vec<(AggFunc, String)>,
    groups: BTreeMap<String, GroupState>,
    emitted: bool,
}

impl GroupBy {
    fn new(keys: Vec<String>, aggs: Vec<(AggFunc, String)>) -> Self {
        GroupBy {
            keys,
            aggs,
            groups: BTreeMap::new(),
            emitted: false,
        }
    }

    /// Fold a *later* partition's partial group state into this one (the
    /// deterministic, source-ordered merge for parallel group-by; #41). Groups
    /// present only in `other` are appended (BTreeMap keeps key order, so the
    /// output row order is identical to a serial run); shared groups merge their
    /// counts and per-aggregate accumulators via [`AggAcc::merge`]. `other` must
    /// have the same keys and aggregates and follow `self` in source order.
    pub(crate) fn merge_from(&mut self, other: GroupBy) {
        for (key, ostate) in other.groups {
            match self.groups.get_mut(&key) {
                Some(s) => {
                    s.count += ostate.count;
                    for (a, oa) in s.accs.iter_mut().zip(ostate.accs.iter()) {
                        a.merge(oa);
                    }
                }
                None => {
                    self.groups.insert(key, ostate);
                }
            }
        }
    }
}

/// Whether a group-by over these aggregates is **byte-identical** under a
/// partition→merge (parallel) execution, given the resolved type of each
/// aggregated column (#41). `min`/`max`/`count`/`count_distinct`/`first`/`last`/
/// percentile are always safe (associative or buffered+sorted); `sum`/`avg` are
/// safe only on an exact lane (decimal — i128 associative); `std` and `sum`/`avg`
/// on f64/integer columns are NOT (f64 addition is non-associative; integer sum
/// rides the f64 accumulator) and keep the serial path.
pub(crate) fn group_parallel_safe(
    aggs: &[(AggFunc, String)],
    col_type: impl Fn(&str) -> Option<DataType>,
) -> bool {
    aggs.iter().all(|(f, col)| match f {
        AggFunc::Min
        | AggFunc::Max
        | AggFunc::CountDistinct
        | AggFunc::First
        | AggFunc::Last
        | AggFunc::Pct(_) => true,
        AggFunc::Sum | AggFunc::Avg => {
            matches!(col_type(col), Some(DataType::Decimal { .. }))
        }
        AggFunc::Std => false,
    })
}

/// Build a `GroupBy` operator from a `GroupBy` op (for the parallel scheduler,
/// which needs the concrete type to merge per-worker state). `None` for any
/// other op.
pub(crate) fn new_group(op: &Op) -> Option<GroupBy> {
    match op {
        Op::GroupBy { keys, aggs } => Some(GroupBy::new(keys.clone(), aggs.clone())),
        _ => None,
    }
}

impl Operator for GroupBy {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        // Resolve every group-key column index; an unknown key warns once and
        // drops the chunk (continue-first — a later, well-formed chunk still
        // aggregates).
        let mut key_idx = Vec::with_capacity(self.keys.len());
        for k in &self.keys {
            match chunk.schema.index_of(k) {
                Some(i) => key_idx.push(i),
                None => {
                    ctx.raise(
                        ErrorEvent::new(
                            Severity::Warn,
                            ErrorScope::Chunk,
                            format!("group: unknown key '{k}'"),
                        )
                        .at_node(ctx.label.clone()),
                    );
                    return Vec::new();
                }
            }
        }
        // Resolve aggregate column indices once per chunk.
        let agg_idx: Vec<Option<usize>> = self
            .aggs
            .iter()
            .map(|(_, c)| chunk.schema.index_of(c))
            .collect();
        // The aggregate funcs, copied out so the group-insert closure doesn't
        // borrow `self.aggs` while `self.groups` is mutably borrowed.
        let funcs: Vec<AggFunc> = self.aggs.iter().map(|(f, _)| *f).collect();

        for row in 0..chunk.len {
            // Composite map key: the key values joined by the ASCII unit
            // separator (0x1F), which can't appear in a parsed CSV field, so
            // distinct key tuples never collide. The parts are kept on the state
            // for output.
            let parts: Vec<String> = key_idx
                .iter()
                .map(|&i| chunk.value(row, i).to_string())
                .collect();
            let composite = parts.join("\u{1f}");
            let state = self.groups.entry(composite).or_insert_with(|| GroupState {
                key_parts: parts,
                count: 0,
                accs: funcs.iter().map(|f| AggAcc::new(*f)).collect(),
            });
            state.count += 1;
            for (j, idx) in agg_idx.iter().enumerate() {
                if let Some(ci) = idx {
                    let v = chunk.value(row, *ci);
                    state.accs[j].observe(&v);
                }
            }
        }
        Vec::new() // group is a materializing boundary; output on finish
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.emitted {
            return Vec::new();
        }
        self.emitted = true;

        // One Str column per group key (values pulled from each group's stored
        // key parts), then the count, then the aggregate columns.
        let mut fields: Vec<Field> = self
            .keys
            .iter()
            .map(|k| Field::new(k.clone(), DataType::Str))
            .collect();
        fields.push(Field::new("count", DataType::I64));

        let mut columns: Vec<Column> = Vec::with_capacity(self.keys.len() + 1 + self.aggs.len());
        for ki in 0..self.keys.len() {
            let col: StrColumn = self
                .groups
                .values()
                .map(|s| s.key_parts[ki].as_str())
                .collect();
            columns.push(Column::Str(col));
        }
        let counts: Vec<i64> = self.groups.values().map(|s| s.count).collect();
        columns.push(Column::I64(counts));

        for (j, (func, col)) in self.aggs.iter().enumerate() {
            let name = format!("{}_{}", func.label(), col);
            let (dtype, column) = match func {
                AggFunc::CountDistinct => (
                    DataType::I64,
                    Column::I64(
                        self.groups
                            .values()
                            .map(|s| s.accs[j].distinct_count())
                            .collect(),
                    ),
                ),
                AggFunc::First | AggFunc::Last => {
                    let mut sc = StrColumn::default();
                    for s in self.groups.values() {
                        let cell = if matches!(func, AggFunc::First) {
                            s.accs[j].first_str()
                        } else {
                            s.accs[j].last_str()
                        };
                        sc.push(cell);
                    }
                    (DataType::Str, Column::Str(sc))
                }
                // sum/avg/min/max/std/pct. On a decimal column these stay exact
                // (i128) when every group produced an exact result; if any group
                // overflowed i128 the whole column degrades to f64 (continue-first,
                // §21.7) so the column stays one uniform type.
                _ => {
                    // Exact datetime min/max → keep the DateTime lane (i64 ticks,
                    // same unit), never an f64 column. #53.
                    let dt_ok = matches!(func, AggFunc::Min | AggFunc::Max)
                        && !self.groups.is_empty()
                        && self.groups.values().all(|s| s.accs[j].dt_value().is_some());
                    if dt_ok {
                        let dts: Vec<DateTime> = self
                            .groups
                            .values()
                            .map(|s| s.accs[j].dt_value().unwrap())
                            .collect();
                        let unit = dts[0].unit;
                        let ticks = dts.iter().map(|d| d.ticks).collect();
                        fields.push(Field::new(name, DataType::DateTime { unit }));
                        columns.push(Column::DateTime(DtColumn { ticks, unit }));
                        continue;
                    }
                    let dec_ok = matches!(
                        func,
                        AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max
                    ) && !self.groups.is_empty()
                        && self
                            .groups
                            .values()
                            .all(|s| s.accs[j].dec_value().is_some());
                    if dec_ok {
                        let decs: Vec<rivus_core::Decimal> = self
                            .groups
                            .values()
                            .map(|s| s.accs[j].dec_value().unwrap())
                            .collect();
                        // All groups share the column's scale (sum/min/max) or
                        // scale+extra (avg), so the output scale is uniform.
                        let scale = decs[0].scale;
                        let unscaled = decs.iter().map(|d| d.unscaled).collect();
                        (
                            DataType::Decimal { scale },
                            Column::Dec(rivus_core::DecColumn { unscaled, scale }),
                        )
                    } else {
                        (
                            DataType::F64,
                            Column::F64(
                                self.groups
                                    .values()
                                    .map(|s| s.accs[j].num_value())
                                    .collect(),
                            ),
                        )
                    }
                }
            };
            fields.push(Field::new(name, dtype));
            columns.push(column);
        }

        let id = ctx.fresh_id();
        vec![Chunk::new(id, Arc::new(Schema::new(fields)), columns)]
    }
}

// ---------------------------------------------------------------------- merge

/// Identity forwarder. Used for `+` merge (n inputs, one output) and as the
/// structural pass-through at a `->` branch point.
struct Merge;

impl Operator for Merge {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        vec![chunk]
    }
}

// ----------------------------------------------------------------------- join

/// Inner hash join `A & B on lkey:rkey`. Buffers both inputs (a blocking,
/// serial pipeline-breaker like sort/group), builds a hash map of the right
/// side keyed by `right_key`, then probes with the left side. The output is the
/// left columns followed by the right columns (minus the join key); a name that
/// collides with a left column is suffixed `_r`. Keys compare by string value,
/// so `30` (i64) and `"30"` (str) match — convenient for loosely-typed CSV.
struct Join {
    left_keys: Vec<String>,
    right_keys: Vec<String>,
    kind: JoinKind,
    left_id: NodeId,
    left_buf: Vec<Chunk>,
    right_buf: Vec<Chunk>,
}

impl Join {
    fn new(
        left_keys: Vec<String>,
        right_keys: Vec<String>,
        kind: JoinKind,
        left_id: NodeId,
    ) -> Self {
        Join {
            left_keys,
            right_keys,
            kind,
            left_id,
            left_buf: Vec::new(),
            right_buf: Vec::new(),
        }
    }
}

/// A row's composite join key: the values at `idxs` joined by the ASCII unit
/// separator (`0x1F`, which can't appear in a parsed CSV field), so distinct key
/// tuples never collide.
fn join_key_at(chunk: &Chunk, idxs: &[usize], row: usize) -> String {
    let mut s = String::new();
    for (n, &ci) in idxs.iter().enumerate() {
        if n > 0 {
            s.push('\u{1f}');
        }
        s.push_str(&chunk.value(row, ci).to_string());
    }
    s
}

/// Concatenate buffered chunks (sharing a schema) into one.
fn concat_chunks(bufs: Vec<Chunk>) -> Option<Chunk> {
    let mut it = bufs.into_iter();
    let first = it.next()?;
    let schema = first.schema.clone();
    let mut cols = first.columns;
    for c in it {
        for (i, col) in c.columns.iter().enumerate() {
            cols[i].append(col);
        }
    }
    Some(Chunk::new(0, schema, cols))
}

impl Join {
    /// Emit one side unchanged (its own schema) — used when the other side has
    /// no rows at all and this join kind keeps the present side.
    fn pass_through(&self, ctx: &mut OpCtx, side: &Chunk) -> Chunk {
        let idx: Vec<usize> = (0..side.len).collect();
        let cols: Vec<Column> = side.columns.iter().map(|c| c.gather(&idx)).collect();
        Chunk::new(ctx.fresh_id(), side.schema.clone(), cols)
    }
}

impl Operator for Join {
    fn process(&mut self, from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        if from == self.left_id {
            self.left_buf.push(chunk);
        } else {
            self.right_buf.push(chunk);
        }
        Vec::new() // blocking: join emitted on finish
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        let left = concat_chunks(std::mem::take(&mut self.left_buf));
        let right = concat_chunks(std::mem::take(&mut self.right_buf));

        // One side entirely absent (no chunks). With no schema to pad against we
        // can only emit the *present* side, and only when this kind keeps it.
        let (left, right) = match (left, right) {
            (Some(l), Some(r)) => (l, r),
            (Some(l), None) => {
                return if self.kind.keeps_left() {
                    vec![self.pass_through(ctx, &l)]
                } else {
                    Vec::new()
                };
            }
            (None, Some(r)) => {
                return if self.kind.keeps_right() {
                    vec![self.pass_through(ctx, &r)]
                } else {
                    Vec::new()
                };
            }
            (None, None) => return Vec::new(),
        };

        let warn = |ctx: &mut OpCtx, side: &str, key: &str| {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Warn,
                    ErrorScope::Branch,
                    format!("join: unknown {side} key '{key}'"),
                )
                .at_node(ctx.label.clone()),
            );
        };
        // Resolve each key column on both sides (composite key, in key order).
        let mut lk = Vec::with_capacity(self.left_keys.len());
        for k in &self.left_keys {
            match left.schema.index_of(k) {
                Some(i) => lk.push(i),
                None => {
                    warn(ctx, "left", k);
                    return Vec::new();
                }
            }
        }
        let mut rk = Vec::with_capacity(self.right_keys.len());
        for k in &self.right_keys {
            match right.schema.index_of(k) {
                Some(i) => rk.push(i),
                None => {
                    warn(ctx, "right", k);
                    return Vec::new();
                }
            }
        }

        // Build the hash table on the right side, then probe with the left.
        // Each output row is a `(Option<left>, Option<right>)` pair: an unmatched
        // left row (left/full) has `None` on the right and pads the right columns
        // with defaults; an unmatched right row (right/full) has `None` on the
        // left and pads the left columns — except the join-key columns, which
        // take the right key so the key is never lost.
        let mut table: HashMap<String, Vec<usize>> = HashMap::new();
        for ri in 0..right.len {
            table
                .entry(join_key_at(&right, &rk, ri))
                .or_default()
                .push(ri);
        }
        let mut right_matched = vec![false; right.len];
        let mut lidx: Vec<Option<usize>> = Vec::new();
        let mut ridx: Vec<Option<usize>> = Vec::new();
        for li in 0..left.len {
            match table.get(&join_key_at(&left, &lk, li)) {
                Some(rs) => {
                    for &ri in rs {
                        right_matched[ri] = true;
                        lidx.push(Some(li));
                        ridx.push(Some(ri));
                    }
                }
                None if self.kind.keeps_left() => {
                    lidx.push(Some(li));
                    ridx.push(None);
                }
                None => {}
            }
        }
        // Right/full: append the right rows that no left row matched.
        if self.kind.keeps_right() {
            for (ri, matched) in right_matched.iter().enumerate() {
                if !*matched {
                    lidx.push(None);
                    ridx.push(Some(ri));
                }
            }
        }

        // Output schema: left fields, then right fields except the join keys
        // (collisions suffixed `_r`). The right key columns are dropped (the
        // left key column carries the value).
        let mut fields = left.schema.fields.clone();
        let mut right_cols = Vec::new();
        for (ci, f) in right.schema.fields.iter().enumerate() {
            if rk.contains(&ci) {
                continue;
            }
            let name = if left.schema.index_of(&f.name).is_some() {
                format!("{}_r", f.name)
            } else {
                f.name.clone()
            };
            fields.push(Field::new(name, f.dtype));
            right_cols.push(ci);
        }

        // Left columns: gather by `lidx`. A join-key column borrows the matching
        // right key when the left side is absent (key-preservation for
        // right/full joins); a non-key left column pads with the type default.
        let mut out: Vec<Column> = Vec::with_capacity(fields.len());
        for (ci, col) in left.columns.iter().enumerate() {
            match lk.iter().position(|&k| k == ci) {
                Some(kpos) => {
                    out.push(join_key_column(col, &lidx, &ridx, &right.columns[rk[kpos]]))
                }
                None => out.push(col.gather_opt(&lidx)),
            }
        }
        for &ci in &right_cols {
            out.push(right.columns[ci].gather_opt(&ridx));
        }
        vec![Chunk::new(
            ctx.fresh_id(),
            Arc::new(Schema::new(fields)),
            out,
        )]
    }
}

/// Build the output join-key column. For a matched/left-present row it takes the
/// left key (`lidx`); for an unmatched-right row (`lidx == None`) it takes the
/// right key (`ridx`), so a right/full join never drops the key value. Falls
/// back to the left column's lane, widening to text only if the right key's
/// string form can't be represented there.
fn join_key_column(
    left_key: &Column,
    lidx: &[Option<usize>],
    ridx: &[Option<usize>],
    right_key: &Column,
) -> Column {
    // Fast path: every row has a left value → a plain gather_opt suffices.
    if lidx.iter().all(|o| o.is_some()) {
        return left_key.gather_opt(lidx);
    }
    // Mixed: assemble values, taking the right key when the left is absent.
    let vals: Vec<rivus_core::Value> = lidx
        .iter()
        .zip(ridx)
        .map(|(l, r)| match (l, r) {
            (Some(i), _) => left_key.value_at(*i),
            (None, Some(j)) => right_key.value_at(*j),
            (None, None) => rivus_core::Value::Str(String::new()),
        })
        .collect();
    eval::column_from_values(vals)
}

// ----------------------------------------------------------------- sink: print

struct SinkPrint;

impl Operator for SinkPrint {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        // Forward so the engine captures it as a leaf for display.
        vec![chunk]
    }
}

// ------------------------------------------------------------------ sink: csv

/// Streaming CSV sink: writes the header on the first chunk and appends rows as
/// chunks arrive (bounded memory), so `open big.csv |? … save out.csv` never
/// buffers the whole output.
struct SinkCsv {
    w: StreamWriter,
    delim: u8,
}

impl SinkCsv {
    fn new(path: String, delim: u8) -> Self {
        SinkCsv {
            w: StreamWriter::new(path),
            delim,
        }
    }

    fn write_chunk(&mut self, chunk: &Chunk) -> std::io::Result<()> {
        let need_header = !self.w.wrote_header;
        let sep = self.delim as char;
        let delim = self.delim;
        {
            let w = self.w.writer()?;
            if need_header {
                writeln!(w, "{}", chunk.schema.field_names().join(&sep.to_string()))?;
            }
            let mut line = String::new();
            for row in 0..chunk.len {
                line.clear();
                for c in 0..chunk.columns.len() {
                    if c > 0 {
                        line.push(sep);
                    }
                    write_cell(&mut line, &chunk.columns[c], row, delim);
                }
                writeln!(w, "{line}")?;
            }
        }
        self.w.wrote_header = true;
        Ok(())
    }
}

impl Operator for SinkCsv {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.w.failed {
            return Vec::new();
        }
        if let Err(e) = self.write_chunk(&chunk) {
            self.w.failed = true;
            ctx.raise(
                ErrorEvent::new(
                    Severity::Critical,
                    ErrorScope::Graph,
                    format!("cannot write '{}': {e}", self.w.path),
                )
                .at_node(ctx.label.clone()),
            );
        }
        Vec::new()
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        if !self.w.failed {
            if let Err(e) = self.w.finish() {
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Critical,
                        ErrorScope::Graph,
                        format!("cannot write '{}': {e}", self.w.path),
                    )
                    .at_node(ctx.label.clone()),
                );
            }
        }
        Vec::new()
    }
}

/// Render `chunks` (sharing a schema) to a CSV file: a header line then rows.
/// Shared by the serial `SinkCsv` and the parallel executor's single-write merge.
pub fn write_csv_file(path: &str, chunks: &[Chunk], delim: u8) -> std::io::Result<()> {
    let Some(first) = chunks.first() else {
        return write_output(path, "");
    };
    // Stream to the writer (bounded memory — only one reused line buffer), instead
    // of building the whole output in one String, and format each cell straight
    // from its column lane (`write_cell`).
    let sink: Box<dyn Write> = if path == "-" {
        Box::new(std::io::stdout())
    } else {
        Box::new(std::fs::File::create(path)?)
    };
    let mut w = BufWriter::with_capacity(256 * 1024, sink);
    let sep = delim as char;
    writeln!(w, "{}", first.schema.field_names().join(&sep.to_string()))?;
    let mut line = String::new();
    for chunk in chunks {
        for row in 0..chunk.len {
            line.clear();
            for c in 0..chunk.columns.len() {
                if c > 0 {
                    line.push(sep);
                }
                write_cell(&mut line, &chunk.columns[c], row, delim);
            }
            writeln!(w, "{line}")?;
        }
    }
    w.flush()
}

/// Append one CSV cell, formatted **directly from its typed column lane** into
/// `line` — no per-cell `Value`/`String` allocation (the hot path on a wide write
/// previously did two allocations per cell: `value()` then an escaped `String`).
/// Byte-identical
/// to that: numeric/bool/decimal lanes never contain the delimiter, `"`, or a
/// newline so they are written verbatim; only a string cell that does is quoted
/// with `"` doubled.
fn write_cell(line: &mut String, col: &Column, row: usize, delim: u8) {
    use std::fmt::Write as _;
    match col {
        Column::I64(v) => {
            let _ = write!(line, "{}", v[row]);
        }
        Column::F64(v) => {
            let _ = write!(line, "{}", v[row]);
        }
        Column::Bool(v) => line.push_str(if v[row] { "true" } else { "false" }),
        Column::Dec(d) => {
            let _ = write!(
                line,
                "{}",
                rivus_core::Decimal::new(d.unscaled[row], d.scale)
            );
        }
        Column::DateTime(d) => {
            let _ = write!(line, "{}", rivus_core::DateTime::new(d.ticks[row], d.unit));
        }
        Column::Duration(d) => {
            let _ = write!(line, "{}", rivus_core::Duration::new(d.ticks[row], d.unit));
        }
        Column::Str(s) => {
            let cell = s.get(row);
            if cell.bytes().any(|b| b == delim) || cell.contains('"') || cell.contains('\n') {
                line.push('"');
                for ch in cell.chars() {
                    if ch == '"' {
                        line.push_str("\"\"");
                    } else {
                        line.push(ch);
                    }
                }
                line.push('"');
            } else {
                line.push_str(cell);
            }
        }
    }
}

// ----------------------------------------------------------------- sink: jsonl

/// Writes JSON Lines (one object per row), mirroring the JSONL source so a flow
/// can read and write the same format. Buffered, written on finish.
/// Streaming JSONL sink: appends one JSON object per row as chunks arrive.
struct SinkJsonl {
    w: StreamWriter,
}

impl SinkJsonl {
    fn new(path: String) -> Self {
        SinkJsonl {
            w: StreamWriter::new(path),
        }
    }

    fn write_chunk(&mut self, chunk: &Chunk) -> std::io::Result<()> {
        let w = self.w.writer()?;
        let names = chunk.schema.field_names();
        let mut out = String::new();
        for row in 0..chunk.len {
            out.clear();
            out.push('{');
            for (c, name) in names.iter().enumerate() {
                if c > 0 {
                    out.push(',');
                }
                json_string(&mut out, name);
                out.push(':');
                write_json_cell(&mut out, &chunk.columns[c], row);
            }
            out.push('}');
            writeln!(w, "{out}")?;
        }
        Ok(())
    }
}

impl Operator for SinkJsonl {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.w.failed {
            return Vec::new();
        }
        if let Err(e) = self.write_chunk(&chunk) {
            self.w.failed = true;
            ctx.raise(
                ErrorEvent::new(
                    Severity::Critical,
                    ErrorScope::Graph,
                    format!("cannot write '{}': {e}", self.w.path),
                )
                .at_node(ctx.label.clone()),
            );
        }
        Vec::new()
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        if !self.w.failed {
            if let Err(e) = self.w.finish() {
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Critical,
                        ErrorScope::Graph,
                        format!("cannot write '{}': {e}", self.w.path),
                    )
                    .at_node(ctx.label.clone()),
                );
            }
        }
        Vec::new()
    }
}

/// Render `chunks` as JSON Lines (one object per row). Shared by the serial
/// `SinkJsonl` and the parallel executor's single-write merge.
pub fn write_jsonl_file(path: &str, chunks: &[Chunk]) -> std::io::Result<()> {
    // Stream (bounded memory — one reused object buffer) and format each cell
    // straight from its column lane (`json_object_row` → `write_json_cell`).
    let sink: Box<dyn Write> = if path == "-" {
        Box::new(std::io::stdout())
    } else {
        Box::new(std::fs::File::create(path)?)
    };
    let mut w = BufWriter::with_capacity(256 * 1024, sink);
    let mut out = String::new();
    for chunk in chunks {
        let names = chunk.schema.field_names();
        for row in 0..chunk.len {
            out.clear();
            json_object_row(&mut out, chunk, &names, row);
            writeln!(w, "{out}")?;
        }
    }
    w.flush()
}

/// Append one row as a JSON object to `out`.
fn json_object_row(out: &mut String, chunk: &Chunk, names: &[&str], row: usize) {
    out.push('{');
    for (c, name) in names.iter().enumerate() {
        if c > 0 {
            out.push(',');
        }
        json_string(out, name);
        out.push(':');
        write_json_cell(out, &chunk.columns[c], row);
    }
    out.push('}');
}

/// Streaming sink for a single JSON **array** (`[{…},{…}]`). Writes the opening
/// `[` on the first row, comma-separates rows across chunks (bounded memory),
/// and closes with `]` on finish. `wrote_header` doubles as "array opened".
struct SinkJson {
    w: StreamWriter,
}

impl SinkJson {
    fn new(path: String) -> Self {
        SinkJson {
            w: StreamWriter::new(path),
        }
    }

    fn write_chunk(&mut self, chunk: &Chunk) -> std::io::Result<()> {
        // Build the chunk's fragment first (no `self.w` borrow), then write it.
        let mut opened = self.w.wrote_header;
        let names = chunk.schema.field_names();
        let mut out = String::new();
        if !opened {
            out.push('['); // open the array on the very first write
        }
        for row in 0..chunk.len {
            // A comma precedes every row except the very first of the array.
            if opened {
                out.push(',');
            } else {
                opened = true;
            }
            json_object_row(&mut out, chunk, &names, row);
        }
        let w = self.w.writer()?;
        write!(w, "{out}")?;
        self.w.wrote_header = opened;
        Ok(())
    }
}

impl Operator for SinkJson {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.w.failed {
            return Vec::new();
        }
        if let Err(e) = self.write_chunk(&chunk) {
            self.w.failed = true;
            ctx.raise(
                ErrorEvent::new(
                    Severity::Critical,
                    ErrorScope::Graph,
                    format!("cannot write '{}': {e}", self.w.path),
                )
                .at_node(ctx.label.clone()),
            );
        }
        Vec::new()
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.w.failed {
            return Vec::new();
        }
        // Close the array. If no row ever arrived, emit `[]`.
        let opened = self.w.wrote_header;
        let res = (|| {
            let w = self.w.writer()?;
            if !opened {
                write!(w, "[")?;
            }
            writeln!(w, "]")
        })();
        let res = res.and_then(|_| self.w.finish());
        if let Err(e) = res {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Critical,
                    ErrorScope::Graph,
                    format!("cannot write '{}': {e}", self.w.path),
                )
                .at_node(ctx.label.clone()),
            );
        }
        Vec::new()
    }
}

/// Render `chunks` as a single JSON array. Shared by the parallel executor's
/// single-write merge (the serial sink streams it incrementally instead).
pub fn write_json_file(path: &str, chunks: &[Chunk]) -> std::io::Result<()> {
    let mut out = String::from("[");
    let mut first = true;
    for chunk in chunks {
        let names = chunk.schema.field_names();
        for row in 0..chunk.len {
            if first {
                first = false;
            } else {
                out.push(',');
            }
            json_object_row(&mut out, chunk, &names, row);
        }
    }
    out.push_str("]\n");
    write_output(path, &out)
}

/// Encode a JSON value from a Rivus scalar.
/// Append one JSON value formatted **straight from its typed column lane** into
/// `out` — no per-cell `Value` materialization (cloning string cells) and no temp
/// `to_string` allocation, but identical output to the per-`Value` formatter.
fn write_json_cell(out: &mut String, col: &Column, row: usize) {
    use std::fmt::Write as _;
    match col {
        Column::I64(v) => {
            let _ = write!(out, "{}", v[row]);
        }
        // JSON has no NaN/Infinity → emit null (continue-first), matching json_value.
        Column::F64(v) => {
            if v[row].is_finite() {
                let _ = write!(out, "{}", v[row]);
            } else {
                out.push_str("null");
            }
        }
        Column::Bool(v) => out.push_str(if v[row] { "true" } else { "false" }),
        Column::Dec(d) => {
            let _ = write!(
                out,
                "{}",
                rivus_core::Decimal::new(d.unscaled[row], d.scale)
            );
        }
        // Datetime has no JSON literal form → emit a quoted ISO-8601 string.
        Column::DateTime(d) => json_string(
            out,
            &rivus_core::DateTime::new(d.ticks[row], d.unit).to_string(),
        ),
        // Duration likewise → quoted human-readable string (#57).
        Column::Duration(d) => json_string(
            out,
            &rivus_core::Duration::new(d.ticks[row], d.unit).to_string(),
        ),
        Column::Str(s) => json_string(out, s.get(row)),
    }
}

/// Append a JSON-escaped string (with quotes) to `out`.
fn json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

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
