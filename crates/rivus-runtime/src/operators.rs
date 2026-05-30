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
    Chunk, Column, DataType, ErrorEvent, ErrorScope, Field, Schema, Severity, StrColumn, Value,
};
use rivus_ir::{AggFunc, BinType, CmpOp, Endian, Expr, NodeId, Op};
use std::collections::BTreeMap;
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

/// A streaming CSV source over one byte range `[start, end)` of a file, used by
/// the parallel streaming executor. The global schema/types are pre-inferred
/// (see [`csv::plan_parallel`]); on open error it yields nothing (continue-first
/// — the worker simply contributes no rows).
#[allow(clippy::too_many_arguments)]
pub fn csv_range_source(
    path: &str,
    dtypes: Vec<rivus_core::DataType>,
    keep: Vec<usize>,
    ncols: usize,
    schema: Arc<Schema>,
    start: u64,
    end: u64,
    chunk_size: usize,
    prefilter: Vec<(usize, CmpOp, f64)>,
) -> Box<dyn Operator> {
    match csv::CsvChunker::for_range(path, dtypes, keep, ncols, start, end, chunk_size, prefilter) {
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
            header,
        } => Box::new(SourceCsv::new(
            path.clone(),
            projection.clone(),
            chunk_size,
            preview,
            prefilter.clone(),
            *header,
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
        Op::Sort { key, desc } => Box::new(Sort::new(key.clone(), *desc)),
        Op::Distinct { keys } => Box::new(Distinct::new(keys.clone())),
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
        Op::GroupBy { key, aggs } => Box::new(GroupBy::new(key.clone(), aggs.clone())),
        Op::Merge => Box::new(Merge),
        Op::Branch => Box::new(Merge), // identity forwarder; fan-out is structural
        Op::Join {
            left_key,
            right_key,
        } => Box::new(Join::new(
            left_key.clone(),
            right_key.clone(),
            inputs.first().copied().unwrap_or(usize::MAX),
        )),
        Op::SinkPrint => Box::new(SinkPrint),
        Op::SinkCsv { path } => Box::new(SinkCsv::new(path.clone())),
        Op::SinkJsonl { path } => Box::new(SinkJsonl::new(path.clone())),
    }
}

/// A streaming CSV sink to `path` (used by the parallel executor to write a
/// worker's byte-range partition to a part file).
pub fn csv_sink(path: String) -> Box<dyn Operator> {
    Box::new(SinkCsv::new(path))
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
    header: bool,
    schema: Arc<Schema>,
    /// Streaming reader for a real file; `None` for stdin / after a load error.
    stream: Option<csv::CsvChunker>,
    /// Buffered fallback (stdin): pre-parsed columns sliced by `pull`.
    columns: Vec<Column>,
    cursor: usize,
    total: usize,
}

impl SourceCsv {
    fn new(
        path: String,
        projection: Option<Vec<String>>,
        chunk_size: usize,
        preview: bool,
        prefilter: Vec<(String, CmpOp, f64)>,
        header: bool,
    ) -> Self {
        SourceCsv {
            path,
            projection,
            chunk_size: chunk_size.max(1),
            loaded: false,
            preview,
            prefilter,
            header,
            schema: Schema::empty(),
            stream: None,
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
            header: true,
            schema,
            stream: Some(chunker),
            columns: Vec::new(),
            cursor: 0,
            total: 0,
        }
    }

    fn load(&mut self, ctx: &mut OpCtx) {
        self.loaded = true;
        if self.path == "-" {
            self.load_stdin(ctx);
        } else {
            match csv::CsvChunker::open(
                &self.path,
                self.projection.as_deref(),
                self.chunk_size,
                self.preview,
                &self.prefilter,
                self.header,
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
        match csv::parse_projected(&text, self.projection.as_deref()) {
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

    fn pull(&mut self, ctx: &mut OpCtx) -> Option<Chunk> {
        if !self.loaded {
            self.load(ctx);
        }
        if let Some(chunker) = self.stream.as_mut() {
            let cols = chunker.next_columns()?;
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
    columns: Vec<Column>,
    cursor: usize,
    total: usize,
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
            columns: Vec::new(),
            cursor: 0,
            total: 0,
            loaded: false,
        }
    }

    fn load(&mut self, ctx: &mut OpCtx) {
        self.loaded = true;
        let bytes = match std::fs::read(&self.path) {
            Ok(b) => b,
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

        // Per-field byte offsets and record size, honoring C natural alignment
        // (repr(C)) when requested, otherwise packed.
        let mut offsets = Vec::with_capacity(self.fields.len());
        let mut acc = 0usize;
        let mut max_align = 1usize;
        for (_, t) in &self.fields {
            if self.c_align {
                let a = t.align();
                max_align = max_align.max(a);
                acc = round_up(acc, a);
            }
            offsets.push(acc);
            acc += t.size();
        }
        let rec_size = if self.c_align {
            round_up(acc, max_align)
        } else {
            acc
        };
        if rec_size == 0 {
            ctx.raise(
                ErrorEvent::new(Severity::Fatal, ErrorScope::Graph, "empty binary layout")
                    .at_node(ctx.label.clone()),
            );
            return;
        }
        let n = bytes.len() / rec_size;
        if bytes.len() % rec_size != 0 {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Recoverable,
                    ErrorScope::Item,
                    format!(
                        "{} trailing byte(s) ignored (partial record)",
                        bytes.len() % rec_size
                    ),
                )
                .at_node(ctx.label.clone()),
            );
        }

        let schema_fields = self
            .fields
            .iter()
            .map(|(name, t)| Field::new(name.clone(), t.lane()))
            .collect();
        self.schema = Arc::new(Schema::new(schema_fields));

        let mut columns = Vec::with_capacity(self.fields.len());
        for (fi, (_, t)) in self.fields.iter().enumerate() {
            let foff = offsets[fi];
            let sz = t.size();
            let cell =
                |r: usize| -> &[u8] { &bytes[r * rec_size + foff..r * rec_size + foff + sz] };
            let e = self.endian;
            let col = match t.lane() {
                DataType::Bool => Column::Bool((0..n).map(|r| cell(r)[0] != 0).collect()),
                DataType::F64 => Column::F64((0..n).map(|r| decode_f64(cell(r), *t, e)).collect()),
                _ => Column::I64((0..n).map(|r| decode_int(cell(r), *t, e)).collect()),
            };
            columns.push(col);
        }
        self.total = n;
        self.columns = columns;
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
            columns: Vec::new(),
            cursor: 0,
            total: 0,
            loaded: false,
        }
    }

    fn load(&mut self, ctx: &mut OpCtx) {
        self.loaded = true;
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
    key: String,
    desc: bool,
    buf: Vec<Chunk>,
    emitted: bool,
}

impl Sort {
    fn new(key: String, desc: bool) -> Self {
        Sort {
            key,
            desc,
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

        let mut idx: Vec<usize> = (0..total).collect();
        match schema.index_of(&self.key) {
            Some(ki) => {
                let key_col = &cols[ki];
                let desc = self.desc;
                idx.sort_by(|&a, &b| {
                    let o = cmp_rows(key_col, a, b);
                    if desc {
                        o.reverse()
                    } else {
                        o
                    }
                });
            }
            None => {
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Warn,
                        ErrorScope::Chunk,
                        format!("sort: unknown key '{}' (emitting unsorted)", self.key),
                    )
                    .at_node(ctx.label.clone()),
                );
            }
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

/// Running accumulator for one aggregate within one group.
#[derive(Clone)]
struct AggAcc {
    sum: f64,
    min: f64,
    max: f64,
    n: i64,
}

impl AggAcc {
    fn new() -> Self {
        AggAcc {
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            n: 0,
        }
    }
    fn add(&mut self, v: f64) {
        self.sum += v;
        self.min = self.min.min(v);
        self.max = self.max.max(v);
        self.n += 1;
    }
    fn value(&self, f: AggFunc) -> f64 {
        match f {
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
        }
    }
}

struct GroupState {
    count: i64,
    accs: Vec<AggAcc>,
}

struct GroupBy {
    key: String,
    aggs: Vec<(AggFunc, String)>,
    groups: BTreeMap<String, GroupState>,
    emitted: bool,
}

impl GroupBy {
    fn new(key: String, aggs: Vec<(AggFunc, String)>) -> Self {
        GroupBy {
            key,
            aggs,
            groups: BTreeMap::new(),
            emitted: false,
        }
    }
}

impl Operator for GroupBy {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        let Some(key_idx) = chunk.schema.index_of(&self.key) else {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Warn,
                    ErrorScope::Chunk,
                    format!("group: unknown key '{}'", self.key),
                )
                .at_node(ctx.label.clone()),
            );
            return Vec::new();
        };
        // Resolve aggregate column indices once per chunk.
        let agg_idx: Vec<Option<usize>> = self
            .aggs
            .iter()
            .map(|(_, c)| chunk.schema.index_of(c))
            .collect();
        let naggs = self.aggs.len();

        for row in 0..chunk.len {
            let k = chunk.value(row, key_idx).to_string();
            let state = self.groups.entry(k).or_insert_with(|| GroupState {
                count: 0,
                accs: vec![AggAcc::new(); naggs],
            });
            state.count += 1;
            for (j, idx) in agg_idx.iter().enumerate() {
                if let Some(ci) = idx {
                    if let Some(v) = chunk.value(row, *ci).as_f64() {
                        state.accs[j].add(v);
                    }
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

        let keys: StrColumn = self.groups.keys().map(String::as_str).collect();
        let counts: Vec<i64> = self.groups.values().map(|s| s.count).collect();

        let mut fields = vec![
            Field::new(self.key.clone(), DataType::Str),
            Field::new("count", DataType::I64),
        ];
        let mut columns: Vec<Column> = vec![Column::Str(keys), Column::I64(counts)];

        for (j, (func, col)) in self.aggs.iter().enumerate() {
            let vals: Vec<f64> = self
                .groups
                .values()
                .map(|s| s.accs[j].value(*func))
                .collect();
            fields.push(Field::new(
                format!("{}_{}", func.as_str(), col),
                DataType::F64,
            ));
            columns.push(Column::F64(vals));
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

/// MVP join: buffers both sides and emits the left side on finish, recording
/// that the synchronized join executor is not yet wired (continue-first). The
/// IR and source already model it fully (design doc 04/05).
struct Join {
    left_key: String,
    right_key: String,
    left_id: NodeId,
    left_buf: Vec<Chunk>,
}

impl Join {
    fn new(left_key: String, right_key: String, left_id: NodeId) -> Self {
        Join {
            left_key,
            right_key,
            left_id,
            left_buf: Vec::new(),
        }
    }
}

impl Operator for Join {
    fn process(&mut self, from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        if from == self.left_id {
            self.left_buf.push(chunk);
        }
        Vec::new()
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        ctx.raise(ErrorEvent::new(
            Severity::Info,
            ErrorScope::Branch,
            format!(
                "synchronized join on {} = {} not yet executed (MVP): forwarding left input",
                self.left_key, self.right_key
            ),
        ));
        std::mem::take(&mut self.left_buf)
    }
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
}

impl SinkCsv {
    fn new(path: String) -> Self {
        SinkCsv {
            w: StreamWriter::new(path),
        }
    }

    fn write_chunk(&mut self, chunk: &Chunk) -> std::io::Result<()> {
        let need_header = !self.w.wrote_header;
        {
            let w = self.w.writer()?;
            if need_header {
                writeln!(w, "{}", chunk.schema.field_names().join(","))?;
            }
            let mut line = String::new();
            for row in 0..chunk.len {
                line.clear();
                for c in 0..chunk.columns.len() {
                    if c > 0 {
                        line.push(',');
                    }
                    line.push_str(&csv_escape(&chunk.value(row, c)));
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
pub fn write_csv_file(path: &str, chunks: &[Chunk]) -> std::io::Result<()> {
    let mut out = String::new();
    if let Some(first) = chunks.first() {
        out.push_str(&first.schema.field_names().join(","));
        out.push('\n');
        for chunk in chunks {
            for row in 0..chunk.len {
                let cells: Vec<String> = (0..chunk.columns.len())
                    .map(|c| csv_escape(&chunk.value(row, c)))
                    .collect();
                out.push_str(&cells.join(","));
                out.push('\n');
            }
        }
    }
    write_output(path, &out)
}

fn csv_escape(v: &Value) -> String {
    let s = v.to_string();
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s
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
                json_value(&mut out, &chunk.value(row, c));
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
    let mut out = String::new();
    for chunk in chunks {
        let names = chunk.schema.field_names();
        for row in 0..chunk.len {
            out.push('{');
            for (c, name) in names.iter().enumerate() {
                if c > 0 {
                    out.push(',');
                }
                json_string(&mut out, name);
                out.push(':');
                json_value(&mut out, &chunk.value(row, c));
            }
            out.push_str("}\n");
        }
    }
    write_output(path, &out)
}

/// Encode a JSON value from a Rivus scalar.
fn json_value(out: &mut String, v: &Value) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::I64(n) => out.push_str(&n.to_string()),
        // JSON has no NaN/Infinity → emit null (continue-first).
        Value::F64(f) if f.is_finite() => out.push_str(&f.to_string()),
        Value::F64(_) => out.push_str("null"),
        Value::Str(s) => json_string(out, s),
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
