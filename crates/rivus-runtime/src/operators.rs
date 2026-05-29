//! Operator implementations.
//!
//! Every flow node compiles to one boxed [`Operator`]. The engine drives them
//! with a chunk-granular, single-threaded push schedule (see `engine.rs`).
//! Fan-out (`->` branch) is handled by the engine via multiple outgoing edges,
//! so there is no dedicated branch operator.

use crate::csv;
use crate::eval;
use rivus_core::{Chunk, Column, DataType, ErrorEvent, ErrorScope, Field, Schema, Severity, Value};
use rivus_ir::{Expr, NodeId, Op};
use std::collections::BTreeMap;
use std::sync::Arc;

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

/// Build the operator for a node from its IR op.
pub fn build(op: &Op, inputs: &[NodeId], chunk_size: usize) -> Box<dyn Operator> {
    match op {
        Op::OpenCsv { path } => Box::new(SourceCsv::new(path.clone(), chunk_size)),
        Op::StreamRef { name } => Box::new(StreamRef { name: name.clone() }),
        Op::Filter { pred } => Box::new(Filter { pred: pred.clone() }),
        Op::Project { fields } => Box::new(Project {
            fields: fields.clone(),
        }),
        Op::FilterProject { preds, fields } => Box::new(FilterProject {
            preds: preds.clone(),
            fields: fields.clone(),
        }),
        Op::GroupBy { key } => Box::new(GroupBy::new(key.clone())),
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
    }
}

// ---------------------------------------------------------------- source (csv)

struct SourceCsv {
    path: String,
    chunk_size: usize,
    schema: Arc<Schema>,
    columns: Vec<Column>,
    cursor: usize,
    total: usize,
    loaded: bool,
}

impl SourceCsv {
    fn new(path: String, chunk_size: usize) -> Self {
        SourceCsv {
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
        let text = match std::fs::read_to_string(&self.path) {
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
        match csv::parse(&text) {
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
        let mut keep = Vec::new();
        for row in 0..chunk.len {
            if eval::eval_predicate(&self.pred, &chunk, row) {
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
        let mut keep = Vec::new();
        for row in 0..chunk.len {
            if self
                .preds
                .iter()
                .all(|p| eval::eval_predicate(p, &chunk, row))
            {
                keep.push(row);
            }
        }
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

struct GroupBy {
    key: String,
    counts: BTreeMap<String, i64>,
    emitted: bool,
}

impl GroupBy {
    fn new(key: String) -> Self {
        GroupBy {
            key,
            counts: BTreeMap::new(),
            emitted: false,
        }
    }
}

impl Operator for GroupBy {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        let Some(col_idx) = chunk.schema.index_of(&self.key) else {
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
        for row in 0..chunk.len {
            let k = chunk.value(row, col_idx).to_string();
            *self.counts.entry(k).or_insert(0) += 1;
        }
        Vec::new() // group is a materializing boundary; output on finish
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.emitted {
            return Vec::new();
        }
        self.emitted = true;
        let keys: Vec<String> = self.counts.keys().cloned().collect();
        let vals: Vec<i64> = self.counts.values().copied().collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new(self.key.clone(), DataType::Str),
            Field::new("count", DataType::I64),
        ]));
        let id = ctx.fresh_id();
        vec![Chunk::new(
            id,
            schema,
            vec![Column::Str(keys), Column::I64(vals)],
        )]
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

struct SinkCsv {
    path: String,
    buf: Vec<Chunk>,
}

impl SinkCsv {
    fn new(path: String) -> Self {
        SinkCsv {
            path,
            buf: Vec::new(),
        }
    }
}

impl Operator for SinkCsv {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        self.buf.push(chunk);
        Vec::new() // consume: written to disk on finish
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        let mut out = String::new();
        if let Some(first) = self.buf.first() {
            out.push_str(&first.schema.field_names().join(","));
            out.push('\n');
            for chunk in &self.buf {
                for row in 0..chunk.len {
                    let cells: Vec<String> = (0..chunk.columns.len())
                        .map(|c| csv_escape(&chunk.value(row, c)))
                        .collect();
                    out.push_str(&cells.join(","));
                    out.push('\n');
                }
            }
        }
        if let Err(e) = std::fs::write(&self.path, out) {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Critical,
                    ErrorScope::Graph,
                    format!("cannot write '{}': {e}", self.path),
                )
                .at_node(ctx.label.clone()),
            );
        }
        Vec::new()
    }
}

fn csv_escape(v: &Value) -> String {
    let s = v.to_string();
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s
    }
}
