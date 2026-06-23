//! Sink operators: CSV / JSONL / JSON writers.
//!
//! Split out of the former monolithic `operators.rs` (design 26 §26.8.1,
//! mechanical move-only split; logic unchanged).

use super::*;

// ----------------------------------------------------------------- sink: print

pub(crate) struct SinkPrint;

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
pub(crate) struct SinkCsv {
    w: StreamWriter,
    delim: u8,
}

impl SinkCsv {
    pub(crate) fn new(path: String, delim: u8) -> Self {
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
        let p = crate::transport::adjust_path(path);
        Box::new(std::fs::File::create(p)?)
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
pub(crate) fn write_cell(line: &mut String, col: &Column, row: usize, delim: u8) {
    use std::fmt::Write as _;
    // Null → an unquoted empty field (design 26 §26.5). A real empty string is a
    // valid `Str` cell and falls through to the lane below, so the two stay
    // distinguishable on round-trip.
    if col.is_null(row) {
        return;
    }
    match col.data() {
        ColumnData::I64(v) => {
            let _ = write!(line, "{}", v[row]);
        }
        ColumnData::F64(v) => {
            let _ = write!(line, "{}", v[row]);
        }
        ColumnData::Bool(v) => line.push_str(if v[row] { "true" } else { "false" }),
        ColumnData::Dec(d) => {
            let _ = write!(
                line,
                "{}",
                rivus_core::Decimal::new(d.unscaled[row], d.scale)
            );
        }
        ColumnData::DateTime(d) => {
            let _ = write!(line, "{}", rivus_core::DateTime::new(d.ticks[row], d.unit));
        }
        ColumnData::Duration(d) => {
            let _ = write!(line, "{}", rivus_core::Duration::new(d.ticks[row], d.unit));
        }
        ColumnData::Date(v) => {
            let _ = write!(line, "{}", rivus_core::Date::new(v[row]));
        }
        ColumnData::Time(v) => {
            let _ = write!(
                line,
                "{}",
                rivus_core::TimeOfDay::new(v[row], rivus_core::TimeUnit::Sec)
            );
        }
        // A resource handle renders its uri (text), with the same CSV quoting.
        ColumnData::Str(s) | ColumnData::Resource(s) => {
            let cell = s.get(row);
            // A real empty string is written **quoted** (`""`) so it round-trips
            // back to an empty string — an *unquoted* empty field is reserved for
            // `null` (handled above). Design 26 §26.5.
            if cell.is_empty()
                || cell.bytes().any(|b| b == delim)
                || cell.contains('"')
                || cell.contains('\n')
            {
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
pub(crate) struct SinkJsonl {
    w: StreamWriter,
}

impl SinkJsonl {
    pub(crate) fn new(path: String) -> Self {
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
        let p = crate::transport::adjust_path(path);
        Box::new(std::fs::File::create(p)?)
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
pub(crate) fn json_object_row(out: &mut String, chunk: &Chunk, names: &[&str], row: usize) {
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
pub(crate) struct SinkJson {
    w: StreamWriter,
}

impl SinkJson {
    pub(crate) fn new(path: String) -> Self {
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
    // Null → a bare JSON `null` (design 26 §26.5); a real empty string is a
    // quoted `""` and falls through to the `Str` lane below.
    if col.is_null(row) {
        out.push_str("null");
        return;
    }
    match col.data() {
        ColumnData::I64(v) => {
            let _ = write!(out, "{}", v[row]);
        }
        // JSON has no NaN/Infinity → emit null (continue-first), matching json_value.
        ColumnData::F64(v) => {
            if v[row].is_finite() {
                let _ = write!(out, "{}", v[row]);
            } else {
                out.push_str("null");
            }
        }
        ColumnData::Bool(v) => out.push_str(if v[row] { "true" } else { "false" }),
        ColumnData::Dec(d) => {
            let _ = write!(
                out,
                "{}",
                rivus_core::Decimal::new(d.unscaled[row], d.scale)
            );
        }
        // Datetime has no JSON literal form → emit a quoted ISO-8601 string.
        ColumnData::DateTime(d) => json_string(
            out,
            &rivus_core::DateTime::new(d.ticks[row], d.unit).to_string(),
        ),
        // Duration likewise → quoted human-readable string (#57).
        ColumnData::Duration(d) => json_string(
            out,
            &rivus_core::Duration::new(d.ticks[row], d.unit).to_string(),
        ),
        // Date → quoted ISO yyyy-MM-dd string (#58).
        ColumnData::Date(v) => json_string(out, &rivus_core::Date::new(v[row]).to_string()),
        // Time → quoted HH:mm:ss string (#58).
        ColumnData::Time(v) => json_string(
            out,
            &rivus_core::TimeOfDay::new(v[row], rivus_core::TimeUnit::Sec).to_string(),
        ),
        // A resource handle → its uri as a quoted JSON string.
        ColumnData::Str(s) | ColumnData::Resource(s) => json_string(out, s.get(row)),
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

// ---------------------------------------------------------------- sink: route

/// Partitioned / dynamic-output sink (design §28.7, ratified #143):
/// `save "out/{country}.csv"` / `save "out/" by k [as flat]`. **Streams** rows
/// to per-partition files as chunks arrive through a bounded LRU handle pool
/// ([`crate::route::RouteWriter`]) — no longer buffering the whole stream, so a
/// high-cardinality save stays bounded-memory. The parallel merge
/// (`write_sink`) streams the merged chunks through the same writer (shared
/// row formatters + within-partition stream order), so byte-identity
/// (serial == parallel == chunk-size) holds.
pub(crate) struct SinkRoute {
    pub(crate) template: String,
    pub(crate) by: Vec<String>,
    pub(crate) flat: bool,
    pub(crate) exprs: Vec<Expr>,
    pub(crate) codec: rivus_ir::SinkCodec,
    pub(crate) writer: Option<crate::route::RouteWriter>,
    pub(crate) eval_fails: u64,
    pub(crate) warned_missing: bool,
}

impl SinkRoute {
    fn writer(&mut self) -> &mut crate::route::RouteWriter {
        let codec = self.codec;
        self.writer
            .get_or_insert_with(|| crate::route::RouteWriter::new(codec))
    }
}

impl Operator for SinkRoute {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        // A key column missing from the live schema folds its rows into the
        // null partition — surfaced once, never silent.
        if !self.warned_missing {
            for k in &self.by {
                if chunk.schema.index_of(k).is_none() {
                    ctx.raise(
                        ErrorEvent::new(
                            Severity::Recoverable,
                            ErrorScope::Chunk,
                            format!(
                                "save route: unknown partition key column '{k}' — its rows \
                                 go to the {} partition",
                                crate::route::NULL_PARTITION
                            ),
                        )
                        .at_node(ctx.label.clone()),
                    );
                    self.warned_missing = true;
                }
            }
        }
        // Route this chunk's rows immediately (bounded memory): group by path,
        // then stream each group to its partition file.
        let groups = crate::route::group_by_path(
            std::slice::from_ref(&chunk),
            &self.template,
            &self.by,
            self.flat,
            self.codec,
            &self.exprs,
            &mut self.eval_fails,
        );
        self.writer().write_groups(groups);
        Vec::new()
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        // Close all partitions (continue-first): one unwritable path surfaces
        // and the rest still land — never a silent fallback (#143 ③).
        let failures = match self.writer.take() {
            Some(w) => w.finish(),
            None => Vec::new(),
        };
        if self.eval_fails > 0 {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Recoverable,
                    ErrorScope::Item,
                    format!(
                        "save route: {} value(s) could not be evaluated in a \
                         computed placeholder; routed to the {} partition",
                        self.eval_fails,
                        crate::route::NULL_PARTITION
                    ),
                )
                .at_node(ctx.label.clone()),
            );
        }
        for (path, e) in failures {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Recoverable,
                    ErrorScope::Graph,
                    format!("save route: could not write partition {path}: {e}; other partitions continue"),
                )
                .at_node(ctx.label.clone()),
            );
        }
        Vec::new()
    }
}
