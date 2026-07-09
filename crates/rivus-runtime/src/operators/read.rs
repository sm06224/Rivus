//! The `read` operator (design §28.3, slice 3c): consume a `Resource` column
//! from upstream and open + decode every handle, concatenating the files
//! **by name** (union-by-name) in deterministic uri order.
//!
//! Source-agnostic: the handle column can come from `ls`, a manifest
//! (`resource(col)`), or a computed path — `read` only cares that there *is* a
//! `Resource` column (default `path`, else the first `Resource`-typed column).
//! Transport is selected per uri (file only today; a non-file/unopenable handle
//! is quarantined on the error stream, never silent). Type reconciliation widens
//! numerically (`int ⊆ float ⊆ decimal`, anything ⊆ `str`) so a column never
//! silently truncates across files; a missing column is null. MVP is **serial**
//! and buffers each file's decoded chunks (parallel / bounded-memory streaming
//! are tracked follow-ups). `size`/`mtime` etc. are unaffected — `read` only
//! reads the handle's bytes.

use super::*;
use crate::codec::Decoder;
use rivus_ir::{Provenance, ReadFmt};

/// Per-file decode format, resolved from `read as FMT` or the uri's extension.
enum FileFmt {
    Csv(u8),
    Jsonl,
}

pub(crate) struct Read {
    fmt: Option<ReadFmt>,
    provenance: Provenance,
    chunk_size: usize,
    /// uris collected from the upstream Resource column (read on `finish`).
    uris: Vec<String>,
    /// An upstream chunk carried a schema but no `Resource` column → a never-silent
    /// error on `finish` (the user piped non-handles into `read`).
    rescol_missing: bool,
}

impl Read {
    pub(crate) fn new(fmt: Option<ReadFmt>, provenance: Provenance, chunk_size: usize) -> Self {
        Read {
            fmt,
            provenance,
            chunk_size: chunk_size.max(1),
            uris: Vec::new(),
            rescol_missing: false,
        }
    }

    /// The format for one uri: an explicit `as FMT` wins; else the extension
    /// (`.jsonl`/`.ndjson`/`.json` → JSONL, `.tsv`/`.tab` → TSV, else CSV).
    fn fmt_for(&self, uri: &str) -> FileFmt {
        match self.fmt {
            Some(ReadFmt::Csv) => FileFmt::Csv(b','),
            Some(ReadFmt::Tsv) => FileFmt::Csv(b'\t'),
            Some(ReadFmt::Jsonl) => FileFmt::Jsonl,
            None => {
                let l = uri.to_ascii_lowercase();
                if l.ends_with(".jsonl") || l.ends_with(".ndjson") || l.ends_with(".json") {
                    FileFmt::Jsonl
                } else {
                    FileFmt::Csv(rivus_ir::delim_for_path(uri))
                }
            }
        }
    }

    /// Open + fully decode one file into `(schema, chunks, bad_rows)`. `Err` (a
    /// non-file / unopenable handle, a fatal decode) is quarantined by the caller.
    fn decode(&self, uri: &str) -> Result<(Schema, Vec<Vec<Column>>, usize), String> {
        match self.fmt_for(uri) {
            FileFmt::Csv(delim) => {
                // Fast path: reuse the parallel-source machinery — infer the
                // schema by streaming newline-aligned ranges IN PARALLEL
                // (`plan_parallel`), then decode each range in file order with
                // the types already known (`for_range`, one typed pass). The old
                // path (`CsvChunker::open`) paid a full serial inference scan and
                // THEN a full decode scan per file — the dominant cost of a
                // multi-file `read` (measured: ~340ms/M rows vs the source's
                // ~155ms/M on the same machine). The inferred schema is pinned
                // byte-identical to the serial reader's by the engine's
                // serial==parallel invariant, and ranges are contiguous in file
                // order, so row order — and therefore the output — is unchanged.
                let threads = std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1)
                    .min(8);
                match crate::csv::plan_parallel(
                    uri,
                    None,
                    threads,
                    &[],
                    &[],
                    true,
                    None,
                    &[],
                    delim,
                ) {
                    Ok(plan) => {
                        let mut chunks = Vec::new();
                        for &(a, b) in &plan.ranges {
                            let mut ch = crate::csv::CsvChunker::for_range(
                                uri,
                                plan.dtypes.clone(),
                                plan.dt_specs.clone(),
                                plan.keep.clone(),
                                plan.ncols,
                                a,
                                b,
                                self.chunk_size,
                                plan.prefilter.clone(),
                                plan.str_prefilter.clone(),
                                delim,
                            )?;
                            while let Some(cols) = ch.decode_chunk() {
                                chunks.push(cols);
                            }
                        }
                        // bad rows are counted on the inference pass (same total
                        // as the serial reader — it counts on ITS inference pass).
                        Ok((plan.schema, chunks, plan.bad_rows))
                    }
                    // Unseekable / unstattable → the buffered serial reader.
                    Err(_) => {
                        let (schema, mut ch) = crate::csv::CsvChunker::open(
                            uri,
                            None,
                            self.chunk_size,
                            false,
                            &[],
                            &[],
                            true,
                            None,
                            &[],
                            delim,
                        )?;
                        let mut chunks = Vec::new();
                        while let Some(cols) = ch.decode_chunk() {
                            chunks.push(cols);
                        }
                        Ok((schema, chunks, ch.bad_rows))
                    }
                }
            }
            FileFmt::Jsonl => {
                let (schema, mut ch) = crate::jsonl::JsonlChunker::open(uri, self.chunk_size)?;
                let mut chunks = Vec::new();
                while let Some(cols) = ch.decode_chunk() {
                    chunks.push(cols);
                }
                Ok((schema, chunks, ch.bad_rows))
            }
        }
    }
}

/// Numeric widening lattice for union-by-name (§28.3): `int ⊆ float ⊆ decimal`
/// (decimal keeps the larger scale), anything-else-mixed ⊆ `str`. A column that
/// is absent (null) in one file does not constrain the type. This avoids the
/// silent truncation a first-seen-wins rule would cause (DuckDB parity).
fn widen(a: DataType, b: DataType) -> DataType {
    use DataType::*;
    if a == b {
        return a;
    }
    if a == Null {
        return b;
    }
    if b == Null {
        return a;
    }
    let rank = |t: &DataType| match t {
        I64 => Some(1u8),
        F64 => Some(2),
        Decimal { .. } => Some(3),
        _ => None,
    };
    if let (Some(ra), Some(rb)) = (rank(&a), rank(&b)) {
        return match ra.max(rb) {
            3 => {
                let sa = if let Decimal { scale } = a { scale } else { 0 };
                let sb = if let Decimal { scale } = b { scale } else { 0 };
                Decimal { scale: sa.max(sb) }
            }
            2 => F64,
            _ => I64,
        };
    }
    // Any other mix (bool/temporal/resource/str) → the universal text lane.
    Str
}

impl Operator for Read {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        match resource_col(&chunk.schema) {
            Some(ci) => {
                for r in 0..chunk.len {
                    if let Value::Resource(res) = chunk.value(r, ci) {
                        self.uris.push(res.uri().to_string());
                    }
                }
            }
            None => {
                if !chunk.schema.fields.is_empty() {
                    self.rescol_missing = true;
                }
            }
        }
        Vec::new()
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.uris.is_empty() {
            if self.rescol_missing {
                // Never-silent: the user piped a non-handle stream into `read`.
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Fatal,
                        ErrorScope::Graph,
                        "read: no Resource column to read (expected a `path` column, or any \
                         resource()-typed column — e.g. from `ls` or `(resource(col)) as path`)"
                            .to_string(),
                    )
                    .at_node(ctx.label.clone()),
                );
            }
            return Vec::new();
        }
        // Deterministic order: concatenate files in uri-ascending order.
        self.uris.sort();

        // Decode each file once (MVP buffers); quarantine the ones that fail.
        let mut decoded: Vec<(String, Schema, Vec<Vec<Column>>)> = Vec::new();
        for uri in &self.uris {
            match self.decode(uri) {
                Ok((schema, chunks, bad_rows)) => {
                    if bad_rows > 0 {
                        ctx.raise(
                            ErrorEvent::new(
                                Severity::Recoverable,
                                ErrorScope::Item,
                                format!("read '{uri}': {bad_rows} malformed row(s) skipped"),
                            )
                            .at_node(ctx.label.clone()),
                        );
                    }
                    decoded.push((uri.clone(), schema, chunks));
                }
                Err(e) => ctx.raise(
                    // Quarantine: surface and skip; other files continue (§24).
                    ErrorEvent::new(
                        Severity::Recoverable,
                        ErrorScope::Item,
                        format!("read: skipped '{uri}': {e}"),
                    )
                    .at_node(ctx.label.clone()),
                ),
            }
        }
        if decoded.is_empty() {
            return Vec::new();
        }

        // union-by-name: ordered first-seen column names; widened types.
        let mut union: Vec<Field> = Vec::new();
        for (_, schema, _) in &decoded {
            for f in &schema.fields {
                match union.iter_mut().find(|u| u.name == f.name) {
                    Some(u) => u.dtype = widen(u.dtype, f.dtype),
                    None => union.push(f.clone()),
                }
            }
        }
        // `with filename` materializes the source path (§27.1); `filename_r` on
        // collision with a data column. `with source` rides the handle only.
        let fname = self.provenance.materializes_filename().then(|| {
            let name = if union.iter().any(|f| f.name == "filename") {
                "filename_r"
            } else {
                "filename"
            };
            union.push(Field::new(name.to_string(), DataType::Str));
            name.to_string()
        });
        let uschema = Arc::new(Schema::new(union.clone()));

        // Reconcile every file's chunks to the union schema and emit, stamping the
        // file's handle as provenance (so `source.uri` works per row).
        let mut out = Vec::new();
        for (uri, schema, chunks) in &decoded {
            let handle = self.provenance.source(uri);
            for cols in chunks {
                let len = cols.first().map(|c| c.len()).unwrap_or(0);
                // union-by-name widening is a lane coercion (int⊆float⊆…⊆str), not
                // a user temporal cast — a parse never fails here, so the cast
                // failure count is discarded.
                let mut _widen_fails = 0u64;
                let rcols: Vec<Column> = union
                    .iter()
                    .map(|f| {
                        if fname.as_deref() == Some(f.name.as_str()) {
                            str_repeat(uri, len)
                        } else {
                            match schema.index_of(&f.name) {
                                Some(i) => {
                                    eval::cast_column(cols[i].clone(), f.dtype, &mut _widen_fails)
                                }
                                // Missing column in this file → an all-null column
                                // of the union type (continue-first).
                                None => eval::cast_column(
                                    eval::column_from_values(vec![Value::Null; len]),
                                    f.dtype,
                                    &mut _widen_fails,
                                ),
                            }
                        }
                    })
                    .collect();
                let id = ctx.fresh_id();
                let mut ch = Chunk::new(id, uschema.clone(), rcols);
                if handle.is_some() {
                    ch.meta.source = handle.clone();
                }
                out.push(ch);
            }
        }
        out
    }
}

/// The Resource column `read` consumes: the `path` column if it is Resource-typed,
/// else the first Resource-typed column. `None` → no handle column present.
fn resource_col(schema: &Schema) -> Option<usize> {
    if let Some(i) = schema.index_of("path") {
        if schema.fields[i].dtype == DataType::Resource {
            return Some(i);
        }
    }
    schema
        .fields
        .iter()
        .position(|f| f.dtype == DataType::Resource)
}

/// An `n`-row `Str` column holding `s` on every row (the `filename` materialize).
fn str_repeat(s: &str, n: usize) -> Column {
    let mut c = StrColumn::with_capacity(n, s.len() * n);
    for _ in 0..n {
        c.push(s);
    }
    Column::str(c)
}
