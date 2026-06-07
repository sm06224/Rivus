//! Source operators: CSV / binary / JSONL readers and the stream-ref stub.
//!
//! Split out of the former monolithic `operators.rs` (design 26 §26.8.1,
//! mechanical move-only split; logic unchanged).

use super::*;
use crate::transport::{read_whole, FileTransport, Scheme};
use rivus_core::Resource;

/// Attach provenance to a freshly produced chunk (design §28.6). Stamps the
/// origin handle on chunk metadata (reachable via the `source.uri` accessor);
/// under `with filename` (`filename == true`) it additionally **materializes** a
/// `filename` Str column at the end of the chunk — the `(source.uri) as filename`
/// sugar (§27.1) — suffixed `filename_r` when the data already has a `filename`
/// column (the join collision rule). Zero-cost when provenance is off (`source`
/// is `None`). Applied **identically** on the serial reader and on every
/// byte-range parallel worker — each derives the same handle from the same path
/// — so the origin (and the materialized column) is partition-independent and
/// byte-identity holds (serial == parallel == chunk-size).
fn attach_provenance(mut chunk: Chunk, source: &Option<Resource>, filename: bool) -> Chunk {
    let Some(handle) = source else {
        return chunk;
    };
    chunk.meta.source = Some(handle.clone());
    if filename {
        // `filename` unless the data already has that column → `filename_r`.
        let name = if chunk.schema.index_of("filename").is_some() {
            "filename_r"
        } else {
            "filename"
        };
        let uri = handle.uri();
        let mut col = StrColumn::with_capacity(chunk.len, uri.len() * chunk.len);
        for _ in 0..chunk.len {
            col.push(uri);
        }
        let mut fields = chunk.schema.fields.clone();
        fields.push(Field::new(name.to_string(), DataType::Str));
        chunk.schema = Arc::new(Schema::new(fields));
        chunk.columns.push(Column::str(col));
    }
    chunk
}

// ---------------------------------------------------------------- source (csv)

/// CSV source. A real file streams (bounded memory, [`csv::CsvChunker`]); the
/// `-` stdin sentinel can't be re-read for two-pass inference, so it falls back
/// to the buffered whole-input parse (stdin is rarely the 15 GB case).
pub(crate) struct SourceCsv {
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
    /// Streaming codec decoder for a real file (plain or compressed); `None` for
    /// stdin / after a load error. The seekable and compressed CSV readers both
    /// present the same [`crate::codec::Decoder`] face (§28.5).
    decoder: Option<Box<dyn crate::codec::Decoder>>,
    /// Buffered fallback (stdin): pre-parsed columns sliced by `pull`.
    columns: Vec<Column>,
    cursor: usize,
    total: usize,
    /// Origin handle stamped onto every produced chunk when `with source` /
    /// `with filename` is set (design §28.6); `None` = provenance off.
    source: Option<Resource>,
    /// `with filename`: also materialize a `filename` column (= source.uri) at
    /// the end of each chunk (§27.1). False for `with source` / off.
    filename: bool,
}

impl SourceCsv {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
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
            decoder: None,
            columns: Vec::new(),
            cursor: 0,
            total: 0,
            source: None,
            filename: false,
        }
    }

    /// Configure provenance (design §28.6) from the IR mode and source path: the
    /// stamped origin handle and whether to materialize the `filename` column.
    /// `Off` leaves provenance off (zero overhead).
    pub(crate) fn with_provenance(mut self, prov: rivus_ir::Provenance, path: &str) -> Self {
        self.source = prov.source(path);
        self.filename = prov.materializes_filename();
        self
    }

    /// A source wrapping an already-built streaming reader (a parallel worker's
    /// byte range), with a schema inferred globally beforehand.
    pub(crate) fn from_stream(schema: Arc<Schema>, chunker: csv::CsvChunker) -> Self {
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
            decoder: Some(Box::new(chunker)),
            columns: Vec::new(),
            cursor: 0,
            total: 0,
            source: None,
            filename: false,
        }
    }

    fn load(&mut self, ctx: &mut OpCtx) {
        self.loaded = true;
        if self.path == "-" {
            self.load_stdin(ctx);
        } else if Scheme::of(&self.path).is_compressed() {
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
                    self.decoder = Some(Box::new(chunker));
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
                self.decoder = Some(Box::new(reader));
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
        let text = match read_whole(&self.path) {
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
        self.decoder
            .as_ref()
            .map(|d| d.inferred().to_vec())
            .unwrap_or_default()
    }

    fn pull(&mut self, ctx: &mut OpCtx) -> Option<Chunk> {
        if !self.loaded {
            self.load(ctx);
        }
        if let Some(dec) = self.decoder.as_mut() {
            match dec.decode_chunk() {
                Some(cols) => {
                    let id = ctx.fresh_id();
                    return Some(attach_provenance(
                        Chunk::new(id, self.schema.clone(), cols),
                        &self.source,
                        self.filename,
                    ));
                }
                None => {
                    // Source exhausted: report how many rows the pushed-down
                    // prefilter skipped building (pure accounting — the result is
                    // unchanged, the downstream FilterProject would drop them).
                    // The compressed reader reports none (trait default), exactly
                    // as before its dedicated branch.
                    let skipped = dec.rows_prefiltered();
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
                    // Per-column parse failures: non-empty cells that couldn't be
                    // parsed into the column's lane (malformed, or an i128 overflow
                    // in the decimal lane) and were defaulted to 0 — surfaced once
                    // on exhaustion so the loss is visible (continue-first; #②④).
                    // `parse_failures` is aligned to the output schema's fields.
                    for (k, &n) in dec.parse_failures().iter().enumerate() {
                        if n > 0 {
                            let col = match self.schema.fields.get(k) {
                                Some(f) => format!("'{}' (as {})", f.name, f.dtype),
                                None => format!("#{k}"),
                            };
                            ctx.raise(
                                ErrorEvent::new(
                                    Severity::Recoverable,
                                    ErrorScope::Item,
                                    format!(
                                        "{n} value(s) in column {col} could not be parsed; set to null"
                                    ),
                                )
                                .at_node(ctx.label.clone()),
                            );
                        }
                    }
                    return None;
                }
            }
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
        Some(attach_provenance(
            Chunk::new(id, self.schema.clone(), columns),
            &self.source,
            self.filename,
        ))
    }

    fn process(&mut self, _from: NodeId, _chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        Vec::new()
    }
}

// ------------------------------------------------------------- source (binary)

/// Reads fixed-width binary records (a C struct dump): fields are packed in
/// declaration order, little-endian. Each field decodes straight into its
/// columnar lane — no text parsing at all, so this is much faster than CSV.
pub(crate) struct SourceBinary {
    path: String,
    fields: Vec<(String, BinType)>,
    endian: Endian,
    c_align: bool,
    chunk_size: usize,
    schema: Arc<Schema>,
    decoder: Option<Box<dyn crate::codec::Decoder>>,
    loaded: bool,
    /// Origin handle stamped onto every produced chunk (design §28.6); `None` =
    /// provenance off.
    source: Option<Resource>,
    /// `with filename`: also materialize a `filename` column (§27.1).
    filename: bool,
}

impl SourceBinary {
    pub(crate) fn new(
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
            decoder: None,
            loaded: false,
            source: None,
            filename: false,
        }
    }

    /// Configure provenance (design §28.6) from the IR mode and source path.
    pub(crate) fn with_provenance(mut self, prov: rivus_ir::Provenance, path: &str) -> Self {
        self.source = prov.source(path);
        self.filename = prov.materializes_filename();
        self
    }

    /// A source wrapping an already-built streaming binary reader (a parallel
    /// worker's record range), with a globally known schema.
    pub(crate) fn from_chunker(schema: Arc<Schema>, chunker: BinChunker) -> Self {
        SourceBinary {
            path: String::new(),
            fields: Vec::new(),
            endian: Endian::Little,
            c_align: false,
            chunk_size: 0,
            schema,
            decoder: Some(Box::new(chunker)),
            loaded: true,
            source: None,
            filename: false,
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
                self.decoder = Some(Box::new(ch));
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
        let dec = self.decoder.as_mut()?;
        let columns = dec.decode_chunk()?;
        let id = ctx.fresh_id();
        Some(attach_provenance(
            Chunk::new(id, self.schema.clone(), columns),
            &self.source,
            self.filename,
        ))
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
                DataType::Bool => Column::bool((0..n).map(|r| cell(r)[0] != 0).collect()),
                DataType::F64 => {
                    Column::f64((0..n).map(|r| decode_f64(cell(r), *t, endian)).collect())
                }
                _ => Column::i64((0..n).map(|r| decode_int(cell(r), *t, endian)).collect()),
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
        let reader = FileTransport::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
        let schema = bin_schema(&fields);
        Ok((
            schema,
            BinChunker {
                reader,
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
        let mut reader =
            FileTransport::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
        std::io::Seek::seek(
            &mut reader,
            std::io::SeekFrom::Start((start_rec * rec_size) as u64),
        )
        .map_err(|e| e.to_string())?;
        Ok(BinChunker {
            reader,
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

/// Codec face (§28.5): fixed-width binary needs no inference and has no
/// prefilter / parse-failure accounting, so the decoder is just the chunk pull.
impl crate::codec::Decoder for BinChunker {
    fn decode_chunk(&mut self) -> Option<Vec<Column>> {
        self.next_columns()
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
    provenance: rivus_ir::Provenance,
) -> Box<dyn Operator> {
    match BinChunker::for_range(
        path, fields, offsets, rec_size, endian, start_rec, n_recs, chunk_size,
    ) {
        Ok(ch) => {
            Box::new(SourceBinary::from_chunker(schema, ch).with_provenance(provenance, path))
        }
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
pub(crate) struct SourceJsonl {
    path: String,
    chunk_size: usize,
    schema: Arc<Schema>,
    /// Line-oriented JSONL streams in bounded memory via the codec decoder; a
    /// top-level array can't be streamed (an element may span lines) so it
    /// materializes via `jsonl::parse` into `columns` instead.
    decoder: Option<Box<dyn crate::codec::Decoder>>,
    columns: Vec<Column>,
    cursor: usize,
    total: usize,
    loaded: bool,
    /// Origin handle stamped onto every produced chunk (design §28.6); `None` =
    /// provenance off.
    source: Option<Resource>,
    /// `with filename`: also materialize a `filename` column (§27.1).
    filename: bool,
}

impl SourceJsonl {
    pub(crate) fn new(path: String, chunk_size: usize) -> Self {
        SourceJsonl {
            path,
            chunk_size: chunk_size.max(1),
            schema: Schema::empty(),
            decoder: None,
            columns: Vec::new(),
            cursor: 0,
            total: 0,
            loaded: false,
            source: None,
            filename: false,
        }
    }

    /// Configure provenance (design §28.6) from the IR mode and source path.
    pub(crate) fn with_provenance(mut self, prov: rivus_ir::Provenance, path: &str) -> Self {
        self.source = prov.source(path);
        self.filename = prov.materializes_filename();
        self
    }

    /// A source wrapping an already-built streaming JSONL reader (a parallel
    /// worker's byte range), with a globally pre-inferred schema.
    pub(crate) fn from_chunker(schema: Arc<Schema>, chunker: jsonl::JsonlChunker) -> Self {
        SourceJsonl {
            path: String::new(),
            chunk_size: 0,
            schema,
            decoder: Some(Box::new(chunker)),
            columns: Vec::new(),
            cursor: 0,
            total: 0,
            loaded: true,
            source: None,
            filename: false,
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
                    self.decoder = Some(Box::new(ch));
                }
                Err(e) => ctx.raise(
                    ErrorEvent::new(Severity::Fatal, ErrorScope::Graph, e)
                        .at_node(ctx.label.clone()),
                ),
            }
            return;
        }
        let text = match read_whole(&self.path) {
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
        if let Some(dec) = self.decoder.as_mut() {
            let columns = dec.decode_chunk()?;
            let id = ctx.fresh_id();
            return Some(attach_provenance(
                Chunk::new(id, self.schema.clone(), columns),
                &self.source,
                self.filename,
            ));
        }
        if self.cursor >= self.total {
            return None;
        }
        let end = (self.cursor + self.chunk_size).min(self.total);
        let idx: Vec<usize> = (self.cursor..end).collect();
        let columns = self.columns.iter().map(|c| c.gather(&idx)).collect();
        let id = ctx.fresh_id();
        self.cursor = end;
        Some(attach_provenance(
            Chunk::new(id, self.schema.clone(), columns),
            &self.source,
            self.filename,
        ))
    }

    fn process(&mut self, _from: NodeId, _chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        Vec::new()
    }
}

// ------------------------------------------------------------ source (discover)

/// `ls "glob"` — discovery-as-flow (design §28.3 / `Codec::Discover`). Enumerates
/// the glob into a deterministic (uri-ascending) list of files and emits them as
/// **ordinary columns** `{ path: Resource, name: str, size: int, mtime: datetime }`,
/// in `chunk_size` batches (so the stream is chunk-size independent). `path` is the
/// composable handle (consumed by `read`, slice 3c); `name`/`size`/`mtime` are
/// derived filter columns. `size`/`mtime` are out of the determinism contract
/// (§00 0.14) — results that depend on them aren't byte-identity/parallel
/// guaranteed; `path`/`name` order is deterministic. 0 matches → warn + empty
/// stream (continue-first).
pub(crate) struct SourceDiscover {
    pattern: String,
    chunk_size: usize,
    loaded: bool,
    uris: Vec<String>,
    cursor: usize,
    schema: Arc<Schema>,
}

impl SourceDiscover {
    pub(crate) fn new(pattern: String, chunk_size: usize) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("path".to_string(), DataType::Resource),
            Field::new("name".to_string(), DataType::Str),
            Field::new("size".to_string(), DataType::I64),
            Field::new(
                "mtime".to_string(),
                DataType::DateTime {
                    unit: TimeUnit::Sec,
                },
            ),
        ]));
        SourceDiscover {
            pattern,
            chunk_size: chunk_size.max(1),
            loaded: false,
            uris: Vec::new(),
            cursor: 0,
            schema,
        }
    }
}

/// The final path segment of a uri (the `name` column): `logs/a.csv` → `a.csv`.
fn basename(uri: &str) -> &str {
    uri.rsplit(['/', '\\']).next().unwrap_or(uri)
}

impl Operator for SourceDiscover {
    fn is_source(&self) -> bool {
        true
    }

    fn pull(&mut self, ctx: &mut OpCtx) -> Option<Chunk> {
        if !self.loaded {
            self.loaded = true;
            self.uris = crate::discovery::glob_paths(&self.pattern);
            if self.uris.is_empty() {
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Warn,
                        ErrorScope::Graph,
                        format!("ls: no files match '{}'", self.pattern),
                    )
                    .at_node(ctx.label.clone()),
                );
            }
        }
        if self.cursor >= self.uris.len() {
            return None;
        }
        let end = (self.cursor + self.chunk_size).min(self.uris.len());
        let n = end - self.cursor;
        let mut path = StrColumn::with_capacity(n, 0);
        let mut name = StrColumn::with_capacity(n, 0);
        let mut size = Vec::with_capacity(n);
        let mut mtime = Vec::with_capacity(n);
        for u in &self.uris[self.cursor..end] {
            path.push(u);
            name.push(basename(u));
            // size / mtime are out-of-contract (§00 0.14); a failed stat → 0.
            let meta = std::fs::metadata(u).ok();
            size.push(meta.as_ref().map(|m| m.len() as i64).unwrap_or(0));
            mtime.push(
                meta.and_then(|m| m.modified().ok())
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0),
            );
        }
        self.cursor = end;
        let id = ctx.fresh_id();
        Some(Chunk::new(
            id,
            self.schema.clone(),
            vec![
                Column::resource(path),
                Column::str(name),
                Column::i64(size),
                Column::datetime(DtColumn {
                    ticks: mtime,
                    unit: TimeUnit::Sec,
                }),
            ],
        ))
    }

    fn process(&mut self, _from: NodeId, _chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        Vec::new()
    }
}

// ----------------------------------------------------------- stream ref (stub)

/// `stream X` replay. The MVP has no checkpoint store yet, so a replay with no
/// recorded history simply produces nothing and notes it on the error stream.
pub(crate) struct StreamRef {
    pub(crate) name: String,
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
