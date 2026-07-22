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

/// One file's decode result: `(schema, chunks-of-columns, malformed rows)`.
type FileDecode = Result<(Schema, Vec<Vec<Column>>, usize), String>;

/// A lazily-draining single-file decoder: the schema is known **up front**
/// (from the inference plan / the compressed sample) while the rows decode on
/// demand — the building block that lets the engine's parallel read→group path
/// stream a file through its per-worker pipeline **without materializing it**
/// (union-by-name needs every file's schema before the first chunk is
/// reconciled, so schema and rows must be separable; 全てが流れ).
// A handful of instances exist at once (one per in-flight file), so the
// variant size spread is irrelevant — boxing would only add indirection.
#[allow(clippy::large_enum_variant)]
pub(crate) enum FileDecoder {
    /// Plain CSV: the range plan (typed single pass), drained range by range.
    CsvRanges {
        uri: String,
        plan: crate::csv::CsvParallelPlan,
        delim: u8,
        cur_range: usize,
        cur: Option<crate::csv::CsvChunker>,
        chunk_size: usize,
    },
    /// Plain JSONL: the range plan, drained range by range.
    JsonlRanges {
        uri: String,
        names: Vec<String>,
        jtypes: Vec<crate::jsonl::JType>,
        ranges: Vec<(u64, u64)>,
        bad_rows: usize,
        cur_range: usize,
        cur: Option<crate::jsonl::JsonlChunker>,
        chunk_size: usize,
    },
    /// Buffered fallback (unseekable / too small to split): already decoded.
    Buffered {
        chunks: std::vec::IntoIter<Vec<Column>>,
        bad_rows: usize,
    },
    /// Stage C speculative stream (#239): a live whole-file chunker whose
    /// schema came from the SAMPLE (first chunk) instead of a full pass-1
    /// scan. Contradictions (any non-empty parse failure) tell the driver to
    /// discard this file's partial and re-run it canonically.
    CsvStream(crate::csv::CsvChunker),
    /// Stage C speculative JSONL stream (#239): the block-walk chunker over
    /// the whole file with a sample-inferred flat-scalar schema (same decode
    /// speed as the canonical range path); contradictions are counted
    /// `lane_mismatches` (JSON is syntax-typed, so the detector is complete
    /// with no Bool exception).
    JsonlStream(crate::jsonl::JsonlChunker),
    /// Compressed CSV stream (sample-inferred; the reader replays its sample).
    #[cfg(any(feature = "gzip", feature = "zstd"))]
    CompCsv(crate::csv::CompressedCsvReader),
    /// Compressed JSONL stream (sample-inferred; replays its sample).
    #[cfg(any(feature = "gzip", feature = "zstd"))]
    CompJsonl(crate::jsonl::StreamJsonlReader),
}

impl FileDecoder {
    /// The next decoded chunk of columns, or `None` at end of file.
    pub(crate) fn next_chunk(&mut self) -> Option<Vec<Column>> {
        match self {
            FileDecoder::CsvRanges {
                uri,
                plan,
                delim,
                cur_range,
                cur,
                chunk_size,
            } => loop {
                if let Some(ch) = cur {
                    if let Some(cols) = ch.decode_chunk() {
                        return Some(cols);
                    }
                    *cur = None;
                }
                if *cur_range >= plan.ranges.len() {
                    return None;
                }
                let (a, b) = plan.ranges[*cur_range];
                *cur_range += 1;
                // An IO error mid-file ends the stream (the plan opened once
                // already, so this is a truly exceptional race); never panics.
                *cur = crate::csv::CsvChunker::for_range(
                    uri,
                    plan.dtypes.clone(),
                    plan.dt_specs.clone(),
                    plan.keep.clone(),
                    plan.ncols,
                    a,
                    b,
                    *chunk_size,
                    plan.prefilter.clone(),
                    plan.str_prefilter.clone(),
                    *delim,
                )
                .ok();
                cur.as_ref()?;
            },
            FileDecoder::JsonlRanges {
                uri,
                names,
                jtypes,
                ranges,
                bad_rows: _,
                cur_range,
                cur,
                chunk_size,
            } => loop {
                if let Some(ch) = cur {
                    if let Some(cols) = ch.decode_chunk() {
                        return Some(cols);
                    }
                    *cur = None;
                }
                if *cur_range >= ranges.len() {
                    return None;
                }
                let (a, b) = ranges[*cur_range];
                *cur_range += 1;
                *cur = crate::jsonl::JsonlChunker::for_range(
                    uri,
                    names.clone(),
                    jtypes.clone(),
                    a,
                    b,
                    *chunk_size,
                )
                .ok();
                cur.as_ref()?;
            },
            FileDecoder::Buffered { chunks, .. } => chunks.next(),
            FileDecoder::CsvStream(ch) => ch.next_columns(),
            FileDecoder::JsonlStream(ch) => ch.next_columns(),
            #[cfg(any(feature = "gzip", feature = "zstd"))]
            FileDecoder::CompCsv(r) => r.decode_chunk(),
            #[cfg(any(feature = "gzip", feature = "zstd"))]
            FileDecoder::CompJsonl(r) => r.decode_chunk(),
        }
    }

    /// Malformed-row count. For plain files the plan's inference pass counted
    /// the whole file up front; for compressed streams the count accrues while
    /// decoding, so read it **after** draining.
    pub(crate) fn bad_rows(&self) -> usize {
        match self {
            FileDecoder::CsvRanges { plan, .. } => plan.bad_rows,
            FileDecoder::JsonlRanges { bad_rows, .. } => *bad_rows,
            FileDecoder::Buffered { bad_rows, .. } => *bad_rows,
            FileDecoder::CsvStream(ch) => ch.bad_rows,
            FileDecoder::JsonlStream(ch) => ch.bad_rows,
            #[cfg(any(feature = "gzip", feature = "zstd"))]
            FileDecoder::CompCsv(r) => r.bad_rows,
            #[cfg(any(feature = "gzip", feature = "zstd"))]
            FileDecoder::CompJsonl(r) => r.bad_rows,
        }
    }

    /// design/42 発動可観測性 (条件④): the dictionary-build status of a
    /// speculative CSV stream — `(candidate columns, chunk escapes)`.
    pub(crate) fn dict_status(&self) -> Option<(usize, u32)> {
        match self {
            FileDecoder::CsvStream(ch) => ch.dict_status(),
            FileDecoder::JsonlStream(ch) => ch.dict_status(),
            _ => None,
        }
    }

    /// Stage C (#239): is this a speculative sampled-schema stream (vs a
    /// canonical two-pass decoder)? Surfaced in `RunResult::strategy` so the
    /// engine swap is never silent (Observable First).
    pub(crate) fn is_speculative(&self) -> bool {
        matches!(
            self,
            FileDecoder::CsvStream(_) | FileDecoder::JsonlStream(_)
        )
    }

    /// Stage C (#239): did a speculative stream contradict its sampled schema
    /// (any non-empty parse failure)? Canonical decoders never do — only the
    /// sampled `CsvStream` path can speculate.
    pub(crate) fn spec_contradicted(&self) -> bool {
        match self {
            FileDecoder::CsvStream(ch) => ch.parse_failures.iter().any(|&n| n > 0),
            FileDecoder::JsonlStream(ch) => ch.lane_mismatches > 0,
            _ => false,
        }
    }
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
    /// design/42 stage (b): column names the plan consumes as join/group keys
    /// — the only dictionary-encoding candidates (speculative opens only).
    /// `None` (the default) disables dictionary lanes entirely.
    dict_keys: Option<std::collections::HashSet<String>>,
    /// Decode-column pruning (#240 キュー3): keep only these columns while
    /// decoding CSV (`None` = keep all). Set from `engine::read_prune_allow`
    /// on BOTH the serial chain and the parallel sink driver (対称方式), so
    /// the decode sets — and the error streams — never diverge between paths.
    /// JSONL decoders take no allow-list and always decode fully.
    allow: Option<Vec<String>>,
}

impl Read {
    pub(crate) fn new(fmt: Option<ReadFmt>, provenance: Provenance, chunk_size: usize) -> Self {
        Read {
            fmt,
            provenance,
            chunk_size: chunk_size.max(1),
            uris: Vec::new(),
            rescol_missing: false,
            dict_keys: None,
            allow: None,
        }
    }

    /// Enable dictionary-lane candidacy for the named key columns (design/42
    /// stage b) — set by the engine's fused read→group path, where the join
    /// probe and group-key encoding consume the ids.
    pub(crate) fn with_dict_keys(
        mut self,
        keys: Option<std::collections::HashSet<String>>,
    ) -> Self {
        self.dict_keys = keys;
        self
    }

    /// Restrict CSV decoding to these columns (decode-column pruning, #240
    /// キュー3). MUST come from `engine::read_prune_allow` so serial and
    /// parallel decode the same set (対称方式).
    pub(crate) fn with_allow(mut self, allow: Option<Vec<String>>) -> Self {
        self.allow = allow;
        self
    }

    /// The format for one uri: an explicit `as FMT` wins; else the extension
    /// (`.jsonl`/`.ndjson`/`.json` → JSONL, `.tsv`/`.tab` → TSV, else CSV). A
    /// compression suffix (`.gz`/`.zst`) is stripped first, so `part.jsonl.gz`
    /// reads as compressed JSONL（全てが流れ — compressed streams are
    /// first-class, 統括指示 2026-07-09）.
    fn fmt_for(&self, uri: &str) -> FileFmt {
        match self.fmt {
            Some(ReadFmt::Csv) => FileFmt::Csv(b','),
            Some(ReadFmt::Tsv) => FileFmt::Csv(b'\t'),
            Some(ReadFmt::Jsonl) => FileFmt::Jsonl,
            None => {
                let l = uri.to_ascii_lowercase();
                let l = l
                    .strip_suffix(".gz")
                    .or_else(|| l.strip_suffix(".zst"))
                    .unwrap_or(&l);
                if l.ends_with(".jsonl") || l.ends_with(".ndjson") || l.ends_with(".json") {
                    FileFmt::Jsonl
                } else {
                    FileFmt::Csv(rivus_ir::delim_for_path(l))
                }
            }
        }
    }

    /// Stage C (#239): open one plain-CSV file **speculatively** — infer the
    /// schema from a `chunk_size`-row sample (one short read instead of the
    /// full pass-1 scan) and stream-decode assuming it. The caller must check
    /// `FileDecoder::spec_contradicted()` after draining: any non-empty parse
    /// failure means a cell fell outside the sampled lanes, and the file must
    /// be re-run through the canonical two-pass `open_file_stream` (design/41
    /// §5). Files whose sample infers a Bool column are ineligible (Bool cells
    /// never emit parse failures, so contradiction detection is blind there)
    /// and fall back to the canonical open, as do compressed inputs and JSONL.
    pub(crate) fn open_file_stream_sampled(
        &self,
        uri: &str,
    ) -> Result<(Schema, FileDecoder), String> {
        if crate::transport::Scheme::of(uri).is_compressed() {
            return self.open_file_stream(uri);
        }
        match self.fmt_for(uri) {
            FileFmt::Csv(delim) => {
                let Ok((schema, mut ch)) = crate::csv::CsvChunker::open(
                    uri,
                    self.allow.as_deref(),
                    self.chunk_size,
                    true, // preview: sample-based inference, no pass-1 scan
                    self.dict_keys.as_ref(),
                    &[],
                    &[],
                    true,
                    None,
                    &[],
                    delim,
                ) else {
                    // Let the canonical open raise its own message (the
                    // quarantine text must match the serial run's).
                    return self.open_file_stream(uri);
                };
                if schema
                    .fields
                    .iter()
                    .any(|f| f.dtype == rivus_core::DataType::Bool)
                {
                    return self.open_file_stream(uri);
                }
                ch.count_stream_bad();
                Ok((schema, FileDecoder::CsvStream(ch)))
            }
            FileFmt::Jsonl => {
                // Only a FLAT all-scalar sample may speculate: the fused block
                // walk is the one path that counts lane mismatches. (No Bool
                // exclusion here: JSON is syntax-typed, a stray `"true"` or
                // `1` in a Bool lane IS a counted mismatch.) A sample-phase
                // error or unusable sample falls back so the canonical open
                // raises its own message (the quarantine text must match the
                // serial run's).
                let Ok(Some((names, jtypes, dict_cols))) =
                    crate::jsonl::sample_infer_flat(uri, self.chunk_size, self.dict_keys.as_ref())
                else {
                    return self.open_file_stream(uri);
                };
                let schema = crate::jsonl::schema_from(&names, &jtypes);
                match crate::jsonl::JsonlChunker::open_speculative(
                    uri,
                    names,
                    jtypes,
                    dict_cols,
                    self.chunk_size,
                ) {
                    Ok(ch) => Ok((schema, FileDecoder::JsonlStream(ch))),
                    Err(_) => self.open_file_stream(uri),
                }
            }
        }
    }

    /// Open one file for **lazy** decoding: the schema now, the rows on demand
    /// (`FileDecoder::next_chunk`). The engine's parallel read→group path uses
    /// this to stream a file straight through a per-worker pipeline without
    /// materializing it; `decode` below stays the materializing form (and keeps
    /// its internal range parallelism for the single-file case).
    pub(crate) fn open_file_stream(&self, uri: &str) -> Result<(Schema, FileDecoder), String> {
        if crate::transport::Scheme::of(uri).is_compressed() {
            return match self.fmt_for(uri) {
                FileFmt::Csv(_delim) => {
                    #[cfg(any(feature = "gzip", feature = "zstd"))]
                    {
                        let (schema, ch) = crate::csv::CompressedCsvReader::open(
                            uri,
                            self.allow.as_deref(),
                            self.chunk_size,
                            true,
                            None,
                            &[],
                            _delim,
                        )?;
                        Ok((schema, FileDecoder::CompCsv(ch)))
                    }
                    #[cfg(not(any(feature = "gzip", feature = "zstd")))]
                    Err(format!(
                        "'{uri}' is compressed; rebuild with `--features gzip`/`zstd` to read it"
                    ))
                }
                FileFmt::Jsonl => {
                    #[cfg(any(feature = "gzip", feature = "zstd"))]
                    {
                        let reader = crate::transport::open_compressed(uri)?;
                        let (schema, ch) =
                            crate::jsonl::StreamJsonlReader::from_reader(reader, self.chunk_size)?;
                        Ok((schema, FileDecoder::CompJsonl(ch)))
                    }
                    #[cfg(not(any(feature = "gzip", feature = "zstd")))]
                    Err(format!(
                        "'{uri}' is compressed; rebuild with `--features gzip`/`zstd` to read it"
                    ))
                }
            };
        }
        let threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(8);
        match self.fmt_for(uri) {
            FileFmt::Csv(delim) => {
                match crate::csv::plan_parallel(
                    uri,
                    self.allow.as_deref(),
                    threads,
                    &[],
                    &[],
                    true,
                    None,
                    &[],
                    delim,
                ) {
                    Ok(plan) => Ok((
                        plan.schema.clone(),
                        FileDecoder::CsvRanges {
                            uri: uri.to_string(),
                            plan,
                            delim,
                            cur_range: 0,
                            cur: None,
                            chunk_size: self.chunk_size,
                        },
                    )),
                    Err(_) => {
                        let (schema, mut ch) = crate::csv::CsvChunker::open(
                            uri,
                            self.allow.as_deref(),
                            self.chunk_size,
                            false,
                            None,
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
                        Ok((
                            schema,
                            FileDecoder::Buffered {
                                chunks: chunks.into_iter(),
                                bad_rows: ch.bad_rows,
                            },
                        ))
                    }
                }
            }
            FileFmt::Jsonl => match crate::jsonl::plan_parallel(uri, threads) {
                Some((schema, names, jtypes, ranges, bad_rows)) => Ok((
                    schema,
                    FileDecoder::JsonlRanges {
                        uri: uri.to_string(),
                        names,
                        jtypes,
                        ranges,
                        bad_rows,
                        cur_range: 0,
                        cur: None,
                        chunk_size: self.chunk_size,
                    },
                )),
                None => {
                    let (schema, mut ch) = crate::jsonl::JsonlChunker::open(uri, self.chunk_size)?;
                    let mut chunks = Vec::new();
                    while let Some(cols) = ch.decode_chunk() {
                        chunks.push(cols);
                    }
                    Ok((
                        schema,
                        FileDecoder::Buffered {
                            chunks: chunks.into_iter(),
                            bad_rows: ch.bad_rows,
                        },
                    ))
                }
            },
        }
    }

    /// Open + fully decode one file into `(schema, chunks, bad_rows)`. `Err` (a
    /// non-file / unopenable handle, a fatal decode) is quarantined by the caller.
    fn decode(&self, uri: &str) -> FileDecode {
        // Compressed handle (`.gz`/`.zst`): decode RIDES the decompression
        // stream — the same single-pass sample-inference readers the source
        // uses for its compressed/HTTP paths (never decompress-to-buffer;
        // 全てが流れ). A build without the compression features quarantines the
        // file with rebuild guidance instead of misreading raw bytes.
        if crate::transport::Scheme::of(uri).is_compressed() {
            return match self.fmt_for(uri) {
                FileFmt::Csv(_delim) => {
                    #[cfg(any(feature = "gzip", feature = "zstd"))]
                    {
                        let (schema, mut ch) = crate::csv::CompressedCsvReader::open(
                            uri,
                            self.allow.as_deref(),
                            self.chunk_size,
                            true,
                            None,
                            &[],
                            _delim,
                        )?;
                        let mut chunks = Vec::new();
                        while let Some(cols) = ch.decode_chunk() {
                            chunks.push(cols);
                        }
                        Ok((schema, chunks, ch.bad_rows))
                    }
                    #[cfg(not(any(feature = "gzip", feature = "zstd")))]
                    Err(format!(
                        "'{uri}' is compressed; rebuild with `--features gzip`/`zstd` to read it"
                    ))
                }
                FileFmt::Jsonl => {
                    #[cfg(any(feature = "gzip", feature = "zstd"))]
                    {
                        let reader = crate::transport::open_compressed(uri)?;
                        let (schema, mut ch) =
                            crate::jsonl::StreamJsonlReader::from_reader(reader, self.chunk_size)?;
                        let mut chunks = Vec::new();
                        while let Some(cols) = ch.decode_chunk() {
                            chunks.push(cols);
                        }
                        Ok((schema, chunks, ch.bad_rows))
                    }
                    #[cfg(not(any(feature = "gzip", feature = "zstd")))]
                    Err(format!(
                        "'{uri}' is compressed; rebuild with `--features gzip`/`zstd` to read it"
                    ))
                }
            };
        }
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
                    self.allow.as_deref(),
                    threads,
                    &[],
                    &[],
                    true,
                    None,
                    &[],
                    delim,
                ) {
                    Ok(plan) => {
                        // Decode the ranges in PARALLEL and splice in range
                        // order (ranges are contiguous, so row order — and the
                        // output — is byte-identical to a serial decode; only
                        // internal chunk boundaries differ, and results are
                        // pinned chunk-size independent).
                        let cz = self.chunk_size;
                        let plan = &plan;
                        let chunks = decode_ranges_parallel(plan.ranges.len(), |i| {
                            let (a, b) = plan.ranges[i];
                            let mut ch = crate::csv::CsvChunker::for_range(
                                uri,
                                plan.dtypes.clone(),
                                plan.dt_specs.clone(),
                                plan.keep.clone(),
                                plan.ncols,
                                a,
                                b,
                                cz,
                                plan.prefilter.clone(),
                                plan.str_prefilter.clone(),
                                delim,
                            )?;
                            let mut out = Vec::new();
                            while let Some(cols) = ch.decode_chunk() {
                                out.push(cols);
                            }
                            Ok(out)
                        })?;
                        // bad rows are counted on the inference pass (same total
                        // as the serial reader — it counts on ITS inference pass).
                        Ok((plan.schema.clone(), chunks, plan.bad_rows))
                    }
                    // Unseekable / unstattable → the buffered serial reader.
                    Err(_) => {
                        let (schema, mut ch) = crate::csv::CsvChunker::open(
                            uri,
                            self.allow.as_deref(),
                            self.chunk_size,
                            false,
                            None,
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
                // Same shape as the CSV fast path: a global-schema range plan
                // (serial inference — JSONL has no parallel infer yet), then the
                // ranges decoded in parallel and spliced in range order (row
                // order unchanged ⇒ byte-identical). A top-level JSON array or
                // a small/unseekable file falls back to the serial reader.
                let threads = std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1)
                    .min(8);
                match crate::jsonl::plan_parallel(uri, threads) {
                    Some((schema, names, jtypes, ranges, bad_rows)) => {
                        let cz = self.chunk_size;
                        let (names, jtypes, ranges) = (&names, &jtypes, &ranges);
                        let chunks = decode_ranges_parallel(ranges.len(), |i| {
                            let (a, b) = ranges[i];
                            let mut ch = crate::jsonl::JsonlChunker::for_range(
                                uri,
                                names.clone(),
                                jtypes.clone(),
                                a,
                                b,
                                cz,
                            )?;
                            let mut out = Vec::new();
                            while let Some(cols) = ch.decode_chunk() {
                                out.push(cols);
                            }
                            Ok(out)
                        })?;
                        Ok((schema, chunks, bad_rows))
                    }
                    None => {
                        let (schema, mut ch) =
                            crate::jsonl::JsonlChunker::open(uri, self.chunk_size)?;
                        let mut chunks = Vec::new();
                        while let Some(cols) = ch.decode_chunk() {
                            chunks.push(cols);
                        }
                        Ok((schema, chunks, ch.bad_rows))
                    }
                }
            }
        }
    }
}

/// Decode `nranges` byte ranges concurrently (one scoped thread each; the
/// planners cap ranges at ~the core count) and splice the decoded chunk lists
/// **in range order**. Ranges are contiguous and newline-aligned, so the spliced
/// row order equals a serial front-to-back decode — the output is
/// byte-identical; only internal chunk boundaries differ (pinned chunk-size
/// independent). An `Err` from any range fails the whole file (the caller
/// quarantines it, same as the serial path).
fn decode_ranges_parallel<F>(nranges: usize, worker: F) -> Result<Vec<Vec<Column>>, String>
where
    F: Fn(usize) -> Result<Vec<Vec<Column>>, String> + Sync,
{
    let worker = &worker;
    let results: Vec<Result<Vec<Vec<Column>>, String>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..nranges).map(|i| s.spawn(move || worker(i))).collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut out = Vec::new();
    for r in results {
        out.extend(r?);
    }
    Ok(out)
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

        // Decode the files IN PARALLEL, in waves of ≤ the core count, collecting
        // into uri-ordered slots — the reconciliation below walks the slots in
        // uri order, so the output is byte-identical to the old sequential loop.
        // File-level parallelism is what a compressed stream needs (a gzip/zstd
        // stream has no splittable ranges, so per-file range parallelism cannot
        // apply — the fan-out across files IS the parallelism; 全てが流れ).
        // Errors are quarantined per file afterwards, in uri order (§24),
        // exactly like the sequential loop reported them.
        let threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(8);
        let mut results: Vec<Option<FileDecode>> = (0..self.uris.len()).map(|_| None).collect();
        for (wave_start, wave) in self
            .uris
            .chunks(threads)
            .enumerate()
            .map(|(w, c)| (w * threads, c))
        {
            let this = &*self;
            let wave_results: Vec<_> = std::thread::scope(|s| {
                let handles: Vec<_> = wave
                    .iter()
                    .map(|uri| s.spawn(move || this.decode(uri)))
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });
            for (i, r) in wave_results.into_iter().enumerate() {
                results[wave_start + i] = Some(r);
            }
        }
        let mut decoded: Vec<(String, Schema, Vec<Vec<Column>>)> = Vec::new();
        for (uri, res) in self.uris.iter().zip(results) {
            match res.expect("every slot filled") {
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
        let (union, fname) = union_by_name(decoded.iter().map(|(_, s, _)| s), self.provenance);
        let uschema = Arc::new(Schema::new(union.clone()));

        // Reconcile every file's chunks to the union schema and emit, stamping the
        // file's handle as provenance (so `source.uri` works per row).
        let mut out = Vec::new();
        for (uri, schema, chunks) in &decoded {
            let handle = self.provenance.source(uri);
            for cols in chunks.iter().cloned() {
                let id = ctx.fresh_id();
                out.push(reconcile_chunk(
                    &union,
                    &uschema,
                    fname.as_deref(),
                    &handle,
                    uri,
                    schema,
                    cols,
                    id,
                ));
            }
        }
        out
    }
}

/// The union-by-name schema over per-file schemas: ordered first-seen names,
/// widened types, plus the materialized `filename` column name when the
/// provenance mode asks for it (`filename_r` on collision, §27.1). Extracted
/// from `Read::finish` so the engine's parallel read→group path derives the
/// **same** union (single source of truth).
pub(crate) fn union_by_name<'a>(
    schemas: impl Iterator<Item = &'a Schema>,
    provenance: Provenance,
) -> (Vec<Field>, Option<String>) {
    let mut union: Vec<Field> = Vec::new();
    for schema in schemas {
        for f in &schema.fields {
            match union.iter_mut().find(|u| u.name == f.name) {
                Some(u) => u.dtype = widen(u.dtype, f.dtype),
                None => union.push(f.clone()),
            }
        }
    }
    let fname = provenance.materializes_filename().then(|| {
        let name = if union.iter().any(|f| f.name == "filename") {
            "filename_r"
        } else {
            "filename"
        };
        union.push(Field::new(name.to_string(), DataType::Str));
        name.to_string()
    });
    (union, fname)
}

/// Reconcile one decoded chunk of `cols` (with its file's `schema`) to the
/// union schema: union column order, widened lanes (a lane coercion — never a
/// counted parse), missing columns as typed nulls, the optional materialized
/// `filename` column, and the provenance handle stamped on the chunk meta.
/// Extracted from `Read::finish`; the engine's parallel read→group workers call
/// it per streamed chunk so both paths reconcile identically.
#[allow(clippy::too_many_arguments)]
pub(crate) fn reconcile_chunk(
    union: &[Field],
    uschema: &Arc<Schema>,
    fname: Option<&str>,
    handle: &Option<rivus_core::Resource>,
    uri: &str,
    schema: &Schema,
    cols: Vec<Column>,
    id: u64,
) -> Chunk {
    let len = cols.first().map(|c| c.len()).unwrap_or(0);
    // Aligned fast path: the file's schema already IS the union (same names,
    // same order, same lanes) and no filename column is materialized — the
    // columns move straight into the chunk. The old form cloned every column
    // of every chunk (a full data copy per read; measured 487ms of a 10M ETL
    // run). Byte-identical: the identity cast returned the same column.
    let aligned = fname.is_none()
        && schema.fields.len() == union.len()
        && schema
            .fields
            .iter()
            .zip(union)
            .all(|(a, b)| a.name == b.name && a.dtype == b.dtype);
    let rcols: Vec<Column> = if aligned {
        cols
    } else {
        // union-by-name widening is a lane coercion (int⊆float⊆…⊆str), not a
        // user temporal cast — a parse never fails, the count is unused. Each
        // matched column MOVES out exactly once (union names are unique).
        let mut _widen_fails = 0u64;
        let mut slots: Vec<Option<Column>> = cols.into_iter().map(Some).collect();
        union
            .iter()
            .map(|f| {
                if fname == Some(f.name.as_str()) {
                    str_repeat(uri, len)
                } else {
                    match schema.index_of(&f.name) {
                        Some(i) => {
                            let col = slots[i].take().expect("each union name is unique");
                            eval::cast_column(col, f.dtype, &mut _widen_fails)
                        }
                        // Missing column in this file → an all-null column of
                        // the union type (continue-first).
                        None => eval::cast_column(
                            eval::column_from_values(vec![Value::Null; len]),
                            f.dtype,
                            &mut _widen_fails,
                        ),
                    }
                }
            })
            .collect()
    };
    let mut ch = Chunk::new(id, uschema.clone(), rcols);
    if handle.is_some() {
        ch.meta.source = handle.clone();
    }
    ch
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

#[cfg(test)]
mod stage_c_tests {
    //! Stage C sampled-open contract (design/41 §5): the detector must be
    //! complete (zero non-empty parse failures ⇔ sample schema == full
    //! inference), Bool-sampled files must fall back to the canonical open,
    //! and the in-stream malformed-row count must equal pass 1's.

    use super::*;

    fn write_tmp(tag: &str, text: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "rivus_stagec_{tag}_{}_{}.csv",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&p, text).expect("write fixture");
        p
    }

    fn reader(chunk_size: usize) -> Read {
        Read::new(
            Some(ReadFmt::Csv),
            rivus_ir::Provenance::default(),
            chunk_size,
        )
    }

    fn drain(dec: &mut FileDecoder) -> usize {
        let mut rows = 0;
        while let Some(cols) = dec.next_chunk() {
            rows += cols.first().map(|c| c.len()).unwrap_or(0);
        }
        rows
    }

    /// Clean file: the sample schema equals the full-pass schema, the stream
    /// never contradicts, and the row count matches the canonical decode.
    #[test]
    fn sampled_clean_file_matches_canonical() {
        let mut text = String::from("id,region,amount\n");
        for i in 0..500 {
            text.push_str(&format!("{i},r{},{}\n", i % 3, i * 7 % 100));
        }
        let p = write_tmp("clean", &text);
        let uri = p.to_str().unwrap();
        let r = reader(64);
        let (s_spec, mut d_spec) = r.open_file_stream_sampled(uri).expect("sampled open");
        let (s_full, mut d_full) = r.open_file_stream(uri).expect("canonical open");
        assert!(
            matches!(d_spec, FileDecoder::CsvStream(_)),
            "must speculate"
        );
        assert_eq!(s_spec, s_full, "sample inference == full inference");
        assert_eq!(drain(&mut d_spec), drain(&mut d_full), "same row count");
        assert!(
            !d_spec.spec_contradicted(),
            "clean stream never contradicts"
        );
        assert_eq!(d_spec.bad_rows(), d_full.bad_rows());
        let _ = std::fs::remove_file(&p);
    }

    /// A type surprise BEYOND the sample window (an unparsable cell under the
    /// sampled i64 lane) must be detected as a contradiction.
    #[test]
    fn sampled_late_type_surprise_contradicts() {
        let mut text = String::from("id,amount\n");
        for i in 0..200 {
            text.push_str(&format!("{i},{}\n", i * 3));
        }
        text.push_str("200,notanumber\n");
        let p = write_tmp("surprise", &text);
        let uri = p.to_str().unwrap();
        let r = reader(64); // sample = 64 rows; the surprise is at row 201
        let (s_spec, mut d_spec) = r.open_file_stream_sampled(uri).expect("sampled open");
        assert_eq!(
            s_spec.fields.iter().map(|f| f.dtype).collect::<Vec<_>>(),
            vec![rivus_core::DataType::I64, rivus_core::DataType::I64],
            "sample sees only integers"
        );
        drain(&mut d_spec);
        assert!(
            d_spec.spec_contradicted(),
            "the stray cell must be detected"
        );
        // The canonical open resolves the column to Str instead.
        let (s_full, _) = r.open_file_stream(uri).expect("canonical open");
        assert_eq!(s_full.fields[1].dtype, rivus_core::DataType::Str);
        let _ = std::fs::remove_file(&p);
    }

    /// Bool lanes never emit parse failures (`"maybe"` folds to a silent
    /// false), so a Bool-sampled file is ineligible and must take the
    /// canonical two-pass open instead.
    #[test]
    fn sampled_bool_column_falls_back_to_canonical() {
        let mut text = String::from("id,flag\n");
        for i in 0..100 {
            text.push_str(&format!(
                "{i},{}\n",
                if i % 2 == 0 { "true" } else { "false" }
            ));
        }
        text.push_str("100,maybe\n"); // would fold to false, silently
        let p = write_tmp("bool", &text);
        let uri = p.to_str().unwrap();
        let r = reader(64);
        let (s_spec, d_spec) = r.open_file_stream_sampled(uri).expect("open");
        assert!(
            !matches!(d_spec, FileDecoder::CsvStream(_)),
            "Bool sample must not speculate (contradiction detection is blind there)"
        );
        // And the schema is therefore the full-pass truth (flag widens to Str).
        assert_eq!(s_spec.fields[1].dtype, rivus_core::DataType::Str);
        let _ = std::fs::remove_file(&p);
    }

    /// Wrong-arity rows both inside and beyond the sample window: the
    /// speculative stream's in-stream count must equal pass 1's count
    /// (no double count of the sample window, no missed tail).
    #[test]
    fn sampled_bad_row_count_matches_pass1() {
        let mut text = String::from("id,region,amount\n");
        for i in 0..300 {
            if i == 10 || i == 250 {
                text.push_str("onlyonefield\n"); // one inside, one beyond the sample
            }
            text.push_str(&format!("{i},r{},{}\n", i % 3, i));
        }
        let p = write_tmp("arity", &text);
        let uri = p.to_str().unwrap();
        let r = reader(64);
        let (_, mut d_spec) = r.open_file_stream_sampled(uri).expect("sampled open");
        let (_, mut d_full) = r.open_file_stream(uri).expect("canonical open");
        assert!(
            matches!(d_spec, FileDecoder::CsvStream(_)),
            "must speculate"
        );
        let (rs, rf) = (drain(&mut d_spec), drain(&mut d_full));
        assert_eq!(rs, rf, "same surviving rows");
        assert!(
            !d_spec.spec_contradicted(),
            "arity dirt is not a contradiction"
        );
        assert_eq!(d_full.bad_rows(), 2, "pass 1 counts both");
        assert_eq!(d_spec.bad_rows(), 2, "in-stream count == pass 1 count");
        let _ = std::fs::remove_file(&p);
    }
}

#[cfg(test)]
mod stage_c_jsonl_tests {
    //! Stage C sampled-open contract for JSONL (design/41 §5, C-2): JSON is
    //! syntax-typed, so lane mismatches are counted (no CSV-style Bool
    //! exception); nested samples fall back to the canonical two-pass.

    use super::*;

    fn write_tmp(tag: &str, text: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "rivus_stagecj_{tag}_{}_{}.jsonl",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&p, text).expect("write fixture");
        p
    }

    fn reader(chunk_size: usize) -> Read {
        Read::new(
            Some(ReadFmt::Jsonl),
            rivus_ir::Provenance::default(),
            chunk_size,
        )
    }

    fn drain(dec: &mut FileDecoder) -> usize {
        let mut rows = 0;
        while let Some(cols) = dec.next_chunk() {
            rows += cols.first().map(|c| c.len()).unwrap_or(0);
        }
        rows
    }

    /// Clean file (with malformed lines both inside and beyond the sample):
    /// sample inference == full inference, no contradiction, and the streamed
    /// bad-line count equals pass 1's.
    #[test]
    fn jsonl_sampled_clean_file_matches_canonical() {
        let mut text = String::new();
        for i in 0..300 {
            if i == 10 || i == 250 {
                text.push_str("{\"id\":oops\n"); // malformed: one in, one past the sample
            }
            text.push_str(&format!(
                "{{\"id\":{i},\"region\":\"r{}\",\"amount\":{}}}\n",
                i % 3,
                i * 7 % 100
            ));
        }
        let p = write_tmp("clean", &text);
        let uri = p.to_str().unwrap();
        let r = reader(64);
        let (s_spec, mut d_spec) = r.open_file_stream_sampled(uri).expect("sampled open");
        let (s_full, mut d_full) = r.open_file_stream(uri).expect("canonical open");
        assert!(
            matches!(d_spec, FileDecoder::JsonlStream(_)),
            "must speculate"
        );
        assert_eq!(s_spec, s_full, "sample inference == full inference");
        assert_eq!(drain(&mut d_spec), drain(&mut d_full), "same row count");
        assert!(
            !d_spec.spec_contradicted(),
            "clean stream never contradicts"
        );
        assert_eq!(d_full.bad_rows(), 2, "pass 1 counts both malformed lines");
        assert_eq!(d_spec.bad_rows(), 2, "in-stream count == pass 1 count");
        let _ = std::fs::remove_file(&p);
    }

    /// A float beyond the int-sampled window is a counted lane mismatch →
    /// contradiction (canonical inference resolves the column to F64).
    #[test]
    fn jsonl_sampled_late_float_contradicts() {
        let mut text = String::new();
        for i in 0..200 {
            text.push_str(&format!("{{\"id\":{i},\"amount\":{}}}\n", i * 3));
        }
        text.push_str("{\"id\":200,\"amount\":1.5}\n");
        let p = write_tmp("float", &text);
        let uri = p.to_str().unwrap();
        let r = reader(64);
        let (s_spec, mut d_spec) = r.open_file_stream_sampled(uri).expect("sampled open");
        assert_eq!(s_spec.fields[1].dtype, rivus_core::DataType::I64);
        drain(&mut d_spec);
        assert!(d_spec.spec_contradicted(), "the float must be detected");
        let (s_full, _) = r.open_file_stream(uri).expect("canonical open");
        assert_eq!(s_full.fields[1].dtype, rivus_core::DataType::F64);
        let _ = std::fs::remove_file(&p);
    }

    /// The CSV Bool blind spot does NOT exist here: a quoted `"true"` (a
    /// string) in a Bool lane is a syntax-class mismatch and is counted.
    #[test]
    fn jsonl_bool_lane_mismatch_is_detected() {
        let mut text = String::new();
        for i in 0..100 {
            text.push_str(&format!(
                "{{\"id\":{i},\"flag\":{}}}\n",
                if i % 2 == 0 { "true" } else { "false" }
            ));
        }
        text.push_str("{\"id\":100,\"flag\":\"true\"}\n"); // a STRING, not a bool
        let p = write_tmp("bool", &text);
        let uri = p.to_str().unwrap();
        let r = reader(64);
        let (s_spec, mut d_spec) = r.open_file_stream_sampled(uri).expect("sampled open");
        assert!(
            matches!(d_spec, FileDecoder::JsonlStream(_)),
            "Bool lanes may speculate in JSONL (syntax-typed)"
        );
        assert_eq!(s_spec.fields[1].dtype, rivus_core::DataType::Bool);
        drain(&mut d_spec);
        assert!(
            d_spec.spec_contradicted(),
            "the stray string must be counted"
        );
        let _ = std::fs::remove_file(&p);
    }

    /// A nested sample streams through the silent general path, so it must
    /// take the canonical two-pass instead of speculating.
    #[test]
    fn jsonl_nested_sample_falls_back_to_canonical() {
        let mut text = String::new();
        for i in 0..100 {
            text.push_str(&format!("{{\"id\":{i},\"meta\":{{\"k\":{i}}}}}\n"));
        }
        let p = write_tmp("nested", &text);
        let uri = p.to_str().unwrap();
        let r = reader(64);
        let (_, d_spec) = r.open_file_stream_sampled(uri).expect("open");
        assert!(
            !matches!(d_spec, FileDecoder::JsonlStream(_)),
            "nested sample must not speculate (mismatches are not counted there)"
        );
        let _ = std::fs::remove_file(&p);
    }
}

#[cfg(test)]
mod dict_stage_b_tests {
    //! design/42 stage (b): the speculative CSV open dictionary-encodes
    //! sample-flagged low-cardinality Str columns; a chunk crossing DICT_CAP
    //! escapes to the plain lane; canonical opens never produce dicts. Every
    //! path must read back the exact bytes the canonical open reads.

    use super::*;

    fn write_tmp(tag: &str, text: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "rivus_dictb_{tag}_{}_{}.csv",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&p, text).expect("write fixture");
        p
    }

    fn reader(chunk_size: usize) -> Read {
        Read::new(
            Some(ReadFmt::Csv),
            rivus_ir::Provenance::default(),
            chunk_size,
        )
    }

    /// A reader whose plan consumes `keys` as join/group keys — the only
    /// configuration under which dictionary candidacy is active.
    fn reader_keyed(chunk_size: usize, keys: &[&str]) -> Read {
        reader(chunk_size).with_dict_keys(Some(keys.iter().map(|s| s.to_string()).collect()))
    }

    fn all_rows(dec: &mut FileDecoder) -> Vec<Vec<rivus_core::Value>> {
        let mut out = Vec::new();
        while let Some(cols) = dec.next_chunk() {
            let n = cols.first().map(|c| c.len()).unwrap_or(0);
            for r in 0..n {
                out.push(
                    cols.iter()
                        .map(|c| {
                            if c.is_null(r) {
                                rivus_core::Value::Null
                            } else {
                                c.value_at(r)
                            }
                        })
                        .collect(),
                );
            }
        }
        out
    }

    /// Low-cardinality column → dict chunks, same values as canonical; the
    /// activation is observable via `dict_status`.
    #[test]
    fn sampled_low_card_column_builds_dicts() {
        let mut text = String::from("id,region,note\n");
        for i in 0..500 {
            text.push_str(&format!("{i},r{},free-text-{i}\n", i % 5));
        }
        let p = write_tmp("lowcard", &text);
        let uri = p.to_str().unwrap();
        let r = reader_keyed(64, &["region", "note"]);
        let (_, mut d_spec) = r.open_file_stream_sampled(uri).expect("sampled open");
        assert!(
            matches!(d_spec, FileDecoder::CsvStream(_)),
            "must speculate"
        );
        // First chunk: `region` must be dictionary-encoded, `note` plain
        // (a key column, but its sample is all-distinct), `id` numeric.
        let cols = d_spec.next_chunk().expect("chunk");
        assert!(
            matches!(cols[1].data(), rivus_core::ColumnData::StrDict(_)),
            "region must be dict-encoded"
        );
        assert!(
            matches!(cols[2].data(), rivus_core::ColumnData::Str(_)),
            "all-distinct note must stay plain"
        );
        drop(cols);
        let (_, mut d_full) = r.open_file_stream(uri).expect("canonical open");
        let (mut d_spec2,) = (r.open_file_stream_sampled(uri).expect("re-open").1,);
        assert_eq!(
            all_rows(&mut d_spec2),
            all_rows(&mut d_full),
            "dict decode must read back the canonical bytes"
        );
        assert_eq!(
            d_spec2.dict_status(),
            Some((1, 0)),
            "one candidate column, zero escapes"
        );
        let _ = std::fs::remove_file(&p);
    }

    /// A candidate whose STREAM cardinality explodes past DICT_CAP escapes to
    /// the plain lane mid-chunk — values identical, escape counted.
    #[test]
    fn dict_cap_escape_hatch_fires() {
        // Dictionaries are chunk-local, so an escape needs a single chunk
        // holding > DICT_CAP distincts — chunk_size must exceed the cap.
        let mut text = String::from("id,cat\n");
        // Sample window (chunk_size rows) sees few distincts...
        for i in 0..8192 {
            text.push_str(&format!("{i},c{}\n", i % 4));
        }
        // ...then the file turns all-distinct (> DICT_CAP = 4096 values
        // inside the second chunk).
        for i in 8192..14192 {
            text.push_str(&format!("{i},unique-{i}\n"));
        }
        let p = write_tmp("escape", &text);
        let uri = p.to_str().unwrap();
        let r = reader_keyed(8192, &["cat"]);
        let (_, mut d_spec) = r.open_file_stream_sampled(uri).expect("sampled open");
        let (_, mut d_full) = r.open_file_stream(uri).expect("canonical open");
        assert_eq!(
            all_rows(&mut d_spec),
            all_rows(&mut d_full),
            "escaped decode must read back the canonical bytes"
        );
        let (n, esc) = d_spec.dict_status().expect("cat was a candidate");
        assert_eq!(n, 1);
        assert!(esc > 0, "crossing DICT_CAP must count an escape");
        let _ = std::fs::remove_file(&p);
    }

    /// Decode-column pruning (#240 キュー3): an allow-listed reader decodes —
    /// and schemas — only the listed columns, on the canonical open (the same
    /// list reaches the sampled/compressed opens through the same field).
    #[test]
    fn allow_prunes_decoded_columns() {
        let p = write_tmp("prune", "a,b,junk\n1,x,zz\n2,y,ww\n");
        let uri = p.to_str().unwrap();
        let r = reader(64).with_allow(Some(vec!["a".into(), "b".into()]));
        let (schema, mut d) = r.open_file_stream(uri).expect("open");
        let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["a", "b"], "junk must be pruned from the schema");
        let cols = d.next_chunk().expect("chunk");
        assert_eq!(cols.len(), 2, "junk must not be decoded");
        let _ = std::fs::remove_file(&p);
    }

    /// Canonical opens never dictionary-encode (the serial oracle stays plain
    /// — that asymmetry is what lets the parallel identity guards pin dict vs
    /// plain end to end), and a sampled open without plan key columns stays
    /// plain too (interning is pure cost off the key paths).
    #[test]
    fn canonical_open_stays_plain() {
        let mut text = String::from("id,region\n");
        for i in 0..300 {
            text.push_str(&format!("{i},r{}\n", i % 3));
        }
        let p = write_tmp("canon", &text);
        let uri = p.to_str().unwrap();
        let r = reader_keyed(64, &["region"]);
        let (_, mut d_full) = r.open_file_stream(uri).expect("canonical open");
        assert_eq!(d_full.dict_status(), None);
        while let Some(cols) = d_full.next_chunk() {
            assert!(
                cols.iter()
                    .all(|c| !matches!(c.data(), rivus_core::ColumnData::StrDict(_))),
                "canonical chunks must stay plain"
            );
        }
        let r = reader(64);
        let (_, mut d_spec) = r.open_file_stream_sampled(uri).expect("sampled open");
        assert_eq!(
            d_spec.dict_status(),
            None,
            "no key columns -> no candidates"
        );
        while let Some(cols) = d_spec.next_chunk() {
            assert!(
                cols.iter()
                    .all(|c| !matches!(c.data(), rivus_core::ColumnData::StrDict(_))),
                "unkeyed sampled chunks must stay plain"
            );
        }
        let _ = std::fs::remove_file(&p);
    }
}

#[cfg(test)]
mod dict_stage_b_jsonl_tests {
    //! design/42 stage (b), JSONL side: the sampled open dictionary-encodes
    //! low-cardinality KEY columns (plan-aware candidacy), escapes past
    //! DICT_CAP, and canonical/unkeyed opens stay plain — the same contract
    //! (and the same serial-oracle asymmetry) as the CSV lane.

    use super::*;

    fn write_tmp(tag: &str, text: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "rivus_dictbj_{tag}_{}_{}.jsonl",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&p, text).expect("write fixture");
        p
    }

    fn reader(chunk_size: usize) -> Read {
        Read::new(
            Some(ReadFmt::Jsonl),
            rivus_ir::Provenance::default(),
            chunk_size,
        )
    }

    fn reader_keyed(chunk_size: usize, keys: &[&str]) -> Read {
        reader(chunk_size).with_dict_keys(Some(keys.iter().map(|s| s.to_string()).collect()))
    }

    fn all_rows(dec: &mut FileDecoder) -> Vec<Vec<rivus_core::Value>> {
        let mut out = Vec::new();
        while let Some(cols) = dec.next_chunk() {
            let n = cols.first().map(|c| c.len()).unwrap_or(0);
            for r in 0..n {
                out.push(
                    cols.iter()
                        .map(|c| {
                            if c.is_null(r) {
                                rivus_core::Value::Null
                            } else {
                                c.value_at(r)
                            }
                        })
                        .collect(),
                );
            }
        }
        out
    }

    #[test]
    fn sampled_low_card_key_builds_dicts() {
        let mut text = String::new();
        for i in 0..500 {
            text.push_str(&format!(
                "{{\"id\":{i},\"region\":\"r{}\",\"note\":\"free-{i}\"}}\n",
                i % 5
            ));
        }
        let p = write_tmp("lowcard", &text);
        let uri = p.to_str().unwrap();
        let r = reader_keyed(64, &["region", "note"]);
        let (_, mut d_spec) = r.open_file_stream_sampled(uri).expect("sampled open");
        let cols = d_spec.next_chunk().expect("chunk");
        assert!(
            matches!(cols[1].data(), rivus_core::ColumnData::StrDict(_)),
            "region must be dict-encoded"
        );
        assert!(
            matches!(cols[2].data(), rivus_core::ColumnData::Str(_)),
            "all-distinct note must stay plain"
        );
        drop(cols);
        let (_, mut d_full) = r.open_file_stream(uri).expect("canonical open");
        let mut d_spec2 = r.open_file_stream_sampled(uri).expect("re-open").1;
        assert_eq!(
            all_rows(&mut d_spec2),
            all_rows(&mut d_full),
            "dict decode must read back the canonical values"
        );
        assert_eq!(d_spec2.dict_status(), Some((1, 0)));
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn dict_cap_escape_hatch_fires() {
        // Chunk-local dictionaries: an escape needs one chunk holding more
        // than DICT_CAP distincts, so chunk_size exceeds the cap; the sample
        // window (chunk_size objects) stays low-cardinality.
        let mut text = String::new();
        for i in 0..8192 {
            text.push_str(&format!("{{\"id\":{i},\"cat\":\"c{}\"}}\n", i % 4));
        }
        for i in 8192..14192 {
            text.push_str(&format!("{{\"id\":{i},\"cat\":\"u-{i}\"}}\n"));
        }
        let p = write_tmp("escape", &text);
        let uri = p.to_str().unwrap();
        let r = reader_keyed(8192, &["cat"]);
        let (_, mut d_spec) = r.open_file_stream_sampled(uri).expect("sampled open");
        let (_, mut d_full) = r.open_file_stream(uri).expect("canonical open");
        assert_eq!(
            all_rows(&mut d_spec),
            all_rows(&mut d_full),
            "escaped decode must read back the canonical values"
        );
        let (n, esc) = d_spec.dict_status().expect("cat was a candidate");
        assert_eq!(n, 1);
        assert!(esc > 0, "crossing DICT_CAP must count an escape");
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn canonical_and_unkeyed_opens_stay_plain() {
        let mut text = String::new();
        for i in 0..300 {
            text.push_str(&format!("{{\"id\":{i},\"region\":\"r{}\"}}\n", i % 3));
        }
        let p = write_tmp("canon", &text);
        let uri = p.to_str().unwrap();
        let r = reader_keyed(64, &["region"]);
        let (_, mut d_full) = r.open_file_stream(uri).expect("canonical open");
        assert_eq!(d_full.dict_status(), None);
        while let Some(cols) = d_full.next_chunk() {
            assert!(cols
                .iter()
                .all(|c| !matches!(c.data(), rivus_core::ColumnData::StrDict(_))));
        }
        let r = reader(64);
        let (_, mut d_spec) = r.open_file_stream_sampled(uri).expect("sampled open");
        assert_eq!(
            d_spec.dict_status(),
            None,
            "no key columns -> no candidates"
        );
        while let Some(cols) = d_spec.next_chunk() {
            assert!(cols
                .iter()
                .all(|c| !matches!(c.data(), rivus_core::ColumnData::StrDict(_))));
        }
        let _ = std::fs::remove_file(&p);
    }
}
