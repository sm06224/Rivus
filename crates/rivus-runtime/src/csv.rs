//! CSV reader with per-column type inference, in two forms:
//!
//! - [`CsvChunker`] — the **streaming** reader for a real file. Bounded memory
//!   regardless of file size: pass 1 streams the file to infer a global schema
//!   (only type flags kept), pass 2 streams it again yielding one chunk of rows
//!   per call. A 1 GB file flows through in ~10 MiB of resident memory.
//! - [`parse_projected`] — the whole-input parser, used for stdin (which can't
//!   be re-read for two-pass inference) and in tests. Reads everything, then
//!   hands out columns.
//!
//! Both share the same inference (`Flags`), split (`split_into`) and column
//! builders, so they produce identical results; the streaming and whole-file
//! paths are kept byte-for-byte equivalent by the stress tests. Quoting is
//! handled just enough for simple fields.
//!
//! Performance: this is a **two-pass, allocation-light** parser. Pass 1 splits
//! each record into borrowed `&str` field slices (no owned `String` per cell)
//! and infers each column's type while scanning. Pass 2 re-splits and parses
//! directly into pre-sized typed column buffers. Only genuine string columns
//! ever allocate per-cell, which closes the column-count throughput gap the
//! Phase-0 baseline exposed (see docs/BENCHMARKS.md). Unquoted records — the
//! overwhelmingly common case — split into pure borrows; quoted records fall
//! back to an owned, escape-aware split.

use rivus_core::{
    Column, ColumnData, DataType, DateTime, DecColumn, Decimal, DtColumn, Field, Schema, StrColumn,
    TimeUnit, Validity,
};
use rivus_ir::CmpOp;
use std::borrow::Cow;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::Arc;

/// Read buffer for the streaming reader. Larger than the 8 KiB default to cut
/// syscalls on big sequential scans (inference and build each stream the file).
const READ_BUF: usize = 256 * 1024;

/// A pushed-down numeric predicate compiled to a raw column index for the
/// reader: `(raw_col, op, rhs)`. Used to skip *building* rows that are
/// definitely out — conservatively (a parse failure never drops a row).
type PreCmp = (usize, CmpOp, f64);

/// Resolve optimizer prefilter `(name, op, rhs)` to compiled `(raw_idx, …)`,
/// keeping only predicates on a kept **numeric** column (so reader-side f64
/// comparison matches the engine's column evaluation).
fn compile_prefilter(
    names: &[String],
    keep: &[usize],
    dtypes: &[DataType],
    pf: &[(String, CmpOp, f64)],
) -> Vec<PreCmp> {
    let mut out = Vec::new();
    for (name, op, rhs) in pf {
        if let Some(raw) = names.iter().position(|n| n == name) {
            if let Some(k) = keep.iter().position(|&c| c == raw) {
                if matches!(dtypes[k], DataType::I64 | DataType::F64) {
                    out.push((raw, *op, *rhs));
                }
            }
        }
    }
    out
}

/// Does a numeric value satisfy `value op rhs`?
#[inline]
fn cmp_num(value: f64, op: CmpOp, rhs: f64) -> bool {
    match op {
        CmpOp::Eq => value == rhs,
        CmpOp::Ne => value != rhs,
        CmpOp::Lt => value < rhs,
        CmpOp::Le => value <= rhs,
        CmpOp::Gt => value > rhs,
        CmpOp::Ge => value >= rhs,
    }
}

/// Conservative reader-side prefilter: skip a row only when a cell parses and
/// the comparison is **definitely false**. A parse failure keeps the row (the
/// authoritative FilterProject downstream decides).
#[inline]
fn row_passes_prefilter(pf: &[PreCmp], line: &str, offsets: &[(usize, usize)]) -> bool {
    for &(idx, op, rhs) in pf {
        let (s, e) = offsets[idx];
        if let Ok(v) = line[s..e].trim().parse::<f64>() {
            if !cmp_num(v, op, rhs) {
                return false;
            }
        }
    }
    true
}

/// Streaming CSV reader: bounded memory regardless of file size.
///
/// Pass 1 streams the whole file once to infer a **global** schema (only
/// per-column type flags are kept — O(1) memory), so the inferred types — and
/// therefore the result — are independent of `chunk_size`, exactly like the
/// whole-file parser. Pass 2 (`next_columns`) re-streams the file and yields one
/// `chunk_size`-row batch of typed columns per call, so a 15 GB file flows
/// through in chunk-sized pieces instead of being slurped into RAM.
pub struct CsvChunker {
    reader: BufReader<File>,
    ncols: usize,
    keep: Vec<usize>,
    dtypes: Vec<DataType>,
    /// Per-kept-column datetime parse spec (design 23): `Some` for a
    /// `:datetime` column (carrying its explicit or auto formats), `None`
    /// otherwise. Aligned to `dtypes`/`keep` order.
    dt_specs: Vec<Option<Arc<DtSpec>>>,
    chunk_size: usize,
    line: String,
    /// Rows skipped in pass 1 for wrong arity (reported once by the source).
    pub bad_rows: usize,
    /// Rows the pushed-down prefilter skipped *building* (definitely-out rows).
    /// Pure accounting for telemetry — the result is unchanged, since the
    /// downstream `FilterProject` would have dropped exactly these rows anyway.
    pub rows_prefiltered: u64,
    /// Per-kept-column count of non-empty cells that failed to parse into the
    /// column's lane and were defaulted to 0 (malformed, or an `i128` overflow
    /// in the decimal lane) — #bugreport ②④. Aligned to `keep`/`dtypes`/the
    /// output schema; surfaced once on exhaustion by the source operator.
    pub parse_failures: Vec<u64>,
    eof: bool,
    /// Current byte offset and an optional end (for streaming one byte range of
    /// the file in a parallel worker). `limit == None` streams to EOF.
    pos: u64,
    limit: Option<u64>,
    /// Compiled pushed-down numeric predicates (skip building failing rows).
    prefilter: Vec<PreCmp>,
    /// Required literal substrings: a raw line lacking any of them is skipped
    /// before splitting (a ripgrep-style superset pre-scan; FilterProject is
    /// still authoritative downstream). Empty = no string pre-scan.
    str_prefilter: Vec<String>,
    /// Per-kept-column inference `(name, type, widened)` for telemetry (A4).
    /// Empty when the schema was declared or sample-inferred.
    inference: Vec<(String, DataType, bool)>,
    /// Field delimiter byte (`b','` for CSV, `b'\t'` for TSV).
    delim: u8,
}

impl CsvChunker {
    /// The per-column inference outcome (A4 telemetry); empty for declared or
    /// sample-inferred schemas.
    pub fn inference(&self) -> &[(String, DataType, bool)] {
        &self.inference
    }
}

impl CsvChunker {
    /// Open `path` for streaming, returning the inferred schema and the reader
    /// positioned just after the header (ready for `next_columns`).
    ///
    /// `preview` trades correctness for latency: instead of streaming the whole
    /// file to infer a global schema, it samples only the first `chunk_size`
    /// rows and seeks back — so a sink-less `open big.csv` preview starts
    /// instantly. Full runs (with a sink) use the global two-pass inference so
    /// types stay chunk-size independent.
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        path: &str,
        allow: Option<&[String]>,
        chunk_size: usize,
        preview: bool,
        prefilter: &[(String, CmpOp, f64)],
        str_prefilter: &[String],
        header: bool,
        declared: Option<&[(String, Option<DataType>)]>,
        dt_formats: &[(String, String)],
        delim: u8,
    ) -> Result<(Schema, CsvChunker), String> {
        if preview {
            return Self::open_preview(
                path,
                allow,
                chunk_size,
                prefilter,
                str_prefilter,
                header,
                declared,
                dt_formats,
                delim,
            );
        }
        // ---- pass 1: infer a global schema by streaming the whole file ----
        let f = File::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
        let mut r = BufReader::with_capacity(READ_BUF, f);
        // `r` is left positioned at the first data row (after the header, or at
        // byte 0 for a header-less file).
        let (names, data_start) = read_header(&mut r, header, declared, delim)?;
        let ncols = names.len();
        if ncols == 0 {
            return Err("CSV has no columns".to_string());
        }
        let keep: Vec<usize> = match allow {
            None => (0..ncols).collect(),
            Some(a) => (0..ncols)
                .filter(|&i| a.iter().any(|n| n == &names[i]))
                .collect(),
        };

        let mut flags: Vec<Flags> = keep.iter().map(|_| Flags::new()).collect();
        let mut bad = 0usize;
        let mut line = String::new();
        let mut offsets: Vec<(usize, usize)> = Vec::with_capacity(ncols);
        loop {
            line.clear();
            if r.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
                break;
            }
            let l = trim_eol(&line);
            if l.trim().is_empty() {
                continue;
            }
            if !observe_line(l, ncols, &keep, &mut flags, &mut offsets, delim) {
                bad += 1;
            }
        }
        let mut dtypes: Vec<DataType> = flags.iter().map(Flags::resolve).collect();
        // Inference outcome (A4 telemetry) — captured before declared types
        // override, so `widened` reflects what the data forced.
        let inference: Vec<(String, DataType, bool)> = keep
            .iter()
            .enumerate()
            .map(|(k, &ci)| (names[ci].clone(), dtypes[k], flags[k].widened()))
            .collect();
        apply_declared_types(&mut dtypes, &keep, declared);
        let dt_specs = build_dt_specs(&names, &keep, &dtypes, dt_formats);

        let mut fields = Vec::with_capacity(keep.len());
        for (k, &ci) in keep.iter().enumerate() {
            fields.push(Field::new(names[ci].clone(), dtypes[k]));
        }
        let schema = Schema::new(fields);
        let pre = compile_prefilter(&names, &keep, &dtypes, prefilter);

        // ---- pass 2 setup: reopen and seek to the first data row ----
        let f2 = File::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
        let mut reader = BufReader::with_capacity(READ_BUF, f2);
        reader
            .seek(SeekFrom::Start(data_start))
            .map_err(|e| e.to_string())?;

        Ok((
            schema,
            CsvChunker {
                reader,
                ncols,
                parse_failures: vec![0; dtypes.len()],
                keep,
                dtypes,
                dt_specs,
                chunk_size: chunk_size.max(1),
                line: String::new(),
                bad_rows: bad,
                rows_prefiltered: 0,
                eof: false,
                prefilter: pre,
                inference,
                str_prefilter: str_prefilter.to_vec(),
                pos: 0,
                limit: None,
                delim,
            },
        ))
    }

    /// Latency-first open: sample the first `chunk_size` rows to infer the
    /// schema, then seek back to the first data row and stream from there.
    #[allow(clippy::too_many_arguments)]
    fn open_preview(
        path: &str,
        allow: Option<&[String]>,
        chunk_size: usize,
        prefilter: &[(String, CmpOp, f64)],
        str_prefilter: &[String],
        header: bool,
        declared: Option<&[(String, Option<DataType>)]>,
        dt_formats: &[(String, String)],
        delim: u8,
    ) -> Result<(Schema, CsvChunker), String> {
        let f = File::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
        let mut reader = BufReader::with_capacity(READ_BUF, f);
        let (names, data_start) = read_header(&mut reader, header, declared, delim)?;
        let ncols = names.len();
        if ncols == 0 {
            return Err("CSV has no columns".to_string());
        }
        let keep: Vec<usize> = match allow {
            None => (0..ncols).collect(),
            Some(a) => (0..ncols)
                .filter(|&i| a.iter().any(|n| n == &names[i]))
                .collect(),
        };

        let mut flags: Vec<Flags> = keep.iter().map(|_| Flags::new()).collect();
        let mut bad = 0usize;
        let mut line = String::new();
        let mut offsets: Vec<(usize, usize)> = Vec::with_capacity(ncols);
        for _ in 0..chunk_size {
            line.clear();
            if reader.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
                break;
            }
            let l = trim_eol(&line);
            if l.trim().is_empty() {
                continue;
            }
            if !observe_line(l, ncols, &keep, &mut flags, &mut offsets, delim) {
                bad += 1;
            }
        }
        let mut dtypes: Vec<DataType> = flags.iter().map(Flags::resolve).collect();
        apply_declared_types(&mut dtypes, &keep, declared);
        let dt_specs = build_dt_specs(&names, &keep, &dtypes, dt_formats);
        let mut fields = Vec::with_capacity(keep.len());
        for (k, &ci) in keep.iter().enumerate() {
            fields.push(Field::new(names[ci].clone(), dtypes[k]));
        }
        let schema = Schema::new(fields);
        let pre = compile_prefilter(&names, &keep, &dtypes, prefilter);

        // Rewind to the first data row and stream from there with this schema.
        reader
            .seek(SeekFrom::Start(data_start))
            .map_err(|e| e.to_string())?;

        Ok((
            schema,
            CsvChunker {
                reader,
                ncols,
                parse_failures: vec![0; dtypes.len()],
                keep,
                dtypes,
                dt_specs,
                chunk_size: chunk_size.max(1),
                line: String::new(),
                bad_rows: bad,
                rows_prefiltered: 0,
                eof: false,
                prefilter: pre,
                inference: Vec::new(),
                str_prefilter: str_prefilter.to_vec(),
                pos: 0,
                limit: None,
                delim,
            },
        ))
    }

    /// Stream one byte range `[start, end)` of the file with an already-inferred
    /// global schema (used by the parallel streaming executor). `start`/`end`
    /// must be newline-aligned offsets into the data region (see `plan_parallel`).
    #[allow(clippy::too_many_arguments)]
    pub fn for_range(
        path: &str,
        dtypes: Vec<DataType>,
        dt_specs: Vec<Option<Arc<DtSpec>>>,
        keep: Vec<usize>,
        ncols: usize,
        start: u64,
        end: u64,
        chunk_size: usize,
        prefilter: Vec<PreCmp>,
        str_prefilter: Vec<String>,
        delim: u8,
    ) -> Result<CsvChunker, String> {
        let mut f = File::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
        f.seek(SeekFrom::Start(start)).map_err(|e| e.to_string())?;
        Ok(CsvChunker {
            reader: BufReader::with_capacity(READ_BUF, f),
            ncols,
            parse_failures: vec![0; dtypes.len()],
            keep,
            dtypes,
            dt_specs,
            chunk_size: chunk_size.max(1),
            line: String::new(),
            bad_rows: 0,
            rows_prefiltered: 0,
            eof: false,
            prefilter,
            inference: Vec::new(),
            str_prefilter,
            pos: start,
            limit: Some(end),
            delim,
        })
    }

    /// Yield the next batch of up to `chunk_size` rows as typed columns, or
    /// `None` at end of file. Malformed rows (wrong arity) are skipped — already
    /// counted in `bad_rows` during pass 1.
    pub fn next_columns(&mut self) -> Option<Vec<Column>> {
        if self.eof {
            return None;
        }
        let mut builders: Vec<ColBuilder> = self
            .dtypes
            .iter()
            .enumerate()
            .map(|(k, d)| {
                ColBuilder::with_capacity_dt(*d, self.chunk_size, self.dt_specs[k].clone())
            })
            .collect();
        // Reused field byte-ranges (no per-row allocation on the unquoted fast
        // path); quoted records fall back to an owned split.
        let mut offsets: Vec<(usize, usize)> = Vec::with_capacity(self.ncols);
        let mut got = 0usize;
        while got < self.chunk_size {
            // Stop at the worker's byte range end (lines never straddle a
            // newline-aligned boundary, so the last line ends exactly at it).
            if matches!(self.limit, Some(end) if self.pos >= end) {
                self.eof = true;
                break;
            }
            self.line.clear();
            let n = match self.reader.read_line(&mut self.line) {
                Ok(0) => {
                    self.eof = true;
                    break;
                }
                Ok(n) => n,
                Err(_) => {
                    self.eof = true;
                    break;
                }
            };
            self.pos += n as u64;
            let l = trim_eol(&self.line);
            if l.trim().is_empty() {
                continue;
            }
            // String prefilter: a required literal substring is missing from the
            // raw line, so no field can satisfy the predicate — skip before
            // splitting (ripgrep-style; FilterProject stays authoritative).
            if !self.str_prefilter.is_empty()
                && !self.str_prefilter.iter().all(|n| l.contains(n.as_str()))
            {
                self.rows_prefiltered += 1;
                continue;
            }
            if split_offsets(l, &mut offsets, self.delim) {
                if offsets.len() != self.ncols {
                    continue;
                }
                // Pushed-down prefilter: skip building rows that are definitely
                // out (conservative; the downstream FilterProject is final).
                if !self.prefilter.is_empty() && !row_passes_prefilter(&self.prefilter, l, &offsets)
                {
                    self.rows_prefiltered += 1;
                    continue;
                }
                for (k, &ci) in self.keep.iter().enumerate() {
                    let (s, e) = offsets[ci];
                    if builders[k].push(&l[s..e], false) {
                        self.parse_failures[k] += 1;
                    }
                }
            } else {
                // Quoted records take the owned slow path and skip the prefilter
                // (rare; FilterProject still filters them downstream).
                let fields = split_record_q(l, self.delim);
                if fields.len() != self.ncols {
                    continue;
                }
                for (k, &ci) in self.keep.iter().enumerate() {
                    if builders[k].push(&fields[ci].0, fields[ci].1) {
                        self.parse_failures[k] += 1;
                    }
                }
            }
            got += 1;
        }
        if got == 0 {
            return None;
        }
        Some(builders.iter_mut().map(ColBuilder::finish).collect())
    }
}

/// A plan for streaming-parallel CSV reading: a global schema plus the
/// newline-aligned byte ranges each worker streams (covering the data region
/// exactly once). Inference itself runs in parallel over the same ranges.
pub struct CsvParallelPlan {
    pub schema: Schema,
    pub dtypes: Vec<DataType>,
    /// Per-kept-column datetime parse spec (design 23); shared with workers.
    pub dt_specs: Vec<Option<Arc<DtSpec>>>,
    pub keep: Vec<usize>,
    pub ncols: usize,
    pub ranges: Vec<(u64, u64)>,
    pub bad_rows: usize,
    /// Compiled pushed-down prefilter (raw col index, op, rhs) for each worker.
    pub prefilter: Vec<PreCmp>,
    /// Required literal substrings for the raw-line pre-scan (#35): each worker
    /// skips a raw line lacking one before splitting it (FilterProject stays
    /// authoritative). Empty = no string pre-scan.
    pub str_prefilter: Vec<String>,
}

/// Build a [`CsvParallelPlan`]: read the header, snap `nthreads` byte ranges to
/// newline boundaries, then infer the global column types by streaming those
/// ranges in parallel and merging the per-range type flags. O(1) memory.
#[allow(clippy::too_many_arguments)]
pub fn plan_parallel(
    path: &str,
    allow: Option<&[String]>,
    nthreads: usize,
    prefilter: &[(String, CmpOp, f64)],
    str_prefilter: &[String],
    header: bool,
    declared: Option<&[(String, Option<DataType>)]>,
    dt_formats: &[(String, String)],
    delim: u8,
) -> Result<CsvParallelPlan, String> {
    let file_len = std::fs::metadata(path)
        .map_err(|e| format!("cannot stat '{path}': {e}"))?
        .len();

    // Header → column names, kept indices, and the first data byte offset.
    let f = File::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
    let mut r = BufReader::with_capacity(READ_BUF, f);
    let (names, data_start) = read_header(&mut r, header, declared, delim)?;
    let ncols = names.len();
    if ncols == 0 {
        return Err("CSV has no columns".to_string());
    }
    let keep: Vec<usize> = match allow {
        None => (0..ncols).collect(),
        Some(a) => (0..ncols)
            .filter(|&i| a.iter().any(|n| n == &names[i]))
            .collect(),
    };

    let ranges = snap_ranges(&mut r, data_start, file_len, nthreads.max(1))?;

    // Infer types per range in parallel, then merge the flags.
    let kept = keep.clone();
    let infers: Vec<(Vec<Flags>, usize)> = std::thread::scope(|s| {
        let handles: Vec<_> = ranges
            .iter()
            .map(|&(a, b)| {
                let kref = &kept;
                s.spawn(move || infer_range(path, kref, ncols, a, b, delim))
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let mut flags: Vec<Flags> = keep.iter().map(|_| Flags::new()).collect();
    let mut bad = 0usize;
    for (fs, b) in &infers {
        bad += *b;
        for (k, f) in fs.iter().enumerate() {
            flags[k].merge(f);
        }
    }
    let mut dtypes: Vec<DataType> = flags.iter().map(Flags::resolve).collect();
    apply_declared_types(&mut dtypes, &keep, declared);
    let dt_specs = build_dt_specs(&names, &keep, &dtypes, dt_formats);
    let mut fields = Vec::with_capacity(keep.len());
    for (k, &ci) in keep.iter().enumerate() {
        fields.push(Field::new(names[ci].clone(), dtypes[k]));
    }
    let pre = compile_prefilter(&names, &keep, &dtypes, prefilter);

    Ok(CsvParallelPlan {
        schema: Schema::new(fields),
        dtypes,
        dt_specs,
        keep,
        ncols,
        ranges,
        bad_rows: bad,
        prefilter: pre,
        str_prefilter: str_prefilter.to_vec(),
    })
}

/// Snap `n` evenly-spaced offsets in `[data_start, file_len)` to the byte just
/// after the next newline, yielding contiguous line-aligned ranges.
fn snap_ranges(
    r: &mut BufReader<File>,
    data_start: u64,
    file_len: u64,
    n: usize,
) -> Result<Vec<(u64, u64)>, String> {
    let mut bounds = vec![data_start];
    let span = file_len.saturating_sub(data_start);
    let mut scratch = String::new();
    for i in 1..n {
        let target = data_start + span * (i as u64) / (n as u64);
        if target <= *bounds.last().unwrap() {
            continue;
        }
        r.seek(SeekFrom::Start(target)).map_err(|e| e.to_string())?;
        scratch.clear();
        let consumed = r.read_line(&mut scratch).map_err(|e| e.to_string())?;
        let next = (target + consumed as u64).min(file_len);
        if next > *bounds.last().unwrap() && next < file_len {
            bounds.push(next);
        }
    }
    bounds.push(file_len);
    Ok(bounds.windows(2).map(|w| (w[0], w[1])).collect())
}

/// Infer column type flags over one byte range (streaming, O(1) memory).
fn infer_range(
    path: &str,
    keep: &[usize],
    ncols: usize,
    start: u64,
    end: u64,
    delim: u8,
) -> (Vec<Flags>, usize) {
    let mut flags: Vec<Flags> = keep.iter().map(|_| Flags::new()).collect();
    let mut bad = 0usize;
    let f = match File::open(path) {
        Ok(f) => f,
        Err(_) => return (flags, bad),
    };
    let mut r = BufReader::with_capacity(READ_BUF, f);
    if r.seek(SeekFrom::Start(start)).is_err() {
        return (flags, bad);
    }
    let mut pos = start;
    let mut line = String::new();
    let mut offsets: Vec<(usize, usize)> = Vec::with_capacity(ncols);
    while pos < end {
        line.clear();
        match r.read_line(&mut line) {
            Ok(0) => break,
            Ok(n) => pos += n as u64,
            Err(_) => break,
        }
        let l = trim_eol(&line);
        if l.trim().is_empty() {
            continue;
        }
        if !observe_line(l, ncols, keep, &mut flags, &mut offsets, delim) {
            bad += 1;
        }
    }
    (flags, bad)
}

/// Read the column names and the byte offset of the first data row, leaving the
/// reader positioned there. Names come from a `declared` schema if given (which
/// also names a header-less file); else from the header line (`header`); else
/// `c0, c1, …` for a header-less file. A header line is always consumed when
/// `header`, even if `declared` overrides its names.
fn read_header(
    r: &mut BufReader<File>,
    header: bool,
    declared: Option<&[(String, Option<DataType>)]>,
    delim: u8,
) -> Result<(Vec<String>, u64), String> {
    let mut first = String::new();
    let n = r.read_line(&mut first).map_err(|e| e.to_string())?;
    if n == 0 {
        return Err("empty CSV".to_string());
    }
    // A UTF-8 BOM (`EF BB BF`) at the very start of the file would otherwise
    // leak into the first column name (`﻿id`). Strip it from the header line.
    // The byte offset `n` is unchanged: the BOM bytes were consumed as part of
    // this first line, so the data still starts at `n` for a header file.
    let first_trimmed = strip_bom(&first);
    if let Some(d) = declared {
        let names = d.iter().map(|(nm, _)| nm.clone()).collect();
        if header {
            Ok((names, n as u64)) // consume the header line, but use declared names
        } else {
            // Header-less + declared names: the first line is data. Seek past a
            // BOM if present so it doesn't corrupt the first cell of row 0.
            let start = bom_len(&first) as u64;
            r.seek(SeekFrom::Start(start)).map_err(|e| e.to_string())?;
            Ok((names, start))
        }
    } else if header {
        Ok((split_owned(trim_eol(first_trimmed), delim), n as u64))
    } else {
        let ncols = split_owned(trim_eol(first_trimmed), delim).len();
        let names = (0..ncols).map(|i| format!("c{i}")).collect();
        let start = bom_len(&first) as u64;
        r.seek(SeekFrom::Start(start)).map_err(|e| e.to_string())?;
        Ok((names, start))
    }
}

/// Length in bytes of a leading UTF-8 BOM (`EF BB BF`), else 0.
fn bom_len(s: &str) -> usize {
    if s.as_bytes().starts_with(&[0xEF, 0xBB, 0xBF]) {
        3
    } else {
        0
    }
}

/// Strip a leading UTF-8 BOM from a string slice (no-op if absent).
fn strip_bom(s: &str) -> &str {
    s.strip_prefix('\u{feff}').unwrap_or(s)
}

/// Override inferred dtypes with any types declared at `open (col:type …)`.
fn apply_declared_types(
    dtypes: &mut [DataType],
    keep: &[usize],
    declared: Option<&[(String, Option<DataType>)]>,
) {
    if let Some(d) = declared {
        for (k, &ci) in keep.iter().enumerate() {
            if let Some((_, Some(t))) = d.get(ci) {
                dtypes[k] = *t;
            }
        }
    }
}

/// Strip a trailing `\n` or `\r\n` (mirrors `str::lines` semantics).
fn trim_eol(s: &str) -> &str {
    s.strip_suffix('\n')
        .map(|s| s.strip_suffix('\r').unwrap_or(s))
        .unwrap_or(s)
}

pub struct CsvData {
    pub schema: Schema,
    pub columns: Vec<Column>,
    /// Number of rows skipped because their arity didn't match the header.
    pub bad_rows: usize,
}

/// Parse CSV text into inferred columns, optionally restricting to a subset of
/// columns by name (`allow`). Never panics on malformed rows: rows with the
/// wrong field count are counted in `bad_rows` and skipped (continue-first).
///
/// Columns not in `allow` are still split past (so record boundaries and arity
/// checks are unaffected) but are never inferred, parsed, or allocated — the
/// projection-pushdown fast path. `allow = None` keeps every column.
pub fn parse_projected(text: &str, allow: Option<&[String]>, delim: u8) -> Result<CsvData, String> {
    // Strip a leading UTF-8 BOM so it doesn't leak into the first column name.
    let text = strip_bom(text);
    let mut lines = text.lines();
    let header = match lines.next() {
        Some(h) => h,
        None => return Err("empty CSV".to_string()),
    };
    let names: Vec<String> = split_owned(header, delim);
    let ncols = names.len();
    if ncols == 0 {
        return Err("CSV header has no columns".to_string());
    }

    // Indices of the columns we will actually build (in header order).
    let keep: Vec<usize> = match allow {
        None => (0..ncols).collect(),
        Some(a) => (0..ncols)
            .filter(|&i| a.iter().any(|n| n == &names[i]))
            .collect(),
    };

    let body = &text[header_end(text)..];

    // Parse serially for small inputs; split across threads for large ones.
    // Both paths produce byte-identical results (row order is preserved); the
    // parallel path is exercised by the stress tests (20k–50k rows).
    let (dtypes, columns, bad_rows) = match choose_threads(body.len()) {
        1 => parse_serial(body, ncols, &keep, delim),
        n => parse_parallel(body, ncols, &keep, n, delim),
    };

    let mut fields = Vec::with_capacity(keep.len());
    for (k, &ci) in keep.iter().enumerate() {
        fields.push(Field::new(names[ci].clone(), dtypes[k]));
    }

    Ok(CsvData {
        schema: Schema::new(fields),
        columns,
        bad_rows,
    })
}

/// How many threads to use for a body of `body_len` bytes. Sequential below a
/// threshold (thread spawn isn't worth it); otherwise the machine parallelism,
/// capped.
fn choose_threads(body_len: usize) -> usize {
    const MIN_PARALLEL_BYTES: usize = 512 * 1024;
    if body_len < MIN_PARALLEL_BYTES {
        return 1;
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .clamp(1, 8)
}

/// Result of inferring types over one slice.
struct Inferred {
    flags: Vec<Flags>,
    nrows: usize,
    bad: usize,
}

fn parse_serial(
    body: &str,
    ncols: usize,
    keep: &[usize],
    delim: u8,
) -> (Vec<DataType>, Vec<Column>, usize) {
    let inf = infer_slice(body, ncols, keep, delim);
    let dtypes: Vec<DataType> = inf.flags.iter().map(Flags::resolve).collect();
    let columns = build_slice(body, ncols, keep, &dtypes, inf.nrows, delim);
    (dtypes, columns, inf.bad)
}

fn parse_parallel(
    body: &str,
    ncols: usize,
    keep: &[usize],
    nthreads: usize,
    delim: u8,
) -> (Vec<DataType>, Vec<Column>, usize) {
    let slices = split_lines(body, nthreads);
    if slices.len() <= 1 {
        return parse_serial(body, ncols, keep, delim);
    }

    // Phase 1: infer types per slice, in parallel.
    let infers: Vec<Inferred> = std::thread::scope(|s| {
        let handles: Vec<_> = slices
            .iter()
            .map(|&sl| s.spawn(move || infer_slice(sl, ncols, keep, delim)))
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    // Reduce per-slice flags to global column types.
    let mut flags: Vec<Flags> = keep.iter().map(|_| Flags::new()).collect();
    let mut bad = 0usize;
    for inf in &infers {
        bad += inf.bad;
        for (k, f) in inf.flags.iter().enumerate() {
            flags[k].merge(f);
        }
    }
    let dtypes: Vec<DataType> = flags.iter().map(Flags::resolve).collect();

    // Phase 2: build each slice's columns in parallel, then concatenate in order.
    let parts: Vec<Vec<Column>> = std::thread::scope(|s| {
        let dtypes = &dtypes;
        let handles: Vec<_> = slices
            .iter()
            .zip(&infers)
            .map(|(&sl, inf)| {
                s.spawn(move || build_slice(sl, ncols, keep, dtypes, inf.nrows, delim))
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let columns = parts
        .into_iter()
        .reduce(|mut acc, part| {
            for (a, b) in acc.iter_mut().zip(part) {
                append_column(a, b);
            }
            acc
        })
        .unwrap_or_default();

    (dtypes, columns, bad)
}

/// Infer column types (for kept columns) and count valid / bad rows in a slice.
fn infer_slice(slice: &str, ncols: usize, keep: &[usize], delim: u8) -> Inferred {
    let mut flags: Vec<Flags> = keep.iter().map(|_| Flags::new()).collect();
    let mut scratch: Vec<(Cow<str>, bool)> = Vec::with_capacity(ncols);
    let mut nrows = 0usize;
    let mut bad = 0usize;
    for line in slice.lines() {
        if line.trim().is_empty() {
            continue;
        }
        scratch.clear();
        split_into(line, &mut scratch, delim);
        if scratch.len() != ncols {
            bad += 1;
            continue;
        }
        for (k, &ci) in keep.iter().enumerate() {
            flags[k].observe(scratch[ci].0.as_ref());
        }
        nrows += 1;
    }
    Inferred { flags, nrows, bad }
}

/// Build the kept columns of a slice into pre-sized typed buffers.
fn build_slice(
    slice: &str,
    ncols: usize,
    keep: &[usize],
    dtypes: &[DataType],
    cap: usize,
    delim: u8,
) -> Vec<Column> {
    let mut builders: Vec<ColBuilder> = dtypes
        .iter()
        .map(|d| ColBuilder::with_capacity(*d, cap))
        .collect();
    let mut scratch: Vec<(Cow<str>, bool)> = Vec::with_capacity(ncols);
    for line in slice.lines() {
        if line.trim().is_empty() {
            continue;
        }
        scratch.clear();
        split_into(line, &mut scratch, delim);
        if scratch.len() != ncols {
            continue; // identical skip rule as inference
        }
        for (k, &ci) in keep.iter().enumerate() {
            builders[k].push(scratch[ci].0.as_ref(), scratch[ci].1);
        }
    }
    builders.iter_mut().map(ColBuilder::finish).collect()
}

/// Split `body` into at most `n` non-overlapping, line-aligned slices that
/// together cover it (each line lies wholly within exactly one slice).
fn split_lines(body: &str, n: usize) -> Vec<&str> {
    let bytes = body.as_bytes();
    let len = bytes.len();
    if len == 0 {
        return Vec::new();
    }
    let mut idx = Vec::with_capacity(n + 1);
    idx.push(0usize);
    for i in 1..n {
        let mut p = len * i / n;
        while p < len && bytes[p] != b'\n' {
            p += 1;
        }
        if p < len {
            p += 1; // start at the byte after the newline
        }
        idx.push(p.min(len));
    }
    idx.push(len);

    let mut out = Vec::with_capacity(n);
    for w in idx.windows(2) {
        if w[0] < w[1] {
            out.push(&body[w[0]..w[1]]);
        }
    }
    out
}

/// Append column `b` onto `a` (same dtype guaranteed by global inference).
fn append_column(a: &mut Column, b: Column) {
    // Concatenate both the value lane and the validity bitmap (design 26): a
    // null in a later slice stays null after the merge. All-valid slices keep
    // the zero-cost path (`Validity::append` no-ops when both sides are None).
    a.append(&b);
}

/// Byte offset just past the first line terminator (handles `\n` and `\r\n`).
fn header_end(text: &str) -> usize {
    match text.find('\n') {
        Some(i) => i + 1,
        None => text.len(),
    }
}

/// Running per-column type inference. Short-circuits parse attempts once a
/// candidate lane is ruled out.
struct Flags {
    any: bool,
    all_int: bool,
    all_float: bool,
    all_bool: bool,
    all_datetime: bool,
    all_date: bool,
    all_time: bool,
}

impl Flags {
    fn new() -> Self {
        Flags {
            any: false,
            all_int: true,
            all_float: true,
            all_bool: true,
            all_datetime: true,
            all_date: true,
            all_time: true,
        }
    }

    fn observe(&mut self, cell: &str) {
        let c = cell.trim();
        if c.is_empty() {
            return;
        }
        self.any = true;
        // Temporal lanes (#92): a cell that doesn't match drops the lane. Numeric
        // columns clear these on their first cell (a number isn't a date/time),
        // and `resolve` gives numeric precedence, so an integer is never
        // mis-inferred as a date. Cheap: a flag, once false, is never re-probed.
        if self.all_datetime && rivus_core::DateTime::parse_auto(c, TimeUnit::Sec).is_none() {
            self.all_datetime = false;
        }
        if self.all_date && rivus_core::Date::parse(c).is_none() {
            self.all_date = false;
        }
        if self.all_time && rivus_core::TimeOfDay::parse_at(c, TimeUnit::Sec).is_none() {
            self.all_time = false;
        }
        // Fast path: while the column is still all-integer, an integer cell is
        // also a float, so skip the redundant f64 parse — but it is never a
        // bool, so clear that lane.
        if self.all_int {
            // SWAR fast path; `None` (edges/overflow) falls back to std so the
            // is-integer decision stays exactly `parse::<i64>().is_ok()`.
            if parse_i64_fast(c).is_some() || c.parse::<i64>().is_ok() {
                self.all_bool = false;
                return;
            }
            self.all_int = false;
        }
        if self.all_float && c.parse::<f64>().is_err() {
            self.all_float = false;
        }
        if self.all_bool && !matches!(c, "true" | "false") {
            self.all_bool = false;
        }
    }

    /// Combine another slice's inference into this one (parallel reduce).
    fn merge(&mut self, other: &Flags) {
        self.any |= other.any;
        self.all_int &= other.all_int;
        self.all_float &= other.all_float;
        self.all_bool &= other.all_bool;
        self.all_datetime &= other.all_datetime;
        self.all_date &= other.all_date;
        self.all_time &= other.all_time;
    }

    fn resolve(&self) -> DataType {
        if !self.any {
            DataType::Str
        } else if self.all_int {
            DataType::I64
        } else if self.all_float {
            DataType::F64
        } else if self.all_bool {
            DataType::Bool
        } else if self.all_date {
            // Date-only `yyyy-MM-dd` columns prefer the date lane over datetime
            // (which also matches `yyyy-MM-dd` at midnight). #92.
            DataType::Date
        } else if self.all_time {
            DataType::Time
        } else if self.all_datetime {
            DataType::DateTime {
                unit: TimeUnit::Sec,
            }
        } else {
            DataType::Str
        }
    }

    /// Did inference "widen" a numeric column? True only for the genuinely
    /// interesting case: all-float but not all-int — i.e. it looked integer
    /// until a later cell forced F64. A purely textual column is not "widened",
    /// it's just `Str`. Pure observation (telemetry); does not affect `resolve`.
    fn widened(&self) -> bool {
        self.any && self.all_float && !self.all_int
    }
}

/// Parse spec for a `:datetime` column (design 23): the resolution `unit` and an
/// ordered list of candidate strptime formats. An explicit `:datetime("fmt")`
/// has a single format; a bare `:datetime` carries the auto-infer list, tried in
/// order per cell (first match wins). Shared across chunks via `Arc` so the
/// per-chunk builders don't re-clone the format strings.
#[derive(Debug)]
pub struct DtSpec {
    unit: TimeUnit,
    formats: Vec<String>,
}

/// Build the per-kept-column datetime parse specs (design 23): `Some` for each
/// `:datetime` column (with its explicit `dt_formats` entry, or the auto list),
/// `None` otherwise. Aligned to `keep`/`dtypes` order.
fn build_dt_specs(
    names: &[String],
    keep: &[usize],
    dtypes: &[DataType],
    dt_formats: &[(String, String)],
) -> Vec<Option<Arc<DtSpec>>> {
    keep.iter()
        .enumerate()
        .map(|(k, &ci)| match dtypes[k] {
            DataType::DateTime { unit } => {
                let spec = match dt_formats.iter().find(|(c, _)| *c == names[ci]) {
                    Some((_, fmt)) => DtSpec {
                        unit,
                        formats: vec![fmt.clone()],
                    },
                    None => DtSpec::auto(unit),
                };
                Some(Arc::new(spec))
            }
            _ => None,
        })
        .collect()
}

impl DtSpec {
    fn auto(unit: TimeUnit) -> Self {
        DtSpec {
            unit,
            // The canonical auto-infer list lives in core, so the reader and the
            // predicate-literal path agree on what a given text means (design 23).
            formats: DateTime::AUTO_FORMATS
                .iter()
                .map(|s| s.to_string())
                .collect(),
        }
    }

    /// Parse one cell to epoch ticks, trying each candidate format in order.
    /// `None` when a (non-empty) cell matches no format, so the caller defaults
    /// to `0` (epoch) *and* counts the failure for the error stream — the same
    /// default-on-parse-failure the int/float/decimal lanes use, now equally
    /// observable (continue-first + Observable First; design 23, #80).
    #[inline]
    fn parse_opt(&self, cell: &str) -> Option<i64> {
        // Normalise a trailing ISO timezone / fractional second (#93) so the
        // reader accepts the same variants as `DateTime::parse_auto`. For an
        // explicit fixed-width format the digit-only cell is unchanged.
        let (s, offset_secs) = DateTime::normalize_iso(cell);
        for fmt in &self.formats {
            if let Some(dt) = DateTime::parse_with_format(s, fmt, self.unit) {
                return Some(dt.ticks - offset_secs * self.unit.per_sec());
            }
        }
        None
    }
}

/// A typed, pre-sized column value lane (the dense backing buffer).
enum Lane {
    Bool(Vec<bool>),
    I64(Vec<i64>),
    F64(Vec<f64>),
    /// Exact fixed-point lane: unscaled i128 values at a fixed column scale.
    Dec(Vec<i128>, u8),
    /// Datetime lane: epoch ticks at the spec's unit, parsed via the spec's
    /// candidate formats (design 23).
    DateTime(Vec<i64>, Arc<DtSpec>),
    /// Duration lane: signed tick spans at a fixed unit, parsed from the human
    /// `[-][Nd ]HH:MM:SS[.frac]` form (design 23 / #57).
    Duration(Vec<i64>, TimeUnit),
    /// Date lane: i32 epoch-day, parsed from ISO `yyyy-MM-dd` (#58).
    Date(Vec<i32>),
    /// Time-of-day lane: i64 ticks since midnight, parsed from `HH:mm:ss[.frac]`
    /// (#58, MVP `Sec`).
    Time(Vec<i64>),
    Str(StrColumn),
}

/// A typed, pre-sized column accumulator that also tracks **per-row validity**
/// (the null model; design 26 §26.3). A null row keeps a type-default backing
/// value in the lane; `valid[row] = false` records that it is missing, so
/// `null` / empty-`""` / `0` stay distinct.
struct ColBuilder {
    lane: Lane,
    /// Per-row validity, tracked **lazily**: it stays empty (no allocation, no
    /// per-cell push) while every cell so far is valid — the common case — and
    /// is only materialized once the first null appears (back-filling the rows
    /// before it as valid). So an all-valid column pays *nothing* here, keeping
    /// the reader's hot path at its pre-null-model cost (the zero-cost promise of
    /// design 26 §26.1 extends to construction, not just representation).
    valid: Vec<bool>,
    /// Rows pushed so far (drives the lazy back-fill above).
    len: usize,
}

impl ColBuilder {
    fn with_capacity(dtype: DataType, cap: usize) -> Self {
        Self::with_capacity_dt(dtype, cap, None)
    }

    /// Like [`with_capacity`], but a `:datetime` column may carry an explicit
    /// parse spec (from `:datetime("fmt")`); `None` falls back to the auto list.
    fn with_capacity_dt(dtype: DataType, cap: usize, dt_spec: Option<Arc<DtSpec>>) -> Self {
        let lane = match dtype {
            DataType::Bool => Lane::Bool(Vec::with_capacity(cap)),
            DataType::I64 => Lane::I64(Vec::with_capacity(cap)),
            DataType::F64 => Lane::F64(Vec::with_capacity(cap)),
            DataType::Decimal { scale } => Lane::Dec(Vec::with_capacity(cap), scale),
            DataType::DateTime { unit } => Lane::DateTime(
                Vec::with_capacity(cap),
                dt_spec.unwrap_or_else(|| Arc::new(DtSpec::auto(unit))),
            ),
            DataType::Duration { unit } => Lane::Duration(Vec::with_capacity(cap), unit),
            DataType::Date => Lane::Date(Vec::with_capacity(cap)),
            DataType::Time => Lane::Time(Vec::with_capacity(cap)),
            // Estimate ~8 bytes per string cell for the backing byte buffer.
            _ => Lane::Str(StrColumn::with_capacity(cap, cap * 8)),
        };
        ColBuilder {
            lane,
            // No pre-allocation: an all-valid column never touches `valid`.
            valid: Vec::new(),
            len: 0,
        }
    }

    /// Record one row's validity, materializing the bitmap lazily (see
    /// [`ColBuilder::valid`]): while all cells are valid this is a single
    /// counter bump; the first null back-fills the prior rows as valid.
    #[inline]
    fn record(&mut self, valid: bool) {
        if !self.valid.is_empty() {
            self.valid.push(valid);
        } else if !valid {
            self.valid = vec![true; self.len];
            self.valid.push(false);
        }
        self.len += 1;
    }

    /// Parse one cell into the lane and record its validity (design 26 §26.3),
    /// returning `true` only if a **non-empty** cell could not be parsed.
    ///
    /// Null-ification rules:
    /// - An **empty** cell is *missing* → `null` (validity = 0), on every lane
    ///   (numeric too: `0` is no longer conflated with blank — this is what
    ///   unblocks BUG-A). It is **not** a parse failure, so it never reports.
    /// - A **non-empty** cell that fails to parse in a typed lane → `null`
    ///   (validity = 0) **and** reports `true` (surfaced as "… set to null",
    ///   #80 reworded). The backing byte stays the type default.
    /// - `Str` keeps an empty cell as a real `""` **only when it was quoted**
    ///   (`a,"",b`); an unquoted empty (`a,,b`) is `null`, like every other lane.
    #[inline]
    fn push(&mut self, cell: &str, quoted: bool) -> bool {
        let t = cell.trim();
        let (valid, fail) = match &mut self.lane {
            Lane::Bool(v) => {
                v.push(t == "true");
                (!t.is_empty(), false)
            }
            Lane::I64(v) => match parse_i64_fast(t).or_else(|| t.parse::<i64>().ok()) {
                Some(n) => {
                    v.push(n);
                    (true, false)
                }
                None => {
                    v.push(0);
                    (false, !t.is_empty())
                }
            },
            Lane::F64(v) => match t.parse::<f64>() {
                Ok(n) => {
                    v.push(n);
                    (true, false)
                }
                Err(_) => {
                    v.push(0.0);
                    (false, !t.is_empty())
                }
            },
            // Exact decimal text → unscaled i128 (no f64). A malformed cell or
            // i128 overflow → null (continue-first; §21.7), reported on the
            // error stream (#bugreport ②④).
            Lane::Dec(v, scale) => match Decimal::parse_scaled(t, *scale) {
                Some(d) => {
                    v.push(d.unscaled);
                    (true, false)
                }
                None => {
                    v.push(0);
                    (false, !t.is_empty())
                }
            },
            // Datetime text → epoch ticks; a malformed non-empty cell → null,
            // reported on the error stream, matching the other lanes (#80).
            Lane::DateTime(v, spec) => match spec.parse_opt(cell) {
                Some(ticks) => {
                    v.push(ticks);
                    (true, false)
                }
                None => {
                    v.push(0);
                    (false, !t.is_empty())
                }
            },
            // Duration text → exact i64 ticks; a malformed non-empty cell → null
            // (continue-first; #57, surfaced in #80).
            Lane::Duration(v, unit) => match rivus_core::Duration::parse_at(cell, *unit) {
                Some(d) => {
                    v.push(d.ticks);
                    (true, false)
                }
                None => {
                    v.push(0);
                    (false, !t.is_empty())
                }
            },
            // Date text → i32 epoch-day; a malformed non-empty cell → null
            // (continue-first + never-silent; #58).
            Lane::Date(v) => match rivus_core::Date::parse(cell) {
                Some(d) => {
                    v.push(d.epoch_day);
                    (true, false)
                }
                None => {
                    v.push(0);
                    (false, !t.is_empty())
                }
            },
            // Time-of-day text → i64 ticks; a malformed non-empty cell → null
            // (continue-first + never-silent; #58).
            Lane::Time(v) => {
                match rivus_core::TimeOfDay::parse_at(cell, rivus_core::TimeUnit::Sec) {
                    Some(tod) => {
                        v.push(tod.ticks);
                        (true, false)
                    }
                    None => {
                        v.push(0);
                        (false, !t.is_empty())
                    }
                }
            }
            // Empty unquoted `Str` is `null`; quoted `""` is a real empty string;
            // anything non-empty is a real value (design 26 §26.3).
            Lane::Str(v) => {
                v.push(cell);
                (!cell.is_empty() || quoted, false)
            }
        };
        self.record(valid);
        fail
    }

    fn finish(&mut self) -> Column {
        let data = match &mut self.lane {
            Lane::Bool(v) => ColumnData::Bool(std::mem::take(v)),
            Lane::I64(v) => ColumnData::I64(std::mem::take(v)),
            Lane::F64(v) => ColumnData::F64(std::mem::take(v)),
            Lane::Dec(v, scale) => ColumnData::Dec(DecColumn {
                unscaled: std::mem::take(v),
                scale: *scale,
            }),
            Lane::DateTime(v, spec) => ColumnData::DateTime(DtColumn {
                ticks: std::mem::take(v),
                unit: spec.unit,
            }),
            Lane::Duration(v, unit) => ColumnData::Duration(rivus_core::DurColumn {
                ticks: std::mem::take(v),
                unit: *unit,
            }),
            Lane::Date(v) => ColumnData::Date(std::mem::take(v)),
            Lane::Time(v) => ColumnData::Time(std::mem::take(v)),
            Lane::Str(v) => ColumnData::Str(std::mem::take(v)),
        };
        // Empty `valid` ⇒ no null ever occurred ⇒ the zero-cost all-valid form.
        let validity = if self.valid.is_empty() {
            Validity::all_valid()
        } else {
            Validity::from_bits(&self.valid)
        };
        self.valid = Vec::new();
        self.len = 0;
        Column::new(data, validity)
    }
}

/// Split an unquoted record into field byte-ranges `(start, end)` into `out`
/// (cleared first), allocating nothing — `out` is reused across rows. Returns
/// `false` when the line contains a `"` (the caller takes the owned slow path).
// SWAR (SIMD-within-a-register) byte search: process 8 bytes per step with
// plain u64 arithmetic — no `core::arch`, no feature gate, host-endian
// independent (words are read little-endian so byte `i` maps to bits
// `i*8..i*8+7`).
const SWAR_LO: u64 = 0x0101_0101_0101_0101;
const SWAR_HI: u64 = 0x8080_8080_8080_8080;
const SWAR_LO7: u64 = 0x7F7F_7F7F_7F7F_7F7F; // !SWAR_HI

/// Broadcast byte `b` into every lane of a u64.
#[inline(always)]
fn swar_splat(b: u8) -> u64 {
    SWAR_LO.wrapping_mul(b as u64)
}

/// For each byte of `word` equal to the byte broadcast in `splat`, set that
/// lane's high bit (`0x80`) and clear the rest — **exactly one bit per match,
/// with no cross-byte contamination**, so `trailing_zeros() >> 3` yields the
/// matching byte index and `m &= m - 1` advances to the next.
///
/// The naive `(x - LO) & ~x & HI` zero-byte trick is only reliable as a
/// *boolean* ("any match?"); its per-byte bits are corrupted by subtraction
/// borrows (a zero byte followed by a `0x01` lane false-positives), which makes
/// it wrong for *locating* matches. This borrow-free variant is exact:
/// `(b & 0x7F) + 0x7F` stays ≤ `0xFE`, so no carry crosses a byte boundary.
#[inline(always)]
fn swar_eq_mask(word: u64, splat: u64) -> u64 {
    let t = word ^ splat; // 0x00 lanes where the byte matches
                          // 0x80 per lane iff that lane is non-zero (carry-free), then flip so 0x80
                          // marks the matching (zero) lanes.
    let nonzero = ((t & SWAR_LO7).wrapping_add(SWAR_LO7) | t) & SWAR_HI;
    nonzero ^ SWAR_HI
}

/// Split an unquoted record into field byte-ranges. Dispatches to a SIMD
/// (AVX2, 32 bytes/step) scan when the host supports it, else the SWAR
/// (8 bytes/step, std-only) scan — both byte-identical to a scalar split:
/// identical delimiter offsets and the identical quote-bail decision (returns
/// `false`, `out` partially filled, when the line contains a `"`). #71.
#[inline]
fn split_offsets(line: &str, out: &mut Vec<(usize, usize)>, delim: u8) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        // `is_x86_feature_detected!` memoizes, so this is a cheap cached branch.
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: only called after confirming the CPU supports AVX2.
            return unsafe { split_offsets_avx2(line, out, delim) };
        }
    }
    split_offsets_swar(line, out, delim)
}

/// AVX2 structural-character scan (`PCMPEQB` + `movemask`, 32 bytes/step):
/// build a quote/delimiter bitmask per 32-byte block, bail on any `"`, and
/// extract every delimiter offset branch-free via `trailing_zeros`. Byte-
/// identical to [`split_offsets_swar`] (same offsets, same quote-bail). #71.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn split_offsets_avx2(line: &str, out: &mut Vec<(usize, usize)>, delim: u8) -> bool {
    use std::arch::x86_64::*;
    out.clear();
    let bytes = line.as_bytes();
    let n = bytes.len();
    let dvec = _mm256_set1_epi8(delim as i8);
    let qvec = _mm256_set1_epi8(b'"' as i8);
    let mut start = 0usize;
    let mut i = 0usize;
    while i + 32 <= n {
        let chunk = _mm256_loadu_si256(bytes.as_ptr().add(i) as *const __m256i);
        // Any `"` in this block → quoted record, take the slow path (matches the
        // SWAR scan's "bail on first quote"; the return value depends only on
        // whether the whole line contains a quote, so the two always agree).
        if _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, qvec)) != 0 {
            return false;
        }
        let mut dmask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, dvec)) as u32;
        while dmask != 0 {
            let j = i + dmask.trailing_zeros() as usize;
            out.push((start, j));
            start = j + 1;
            dmask &= dmask - 1;
        }
        i += 32;
    }
    // Scalar tail (< 32 bytes), same predicate as the SIMD body.
    while i < n {
        let b = bytes[i];
        if b == b'"' {
            return false;
        }
        if b == delim {
            out.push((start, i));
            start = i + 1;
        }
        i += 1;
    }
    out.push((start, n));
    true
}

/// Split an unquoted record into field byte-ranges. Scans 8 bytes at a time
/// (SWAR), recording every `delim` position; bails to the owned slow path
/// (returns `false`, leaving `out` partially filled — callers re-split and
/// ignore it) the moment a `"` appears. Byte-identical to a scalar scan: it
/// finds the exact same delimiter offsets and the exact same quote condition.
fn split_offsets_swar(line: &str, out: &mut Vec<(usize, usize)>, delim: u8) -> bool {
    out.clear();
    let bytes = line.as_bytes();
    let n = bytes.len();
    let dsplat = swar_splat(delim);
    let qsplat = swar_splat(b'"');
    let mut start = 0usize;
    let mut i = 0usize;
    while i + 8 <= n {
        let word = u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        // Any `"` in this word → quoted record, take the slow path.
        if swar_eq_mask(word, qsplat) != 0 {
            return false;
        }
        let mut m = swar_eq_mask(word, dsplat);
        while m != 0 {
            let j = i + (m.trailing_zeros() as usize >> 3);
            out.push((start, j));
            start = j + 1;
            m &= m - 1;
        }
        i += 8;
    }
    // Scalar tail (< 8 bytes), same predicate as the SWAR body.
    while i < n {
        let b = bytes[i];
        if b == b'"' {
            return false;
        }
        if b == delim {
            out.push((start, i));
            start = i + 1;
        }
        i += 1;
    }
    out.push((start, n));
    true
}

// ---------------------------------------------------------------------------
// SWAR integer parse (#71 step 2): vectorized-within-register digit conversion.
// `parse_i64_fast` is **byte-identical by construction** to
// `s.parse::<i64>().unwrap_or(0)`: it returns `Some(v)` *only* when `v` is
// provably the value `i64::from_str` would yield, and `None` (defer to std) for
// every edge — empty, a sign with no digits, any non-digit byte, or ≥19 digits
// (potential overflow; let std's checked path decide). So the caller's result
// is identical whichever path runs; the SWAR path just skips std's per-digit
// `checked_mul`/`checked_add` for the common ≤18-digit case. Exact i64, no f64.
// ---------------------------------------------------------------------------

/// Fast path for `s.parse::<i64>()`. `Some(v)` only when byte-identical to std
/// (see the section comment); `None` defers to std for every edge case.
#[inline]
fn parse_i64_fast(s: &str) -> Option<i64> {
    let bytes = s.as_bytes();
    let n = bytes.len();
    if n == 0 {
        return None;
    }
    // Optional single leading sign, exactly as `i64::from_str` accepts.
    let (neg, mut i) = match bytes[0] {
        b'-' => (true, 1usize),
        b'+' => (false, 1usize),
        _ => (false, 0usize),
    };
    let ndigits = n - i;
    // No digits after a lone sign, or ≥19 digits (may overflow i64) → let std
    // decide. 18 digits always fit (< 10^18 < i64::MAX) and parse without carry.
    if ndigits == 0 || ndigits > 18 {
        return None;
    }
    let mut val: u64 = 0;
    // SWAR 8-digit blocks (each validated; any non-digit byte → defer to std).
    while i + 8 <= n {
        let word = u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        if !rivus_core::numparse::is_eight_digits(word) {
            return None;
        }
        val = val * 100_000_000 + rivus_core::numparse::parse_8_digits(word);
        i += 8;
    }
    // Scalar remainder (< 8 bytes), same digit predicate.
    while i < n {
        let d = bytes[i].wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        val = val * 10 + d as u64;
        i += 1;
    }
    // val ≤ 10^18 - 1 < i64::MAX, so both casts are exact (incl. -10^18).
    Some(if neg { -(val as i64) } else { val as i64 })
}

/// `s.parse::<i64>().unwrap_or(0)` on the continue-first default, via the SWAR
/// fast path when it applies (byte-identical) else std. `s` must be pre-trimmed.
// Now only exercised by tests (the I64 lane build inlines the checked form);
// kept as the reference the equivalence tests assert against.
#[cfg(test)]
#[inline]
fn parse_i64_cell(s: &str) -> i64 {
    match parse_i64_fast(s) {
        Some(v) => v,
        None => s.parse::<i64>().unwrap_or(0),
    }
}

/// Observe one record into per-column type `flags` (kept columns only), using
/// the allocation-free offset split on the fast path. `offsets` is reused.
/// Returns `false` for a malformed (wrong-arity) record.
fn observe_line(
    line: &str,
    ncols: usize,
    keep: &[usize],
    flags: &mut [Flags],
    offsets: &mut Vec<(usize, usize)>,
    delim: u8,
) -> bool {
    if split_offsets(line, offsets, delim) {
        if offsets.len() != ncols {
            return false;
        }
        for (k, &ci) in keep.iter().enumerate() {
            let (s, e) = offsets[ci];
            flags[k].observe(&line[s..e]);
        }
    } else {
        let fields = split_record(line, delim);
        if fields.len() != ncols {
            return false;
        }
        for (k, &ci) in keep.iter().enumerate() {
            flags[k].observe(&fields[ci]);
        }
    }
    true
}

/// Split a record into fields. Fast path: records without `"` split into
/// borrowed slices with zero allocation. Slow path: quote/escape-aware owned
/// split. Results are appended to `out` (reused across rows).
/// Each field is paired with a `quoted` flag: `true` when its source token was
/// quoted (so an empty `""` is a real empty string, not a `null`; design 26
/// §26.3). The unquoted fast path is never quoted.
fn split_into<'a>(line: &'a str, out: &mut Vec<(Cow<'a, str>, bool)>, delim: u8) {
    if !line.as_bytes().contains(&b'"') {
        for f in line.split(delim as char) {
            out.push((Cow::Borrowed(f), false));
        }
    } else {
        for (f, quoted) in split_record_q(line, delim) {
            out.push((Cow::Owned(f), quoted));
        }
    }
}

/// Owned split for the header (rare, runs once).
fn split_owned(line: &str, delim: u8) -> Vec<String> {
    if !line.as_bytes().contains(&b'"') {
        line.split(delim as char).map(|s| s.to_string()).collect()
    } else {
        split_record(line, delim)
    }
}

/// Split a record on `delim`, honoring `"..."` quoting with `""` escapes.
fn split_record(line: &str, delim: u8) -> Vec<String> {
    split_record_q(line, delim)
        .into_iter()
        .map(|(s, _)| s)
        .collect()
}

/// Like [`split_record`], but pairs each field with a `quoted` flag (`true` if
/// the field's source token contained a quote). Lets the reader tell a quoted
/// empty `""` (a real empty string) from an unquoted empty (`null`); design 26
/// §26.3.
fn split_record_q(line: &str, delim: u8) -> Vec<(String, bool)> {
    let sep = delim as char;
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut field_quoted = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if in_quotes => {
                if chars.peek() == Some(&'"') {
                    cur.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            }
            '"' => {
                in_quotes = true;
                field_quoted = true;
            }
            c if c == sep && !in_quotes => {
                out.push((std::mem::take(&mut cur), field_quoted));
                field_quoted = false;
            }
            _ => cur.push(c),
        }
    }
    out.push((cur, field_quoted));
    out
}

// ----------------------------------------------------------- compressed input

/// Streaming CSV reader for a **compressed** file (gzip `.gz` with feature
/// `gzip`, zstd `.zst` with feature `zstd`).
///
/// A compressed stream can't be seeked, so the two-pass / byte-range readers
/// don't apply. Instead this does a single forward pass with *sample inference*:
/// it reads and buffers the first `chunk_size` data rows, infers the schema from
/// them (exactly like `open_preview`), then yields the whole file — buffered
/// sample first, then the rest decoded on the fly. Bounded memory (one chunk of
/// buffered rows + the decode buffer), serial, no parallelism. Inference is over
/// a sample, so on a column whose type only "widens" after the sample (e.g. an
/// int column that turns float/text deep in the file) it can mis-type — the
/// documented trade-off for not being able to re-read a compressed stream.
#[cfg(any(feature = "gzip", feature = "zstd"))]
pub struct CompressedCsvReader {
    reader: Box<dyn BufRead + Send>,
    ncols: usize,
    keep: Vec<usize>,
    dtypes: Vec<DataType>,
    /// Per-kept-column datetime parse spec (design 23); aligned to `dtypes`.
    dt_specs: Vec<Option<Arc<DtSpec>>>,
    chunk_size: usize,
    delim: u8,
    /// Sample rows buffered during inference, emitted before streaming the rest.
    pending: Vec<String>,
    pending_pos: usize,
    line: String,
    eof: bool,
    pub bad_rows: usize,
}

/// Wrap `path`'s file in the right streaming decoder for its extension. `.gz`
/// needs feature `gzip`, `.zst`/`.zstd` need feature `zstd`; an unsupported
/// (or feature-disabled) extension returns an actionable error.
#[cfg(any(feature = "gzip", feature = "zstd"))]
fn open_decoder(path: &str) -> Result<Box<dyn BufRead + Send>, String> {
    let f = File::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".gz") {
        #[cfg(feature = "gzip")]
        {
            return Ok(Box::new(BufReader::with_capacity(
                READ_BUF,
                flate2::read::MultiGzDecoder::new(f),
            )));
        }
        #[cfg(not(feature = "gzip"))]
        return Err(format!(
            "'{path}' is gzip-compressed; rebuild with `--features gzip`"
        ));
    }
    if lower.ends_with(".zst") || lower.ends_with(".zstd") {
        #[cfg(feature = "zstd")]
        {
            let dec = ruzstd::decoding::StreamingDecoder::new(f)
                .map_err(|e| format!("cannot read zstd '{path}': {e}"))?;
            return Ok(Box::new(BufReader::with_capacity(READ_BUF, dec)));
        }
        #[cfg(not(feature = "zstd"))]
        return Err(format!(
            "'{path}' is zstd-compressed; rebuild with `--features zstd`"
        ));
    }
    Err(format!("'{path}' has no supported compression extension"))
}

#[cfg(any(feature = "gzip", feature = "zstd"))]
impl CompressedCsvReader {
    /// Open `path`, wrap it in the right decoder, read the CSV header + a sample
    /// to infer the schema, and return the reader positioned to yield every row.
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        path: &str,
        allow: Option<&[String]>,
        chunk_size: usize,
        header: bool,
        declared: Option<&[(String, Option<DataType>)]>,
        dt_formats: &[(String, String)],
        delim: u8,
    ) -> Result<(Schema, CompressedCsvReader), String> {
        let mut reader = open_decoder(path)?;

        // Column names: a declared schema, else the header line, else c0,c1,….
        // A header line is consumed when `header` even if `declared` overrides.
        let mut first = String::new();
        if reader.read_line(&mut first).map_err(|e| e.to_string())? == 0 {
            return Err("empty compressed CSV".to_string());
        }
        let mut pending: Vec<String> = Vec::new();
        let names: Vec<String> = if let Some(d) = declared {
            if !header {
                pending.push(trim_eol(&first).to_string()); // first line is data
            }
            d.iter().map(|(nm, _)| nm.clone()).collect()
        } else if header {
            split_owned(trim_eol(&first), delim)
        } else {
            let n = split_owned(trim_eol(&first), delim).len();
            pending.push(trim_eol(&first).to_string());
            (0..n).map(|i| format!("c{i}")).collect()
        };
        let ncols = names.len();
        if ncols == 0 {
            return Err("compressed CSV has no columns".to_string());
        }
        let keep: Vec<usize> = match allow {
            None => (0..ncols).collect(),
            Some(a) => (0..ncols)
                .filter(|&i| a.iter().any(|n| n == &names[i]))
                .collect(),
        };

        // Sample up to `chunk_size` data rows, buffering them and inferring types.
        let mut flags: Vec<Flags> = keep.iter().map(|_| Flags::new()).collect();
        let mut bad = 0usize;
        let mut offsets: Vec<(usize, usize)> = Vec::with_capacity(ncols);
        while pending.len() < chunk_size.max(1) {
            let mut l = String::new();
            if reader.read_line(&mut l).map_err(|e| e.to_string())? == 0 {
                break;
            }
            let t = trim_eol(&l);
            if t.trim().is_empty() {
                continue;
            }
            if !observe_line(t, ncols, &keep, &mut flags, &mut offsets, delim) {
                bad += 1;
                continue;
            }
            pending.push(t.to_string());
        }
        let mut dtypes: Vec<DataType> = flags.iter().map(Flags::resolve).collect();
        apply_declared_types(&mut dtypes, &keep, declared);
        let dt_specs = build_dt_specs(&names, &keep, &dtypes, dt_formats);

        let mut fields = Vec::with_capacity(keep.len());
        for (k, &ci) in keep.iter().enumerate() {
            fields.push(Field::new(names[ci].clone(), dtypes[k]));
        }
        let schema = Schema::new(fields);

        Ok((
            schema,
            CompressedCsvReader {
                reader,
                ncols,
                keep,
                dtypes,
                dt_specs,
                chunk_size: chunk_size.max(1),
                delim,
                pending,
                pending_pos: 0,
                line: String::new(),
                eof: false,
                bad_rows: bad,
            },
        ))
    }

    /// Push one record's kept cells into the per-column builders, honoring the
    /// quoted slow path. Returns `false` for a wrong-arity record (skipped).
    fn push_record(&self, builders: &mut [ColBuilder], line: &str) -> bool {
        let mut offsets: Vec<(usize, usize)> = Vec::with_capacity(self.ncols);
        if split_offsets(line, &mut offsets, self.delim) {
            if offsets.len() != self.ncols {
                return false;
            }
            for (k, &ci) in self.keep.iter().enumerate() {
                let (s, e) = offsets[ci];
                builders[k].push(&line[s..e], false);
            }
        } else {
            let fields = split_record_q(line, self.delim);
            if fields.len() != self.ncols {
                return false;
            }
            for (k, &ci) in self.keep.iter().enumerate() {
                builders[k].push(&fields[ci].0, fields[ci].1);
            }
        }
        true
    }

    /// Yield the next chunk of up to `chunk_size` typed rows, or `None` at EOF.
    pub fn next_columns(&mut self) -> Option<Vec<Column>> {
        if self.eof && self.pending_pos >= self.pending.len() {
            return None;
        }
        let mut builders: Vec<ColBuilder> = self
            .dtypes
            .iter()
            .enumerate()
            .map(|(k, d)| {
                ColBuilder::with_capacity_dt(*d, self.chunk_size, self.dt_specs[k].clone())
            })
            .collect();
        let mut got = 0usize;

        // Drain the buffered sample rows first (already arity-checked).
        while got < self.chunk_size && self.pending_pos < self.pending.len() {
            let idx = self.pending_pos;
            self.pending_pos += 1;
            // Borrow-safe: clone the small line out before the &mut builders call.
            let line = std::mem::take(&mut self.pending[idx]);
            if self.push_record(&mut builders, &line) {
                got += 1;
            }
        }
        // Then stream the rest of the file.
        while got < self.chunk_size && !self.eof {
            self.line.clear();
            match self.reader.read_line(&mut self.line) {
                Ok(0) => {
                    self.eof = true;
                    break;
                }
                Ok(_) => {}
                Err(_) => {
                    self.eof = true;
                    break;
                }
            }
            let t = trim_eol(&self.line).to_string();
            if t.trim().is_empty() {
                continue;
            }
            if self.push_record(&mut builders, &t) {
                got += 1;
            } else {
                self.bad_rows += 1;
            }
        }
        if got == 0 {
            return None;
        }
        Some(builders.iter_mut().map(ColBuilder::finish).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infers_and_parses_types() {
        let data = parse_projected("a,b,c,d\n1,1.5,true,x\n2,2.0,false,y\n", None, b',').unwrap();
        assert_eq!(data.schema.fields[0].dtype, DataType::I64);
        assert_eq!(data.schema.fields[1].dtype, DataType::F64);
        assert_eq!(data.schema.fields[2].dtype, DataType::Bool);
        assert_eq!(data.schema.fields[3].dtype, DataType::Str);
        assert_eq!(data.bad_rows, 0);
        match data.columns[0].data() {
            ColumnData::I64(v) => assert_eq!(v, &[1, 2]),
            _ => panic!("expected i64"),
        }
    }

    #[test]
    fn strips_utf8_bom_from_header() {
        // A BOM before the header must not leak into the first column name, and
        // the first column's data must still parse (here as i64).
        let data = parse_projected("\u{feff}id,age\n1,30\n2,15\n", None, b',').unwrap();
        assert_eq!(data.schema.fields[0].name, "id", "BOM leaked into name");
        assert_eq!(data.schema.fields[1].name, "age");
        match data.columns[0].data() {
            ColumnData::I64(v) => assert_eq!(v, &[1, 2]),
            _ => panic!("first column should parse as i64 once the BOM is gone"),
        }
    }

    #[test]
    fn bom_helpers() {
        assert_eq!(bom_len("\u{feff}x"), 3);
        assert_eq!(bom_len("x"), 0);
        assert_eq!(strip_bom("\u{feff}id,age"), "id,age");
        assert_eq!(strip_bom("id,age"), "id,age");
    }

    #[test]
    fn skips_malformed_rows() {
        let data = parse_projected("a,b\n1,2\nonly_one_field\n3,4\n", None, b',').unwrap();
        assert_eq!(data.bad_rows, 1);
        match data.columns[0].data() {
            ColumnData::I64(v) => assert_eq!(v, &[1, 3]),
            _ => panic!("expected i64"),
        }
    }

    #[test]
    fn handles_quoted_fields_with_commas() {
        let data =
            parse_projected("name,note\n\"a,b\",\"he said \"\"hi\"\"\"\n", None, b',').unwrap();
        match data.columns[0].data() {
            ColumnData::Str(v) => assert_eq!(v.get(0), "a,b"),
            _ => panic!("expected str"),
        }
        match data.columns[1].data() {
            ColumnData::Str(v) => assert_eq!(v.get(0), "he said \"hi\""),
            _ => panic!("expected str"),
        }
    }

    #[test]
    fn mixed_column_falls_back_to_str() {
        let data = parse_projected("v\n1\n2\nN/A\n", None, b',').unwrap();
        assert_eq!(data.schema.fields[0].dtype, DataType::Str);
    }

    // Reference scalar splitter: the byte-by-byte version `split_offsets`
    // replaced. The SWAR scan must produce identical offsets and the identical
    // quote-bail decision for every input.
    fn split_offsets_scalar(line: &str, out: &mut Vec<(usize, usize)>, delim: u8) -> bool {
        out.clear();
        let bytes = line.as_bytes();
        if bytes.contains(&b'"') {
            return false;
        }
        let mut start = 0usize;
        for (i, &b) in bytes.iter().enumerate() {
            if b == delim {
                out.push((start, i));
                start = i + 1;
            }
        }
        out.push((start, bytes.len()));
        true
    }

    #[test]
    fn swar_split_stress_lines() {
        for i in 0..4000usize {
            let v = (i as f64 % 200.0) * 0.5 - 50.0;
            let name = if i % 3 == 0 {
                String::new()
            } else {
                format!("n{i}")
            };
            let line = format!("{i},{v},{name}");
            let mut a = Vec::new();
            let mut b = Vec::new();
            let ra = split_offsets(&line, &mut a, b',');
            let rb = split_offsets_scalar(&line, &mut b, b',');
            assert_eq!(ra, rb, "bail mismatch on {line:?}");
            if ra {
                assert_eq!(a, b, "offset mismatch on {line:?} (len {})", line.len());
            }
        }
    }

    #[test]
    fn swar_split_matches_scalar() {
        // Cross every word boundary and tail length, with delimiters and quotes
        // at varied positions, plus empties and runs.
        let cases = [
            "",
            "a",
            ",",
            "a,b",
            "a,b,c",
            "1,22,333,4444,55555,666666,7777777,88888888,9",
            ",,,",
            "abcdefgh,ijklmnop,q",   // 8-byte-aligned fields
            "abcdefg,hij",           // delim straddling the 8-byte mark
            "no_delims_here_at_all", // > 8 bytes, zero delims
            "tab\tsep\there",
            "has\"quote",
            "trailing_quote\"",
            "\"leading_quote",
            "field1,field2\"x,field3", // quote after some delims → bail
            "1234567\"",               // quote exactly at the 8th byte (tail)
            "12345678\"9",             // quote just past a full word
        ];
        for case in cases {
            for &delim in b",\t" {
                let mut a = Vec::new();
                let mut b = Vec::new();
                let ra = split_offsets(case, &mut a, delim);
                let rb = split_offsets_scalar(case, &mut b, delim);
                assert_eq!(ra, rb, "quote-bail mismatch on {case:?} delim={delim}");
                if ra {
                    assert_eq!(a, b, "offset mismatch on {case:?} delim={delim}");
                }
            }
        }
    }

    /// `parse_i64_cell` must be byte-identical to `s.parse::<i64>().unwrap_or(0)`
    /// for every input, and `parse_i64_fast(s).is_some()` must imply
    /// `s.parse::<i64>().is_ok()` (so the inference decision is unchanged). #71.
    #[test]
    fn swar_int_parse_matches_std() {
        let mut cases: Vec<String> = vec![
            String::new(),
            "-".into(),
            "+".into(),
            "0".into(),
            "-0".into(),
            "+7".into(),
            "00042".into(),
            "007".into(),
            "12".into(),
            "123456789".into(),          // crosses the 8-digit block
            "1234567890123456".into(),   // 16 digits
            "999999999999999999".into(), // 18 nines (max fast width)
            "-999999999999999999".into(),
            "1000000000000000000".into(),  // 19 digits, fits → std path
            "9223372036854775807".into(),  // i64::MAX (19 digits)
            "9223372036854775808".into(),  // i64::MAX+1 → Err → 0
            "-9223372036854775808".into(), // i64::MIN
            "-9223372036854775809".into(), // i64::MIN-1 → Err → 0
            "99999999999999999999999".into(), // way overflow
            "12a45".into(),
            "1.5".into(),
            " 12".into(), // not trimmed → non-digit lead → defer/Err
            "12 ".into(),
            "1_000".into(),
            "++1".into(),
            "--1".into(),
            "0x1F".into(),
            "日本1".into(),
        ];
        // Exhaustive small range + every 1..=20-digit boundary length.
        for v in -2000i64..=2000 {
            cases.push(v.to_string());
        }
        for d in 1..=20usize {
            cases.push("9".repeat(d));
            cases.push(format!("-{}", "9".repeat(d)));
            cases.push(format!("1{}", "0".repeat(d.saturating_sub(1))));
        }
        for s in &cases {
            let std = s.parse::<i64>().unwrap_or(0);
            assert_eq!(parse_i64_cell(s), std, "value mismatch on {s:?}");
            if parse_i64_fast(s).is_some() {
                assert!(s.parse::<i64>().is_ok(), "fast accepted a non-i64: {s:?}");
            }
        }
    }

    /// Micro-benchmark (ignored; run with
    /// `cargo test -p rivus-runtime --release --lib bench_int_parse -- --ignored --nocapture`):
    /// integer-parse throughput, std `from_str` vs the SWAR fast path. #71.
    #[test]
    #[ignore]
    fn bench_int_parse() {
        use std::time::Instant;
        // A realistic mix: short ids, mid values, and a few wide ones.
        let samples: Vec<String> = (0..1024)
            .map(|i: u64| match i % 4 {
                0 => (i % 1000).to_string(),
                1 => (i.wrapping_mul(2_654_435_761) % 1_000_000).to_string(),
                2 => format!("-{}", i.wrapping_mul(40_503) % 100_000),
                _ => (i.wrapping_mul(1_000_003) as i64).to_string(),
            })
            .collect();
        let total_bytes: usize = samples.iter().map(|s| s.len()).sum::<usize>();
        let reps = 4000usize;

        let t = Instant::now();
        let mut acc = 0i64;
        for _ in 0..reps {
            for s in &samples {
                acc = acc.wrapping_add(s.parse::<i64>().unwrap_or(0));
            }
        }
        let std_t = t.elapsed();

        let t = Instant::now();
        let mut acc2 = 0i64;
        for _ in 0..reps {
            for s in &samples {
                acc2 = acc2.wrapping_add(parse_i64_cell(s));
            }
        }
        let swar_t = t.elapsed();
        assert_eq!(acc, acc2);

        let mbps = |d: std::time::Duration| (total_bytes * reps) as f64 / d.as_secs_f64() / 1e6;
        println!(
            "\n[#71 int-parse | short/mixed] {} samples × {reps} reps (acc={acc})",
            samples.len()
        );
        println!("  std from_str: {:?}  {:.0} MB/s", std_t, mbps(std_t));
        println!(
            "  SWAR fast:    {:?}  {:.0} MB/s  ({:.2}x std)",
            swar_t,
            mbps(swar_t),
            std_t.as_secs_f64() / swar_t.as_secs_f64()
        );

        // Wide regime (15–18 digit ids): where the 8-digit SWAR block pays off.
        let wide: Vec<String> = (0..1024u64)
            .map(|i| (1_000_000_000_000_000u64 + i.wrapping_mul(2_654_435_761) % 999).to_string())
            .collect();
        let wbytes: usize = wide.iter().map(|s| s.len()).sum();
        let t = Instant::now();
        let mut a3 = 0i64;
        for _ in 0..reps {
            for s in &wide {
                a3 = a3.wrapping_add(s.parse::<i64>().unwrap_or(0));
            }
        }
        let wstd = t.elapsed();
        let t = Instant::now();
        let mut a4 = 0i64;
        for _ in 0..reps {
            for s in &wide {
                a4 = a4.wrapping_add(parse_i64_cell(s));
            }
        }
        let wswar = t.elapsed();
        assert_eq!(a3, a4);
        let wmbps = |d: std::time::Duration| (wbytes * reps) as f64 / d.as_secs_f64() / 1e6;
        println!("[#71 int-parse | wide 16-digit]");
        println!("  std from_str: {:?}  {:.0} MB/s", wstd, wmbps(wstd));
        println!(
            "  SWAR fast:    {:?}  {:.0} MB/s  ({:.2}x std)",
            wswar,
            wmbps(wswar),
            wstd.as_secs_f64() / wswar.as_secs_f64()
        );
    }

    /// Micro-benchmark (ignored; run with
    /// `cargo test -p rivus-runtime --release --lib bench_split_scan -- --ignored --nocapture`):
    /// structural-scan throughput, SWAR (8B/step) vs AVX2 (32B/step). #71.
    #[test]
    #[ignore]
    fn bench_split_scan() {
        use std::time::Instant;
        // ~12-field numeric rows, ~64 bytes each (a realistic CSV line width).
        let line = "12345,-67.89,2026-06-01T14:30:00,abc,42,7,100,250,3,9,foo,barbaz";
        let bytes_per: usize = line.len();
        let iters = 2_000_000usize;
        let mut out = Vec::with_capacity(16);

        let t = Instant::now();
        let mut acc = 0usize;
        for _ in 0..iters {
            let _ = split_offsets_swar(line, &mut out, b',');
            acc += out.len();
        }
        let swar = t.elapsed();

        let avx = if cfg!(target_arch = "x86_64") && std::is_x86_feature_detected!("avx2") {
            let t = Instant::now();
            for _ in 0..iters {
                #[cfg(target_arch = "x86_64")]
                // SAFETY: guarded by runtime AVX2 detection.
                let _ = unsafe { split_offsets_avx2(line, &mut out, b',') };
                acc += out.len();
            }
            Some(t.elapsed())
        } else {
            None
        };

        let mbps = |d: std::time::Duration| (bytes_per * iters) as f64 / d.as_secs_f64() / 1e6;
        println!("\n[#71 split-scan] line={bytes_per}B iters={iters} (acc={acc})");
        println!("  SWAR: {:?}  {:.0} MB/s", swar, mbps(swar));
        if let Some(a) = avx {
            println!(
                "  AVX2: {:?}  {:.0} MB/s  ({:.2}x SWAR)",
                a,
                mbps(a),
                swar.as_secs_f64() / a.as_secs_f64()
            );
        }
    }

    /// Both backends (SWAR always, AVX2 when the host supports it) must match the
    /// scalar reference across every length that crosses the 8/32/64-byte block
    /// boundaries, with delimiters and quotes at varied offsets. #71.
    #[test]
    fn simd_split_backends_match_scalar() {
        let mut cases: Vec<String> = Vec::new();
        // Lengths 0..80 of pure data, then with a delimiter / quote injected at
        // each position — exercises every block boundary and tail remainder.
        for len in 0..80usize {
            let base: String = (0..len).map(|k| (b'a' + (k % 26) as u8) as char).collect();
            cases.push(base.clone());
            for pos in 0..len {
                let mut d = base.clone().into_bytes();
                d[pos] = b',';
                cases.push(String::from_utf8(d).unwrap());
                let mut q = base.clone().into_bytes();
                q[pos] = b'"';
                cases.push(String::from_utf8(q).unwrap());
            }
        }
        // A few multibyte-UTF8 lines (continuation bytes are ≥0x80, never a
        // delim/quote false-match).
        cases.push("日本語,café,naïve,x".to_string());
        cases.push("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,日本語".to_string()); // 32B then comma

        for case in &cases {
            for &delim in b",\t" {
                let mut want = Vec::new();
                let rw = split_offsets_scalar(case, &mut want, delim);

                let mut s = Vec::new();
                let rs = split_offsets_swar(case, &mut s, delim);
                assert_eq!(rs, rw, "SWAR bail mismatch on {case:?} delim={delim}");
                if rw {
                    assert_eq!(s, want, "SWAR offset mismatch on {case:?} delim={delim}");
                }

                #[cfg(target_arch = "x86_64")]
                if std::is_x86_feature_detected!("avx2") {
                    let mut v = Vec::new();
                    // SAFETY: guarded by runtime AVX2 detection.
                    let rv = unsafe { split_offsets_avx2(case, &mut v, delim) };
                    assert_eq!(rv, rw, "AVX2 bail mismatch on {case:?} delim={delim}");
                    if rw {
                        assert_eq!(v, want, "AVX2 offset mismatch on {case:?} delim={delim}");
                    }
                }
            }
        }
    }
}
