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

use rivus_core::{Column, DataType, Field, Schema, StrColumn};
use std::borrow::Cow;
use std::fs::File;
use std::io::{BufRead, BufReader};

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
    chunk_size: usize,
    line: String,
    /// Rows skipped in pass 1 for wrong arity (reported once by the source).
    pub bad_rows: usize,
    eof: bool,
}

impl CsvChunker {
    /// Open `path` for streaming, returning the inferred schema and the reader
    /// positioned just after the header (ready for `next_columns`).
    pub fn open(
        path: &str,
        allow: Option<&[String]>,
        chunk_size: usize,
    ) -> Result<(Schema, CsvChunker), String> {
        // ---- pass 1: infer a global schema by streaming the whole file ----
        let f = File::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
        let mut r = BufReader::new(f);
        let mut header = String::new();
        if r.read_line(&mut header).map_err(|e| e.to_string())? == 0 {
            return Err("empty CSV".to_string());
        }
        let names = split_owned(trim_eol(&header));
        let ncols = names.len();
        if ncols == 0 {
            return Err("CSV header has no columns".to_string());
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
        loop {
            line.clear();
            if r.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
                break;
            }
            let l = trim_eol(&line);
            if l.trim().is_empty() {
                continue;
            }
            let mut scratch: Vec<Cow<str>> = Vec::with_capacity(ncols);
            split_into(l, &mut scratch);
            if scratch.len() != ncols {
                bad += 1;
                continue;
            }
            for (k, &ci) in keep.iter().enumerate() {
                flags[k].observe(scratch[ci].as_ref());
            }
        }
        let dtypes: Vec<DataType> = flags.iter().map(Flags::resolve).collect();

        let mut fields = Vec::with_capacity(keep.len());
        for (k, &ci) in keep.iter().enumerate() {
            fields.push(Field::new(names[ci].clone(), dtypes[k]));
        }
        let schema = Schema::new(fields);

        // ---- pass 2 setup: reopen and skip the header line ----
        let f2 = File::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
        let mut reader = BufReader::new(f2);
        let mut skip = String::new();
        reader.read_line(&mut skip).map_err(|e| e.to_string())?;

        Ok((
            schema,
            CsvChunker {
                reader,
                ncols,
                keep,
                dtypes,
                chunk_size: chunk_size.max(1),
                line: String::new(),
                bad_rows: bad,
                eof: false,
            },
        ))
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
            .map(|d| ColBuilder::with_capacity(*d, self.chunk_size))
            .collect();
        let mut got = 0usize;
        while got < self.chunk_size {
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
            let l = trim_eol(&self.line);
            if l.trim().is_empty() {
                continue;
            }
            let mut scratch: Vec<Cow<str>> = Vec::with_capacity(self.ncols);
            split_into(l, &mut scratch);
            if scratch.len() != self.ncols {
                continue;
            }
            for (k, &ci) in self.keep.iter().enumerate() {
                builders[k].push(scratch[ci].as_ref());
            }
            got += 1;
        }
        if got == 0 {
            return None;
        }
        Some(builders.iter_mut().map(ColBuilder::finish).collect())
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
pub fn parse_projected(text: &str, allow: Option<&[String]>) -> Result<CsvData, String> {
    let mut lines = text.lines();
    let header = match lines.next() {
        Some(h) => h,
        None => return Err("empty CSV".to_string()),
    };
    let names: Vec<String> = split_owned(header);
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
        1 => parse_serial(body, ncols, &keep),
        n => parse_parallel(body, ncols, &keep, n),
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

fn parse_serial(body: &str, ncols: usize, keep: &[usize]) -> (Vec<DataType>, Vec<Column>, usize) {
    let inf = infer_slice(body, ncols, keep);
    let dtypes: Vec<DataType> = inf.flags.iter().map(Flags::resolve).collect();
    let columns = build_slice(body, ncols, keep, &dtypes, inf.nrows);
    (dtypes, columns, inf.bad)
}

fn parse_parallel(
    body: &str,
    ncols: usize,
    keep: &[usize],
    nthreads: usize,
) -> (Vec<DataType>, Vec<Column>, usize) {
    let slices = split_lines(body, nthreads);
    if slices.len() <= 1 {
        return parse_serial(body, ncols, keep);
    }

    // Phase 1: infer types per slice, in parallel.
    let infers: Vec<Inferred> = std::thread::scope(|s| {
        let handles: Vec<_> = slices
            .iter()
            .map(|&sl| s.spawn(move || infer_slice(sl, ncols, keep)))
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
            .map(|(&sl, inf)| s.spawn(move || build_slice(sl, ncols, keep, dtypes, inf.nrows)))
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
fn infer_slice(slice: &str, ncols: usize, keep: &[usize]) -> Inferred {
    let mut flags: Vec<Flags> = keep.iter().map(|_| Flags::new()).collect();
    let mut scratch: Vec<Cow<str>> = Vec::with_capacity(ncols);
    let mut nrows = 0usize;
    let mut bad = 0usize;
    for line in slice.lines() {
        if line.trim().is_empty() {
            continue;
        }
        scratch.clear();
        split_into(line, &mut scratch);
        if scratch.len() != ncols {
            bad += 1;
            continue;
        }
        for (k, &ci) in keep.iter().enumerate() {
            flags[k].observe(scratch[ci].as_ref());
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
) -> Vec<Column> {
    let mut builders: Vec<ColBuilder> = dtypes
        .iter()
        .map(|d| ColBuilder::with_capacity(*d, cap))
        .collect();
    let mut scratch: Vec<Cow<str>> = Vec::with_capacity(ncols);
    for line in slice.lines() {
        if line.trim().is_empty() {
            continue;
        }
        scratch.clear();
        split_into(line, &mut scratch);
        if scratch.len() != ncols {
            continue; // identical skip rule as inference
        }
        for (k, &ci) in keep.iter().enumerate() {
            builders[k].push(scratch[ci].as_ref());
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
    match (a, b) {
        (Column::Bool(x), Column::Bool(y)) => x.extend(y),
        (Column::I64(x), Column::I64(y)) => x.extend(y),
        (Column::F64(x), Column::F64(y)) => x.extend(y),
        (Column::Str(x), Column::Str(y)) => x.append(&y),
        _ => unreachable!("column dtype mismatch across slices"),
    }
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
}

impl Flags {
    fn new() -> Self {
        Flags {
            any: false,
            all_int: true,
            all_float: true,
            all_bool: true,
        }
    }

    fn observe(&mut self, cell: &str) {
        let c = cell.trim();
        if c.is_empty() {
            return;
        }
        self.any = true;
        // Fast path: while the column is still all-integer, an integer cell is
        // also a float, so skip the redundant f64 parse — but it is never a
        // bool, so clear that lane.
        if self.all_int {
            if c.parse::<i64>().is_ok() {
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
        } else {
            DataType::Str
        }
    }
}

/// A typed, pre-sized column accumulator.
enum ColBuilder {
    Bool(Vec<bool>),
    I64(Vec<i64>),
    F64(Vec<f64>),
    Str(StrColumn),
}

impl ColBuilder {
    fn with_capacity(dtype: DataType, cap: usize) -> Self {
        match dtype {
            DataType::Bool => ColBuilder::Bool(Vec::with_capacity(cap)),
            DataType::I64 => ColBuilder::I64(Vec::with_capacity(cap)),
            DataType::F64 => ColBuilder::F64(Vec::with_capacity(cap)),
            // Estimate ~8 bytes per string cell for the backing byte buffer.
            _ => ColBuilder::Str(StrColumn::with_capacity(cap, cap * 8)),
        }
    }

    #[inline]
    fn push(&mut self, cell: &str) {
        match self {
            ColBuilder::Bool(v) => v.push(cell.trim() == "true"),
            ColBuilder::I64(v) => v.push(cell.trim().parse().unwrap_or(0)),
            ColBuilder::F64(v) => v.push(cell.trim().parse().unwrap_or(0.0)),
            ColBuilder::Str(v) => v.push(cell),
        }
    }

    fn finish(&mut self) -> Column {
        match self {
            ColBuilder::Bool(v) => Column::Bool(std::mem::take(v)),
            ColBuilder::I64(v) => Column::I64(std::mem::take(v)),
            ColBuilder::F64(v) => Column::F64(std::mem::take(v)),
            ColBuilder::Str(v) => Column::Str(std::mem::take(v)),
        }
    }
}

/// Split a record into fields. Fast path: records without `"` split into
/// borrowed slices with zero allocation. Slow path: quote/escape-aware owned
/// split. Results are appended to `out` (reused across rows).
fn split_into<'a>(line: &'a str, out: &mut Vec<Cow<'a, str>>) {
    if !line.as_bytes().contains(&b'"') {
        for f in line.split(',') {
            out.push(Cow::Borrowed(f));
        }
    } else {
        for f in split_record(line) {
            out.push(Cow::Owned(f));
        }
    }
}

/// Owned split for the header (rare, runs once).
fn split_owned(line: &str) -> Vec<String> {
    if !line.as_bytes().contains(&b'"') {
        line.split(',').map(|s| s.to_string()).collect()
    } else {
        split_record(line)
    }
}

/// Split a CSV record on commas, honoring `"..."` quoting with `""` escapes.
fn split_record(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
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
            '"' => in_quotes = true,
            ',' if !in_quotes => {
                out.push(std::mem::take(&mut cur));
            }
            _ => cur.push(c),
        }
    }
    out.push(cur);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infers_and_parses_types() {
        let data = parse_projected("a,b,c,d\n1,1.5,true,x\n2,2.0,false,y\n", None).unwrap();
        assert_eq!(data.schema.fields[0].dtype, DataType::I64);
        assert_eq!(data.schema.fields[1].dtype, DataType::F64);
        assert_eq!(data.schema.fields[2].dtype, DataType::Bool);
        assert_eq!(data.schema.fields[3].dtype, DataType::Str);
        assert_eq!(data.bad_rows, 0);
        match &data.columns[0] {
            Column::I64(v) => assert_eq!(v, &[1, 2]),
            _ => panic!("expected i64"),
        }
    }

    #[test]
    fn skips_malformed_rows() {
        let data = parse_projected("a,b\n1,2\nonly_one_field\n3,4\n", None).unwrap();
        assert_eq!(data.bad_rows, 1);
        match &data.columns[0] {
            Column::I64(v) => assert_eq!(v, &[1, 3]),
            _ => panic!("expected i64"),
        }
    }

    #[test]
    fn handles_quoted_fields_with_commas() {
        let data = parse_projected("name,note\n\"a,b\",\"he said \"\"hi\"\"\"\n", None).unwrap();
        match &data.columns[0] {
            Column::Str(v) => assert_eq!(v.get(0), "a,b"),
            _ => panic!("expected str"),
        }
        match &data.columns[1] {
            Column::Str(v) => assert_eq!(v.get(0), "he said \"hi\""),
            _ => panic!("expected str"),
        }
    }

    #[test]
    fn mixed_column_falls_back_to_str() {
        let data = parse_projected("v\n1\n2\nN/A\n", None).unwrap();
        assert_eq!(data.schema.fields[0].dtype, DataType::Str);
    }
}
