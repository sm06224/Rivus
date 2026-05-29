//! Minimal CSV reader with per-column type inference.
//!
//! MVP-grade: the whole file is read and materialized, then handed out in
//! chunks. The design doc (03-stream-chunk-model.md) describes the streaming,
//! Arrow-backed reader that replaces this behind the same `Operator` boundary.
//! Quoting is handled just enough for simple fields.
//!
//! Performance: this is a **two-pass, allocation-light** parser. Pass 1 splits
//! each record into borrowed `&str` field slices (no owned `String` per cell)
//! and infers each column's type while scanning. Pass 2 re-splits and parses
//! directly into pre-sized typed column buffers. Only genuine string columns
//! ever allocate per-cell, which closes the column-count throughput gap the
//! Phase-0 baseline exposed (see docs/BENCHMARKS.md). Unquoted records — the
//! overwhelmingly common case — split into pure borrows; quoted records fall
//! back to an owned, escape-aware split.

use rivus_core::{Column, DataType, Field, Schema};
use std::borrow::Cow;

pub struct CsvData {
    pub schema: Schema,
    pub columns: Vec<Column>,
    /// Number of rows skipped because their arity didn't match the header.
    pub bad_rows: usize,
}

/// Parse CSV text into inferred columns. Never panics on malformed rows: rows
/// with the wrong field count are counted in `bad_rows` and skipped
/// (continue-first).
pub fn parse(text: &str) -> Result<CsvData, String> {
    let mut lines = text.lines();
    let header = match lines.next() {
        Some(h) => h,
        None => return Err("empty CSV".to_string()),
    };
    let names: Vec<String> = split_owned(header).into_iter().collect();
    let ncols = names.len();
    if ncols == 0 {
        return Err("CSV header has no columns".to_string());
    }

    // Body starts after the header line.
    let body = &text[header_end(text)..];

    // --- Pass 1: infer column types and count valid / bad rows -------------
    let mut flags: Vec<Flags> = (0..ncols).map(|_| Flags::new()).collect();
    let mut scratch: Vec<Cow<str>> = Vec::with_capacity(ncols);
    let mut nrows = 0usize;
    let mut bad_rows = 0usize;
    for line in body.lines() {
        if line.trim().is_empty() {
            continue;
        }
        scratch.clear();
        split_into(line, &mut scratch);
        if scratch.len() != ncols {
            bad_rows += 1;
            continue;
        }
        for (i, cell) in scratch.iter().enumerate() {
            flags[i].observe(cell.as_ref());
        }
        nrows += 1;
    }
    let dtypes: Vec<DataType> = flags.iter().map(Flags::resolve).collect();

    // --- Pass 2: build columns directly into pre-sized buffers -------------
    let mut builders: Vec<ColBuilder> = dtypes
        .iter()
        .map(|d| ColBuilder::with_capacity(*d, nrows))
        .collect();
    for line in body.lines() {
        if line.trim().is_empty() {
            continue;
        }
        scratch.clear();
        split_into(line, &mut scratch);
        if scratch.len() != ncols {
            continue; // identical skip rule as pass 1
        }
        for (i, cell) in scratch.iter().enumerate() {
            builders[i].push(cell.as_ref());
        }
    }

    let mut columns = Vec::with_capacity(ncols);
    let mut fields = Vec::with_capacity(ncols);
    for (i, name) in names.into_iter().enumerate() {
        fields.push(Field::new(name, dtypes[i]));
        columns.push(builders[i].finish());
    }

    Ok(CsvData {
        schema: Schema::new(fields),
        columns,
        bad_rows,
    })
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
        if self.all_int && c.parse::<i64>().is_err() {
            self.all_int = false;
        }
        if self.all_float && c.parse::<f64>().is_err() {
            self.all_float = false;
        }
        if self.all_bool && !matches!(c, "true" | "false") {
            self.all_bool = false;
        }
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
    Str(Vec<String>),
}

impl ColBuilder {
    fn with_capacity(dtype: DataType, cap: usize) -> Self {
        match dtype {
            DataType::Bool => ColBuilder::Bool(Vec::with_capacity(cap)),
            DataType::I64 => ColBuilder::I64(Vec::with_capacity(cap)),
            DataType::F64 => ColBuilder::F64(Vec::with_capacity(cap)),
            _ => ColBuilder::Str(Vec::with_capacity(cap)),
        }
    }

    #[inline]
    fn push(&mut self, cell: &str) {
        match self {
            ColBuilder::Bool(v) => v.push(cell.trim() == "true"),
            ColBuilder::I64(v) => v.push(cell.trim().parse().unwrap_or(0)),
            ColBuilder::F64(v) => v.push(cell.trim().parse().unwrap_or(0.0)),
            ColBuilder::Str(v) => v.push(cell.to_string()),
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
        let data = parse("a,b,c,d\n1,1.5,true,x\n2,2.0,false,y\n").unwrap();
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
        let data = parse("a,b\n1,2\nonly_one_field\n3,4\n").unwrap();
        assert_eq!(data.bad_rows, 1);
        match &data.columns[0] {
            Column::I64(v) => assert_eq!(v, &[1, 3]),
            _ => panic!("expected i64"),
        }
    }

    #[test]
    fn handles_quoted_fields_with_commas() {
        let data = parse("name,note\n\"a,b\",\"he said \"\"hi\"\"\"\n").unwrap();
        match &data.columns[0] {
            Column::Str(v) => assert_eq!(v[0], "a,b"),
            _ => panic!("expected str"),
        }
        match &data.columns[1] {
            Column::Str(v) => assert_eq!(v[0], "he said \"hi\""),
            _ => panic!("expected str"),
        }
    }

    #[test]
    fn mixed_column_falls_back_to_str() {
        let data = parse("v\n1\n2\nN/A\n").unwrap();
        assert_eq!(data.schema.fields[0].dtype, DataType::Str);
    }
}
