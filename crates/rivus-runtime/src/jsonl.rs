//! Minimal JSON Lines (NDJSON) reader: one flat JSON object per line.
//!
//! MVP scope (continue-first):
//! - Each non-empty line must be a JSON object `{ "k": value, ... }`. Lines that
//!   aren't are counted as `bad_rows` and skipped (never panics).
//! - Scalar values (string / number / bool / null) map onto the columnar lanes.
//!   A nested `{...}` / `[...]` value is captured as its raw JSON text on the
//!   string lane (degraded, not an error).
//! - The column set and order come from the first valid object; later objects
//!   fill by key (missing key → null/default, extra keys ignored).
//!
//! A flat, allocation-conscious parser — no external dependencies (the shipped
//! runtime stays std-only). Nested objects and arrays as first-class columns
//! are future work (design doc 03 nested columns / doc 18).

use crate::transport::FileTransport;
use rivus_core::{Column, ColumnData, DataType, Field, Schema, StrColumn, Validity};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};

pub struct JsonlData {
    pub schema: Schema,
    pub columns: Vec<Column>,
    pub bad_rows: usize,
}

#[derive(Clone)]
enum JVal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    /// Nested object/array kept as raw JSON text (degraded scalar).
    Raw(String),
}

pub fn parse(text: &str) -> Result<JsonlData, String> {
    let mut names: Vec<String> = Vec::new();
    let mut started = false;
    let mut rows: Vec<Vec<(String, JVal)>> = Vec::new();
    let mut bad_rows = 0;

    // A document beginning with `[` is a JSON array of objects (e.g. an API
    // response); otherwise it is JSON Lines (one object per line).
    if text.trim_start().starts_with('[') {
        collect_array(text, &mut names, &mut started, &mut rows, &mut bad_rows);
    } else {
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match parse_object(line) {
                Some(obj) => {
                    if !started {
                        names = obj.iter().map(|(k, _)| k.clone()).collect();
                        started = true;
                    }
                    rows.push(obj);
                }
                None => bad_rows += 1,
            }
        }
    }

    if names.is_empty() {
        return Err("JSON has no valid objects".to_string());
    }

    // Gather per-column values (by key), then infer a lane and build.
    let nrows = rows.len();
    let mut columns = Vec::with_capacity(names.len());
    let mut fields = Vec::with_capacity(names.len());
    for name in &names {
        let mut vals: Vec<JVal> = Vec::with_capacity(nrows);
        for obj in &rows {
            let v = obj.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone());
            vals.push(v.unwrap_or(JVal::Null));
        }
        let dtype = infer(&vals);
        columns.push(build_column(dtype, &vals));
        fields.push(Field::new(name.clone(), dtype));
    }

    Ok(JsonlData {
        schema: Schema::new(fields),
        columns,
        bad_rows,
    })
}

/// Collect objects from a top-level JSON array `[ {..}, {..}, ... ]` (which may
/// span multiple lines). Non-object elements are counted as bad rows and
/// skipped (continue-first).
fn collect_array(
    text: &str,
    names: &mut Vec<String>,
    started: &mut bool,
    rows: &mut Vec<Vec<(String, JVal)>>,
    bad_rows: &mut usize,
) {
    let b = text.as_bytes();
    let mut i = 0;
    while i < b.len() && b[i] != b'[' {
        i += 1;
    }
    i += 1; // past '['
    loop {
        skip_ws(b, &mut i);
        match b.get(i) {
            None | Some(b']') => break,
            Some(b',') => i += 1,
            Some(b'{') => {
                let start = i;
                // Capture the balanced object, then parse it.
                if parse_nested(b, &mut i, b'{', b'}').is_some() {
                    if let Some(obj) = parse_object(&text[start..i]) {
                        if !*started {
                            *names = obj.iter().map(|(k, _)| k.clone()).collect();
                            *started = true;
                        }
                        rows.push(obj);
                    } else {
                        *bad_rows += 1;
                    }
                } else {
                    break; // unterminated
                }
            }
            Some(_) => {
                // A non-object element: count it and skip past it.
                *bad_rows += 1;
                if parse_value(b, &mut i).is_none() {
                    break;
                }
            }
        }
    }
}

fn infer(vals: &[JVal]) -> DataType {
    let mut any = false;
    let mut all_int = true;
    let mut all_num = true;
    let mut all_bool = true;
    for v in vals {
        match v {
            JVal::Null => {}
            JVal::Int(_) => {
                any = true;
                all_bool = false;
            }
            JVal::Float(_) => {
                any = true;
                all_int = false;
                all_bool = false;
            }
            JVal::Bool(_) => {
                any = true;
                all_int = false;
                all_num = false;
            }
            JVal::Str(_) | JVal::Raw(_) => {
                any = true;
                all_int = false;
                all_num = false;
                all_bool = false;
            }
        }
    }
    if !any {
        DataType::Str
    } else if all_int {
        DataType::I64
    } else if all_num {
        DataType::F64
    } else if all_bool {
        DataType::Bool
    } else {
        DataType::Str
    }
}

/// Build one typed column, tracking **validity** (design 26 §26.3): a JSON
/// `null` — and a **missing key** (assembled as `JVal::Null` upstream) — becomes
/// a `null` (validity = 0), never a silent `0`/`""`. A JSON empty string `""`
/// stays a real empty string (validity = 1).
fn build_column(dtype: DataType, vals: &[JVal]) -> Column {
    let mut valid = Vec::with_capacity(vals.len());
    let data = match dtype {
        DataType::I64 => ColumnData::I64(
            vals.iter()
                .map(|v| match v {
                    JVal::Int(i) => {
                        valid.push(true);
                        *i
                    }
                    _ => {
                        valid.push(false);
                        0
                    }
                })
                .collect(),
        ),
        DataType::F64 => ColumnData::F64(
            vals.iter()
                .map(|v| match v {
                    JVal::Int(i) => {
                        valid.push(true);
                        *i as f64
                    }
                    JVal::Float(f) => {
                        valid.push(true);
                        *f
                    }
                    _ => {
                        valid.push(false);
                        0.0
                    }
                })
                .collect(),
        ),
        DataType::Bool => ColumnData::Bool(
            vals.iter()
                .map(|v| match v {
                    JVal::Bool(b) => {
                        valid.push(true);
                        *b
                    }
                    _ => {
                        valid.push(false);
                        false
                    }
                })
                .collect(),
        ),
        _ => {
            let mut s = StrColumn::with_capacity(vals.len(), vals.len() * 8);
            for v in vals {
                match v {
                    // JSON `null` / missing key → null (validity = 0). A real
                    // empty string arrives as `JVal::Str("")` and stays valid.
                    JVal::Null => {
                        s.push("");
                        valid.push(false);
                    }
                    JVal::Bool(b) => {
                        s.push(if *b { "true" } else { "false" });
                        valid.push(true);
                    }
                    JVal::Int(i) => {
                        s.push(&i.to_string());
                        valid.push(true);
                    }
                    JVal::Float(f) => {
                        s.push(&f.to_string());
                        valid.push(true);
                    }
                    JVal::Str(x) | JVal::Raw(x) => {
                        s.push(x);
                        valid.push(true);
                    }
                }
            }
            ColumnData::Str(s)
        }
    };
    Column::new(data, Validity::from_bits(&valid))
}

// ------------------------------------------------------------- streaming reader

/// Streaming per-key type flags — `infer` accumulated one value at a time so the
/// reader needn't buffer a whole column. Resolves identically to [`infer`].
#[derive(Clone)]
struct Flags {
    any: bool,
    all_int: bool,
    all_num: bool,
    all_bool: bool,
}

impl Flags {
    fn new() -> Self {
        Flags {
            any: false,
            all_int: true,
            all_num: true,
            all_bool: true,
        }
    }
    fn observe(&mut self, v: &JVal) {
        match v {
            JVal::Null => {}
            JVal::Int(_) => {
                self.any = true;
                self.all_bool = false;
            }
            JVal::Float(_) => {
                self.any = true;
                self.all_int = false;
                self.all_bool = false;
            }
            JVal::Bool(_) => {
                self.any = true;
                self.all_int = false;
                self.all_num = false;
            }
            JVal::Str(_) | JVal::Raw(_) => {
                self.any = true;
                self.all_int = false;
                self.all_num = false;
                self.all_bool = false;
            }
        }
    }
    fn resolve(&self) -> DataType {
        if !self.any {
            DataType::Str
        } else if self.all_int {
            DataType::I64
        } else if self.all_num {
            DataType::F64
        } else if self.all_bool {
            DataType::Bool
        } else {
            DataType::Str
        }
    }
}

/// Does the file begin with a top-level JSON array (`[ … ]`)? Such a document is
/// not line-oriented (an element can span lines), so it can't be streamed or
/// byte-range split — the caller falls back to the whole-file [`parse`].
pub fn is_json_array(path: &str) -> bool {
    let Ok(mut r) = FileTransport::open(path) else {
        return false;
    };
    let mut byte = [0u8; 1];
    loop {
        match r.read(&mut byte) {
            Ok(0) => return false,
            Ok(_) => {
                let c = byte[0];
                if matches!(c, b' ' | b'\t' | b'\r' | b'\n') {
                    continue;
                }
                return c == b'[';
            }
            Err(_) => return false,
        }
    }
}

/// Global schema for a JSON-Lines file (pass 1): the column order from the first
/// valid object and each key's lane inferred over every row, plus the malformed
/// line count. Byte-identical to the schema [`parse`] derives.
fn infer_global(path: &str) -> Result<(Vec<String>, Vec<DataType>, usize), String> {
    let mut r = FileTransport::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
    let mut names: Vec<String> = Vec::new();
    let mut flags: Vec<Flags> = Vec::new();
    let mut bad_rows = 0usize;
    let mut started = false;
    let mut line = String::new();
    loop {
        line.clear();
        match r.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break,
        }
        let l = line.trim_end_matches(['\n', '\r']);
        if l.trim().is_empty() {
            continue;
        }
        match parse_object(l) {
            Some(obj) => {
                if !started {
                    names = obj.iter().map(|(k, _)| k.clone()).collect();
                    flags = names.iter().map(|_| Flags::new()).collect();
                    started = true;
                }
                for (k, v) in &obj {
                    if let Some(i) = names.iter().position(|n| n == k) {
                        flags[i].observe(v);
                    }
                }
            }
            None => bad_rows += 1,
        }
    }
    if !started {
        return Err("JSON has no valid objects".to_string());
    }
    let dtypes = flags.iter().map(|f| f.resolve()).collect();
    Ok((names, dtypes, bad_rows))
}

/// A streaming JSON-Lines reader (bounded memory), two-pass like the CSV reader:
/// pass 1 ([`infer_global`]) fixes the schema, pass 2 ([`Self::next_columns`])
/// re-streams the file (or one byte range) yielding one chunk of typed columns at
/// a time. Byte-identical to the whole-file [`parse`] for line-oriented input.
pub struct JsonlChunker {
    reader: BufReader<File>,
    names: Vec<String>,
    dtypes: Vec<DataType>,
    chunk_size: usize,
    line: String,
    eof: bool,
    pos: u64,
    limit: Option<u64>,
    pub bad_rows: usize,
}

impl JsonlChunker {
    /// Open `path` for whole-file streaming (serial, bounded memory).
    pub fn open(path: &str, chunk_size: usize) -> Result<(Schema, JsonlChunker), String> {
        let (names, dtypes, bad_rows) = infer_global(path)?;
        let schema = Schema::new(
            names
                .iter()
                .zip(&dtypes)
                .map(|(n, d)| Field::new(n.clone(), *d))
                .collect(),
        );
        let reader = FileTransport::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
        Ok((
            schema,
            JsonlChunker {
                reader,
                names,
                dtypes,
                chunk_size: chunk_size.max(1),
                line: String::new(),
                eof: false,
                pos: 0,
                limit: None,
                bad_rows,
            },
        ))
    }

    /// Open `path` for streaming one newline-aligned byte range `[start, end)`
    /// with a pre-inferred global schema — one parallel worker.
    pub fn for_range(
        path: &str,
        names: Vec<String>,
        dtypes: Vec<DataType>,
        start: u64,
        end: u64,
        chunk_size: usize,
    ) -> Result<JsonlChunker, String> {
        let mut reader =
            FileTransport::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
        reader
            .seek(SeekFrom::Start(start))
            .map_err(|e| e.to_string())?;
        Ok(JsonlChunker {
            reader,
            names,
            dtypes,
            chunk_size: chunk_size.max(1),
            line: String::new(),
            eof: false,
            pos: start,
            limit: Some(end),
            bad_rows: 0,
        })
    }

    /// Yield up to `chunk_size` rows as typed columns, or `None` at the end of
    /// the file / byte range. Malformed lines are skipped (counted in pass 1).
    pub fn next_columns(&mut self) -> Option<Vec<Column>> {
        if self.eof {
            return None;
        }
        let mut per_col: Vec<Vec<JVal>> = self.names.iter().map(|_| Vec::new()).collect();
        let mut got = 0usize;
        while got < self.chunk_size {
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
            let l = self.line.trim_end_matches(['\n', '\r']);
            if l.trim().is_empty() {
                continue;
            }
            // Malformed lines are skipped (already counted in pass 1).
            if let Some(obj) = parse_object(l) {
                for (i, name) in self.names.iter().enumerate() {
                    let v = obj
                        .iter()
                        .find(|(k, _)| k == name)
                        .map(|(_, v)| v.clone())
                        .unwrap_or(JVal::Null);
                    per_col[i].push(v);
                }
                got += 1;
            }
        }
        if got == 0 {
            return None;
        }
        Some(
            self.dtypes
                .iter()
                .zip(&per_col)
                .map(|(d, vals)| build_column(*d, vals))
                .collect(),
        )
    }
}

/// Plan a byte-range parallel read of a JSON-Lines file: the global schema and
/// `nparts` newline-aligned ranges covering the file exactly once. Returns
/// `None` for a top-level array (not splittable) — the caller stays serial.
/// `(schema, column names, lanes, newline-aligned byte ranges, malformed rows)`.
pub type JsonlPlan = (Schema, Vec<String>, Vec<DataType>, Vec<(u64, u64)>, usize);

pub fn plan_parallel(path: &str, nparts: usize) -> Option<JsonlPlan> {
    if is_json_array(path) {
        return None;
    }
    let (names, dtypes, bad_rows) = infer_global(path).ok()?;
    let ranges = snap_ranges(path, nparts)?;
    if ranges.len() < 2 {
        return None;
    }
    let schema = Schema::new(
        names
            .iter()
            .zip(&dtypes)
            .map(|(n, d)| Field::new(n.clone(), *d))
            .collect(),
    );
    Some((schema, names, dtypes, ranges, bad_rows))
}

/// Split the file into ≤ `nparts` newline-aligned `[start, end)` ranges (no
/// header, so the first range starts at 0). Each boundary is snapped forward to
/// the byte just after the next `\n`, so a line never straddles two ranges.
fn snap_ranges(path: &str, nparts: usize) -> Option<Vec<(u64, u64)>> {
    let len = std::fs::metadata(path).ok()?.len();
    if len == 0 {
        return None;
    }
    let mut f = FileTransport::open(path).ok()?;
    let mut bounds = vec![0u64];
    let mut scratch = String::new();
    for i in 1..nparts {
        let approx = len * (i as u64) / (nparts as u64);
        if approx <= *bounds.last().unwrap() {
            continue;
        }
        if f.seek(SeekFrom::Start(approx)).is_err() {
            continue;
        }
        scratch.clear();
        let consumed = f.read_line(&mut scratch).ok()?; // finish the partial line
        let boundary = approx + consumed as u64;
        if boundary < len && boundary > *bounds.last().unwrap() {
            bounds.push(boundary);
        }
    }
    bounds.push(len);
    Some(bounds.windows(2).map(|w| (w[0], w[1])).collect())
}

// ----------------------------------------------------------------- JSON parsing

/// Parse a single flat JSON object line into `(key, value)` pairs. Returns
/// `None` if the line is not a well-formed object (→ counted as a bad row).
fn parse_object(line: &str) -> Option<Vec<(String, JVal)>> {
    let b = line.as_bytes();
    let mut i = 0usize;
    skip_ws(b, &mut i);
    if i >= b.len() || b[i] != b'{' {
        return None;
    }
    i += 1;
    let mut out = Vec::new();
    skip_ws(b, &mut i);
    if i < b.len() && b[i] == b'}' {
        return Some(out); // empty object
    }
    loop {
        skip_ws(b, &mut i);
        let key = parse_string(b, &mut i)?;
        skip_ws(b, &mut i);
        if i >= b.len() || b[i] != b':' {
            return None;
        }
        i += 1;
        skip_ws(b, &mut i);
        let val = parse_value(b, &mut i)?;
        out.push((key, val));
        skip_ws(b, &mut i);
        match b.get(i) {
            Some(b',') => {
                i += 1;
                continue;
            }
            Some(b'}') => break, // object closed; `i` no longer needed
            _ => return None,
        }
    }
    Some(out)
}

fn skip_ws(b: &[u8], i: &mut usize) {
    while *i < b.len() && matches!(b[*i], b' ' | b'\t' | b'\r' | b'\n') {
        *i += 1;
    }
}

fn parse_string(b: &[u8], i: &mut usize) -> Option<String> {
    if *i >= b.len() || b[*i] != b'"' {
        return None;
    }
    *i += 1;
    let mut s = String::new();
    while *i < b.len() {
        let c = b[*i];
        *i += 1;
        match c {
            b'"' => return Some(s),
            b'\\' => {
                let e = *b.get(*i)?;
                *i += 1;
                match e {
                    b'"' => s.push('"'),
                    b'\\' => s.push('\\'),
                    b'/' => s.push('/'),
                    b'n' => s.push('\n'),
                    b't' => s.push('\t'),
                    b'r' => s.push('\r'),
                    b'b' => s.push('\u{8}'),
                    b'f' => s.push('\u{c}'),
                    b'u' => {
                        // \uXXXX — decode a BMP code point (no surrogate pairing).
                        let hex = b.get(*i..*i + 4)?;
                        let code = u32::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok()?;
                        *i += 4;
                        s.push(char::from_u32(code).unwrap_or('\u{fffd}'));
                    }
                    other => s.push(other as char),
                }
            }
            // Multi-byte UTF-8 continuation: push raw bytes through.
            _ => {
                // Reconstruct the original char from this byte and any
                // continuation bytes (the slice is valid UTF-8).
                let start = *i - 1;
                while *i < b.len() && (b[*i] & 0xC0) == 0x80 {
                    *i += 1;
                }
                s.push_str(std::str::from_utf8(&b[start..*i]).ok()?);
            }
        }
    }
    None // unterminated string
}

fn parse_value(b: &[u8], i: &mut usize) -> Option<JVal> {
    skip_ws(b, i);
    match b.get(*i)? {
        b'"' => parse_string(b, i).map(JVal::Str),
        b'{' => parse_nested(b, i, b'{', b'}').map(JVal::Raw),
        b'[' => parse_nested(b, i, b'[', b']').map(JVal::Raw),
        b't' => parse_lit(b, i, "true", JVal::Bool(true)),
        b'f' => parse_lit(b, i, "false", JVal::Bool(false)),
        b'n' => parse_lit(b, i, "null", JVal::Null),
        _ => parse_number(b, i),
    }
}

fn parse_lit(b: &[u8], i: &mut usize, lit: &str, val: JVal) -> Option<JVal> {
    if b[*i..].starts_with(lit.as_bytes()) {
        *i += lit.len();
        Some(val)
    } else {
        None
    }
}

fn parse_number(b: &[u8], i: &mut usize) -> Option<JVal> {
    let start = *i;
    let mut is_float = false;
    if b.get(*i) == Some(&b'-') {
        *i += 1;
    }
    while *i < b.len() {
        match b[*i] {
            b'0'..=b'9' => *i += 1,
            b'.' | b'e' | b'E' | b'+' | b'-' => {
                is_float = true;
                *i += 1;
            }
            _ => break,
        }
    }
    let text = std::str::from_utf8(&b[start..*i]).ok()?;
    if text.is_empty() || text == "-" {
        return None;
    }
    if is_float {
        text.parse::<f64>().ok().map(JVal::Float)
    } else {
        match text.parse::<i64>() {
            Ok(n) => Some(JVal::Int(n)),
            Err(_) => text.parse::<f64>().ok().map(JVal::Float),
        }
    }
}

/// Capture a balanced `{...}` or `[...]` (string-aware) as raw text.
fn parse_nested(b: &[u8], i: &mut usize, open: u8, close: u8) -> Option<String> {
    let start = *i;
    let mut depth = 0i32;
    let mut in_str = false;
    while *i < b.len() {
        let c = b[*i];
        *i += 1;
        if in_str {
            match c {
                b'\\' => {
                    *i += 1;
                }
                b'"' => in_str = false,
                _ => {}
            }
            continue;
        }
        match c {
            b'"' => in_str = true,
            x if x == open => depth += 1,
            x if x == close => {
                depth -= 1;
                if depth == 0 {
                    return std::str::from_utf8(&b[start..*i])
                        .ok()
                        .map(|s| s.to_string());
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_flat_objects_and_infers_types() {
        let text = "{\"name\":\"aki\",\"age\":30,\"score\":1.5,\"ok\":true}\n\
                    {\"name\":\"ben\",\"age\":15,\"score\":2.0,\"ok\":false}\n";
        let d = parse(text).unwrap();
        assert_eq!(d.bad_rows, 0);
        assert_eq!(d.schema.field_names(), vec!["name", "age", "score", "ok"]);
        assert_eq!(d.schema.fields[1].dtype, DataType::I64);
        assert_eq!(d.schema.fields[2].dtype, DataType::F64);
        assert_eq!(d.schema.fields[3].dtype, DataType::Bool);
        match d.columns[0].data() {
            ColumnData::Str(s) => assert_eq!(s.get(0), "aki"),
            _ => panic!("expected str"),
        }
    }

    #[test]
    fn bad_lines_are_skipped() {
        let text = "{\"a\":1}\nnot json\n{\"a\":2}\n";
        let d = parse(text).unwrap();
        assert_eq!(d.bad_rows, 1);
        match d.columns[0].data() {
            ColumnData::I64(v) => assert_eq!(v, &[1, 2]),
            _ => panic!("expected i64"),
        }
    }

    #[test]
    fn parses_json_array_multiline() {
        // A top-level array (possibly pretty-printed) of objects, like an API
        // response, parses the same as JSON Lines.
        let text = "[\n  {\"name\":\"aki\",\"age\":30},\n  {\"name\":\"ben\",\"age\":15},\n  42,\n  {\"name\":\"cho\",\"age\":40}\n]";
        let d = parse(text).unwrap();
        assert_eq!(d.schema.field_names(), vec!["name", "age"]);
        assert_eq!(d.bad_rows, 1); // the bare `42` element
        match d.columns[1].data() {
            ColumnData::I64(v) => assert_eq!(v, &[30, 15, 40]),
            _ => panic!("expected i64 age"),
        }
    }

    #[test]
    fn nested_value_kept_as_raw_string() {
        let text = "{\"id\":1,\"meta\":{\"x\":2}}\n";
        let d = parse(text).unwrap();
        let idx = d.schema.index_of("meta").unwrap();
        match d.columns[idx].data() {
            ColumnData::Str(s) => assert!(s.get(0).contains("\"x\"")),
            _ => panic!("expected raw string column"),
        }
    }
}
