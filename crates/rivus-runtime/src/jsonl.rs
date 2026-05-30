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

use rivus_core::{Column, DataType, Field, Schema, StrColumn};

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
    // First pass: column order from the first valid object.
    let mut names: Vec<String> = Vec::new();
    let mut started = false;
    let mut rows: Vec<Vec<(String, JVal)>> = Vec::new();
    let mut bad_rows = 0;

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

    if names.is_empty() {
        return Err("JSONL has no valid object lines".to_string());
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

fn build_column(dtype: DataType, vals: &[JVal]) -> Column {
    match dtype {
        DataType::I64 => Column::I64(
            vals.iter()
                .map(|v| match v {
                    JVal::Int(i) => *i,
                    _ => 0,
                })
                .collect(),
        ),
        DataType::F64 => Column::F64(
            vals.iter()
                .map(|v| match v {
                    JVal::Int(i) => *i as f64,
                    JVal::Float(f) => *f,
                    _ => 0.0,
                })
                .collect(),
        ),
        DataType::Bool => {
            Column::Bool(vals.iter().map(|v| matches!(v, JVal::Bool(true))).collect())
        }
        _ => {
            let mut s = StrColumn::with_capacity(vals.len(), vals.len() * 8);
            for v in vals {
                match v {
                    JVal::Null => s.push(""),
                    JVal::Bool(b) => s.push(if *b { "true" } else { "false" }),
                    JVal::Int(i) => s.push(&i.to_string()),
                    JVal::Float(f) => s.push(&f.to_string()),
                    JVal::Str(x) | JVal::Raw(x) => s.push(x),
                }
            }
            Column::Str(s)
        }
    }
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
        match &d.columns[0] {
            Column::Str(s) => assert_eq!(s.get(0), "aki"),
            _ => panic!("expected str"),
        }
    }

    #[test]
    fn bad_lines_are_skipped() {
        let text = "{\"a\":1}\nnot json\n{\"a\":2}\n";
        let d = parse(text).unwrap();
        assert_eq!(d.bad_rows, 1);
        match &d.columns[0] {
            Column::I64(v) => assert_eq!(v, &[1, 2]),
            _ => panic!("expected i64"),
        }
    }

    #[test]
    fn nested_value_kept_as_raw_string() {
        let text = "{\"id\":1,\"meta\":{\"x\":2}}\n";
        let d = parse(text).unwrap();
        let idx = d.schema.index_of("meta").unwrap();
        match &d.columns[idx] {
            Column::Str(s) => assert!(s.get(0).contains("\"x\"")),
            _ => panic!("expected raw string column"),
        }
    }
}
