//! Minimal CSV reader with per-column type inference.
//!
//! MVP-grade: the whole file is read and materialized, then handed out in
//! chunks. The design doc (03-stream-chunk-model.md) describes the streaming,
//! Arrow-backed reader that replaces this behind the same `Operator` boundary.
//! Quoting is handled just enough for simple fields.

use rivus_core::{Column, DataType, Field, Schema};

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
    let names: Vec<String> = split_record(header)
        .into_iter()
        .map(|s| s.to_string())
        .collect();
    let ncols = names.len();

    let mut raw: Vec<Vec<String>> = vec![Vec::new(); ncols];
    let mut bad_rows = 0;
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let fields = split_record(line);
        if fields.len() != ncols {
            bad_rows += 1;
            continue;
        }
        for (i, f) in fields.into_iter().enumerate() {
            raw[i].push(f);
        }
    }

    let mut columns = Vec::with_capacity(ncols);
    let mut fields = Vec::with_capacity(ncols);
    for (i, name) in names.iter().enumerate() {
        let dtype = infer(&raw[i]);
        let col = build_column(dtype, &raw[i]);
        fields.push(Field::new(name.clone(), dtype));
        columns.push(col);
    }

    Ok(CsvData {
        schema: Schema::new(fields),
        columns,
        bad_rows,
    })
}

fn infer(cells: &[String]) -> DataType {
    let mut all_int = true;
    let mut all_float = true;
    let mut all_bool = true;
    let mut any = false;
    for c in cells {
        let c = c.trim();
        if c.is_empty() {
            continue;
        }
        any = true;
        if c.parse::<i64>().is_err() {
            all_int = false;
        }
        if c.parse::<f64>().is_err() {
            all_float = false;
        }
        if !matches!(c, "true" | "false") {
            all_bool = false;
        }
    }
    if !any {
        return DataType::Str;
    }
    if all_int {
        DataType::I64
    } else if all_float {
        DataType::F64
    } else if all_bool {
        DataType::Bool
    } else {
        DataType::Str
    }
}

fn build_column(dtype: DataType, cells: &[String]) -> Column {
    match dtype {
        DataType::I64 => Column::I64(
            cells
                .iter()
                .map(|c| c.trim().parse().unwrap_or(0))
                .collect(),
        ),
        DataType::F64 => Column::F64(
            cells
                .iter()
                .map(|c| c.trim().parse().unwrap_or(0.0))
                .collect(),
        ),
        DataType::Bool => Column::Bool(cells.iter().map(|c| c.trim() == "true").collect()),
        _ => Column::Str(cells.to_vec()),
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
