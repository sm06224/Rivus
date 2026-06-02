//! The Chunk: Rivus's first-class unit of execution.
//!
//! Chunk-native (Master principle #6): the runtime moves *chunks*, never single
//! items. A chunk is columnar (SIMD-friendly), carries observable metadata, and
//! is checkpointable. The MVP stores columns as plain `Vec`s; the design doc
//! `03-stream-chunk-model.md` describes the Arrow-backed, zero-copy successor
//! that slots in behind this same API.

use crate::schema::Schema;
use crate::value::{DataType, Value};
use std::sync::Arc;
use std::time::Instant;

/// Runtime execution mode (Observability spec §5). Carried on every chunk so
/// the mode at the time of production is observable downstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mode {
    #[default]
    Normal,
    Degraded,
    Recovery,
    Isolation,
    Emergency,
    Halted,
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Mode::Normal => "normal",
            Mode::Degraded => "degraded",
            Mode::Recovery => "recovery",
            Mode::Isolation => "isolation",
            Mode::Emergency => "emergency",
            Mode::Halted => "halted",
        };
        f.write_str(s)
    }
}

/// Per-chunk observable metadata (Observability spec §9).
#[derive(Debug, Clone)]
pub struct ChunkMeta {
    pub id: u64,
    pub created_at: Instant,
    pub warnings: Vec<String>,
    pub corrupt: bool,
    pub mode: Mode,
}

impl ChunkMeta {
    pub fn new(id: u64) -> Self {
        ChunkMeta {
            id,
            created_at: Instant::now(),
            warnings: Vec::new(),
            corrupt: false,
            mode: Mode::Normal,
        }
    }
}

/// An Arrow-like UTF-8 string column: one contiguous byte buffer plus per-row
/// offsets. A cell is a `&str` slice — there is **no allocation per cell**,
/// which both speeds the reader and removes the cross-thread allocator
/// contention measured in `docs/BENCHMARKS.md` (Phase 0.5).
#[derive(Debug, Clone)]
pub struct StrColumn {
    /// `offsets.len() == rows + 1`, `offsets[0] == 0`, monotonically increasing.
    offsets: Vec<u32>,
    data: Vec<u8>,
}

impl Default for StrColumn {
    /// A valid **empty** column: `offsets == [0]` (not an empty vec), so the
    /// `offsets.len() == rows + 1` invariant holds and `push` works correctly.
    fn default() -> Self {
        StrColumn {
            offsets: vec![0],
            data: Vec::new(),
        }
    }
}

impl StrColumn {
    pub fn with_capacity(rows: usize, bytes: usize) -> Self {
        let mut offsets = Vec::with_capacity(rows + 1);
        offsets.push(0);
        StrColumn {
            offsets,
            data: Vec::with_capacity(bytes),
        }
    }

    pub fn push(&mut self, s: &str) {
        self.data.extend_from_slice(s.as_bytes());
        self.offsets.push(self.data.len() as u32);
    }

    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, row: usize) -> &str {
        let start = self.offsets[row] as usize;
        let end = self.offsets[row + 1] as usize;
        // SAFETY: `data` is only ever appended to via `push`, whose argument is
        // a `&str` (valid UTF-8), and offsets fall on those append boundaries,
        // so every `[start, end)` slice is itself valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(&self.data[start..end]) }
    }

    pub fn gather(&self, indices: &[usize]) -> StrColumn {
        let mut out = StrColumn::with_capacity(indices.len(), self.data.len());
        for &i in indices {
            out.push(self.get(i));
        }
        out
    }

    pub fn append(&mut self, other: &StrColumn) {
        for i in 0..other.len() {
            self.push(other.get(i));
        }
    }
}

impl<'a> FromIterator<&'a str> for StrColumn {
    fn from_iter<I: IntoIterator<Item = &'a str>>(iter: I) -> Self {
        let mut c = StrColumn::default(); // already seeded with offsets == [0]
        for s in iter {
            c.push(s);
        }
        c
    }
}

impl From<Vec<String>> for StrColumn {
    fn from(v: Vec<String>) -> Self {
        let bytes = v.iter().map(|s| s.len()).sum();
        let mut c = StrColumn::with_capacity(v.len(), bytes);
        for s in &v {
            c.push(s);
        }
        c
    }
}

/// An exact fixed-point column: contiguous `i128` unscaled integers sharing one
/// `scale` (design doc 21). The whole column rides the decimal lane, so a value
/// is `unscaled[i] × 10^(−scale)`. Addition over `i128` is exact and
/// associative, which is what makes parallel aggregation byte-identical.
#[derive(Debug, Clone, PartialEq)]
pub struct DecColumn {
    pub unscaled: Vec<i128>,
    pub scale: u8,
}

/// A datetime column: contiguous epoch `ticks` (i64) sharing one `unit` (design
/// doc 23). Integer representation → exact and associative, like the decimal
/// lane, so `min`/`max`/`count`/`first`/`last` parallelize byte-identically.
#[derive(Debug, Clone, PartialEq)]
pub struct DtColumn {
    pub ticks: Vec<i64>,
    pub unit: crate::value::TimeUnit,
}

/// A columnar buffer. One variant per execution lane (MVP subset).
#[derive(Debug, Clone)]
pub enum Column {
    Bool(Vec<bool>),
    I64(Vec<i64>),
    F64(Vec<f64>),
    /// Exact fixed-point lane (opt-in; design doc 21).
    Dec(DecColumn),
    /// Datetime lane (epoch ticks; design doc 23).
    DateTime(DtColumn),
    Str(StrColumn),
}

impl Column {
    pub fn len(&self) -> usize {
        match self {
            Column::Bool(v) => v.len(),
            Column::I64(v) => v.len(),
            Column::F64(v) => v.len(),
            Column::Dec(v) => v.unscaled.len(),
            Column::DateTime(v) => v.ticks.len(),
            Column::Str(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn dtype(&self) -> DataType {
        match self {
            Column::Bool(_) => DataType::Bool,
            Column::I64(_) => DataType::I64,
            Column::F64(_) => DataType::F64,
            Column::Dec(v) => DataType::Decimal { scale: v.scale },
            Column::DateTime(v) => DataType::DateTime { unit: v.unit },
            Column::Str(_) => DataType::Str,
        }
    }

    pub fn value_at(&self, row: usize) -> Value {
        match self {
            Column::Bool(v) => Value::Bool(v[row]),
            Column::I64(v) => Value::I64(v[row]),
            Column::F64(v) => Value::F64(v[row]),
            Column::Dec(v) => Value::Dec(crate::value::Decimal::new(v.unscaled[row], v.scale)),
            Column::DateTime(v) => {
                Value::DateTime(crate::value::DateTime::new(v.ticks[row], v.unit))
            }
            Column::Str(v) => Value::Str(v.get(row).to_string()),
        }
    }

    /// Append another column of the same variant (used to concatenate buffered
    /// chunks before a blocking sort). Mismatched variants are ignored — within
    /// one stream every chunk shares the schema, so this never happens there.
    pub fn append(&mut self, other: &Column) {
        match (self, other) {
            (Column::Bool(a), Column::Bool(b)) => a.extend_from_slice(b),
            (Column::I64(a), Column::I64(b)) => a.extend_from_slice(b),
            (Column::F64(a), Column::F64(b)) => a.extend_from_slice(b),
            (Column::Dec(a), Column::Dec(b)) => a.unscaled.extend_from_slice(&b.unscaled),
            (Column::DateTime(a), Column::DateTime(b)) => a.ticks.extend_from_slice(&b.ticks),
            (Column::Str(a), Column::Str(b)) => a.append(b),
            _ => {}
        }
    }

    /// Gather a new column from optional row indices: `Some(i)` takes row `i`,
    /// `None` writes the type's default (`false` / `0` / `0.0` / `""`). Used by
    /// outer joins, where an unmatched side contributes a null-like default.
    pub fn gather_opt(&self, indices: &[Option<usize>]) -> Column {
        match self {
            Column::Bool(v) => {
                Column::Bool(indices.iter().map(|o| o.is_some_and(|i| v[i])).collect())
            }
            Column::I64(v) => Column::I64(indices.iter().map(|o| o.map_or(0, |i| v[i])).collect()),
            Column::F64(v) => {
                Column::F64(indices.iter().map(|o| o.map_or(0.0, |i| v[i])).collect())
            }
            Column::Dec(v) => Column::Dec(DecColumn {
                unscaled: indices
                    .iter()
                    .map(|o| o.map_or(0, |i| v.unscaled[i]))
                    .collect(),
                scale: v.scale,
            }),
            Column::DateTime(v) => Column::DateTime(DtColumn {
                ticks: indices
                    .iter()
                    .map(|o| o.map_or(0, |i| v.ticks[i]))
                    .collect(),
                unit: v.unit,
            }),
            Column::Str(v) => {
                let mut out = StrColumn::with_capacity(indices.len(), 0);
                for o in indices {
                    out.push(o.map_or("", |i| v.get(i)));
                }
                Column::Str(out)
            }
        }
    }

    /// Gather a new column from selected row indices (used by filter/join).
    pub fn gather(&self, indices: &[usize]) -> Column {
        match self {
            Column::Bool(v) => Column::Bool(indices.iter().map(|&i| v[i]).collect()),
            Column::I64(v) => Column::I64(indices.iter().map(|&i| v[i]).collect()),
            Column::F64(v) => Column::F64(indices.iter().map(|&i| v[i]).collect()),
            Column::Dec(v) => Column::Dec(DecColumn {
                unscaled: indices.iter().map(|&i| v.unscaled[i]).collect(),
                scale: v.scale,
            }),
            Column::DateTime(v) => Column::DateTime(DtColumn {
                ticks: indices.iter().map(|&i| v.ticks[i]).collect(),
                unit: v.unit,
            }),
            Column::Str(v) => Column::Str(v.gather(indices)),
        }
    }
}

/// A bounded, columnar, metadata-bearing batch of rows.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub meta: ChunkMeta,
    pub schema: Arc<Schema>,
    pub columns: Vec<Column>,
    pub len: usize,
}

impl Chunk {
    pub fn new(id: u64, schema: Arc<Schema>, columns: Vec<Column>) -> Self {
        let len = columns.first().map(|c| c.len()).unwrap_or(0);
        debug_assert!(
            columns.iter().all(|c| c.len() == len),
            "ragged chunk: all columns must share length"
        );
        Chunk {
            meta: ChunkMeta::new(id),
            schema,
            columns,
            len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        let idx = self.schema.index_of(name)?;
        self.columns.get(idx)
    }

    pub fn value(&self, row: usize, col: usize) -> Value {
        self.columns[col].value_at(row)
    }

    /// Keep only the rows whose index appears in `indices`, preserving schema
    /// and metadata. The backbone of filter, branch and join.
    pub fn gather(&self, indices: &[usize]) -> Chunk {
        let columns = self.columns.iter().map(|c| c.gather(indices)).collect();
        Chunk {
            meta: self.meta.clone(),
            schema: self.schema.clone(),
            columns,
            len: indices.len(),
        }
    }

    /// Project to a subset of columns by name (the `|>` transform). Returns the
    /// original chunk unchanged if any name is missing (continue-first).
    pub fn project(&self, names: &[String]) -> Option<Chunk> {
        let schema = self.schema.project(names)?;
        let mut columns = Vec::with_capacity(names.len());
        for n in names {
            let idx = self.schema.index_of(n)?;
            columns.push(self.columns[idx].clone());
        }
        Some(Chunk {
            meta: self.meta.clone(),
            schema: Arc::new(schema),
            columns,
            len: self.len,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::StrColumn;

    #[test]
    fn default_strcolumn_is_a_valid_empty_column() {
        // `default()` must seed offsets == [0] so the first push is addressable
        // (regression: an empty offsets vec lost/shifted the first cell).
        let mut c = StrColumn::default();
        assert_eq!(c.len(), 0);
        c.push("first");
        c.push("second");
        assert_eq!(c.len(), 2);
        assert_eq!(c.get(0), "first");
        assert_eq!(c.get(1), "second");
    }
}
