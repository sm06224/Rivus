//! The Chunk: Rivus's first-class unit of execution.
//!
//! Chunk-native (Master principle #6): the runtime moves *chunks*, never single
//! items. A chunk is columnar (SIMD-friendly), carries observable metadata, and
//! is checkpointable. The MVP stores columns as plain `Vec`s; the design doc
//! `03-stream-chunk-model.md` describes the Arrow-backed, zero-copy successor
//! that slots in behind this same API.

use crate::schema::Schema;
use crate::value::{DataType, Resource, Value};
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
    /// Origin handle of this chunk (design §28.6 provenance): set by a source
    /// operator when `with source` / `with filename` is requested, reachable
    /// downstream via the `source.uri` accessor. `None` by default — zero
    /// overhead when provenance is off. Only the uri is in-contract (§00 0.14),
    /// and every reader (serial and each byte-range parallel worker) derives the
    /// same handle from the same path, so it stays byte-identical.
    pub source: Option<Resource>,
}

impl ChunkMeta {
    pub fn new(id: u64) -> Self {
        ChunkMeta {
            id,
            created_at: Instant::now(),
            warnings: Vec::new(),
            corrupt: false,
            mode: Mode::Normal,
            source: None,
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

/// A duration column: contiguous signed tick spans (i64) sharing one `unit`
/// (design 23 / #57). Integer representation → exact and **associative**, so
/// `sum`/`avg`/`min`/`max` parallelize byte-identically (unlike a datetime
/// instant, a duration's sum/avg are meaningful).
#[derive(Debug, Clone, PartialEq)]
pub struct DurColumn {
    pub ticks: Vec<i64>,
    pub unit: crate::value::TimeUnit,
}

/// Per-column **validity bitmap** (the null model; design doc 26 §26.1).
///
/// One bit per row: bit = 1 means *valid* (a real value), bit = 0 means *null*
/// (missing). `None` means "this column has no nulls" — **zero overhead**, so
/// the dense fast path (SWAR/SIMD scans, the exact integer/decimal/datetime
/// lanes) is untouched for the common all-valid case. Only a column that
/// actually carries a null pays 1 bit/row. This is the Arrow-compatible shape,
/// so a future backend maps onto it directly (design 01, "Operator boundary is
/// thin").
///
/// `null` is structurally distinct from empty-string `""` (a real `Str` value)
/// and from `0`/epoch-0 (a real integer value): those keep validity = 1 with
/// their real backing byte; only a genuinely missing cell gets validity = 0.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Validity(Option<Box<[u64]>>);

impl Validity {
    /// The all-valid column (no nulls) — the zero-cost default.
    pub const fn all_valid() -> Self {
        Validity(None)
    }

    /// An **all-null** column of `len` rows (every bit 0). Used where a value is
    /// known missing for the whole column (e.g. a `null` constant). `len == 0`
    /// stays the zero-cost all-valid form (no rows to be null).
    pub fn all_null(len: usize) -> Self {
        if len == 0 {
            return Validity(None);
        }
        Validity(Some(vec![0u64; len.div_ceil(64)].into_boxed_slice()))
    }

    /// Does this column carry at least one null? `false` keeps the fast path.
    pub fn has_nulls(&self) -> bool {
        self.0.is_some()
    }

    /// Is `row` null? All-valid columns answer `false` without touching memory.
    #[inline]
    pub fn is_null(&self, row: usize) -> bool {
        match &self.0 {
            None => false,
            Some(words) => (words[row >> 6] >> (row & 63)) & 1 == 0,
        }
    }

    /// Build from per-row validity bits (`true` = valid). Collapses to the
    /// zero-cost all-valid form when every bit is set (the common case), so a
    /// column only allocates a bitmap once it truly has a null.
    pub fn from_bits(bits: &[bool]) -> Self {
        if bits.iter().all(|&b| b) {
            return Validity(None);
        }
        let words = bits.len().div_ceil(64);
        let mut w = vec![0u64; words].into_boxed_slice();
        for (i, &b) in bits.iter().enumerate() {
            if b {
                w[i >> 6] |= 1u64 << (i & 63);
            }
        }
        Validity(Some(w))
    }

    /// Gather validity for selected rows (parallels `Column::gather`). Stays
    /// all-valid (zero-cost) when the source has no nulls.
    pub fn gather(&self, indices: &[usize]) -> Self {
        if self.0.is_none() {
            return Validity(None);
        }
        let bits: Vec<bool> = indices.iter().map(|&i| !self.is_null(i)).collect();
        Validity::from_bits(&bits)
    }

    /// Gather with optional indices: `None` (an unmatched outer-join side)
    /// contributes a **null** (validity = 0), matching `Column::gather_opt`.
    pub fn gather_opt(&self, indices: &[Option<usize>]) -> Self {
        let bits: Vec<bool> = indices
            .iter()
            .map(|o| match o {
                Some(i) => !self.is_null(*i),
                None => false,
            })
            .collect();
        Validity::from_bits(&bits)
    }

    /// Append `other` (of length `other_len`) after `self` (of length
    /// `self_len`). Stays all-valid (zero-cost) when both sides are all-valid,
    /// so concatenating dense chunks never allocates a bitmap.
    pub fn append(&mut self, self_len: usize, other: &Validity, other_len: usize) {
        // Both dense → stays all-valid (zero-cost); concatenating dense chunks
        // never allocates a bitmap.
        if self.0.is_none() && other.0.is_none() {
            return;
        }
        // Word-granular in-place append. The previous form materialized the
        // ENTIRE accumulated bitmap into a `Vec<bool>` and repacked it on every
        // call, so a buffering operator concatenating N chunks (join/sort/group/
        // merge) paid O(N²) once any column had a null — a 735-chunk left join
        // spent 5.3s here. This grows the packed word buffer and OR-sets only the
        // newly-appended range, so a full concat is ~O(total_rows / 64). The bits
        // written are identical, so byte-identity holds.
        let new_len = self_len + other_len;
        let words = new_len.div_ceil(64);
        let mut w: Vec<u64> = match self.0.take() {
            Some(b) => {
                let mut v = b.into_vec();
                v.resize(words, 0);
                v
            }
            None => {
                // `self` was all-valid: bits [0, self_len) are all 1.
                let mut v = vec![0u64; words];
                for i in 0..self_len {
                    v[i >> 6] |= 1u64 << (i & 63);
                }
                v
            }
        };
        match &other.0 {
            // `other` all-valid: set [self_len, new_len) to 1.
            None => {
                for i in self_len..new_len {
                    w[i >> 6] |= 1u64 << (i & 63);
                }
            }
            // `other` has a bitmap: copy each valid bit to offset self_len + j.
            Some(ob) => {
                for j in 0..other_len {
                    if (ob[j >> 6] >> (j & 63)) & 1 == 1 {
                        let i = self_len + j;
                        w[i >> 6] |= 1u64 << (i & 63);
                    }
                }
            }
        }
        self.0 = Some(w.into_boxed_slice());
    }
}

/// The backing buffer of a column — one variant per execution lane (MVP
/// subset). This is the dense, **validity-free** value lane; null-ness rides
/// alongside in [`Column::validity`]. A null row keeps a type-default backing
/// value (`0`/`0.0`/`""`) so SWAR/SIMD scans and the exact integer lanes stay
/// branch-free; the validity bit, not the backing byte, decides null.
#[derive(Debug, Clone)]
pub enum ColumnData {
    Bool(Vec<bool>),
    I64(Vec<i64>),
    F64(Vec<f64>),
    /// Exact fixed-point lane (opt-in; design doc 21).
    Dec(DecColumn),
    /// Datetime lane (epoch ticks; design doc 23).
    DateTime(DtColumn),
    /// Duration lane (signed tick span; design 23 / #57).
    Duration(DurColumn),
    /// Calendar date lane (i32 epoch-day, no time-of-day; #58). Integer → exact
    /// and associative, like the datetime/duration lanes.
    Date(Vec<i32>),
    /// Time-of-day lane (i64 ticks since midnight; #58, MVP `Sec`).
    Time(Vec<i64>),
    Str(StrColumn),
    /// **Dictionary-encoded** Str lane (design/42, 批准 2026-07-19): a small
    /// distinct-value dictionary plus per-row codes. Strictly a REPRESENTATION,
    /// never a type — the schema dtype stays `Str`, `value(row)` returns the
    /// same `Value::Str`, and every byte that leaves the engine (keys, casts,
    /// `write_cell`) is identical to the plain lane's (pinned by the
    /// dict-vs-plain property tests). Produced selectively by readers for
    /// low-cardinality columns (stage b); consumers that don't know about it
    /// fall through the same accessors and stay correct.
    StrDict(DictColumn),
    /// I/O resource handle lane (design §28.1): uri-backed, like [`StrColumn`].
    /// The uri is the in-contract identity; `size`/`mtime` (out of the
    /// determinism contract, §00 0.14) ride on the scalar [`Value::Resource`]
    /// when present and are not stored on the bulk lane.
    Resource(StrColumn),
    /// **Struct** lane (§32 s3): a bundle of named child columns, all the same
    /// length as the struct column (Arrow struct layout). Each child carries its
    /// own validity, so the null model (§26) recurses. The flat scalar lanes
    /// above are the untouched fast path; nesting is recursive over them.
    Struct(StructColumn),
    /// **List** lane (§32 s3): `i32` offsets + a single child column (Arrow list
    /// layout). Row `i` spans `offsets[i]..offsets[i+1]` of the child; the column
    /// length is `offsets.len() - 1`.
    List(ListColumn),
}

/// Backing for [`ColumnData::StrDict`] (design/42): distinct values in
/// `dict`, one `u32` code per row. The dictionary is chunk-owned and small by
/// construction (the reader's escape hatch caps distinct counts), so `Clone`
/// is a bounded copy. Row nullability rides the column's `Validity` exactly
/// like the plain Str lane — a null row's code is 0 and never read.
#[derive(Debug, Clone)]
pub struct DictColumn {
    pub dict: StrColumn,
    pub codes: Vec<u32>,
}

impl DictColumn {
    /// The row's string — the dictionary entry its code points at.
    #[inline]
    pub fn get(&self, row: usize) -> &str {
        self.dict.get(self.codes[row] as usize)
    }
    pub fn len(&self) -> usize {
        self.codes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.codes.is_empty()
    }
    /// Decode into the plain lane — row-for-row the same bytes.
    pub fn materialize(&self) -> StrColumn {
        let mut out = StrColumn::with_capacity(self.len(), 0);
        for &c in &self.codes {
            out.push(self.dict.get(c as usize));
        }
        out
    }
}

// Backing for [`ColumnData::Struct`]: named child columns (Arrow struct).
#[derive(Debug, Clone)]
pub struct StructColumn {
    /// Child field names, parallel to `columns`.
    pub names: Vec<String>,
    /// Child columns, each the same length as the struct column.
    pub columns: Vec<Column>,
    /// Row count (kept explicit so a childless struct still has a length).
    pub len: usize,
}

/// Backing for [`ColumnData::List`]: offsets + child column (Arrow list).
#[derive(Debug, Clone)]
pub struct ListColumn {
    /// `len + 1` monotonically non-decreasing offsets into `child`.
    pub offsets: Vec<i32>,
    /// The flattened element column.
    pub child: Box<Column>,
}

impl ColumnData {
    pub fn len(&self) -> usize {
        match self {
            ColumnData::Bool(v) => v.len(),
            ColumnData::I64(v) => v.len(),
            ColumnData::F64(v) => v.len(),
            ColumnData::Dec(v) => v.unscaled.len(),
            ColumnData::DateTime(v) => v.ticks.len(),
            ColumnData::Duration(v) => v.ticks.len(),
            ColumnData::Date(v) => v.len(),
            ColumnData::Time(v) => v.len(),
            ColumnData::Str(v) => v.len(),
            ColumnData::StrDict(v) => v.len(),
            ColumnData::Resource(v) => v.len(),
            ColumnData::Struct(s) => s.len,
            ColumnData::List(l) => l.offsets.len().saturating_sub(1),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn dtype(&self) -> DataType {
        match self {
            ColumnData::Bool(_) => DataType::Bool,
            ColumnData::I64(_) => DataType::I64,
            ColumnData::F64(_) => DataType::F64,
            ColumnData::Dec(v) => DataType::Decimal { scale: v.scale },
            ColumnData::DateTime(v) => DataType::DateTime { unit: v.unit },
            ColumnData::Duration(v) => DataType::Duration { unit: v.unit },
            ColumnData::Date(_) => DataType::Date,
            ColumnData::Time(_) => DataType::Time,
            // Representation, not a type (design/42 §2): a dict lane IS Str.
            ColumnData::Str(_) | ColumnData::StrDict(_) => DataType::Str,
            ColumnData::Resource(_) => DataType::Resource,
            ColumnData::Struct(_) => DataType::Struct,
            ColumnData::List(_) => DataType::List,
        }
    }

    pub fn value_at(&self, row: usize) -> Value {
        match self {
            ColumnData::Bool(v) => Value::Bool(v[row]),
            ColumnData::I64(v) => Value::I64(v[row]),
            ColumnData::F64(v) => Value::F64(v[row]),
            ColumnData::Dec(v) => Value::Dec(crate::value::Decimal::new(v.unscaled[row], v.scale)),
            ColumnData::DateTime(v) => {
                Value::DateTime(crate::value::DateTime::new(v.ticks[row], v.unit))
            }
            ColumnData::Duration(v) => {
                Value::Duration(crate::value::Duration::new(v.ticks[row], v.unit))
            }
            ColumnData::Date(v) => Value::Date(crate::value::Date::new(v[row])),
            ColumnData::Time(v) => Value::Time(crate::value::TimeOfDay::new(
                v[row],
                crate::value::TimeUnit::Sec,
            )),
            ColumnData::Str(v) => Value::Str(v.get(row).to_string()),
            ColumnData::StrDict(v) => Value::Str(v.get(row).to_string()),
            ColumnData::Resource(v) => Value::Resource(crate::value::Resource::new(v.get(row))),
            // Nested (§32 s3): recurse, honoring each child's validity. A struct
            // row is its named children at `row`; a list row is its child slice
            // `offsets[row]..offsets[row+1]`.
            ColumnData::Struct(s) => Value::Struct(
                s.names
                    .iter()
                    .zip(s.columns.iter())
                    .map(|(n, c)| (n.clone(), c.value_at(row)))
                    .collect(),
            ),
            ColumnData::List(l) => {
                let (a, b) = (l.offsets[row] as usize, l.offsets[row + 1] as usize);
                Value::List((a..b).map(|i| l.child.value_at(i)).collect())
            }
        }
    }

    /// Append another column of the same variant (used to concatenate buffered
    /// chunks before a blocking sort). Mismatched variants are ignored — within
    /// one stream every chunk shares the schema, so this never happens there.
    pub fn append(&mut self, other: &ColumnData) {
        // A dict lane materializes before concatenation: chunks of one stream
        // may mix representations once readers emit dicts per file, and the
        // `_ => {}` fallthrough below would silently truncate (design/42 §2).
        if let ColumnData::StrDict(d) = self {
            if matches!(other, ColumnData::Str(_) | ColumnData::StrDict(_)) {
                *self = ColumnData::Str(d.materialize());
            }
        }
        match (self, other) {
            (ColumnData::Bool(a), ColumnData::Bool(b)) => a.extend_from_slice(b),
            (ColumnData::I64(a), ColumnData::I64(b)) => a.extend_from_slice(b),
            (ColumnData::F64(a), ColumnData::F64(b)) => a.extend_from_slice(b),
            (ColumnData::Dec(a), ColumnData::Dec(b)) => a.unscaled.extend_from_slice(&b.unscaled),
            (ColumnData::DateTime(a), ColumnData::DateTime(b)) => {
                a.ticks.extend_from_slice(&b.ticks)
            }
            (ColumnData::Duration(a), ColumnData::Duration(b)) => {
                a.ticks.extend_from_slice(&b.ticks)
            }
            (ColumnData::Date(a), ColumnData::Date(b)) => a.extend_from_slice(b),
            (ColumnData::Time(a), ColumnData::Time(b)) => a.extend_from_slice(b),
            (ColumnData::Str(a), ColumnData::Str(b)) => a.append(b),
            (ColumnData::Str(a), ColumnData::StrDict(b)) => {
                for i in 0..b.len() {
                    a.push(b.get(i));
                }
            }
            (ColumnData::Resource(a), ColumnData::Resource(b)) => a.append(b),
            // Nested (§32 s3): concatenate child-wise. A buffering operator (sort,
            // group, distinct, unbounded merge) relies on this — a no-op here
            // silently truncates a nested column to the first chunk.
            (ColumnData::Struct(a), ColumnData::Struct(b)) => {
                for (ca, cb) in a.columns.iter_mut().zip(&b.columns) {
                    ca.append(cb);
                }
                a.len += b.len;
            }
            (ColumnData::List(a), ColumnData::List(b)) => {
                // Shift `b`'s offsets by `a`'s current child length, then append
                // the child elements; `b.offsets[0]` (== 0) is dropped (it is the
                // same point as `a`'s last offset).
                let base = *a.offsets.last().unwrap_or(&0);
                a.offsets
                    .extend(b.offsets.iter().skip(1).map(|&o| o + base));
                a.child.append(&b.child);
            }
            _ => {}
        }
    }

    /// Gather a new column from optional row indices: `Some(i)` takes row `i`,
    /// `None` writes the type's default (`false` / `0` / `0.0` / `""`). Used by
    /// outer joins, where an unmatched side contributes a null-like default.
    pub fn gather_opt(&self, indices: &[Option<usize>]) -> ColumnData {
        match self {
            ColumnData::Bool(v) => {
                ColumnData::Bool(indices.iter().map(|o| o.is_some_and(|i| v[i])).collect())
            }
            ColumnData::I64(v) => {
                ColumnData::I64(indices.iter().map(|o| o.map_or(0, |i| v[i])).collect())
            }
            ColumnData::F64(v) => {
                ColumnData::F64(indices.iter().map(|o| o.map_or(0.0, |i| v[i])).collect())
            }
            ColumnData::Dec(v) => ColumnData::Dec(DecColumn {
                unscaled: indices
                    .iter()
                    .map(|o| o.map_or(0, |i| v.unscaled[i]))
                    .collect(),
                scale: v.scale,
            }),
            ColumnData::DateTime(v) => ColumnData::DateTime(DtColumn {
                ticks: indices
                    .iter()
                    .map(|o| o.map_or(0, |i| v.ticks[i]))
                    .collect(),
                unit: v.unit,
            }),
            ColumnData::Duration(v) => ColumnData::Duration(DurColumn {
                ticks: indices
                    .iter()
                    .map(|o| o.map_or(0, |i| v.ticks[i]))
                    .collect(),
                unit: v.unit,
            }),
            ColumnData::Date(v) => {
                ColumnData::Date(indices.iter().map(|o| o.map_or(0, |i| v[i])).collect())
            }
            ColumnData::Time(v) => {
                ColumnData::Time(indices.iter().map(|o| o.map_or(0, |i| v[i])).collect())
            }
            ColumnData::Str(v) => {
                let mut out = StrColumn::with_capacity(indices.len(), 0);
                for o in indices {
                    out.push(o.map_or("", |i| v.get(i)));
                }
                ColumnData::Str(out)
            }
            // An outer-join gather materializes the dict lane (the `None`
            // default `""` need not be a dictionary entry); output bytes are
            // exactly the plain arm's.
            ColumnData::StrDict(v) => {
                let mut out = StrColumn::with_capacity(indices.len(), 0);
                for o in indices {
                    out.push(o.map_or("", |i| v.get(i)));
                }
                ColumnData::Str(out)
            }
            ColumnData::Resource(v) => {
                let mut out = StrColumn::with_capacity(indices.len(), 0);
                for o in indices {
                    out.push(o.map_or("", |i| v.get(i)));
                }
                ColumnData::Resource(out)
            }
            // Nested (§32 s3): recurse on each child Column (data + validity); a
            // `None` index contributes a default/empty element, like the flat
            // lanes above.
            ColumnData::Struct(s) => ColumnData::Struct(StructColumn {
                names: s.names.clone(),
                columns: s
                    .columns
                    .iter()
                    .map(|c| {
                        Column::new(
                            c.data().gather_opt(indices),
                            c.validity().gather_opt(indices),
                        )
                    })
                    .collect(),
                len: indices.len(),
            }),
            ColumnData::List(l) => {
                let (offsets, child_idx) = list_gather_opt(&l.offsets, indices);
                ColumnData::List(ListColumn {
                    offsets,
                    child: Box::new(Column::new(
                        l.child.data().gather(&child_idx),
                        l.child.validity().gather(&child_idx),
                    )),
                })
            }
        }
    }

    /// Gather a new column from selected row indices (used by filter/join).
    pub fn gather(&self, indices: &[usize]) -> ColumnData {
        match self {
            ColumnData::Bool(v) => ColumnData::Bool(indices.iter().map(|&i| v[i]).collect()),
            ColumnData::I64(v) => ColumnData::I64(indices.iter().map(|&i| v[i]).collect()),
            ColumnData::F64(v) => ColumnData::F64(indices.iter().map(|&i| v[i]).collect()),
            ColumnData::Dec(v) => ColumnData::Dec(DecColumn {
                unscaled: indices.iter().map(|&i| v.unscaled[i]).collect(),
                scale: v.scale,
            }),
            ColumnData::DateTime(v) => ColumnData::DateTime(DtColumn {
                ticks: indices.iter().map(|&i| v.ticks[i]).collect(),
                unit: v.unit,
            }),
            ColumnData::Duration(v) => ColumnData::Duration(DurColumn {
                ticks: indices.iter().map(|&i| v.ticks[i]).collect(),
                unit: v.unit,
            }),
            ColumnData::Date(v) => ColumnData::Date(indices.iter().map(|&i| v[i]).collect()),
            ColumnData::Time(v) => ColumnData::Time(indices.iter().map(|&i| v[i]).collect()),
            ColumnData::Str(v) => ColumnData::Str(v.gather(indices)),
            // A filter/join gather keeps the representation: codes gather,
            // dictionary (small by the reader's escape hatch) is cloned.
            ColumnData::StrDict(v) => ColumnData::StrDict(DictColumn {
                dict: v.dict.clone(),
                codes: indices.iter().map(|&i| v.codes[i]).collect(),
            }),
            ColumnData::Resource(v) => ColumnData::Resource(v.gather(indices)),
            // Nested (§32 s3): recurse on each child Column; for a list, rebuild
            // offsets over the selected sub-lists.
            ColumnData::Struct(s) => ColumnData::Struct(StructColumn {
                names: s.names.clone(),
                columns: s
                    .columns
                    .iter()
                    .map(|c| Column::new(c.data().gather(indices), c.validity().gather(indices)))
                    .collect(),
                len: indices.len(),
            }),
            ColumnData::List(l) => {
                let opt: Vec<Option<usize>> = indices.iter().map(|&i| Some(i)).collect();
                let (offsets, child_idx) = list_gather_opt(&l.offsets, &opt);
                ColumnData::List(ListColumn {
                    offsets,
                    child: Box::new(Column::new(
                        l.child.data().gather(&child_idx),
                        l.child.validity().gather(&child_idx),
                    )),
                })
            }
        }
    }
}

/// Rebuild list offsets when gathering rows (§32 s3): for each selected row,
/// copy its child slice `offsets[i]..offsets[i+1]` and advance the new offsets;
/// a `None` index yields an empty sub-list. Returns `(new_offsets, child_idx)`
/// where `child_idx` selects the flattened child elements to keep.
fn list_gather_opt(offsets: &[i32], indices: &[Option<usize>]) -> (Vec<i32>, Vec<usize>) {
    let mut new_offsets = Vec::with_capacity(indices.len() + 1);
    new_offsets.push(0i32);
    let mut child_idx = Vec::new();
    let mut acc = 0i32;
    for o in indices {
        if let Some(i) = *o {
            let (a, b) = (offsets[i] as usize, offsets[i + 1] as usize);
            child_idx.extend(a..b);
            acc += (b - a) as i32;
        }
        new_offsets.push(acc);
    }
    (new_offsets, child_idx)
}

/// A columnar buffer **with a null model**: a dense value lane ([`ColumnData`])
/// plus a per-row [`Validity`] bitmap (design doc 26 §26.1). `validity` is
/// `None` (all-valid) by default, so an all-valid column is byte-for-byte the
/// old representation with zero overhead. Null-ness rides here, never in the
/// backing bytes, so `null` / empty-`""` / `0` stay three distinct things.
#[derive(Debug, Clone)]
pub struct Column {
    data: ColumnData,
    validity: Validity,
}

impl Column {
    /// design/42: decode a dict lane in place to the plain Str lane (no-op on
    /// every other representation). Operators that REWRITE string cells (the
    /// fill family) call this first, so their `Str`-shaped logic applies
    /// unchanged and behavior can never diverge by representation.
    pub fn undict(&mut self) {
        if let ColumnData::StrDict(d) = &self.data {
            self.data = ColumnData::Str(d.materialize());
        }
    }
}

impl std::ops::Deref for Column {
    type Target = ColumnData;
    /// Read-only access to the dense value lane (so `col.len()`, a `match
    /// &*col`, etc. reach `ColumnData` without naming the field).
    fn deref(&self) -> &ColumnData {
        &self.data
    }
}

impl From<ColumnData> for Column {
    /// Wrap a dense lane as an **all-valid** column (no nulls; zero-cost).
    fn from(data: ColumnData) -> Self {
        Column {
            data,
            validity: Validity::all_valid(),
        }
    }
}

impl Column {
    /// Build a column from a dense lane and an explicit validity bitmap.
    pub fn new(data: ColumnData, validity: Validity) -> Self {
        Column { data, validity }
    }

    /// Borrow the dense value lane (the `match` target for lane-typed code).
    pub fn data(&self) -> &ColumnData {
        &self.data
    }

    /// Consume the column, yielding its dense value lane (drops validity).
    pub fn into_data(self) -> ColumnData {
        self.data
    }

    /// Mutably borrow the dense value lane (for in-place lane edits that do not
    /// change null-ness, e.g. concatenating same-typed reader slices).
    pub fn data_mut(&mut self) -> &mut ColumnData {
        &mut self.data
    }

    /// Borrow the validity bitmap.
    pub fn validity(&self) -> &Validity {
        &self.validity
    }

    /// Is `row` null (missing)? All-valid columns answer `false` for free.
    #[inline]
    pub fn is_null(&self, row: usize) -> bool {
        self.validity.is_null(row)
    }

    /// Does this column carry at least one null?
    pub fn has_nulls(&self) -> bool {
        self.validity.has_nulls()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn dtype(&self) -> DataType {
        self.data.dtype()
    }

    /// The value at `row` — **null-aware**: a null row yields [`Value::Null`]
    /// regardless of the (type-default) backing byte. Never panics on null.
    pub fn value_at(&self, row: usize) -> Value {
        if self.validity.is_null(row) {
            Value::Null
        } else {
            self.data.value_at(row)
        }
    }

    /// All-valid lane constructors. These keep the old `Column::I64(v)` call
    /// sites readable (now `Column::i64(v)`) while making "all-valid" explicit.
    pub fn bool(v: Vec<bool>) -> Self {
        ColumnData::Bool(v).into()
    }
    pub fn i64(v: Vec<i64>) -> Self {
        ColumnData::I64(v).into()
    }
    pub fn f64(v: Vec<f64>) -> Self {
        ColumnData::F64(v).into()
    }
    pub fn dec(v: DecColumn) -> Self {
        ColumnData::Dec(v).into()
    }
    pub fn datetime(v: DtColumn) -> Self {
        ColumnData::DateTime(v).into()
    }
    pub fn duration(v: DurColumn) -> Self {
        ColumnData::Duration(v).into()
    }
    pub fn date(v: Vec<i32>) -> Self {
        ColumnData::Date(v).into()
    }
    pub fn time(v: Vec<i64>) -> Self {
        ColumnData::Time(v).into()
    }
    pub fn str(v: StrColumn) -> Self {
        ColumnData::Str(v).into()
    }
    /// A resource-handle column (uri-backed; design §28.1).
    pub fn resource(v: StrColumn) -> Self {
        ColumnData::Resource(v).into()
    }

    /// Gather selected rows, **carrying validity** so null positions survive
    /// filter/join/sort (design 26 §26.1).
    pub fn gather(&self, indices: &[usize]) -> Column {
        Column {
            data: self.data.gather(indices),
            validity: self.validity.gather(indices),
        }
    }

    /// Gather with optional indices: a `None` (unmatched outer-join row) is a
    /// **null** in both the value lane (type default) and the validity bitmap.
    pub fn gather_opt(&self, indices: &[Option<usize>]) -> Column {
        Column {
            data: self.data.gather_opt(indices),
            validity: self.validity.gather_opt(indices),
        }
    }

    /// Append another column of the same variant, concatenating both the value
    /// lane and the validity bitmap (used before a blocking sort / in merges).
    pub fn append(&mut self, other: &Column) {
        let self_len = self.data.len();
        let other_len = other.data.len();
        self.data.append(&other.data);
        self.validity.append(self_len, &other.validity, other_len);
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
    use super::{Column, ColumnData, ListColumn, StrColumn, StructColumn, Validity};
    use crate::value::{DataType, Value};

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

    #[test]
    fn validity_all_valid_is_zero_cost_and_reports_no_nulls() {
        // The default (`None`) form is the zero-overhead all-valid case: it must
        // not allocate a bitmap and must answer `is_null == false` for any row.
        let v = Validity::all_valid();
        assert!(!v.has_nulls());
        assert!(!v.is_null(0));
        assert!(!v.is_null(63));
        assert!(!v.is_null(1_000_000));
        // Building from an all-true bit vector collapses back to all-valid.
        let from = Validity::from_bits(&[true, true, true]);
        assert!(!from.has_nulls(), "all-true must stay zero-cost (None)");
    }

    #[test]
    fn validity_from_bits_marks_nulls_across_word_boundary() {
        // A null past row 63 exercises the second bitmap word (1 bit/row, packed
        // into u64s) — guards the word-index/shift arithmetic.
        let mut bits = vec![true; 130];
        bits[0] = false;
        bits[64] = false;
        bits[129] = false;
        let v = Validity::from_bits(&bits);
        assert!(v.has_nulls());
        for (i, &b) in bits.iter().enumerate() {
            assert_eq!(v.is_null(i), !b, "row {i}");
        }
    }

    /// An `I64` column whose middle row is null: backing byte is the type
    /// default (0) but `value_at` must answer `Value::Null`, proving null is
    /// distinct from a real `0` (the BUG-A confusion the null model removes).
    fn i64_with_null() -> Column {
        Column::new(
            ColumnData::I64(vec![10, 0, 30]),
            Validity::from_bits(&[true, false, true]),
        )
    }

    #[test]
    fn null_is_distinct_from_zero_in_value_at() {
        let c = i64_with_null();
        assert_eq!(c.value_at(0), Value::I64(10));
        assert_eq!(c.value_at(1), Value::Null, "null row, not the backing 0");
        assert_eq!(c.value_at(2), Value::I64(30));
        assert!(c.is_null(1) && !c.is_null(0));
    }

    #[test]
    fn gather_carries_validity() {
        // Selecting rows [2, 1] must carry their null-ness with them.
        let g = i64_with_null().gather(&[2, 1]);
        assert_eq!(g.value_at(0), Value::I64(30));
        assert_eq!(g.value_at(1), Value::Null);
    }

    #[test]
    fn append_concatenates_validity() {
        // All-valid + has-null, and the reverse, both preserve null positions.
        let mut a = Column::i64(vec![1, 2]); // all-valid
        a.append(&i64_with_null()); // [1,2, 10,null,30]
        assert_eq!(a.len(), 5);
        assert_eq!(a.value_at(1), Value::I64(2));
        assert_eq!(a.value_at(3), Value::Null);
        assert_eq!(a.value_at(4), Value::I64(30));

        // has-null first, all-valid second.
        let mut b = i64_with_null();
        b.append(&Column::i64(vec![99]));
        assert_eq!(b.value_at(1), Value::Null);
        assert_eq!(b.value_at(3), Value::I64(99));
    }

    #[test]
    fn append_two_all_valid_stays_zero_cost() {
        // Concatenating dense chunks must not materialize a bitmap.
        let mut a = Column::i64(vec![1, 2]);
        a.append(&Column::i64(vec![3, 4]));
        assert!(!a.has_nulls(), "all-valid append must stay None");
        assert_eq!(a.value_at(2), Value::I64(3));
    }

    #[test]
    fn nested_append_concatenates_struct_and_list() {
        // §32 s3: appending nested columns must concatenate child-wise (a buffering
        // sort/group relies on it). A no-op here silently truncates to chunk 0.
        // --- List: [[1,2],[3]] ++ [[4,5,6]] = [[1,2],[3],[4,5,6]] ---
        let mk_list = |offsets: Vec<i32>, child: Vec<i64>| {
            Column::new(
                ColumnData::List(ListColumn {
                    offsets,
                    child: Box::new(Column::i64(child)),
                }),
                Validity::all_valid(),
            )
        };
        let mut la = mk_list(vec![0, 2, 3], vec![1, 2, 3]);
        la.append(&mk_list(vec![0, 3], vec![4, 5, 6]));
        assert_eq!(la.len(), 3);
        assert_eq!(
            la.value_at(0),
            Value::List(vec![Value::I64(1), Value::I64(2)])
        );
        assert_eq!(
            la.value_at(2),
            Value::List(vec![Value::I64(4), Value::I64(5), Value::I64(6)])
        );

        // --- Struct: {x} with [10] ++ [20,30] = [10,20,30] ---
        let mk_struct = |xs: Vec<i64>| {
            let len = xs.len();
            Column::new(
                ColumnData::Struct(StructColumn {
                    names: vec!["x".into()],
                    columns: vec![Column::i64(xs)],
                    len,
                }),
                Validity::all_valid(),
            )
        };
        let mut sa = mk_struct(vec![10]);
        sa.append(&mk_struct(vec![20, 30]));
        assert_eq!(sa.len(), 3);
        assert_eq!(
            sa.value_at(2),
            Value::Struct(vec![("x".into(), Value::I64(30))])
        );
    }

    #[test]
    fn all_null_marks_every_row_missing() {
        // A whole-column null constant: every row is null, none is a real 0/NaN.
        let v = Validity::all_null(70); // spans two bitmap words
        assert!(v.has_nulls());
        for i in 0..70 {
            assert!(v.is_null(i), "row {i} must be null");
        }
        // Degenerate: zero rows can't be null, so stay zero-cost all-valid.
        assert!(!Validity::all_null(0).has_nulls());
    }

    #[test]
    fn gather_opt_none_index_is_null() {
        // The outer-join fill path: a `None` index is a null on both lanes.
        let g = Column::i64(vec![7, 8]).gather_opt(&[Some(1), None, Some(0)]);
        assert_eq!(g.value_at(0), Value::I64(8));
        assert_eq!(g.value_at(1), Value::Null);
        assert_eq!(g.value_at(2), Value::I64(7));
    }

    #[test]
    fn resource_column_lane_value_at_and_gather() {
        // The uri-backed resource lane (§28.1): dtype, value_at, gather, and
        // null-awareness all behave like the string lane it mirrors.
        let uris: StrColumn = ["file:///a.csv", "s3://b/k", "http://h/x"]
            .into_iter()
            .collect();
        let col = Column::resource(uris);
        assert_eq!(col.dtype(), DataType::Resource);
        assert_eq!(col.len(), 3);
        assert_eq!(
            col.value_at(1),
            Value::Resource(crate::value::Resource::new("s3://b/k"))
        );
        // gather preserves order and uri identity.
        let g = col.gather(&[2, 0]);
        assert_eq!(
            g.value_at(0),
            Value::Resource(crate::value::Resource::new("http://h/x"))
        );
        assert_eq!(
            g.value_at(1),
            Value::Resource(crate::value::Resource::new("file:///a.csv"))
        );
    }
}
