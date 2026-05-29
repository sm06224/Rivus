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

/// A columnar buffer. One variant per execution lane (MVP subset).
#[derive(Debug, Clone)]
pub enum Column {
    Bool(Vec<bool>),
    I64(Vec<i64>),
    F64(Vec<f64>),
    Str(Vec<String>),
}

impl Column {
    pub fn len(&self) -> usize {
        match self {
            Column::Bool(v) => v.len(),
            Column::I64(v) => v.len(),
            Column::F64(v) => v.len(),
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
            Column::Str(_) => DataType::Str,
        }
    }

    pub fn value_at(&self, row: usize) -> Value {
        match self {
            Column::Bool(v) => Value::Bool(v[row]),
            Column::I64(v) => Value::I64(v[row]),
            Column::F64(v) => Value::F64(v[row]),
            Column::Str(v) => Value::Str(v[row].clone()),
        }
    }

    /// Gather a new column from selected row indices (used by filter/join).
    pub fn gather(&self, indices: &[usize]) -> Column {
        match self {
            Column::Bool(v) => Column::Bool(indices.iter().map(|&i| v[i]).collect()),
            Column::I64(v) => Column::I64(indices.iter().map(|&i| v[i]).collect()),
            Column::F64(v) => Column::F64(indices.iter().map(|&i| v[i]).collect()),
            Column::Str(v) => Column::Str(indices.iter().map(|&i| v[i].clone()).collect()),
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
