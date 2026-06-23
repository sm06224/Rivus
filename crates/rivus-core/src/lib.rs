//! `rivus-core` — the shared data model for the Rivus stream runtime.
//!
//! Everything downstream (IR, parser, runtime, CLI) speaks in terms of the
//! types defined here:
//!
//! - [`Chunk`] / [`Column`] / [`ChunkMeta`]: the chunk-native data unit.
//! - [`Schema`] / [`Field`] / [`DataType`]: structural, execution-lane typing.
//! - [`Value`]: scalars for literals and predicate evaluation.
//! - [`Mode`]: the runtime mode state machine's alphabet.
//! - [`ErrorEvent`] / [`Severity`]: the continue-first error stream.

pub mod chunk;
pub mod error;
pub mod numparse;
pub mod schema;
pub mod value;

pub use chunk::{
    Chunk, ChunkMeta, Column, ColumnData, DecColumn, DtColumn, DurColumn, ListColumn, Mode,
    StrColumn, StructColumn, Validity,
};
pub use error::{ErrorEvent, ErrorScope, Result, RivusError, Severity};
pub use schema::{Field, Nested, Schema};
pub use value::{
    DataType, Date, DateTime, Decimal, Duration, Resource, TimeOfDay, TimeUnit, Value,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn sample() -> Chunk {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Str),
            Field::new("age", DataType::I64),
        ]));
        let columns = vec![
            Column::str(["aki", "ben", "cho"].into_iter().collect()),
            Column::i64(vec![30, 15, 40]),
        ];
        Chunk::new(0, schema, columns)
    }

    #[test]
    fn gather_filters_rows() {
        let c = sample();
        let kept = c.gather(&[0, 2]);
        assert_eq!(kept.len, 2);
        assert_eq!(kept.value(0, 1), Value::I64(30));
        assert_eq!(kept.value(1, 1), Value::I64(40));
    }

    #[test]
    fn project_keeps_named_columns() {
        let c = sample();
        let p = c.project(&["name".into()]).unwrap();
        assert_eq!(p.columns.len(), 1);
        assert_eq!(p.schema.field_names(), vec!["name"]);
    }

    #[test]
    fn str_column_roundtrips_including_multibyte() {
        // Locks the StrColumn unsafe-utf8 invariant: multibyte and empty cells.
        let mut c = chunk::StrColumn::with_capacity(0, 0);
        for s in ["", "ascii", "日本語", "café", ""] {
            c.push(s);
        }
        assert_eq!(c.len(), 5);
        assert_eq!(c.get(0), "");
        assert_eq!(c.get(2), "日本語");
        assert_eq!(c.get(3), "café");
        // gather preserves contents and order.
        let g = c.gather(&[2, 1]);
        assert_eq!(g.get(0), "日本語");
        assert_eq!(g.get(1), "ascii");
        // append concatenates.
        let mut a: chunk::StrColumn = ["x"].into_iter().collect();
        a.append(&g);
        assert_eq!(a.len(), 3);
        assert_eq!(a.get(2), "ascii");
    }

    #[test]
    fn severity_ordering_supports_thresholds() {
        assert!(Severity::Critical >= Severity::Warn);
        assert!(Severity::parse("warning").unwrap() == Severity::Warn);
    }
}
