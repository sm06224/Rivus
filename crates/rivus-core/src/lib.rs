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
pub mod schema;
pub mod value;

pub use chunk::{Chunk, ChunkMeta, Column, Mode};
pub use error::{ErrorEvent, ErrorScope, Result, RivusError, Severity};
pub use schema::{Field, Schema};
pub use value::{DataType, Value};

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
            Column::Str(vec!["aki".into(), "ben".into(), "cho".into()]),
            Column::I64(vec![30, 15, 40]),
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
    fn severity_ordering_supports_thresholds() {
        assert!(Severity::Critical >= Severity::Warn);
        assert!(Severity::parse("warning").unwrap() == Severity::Warn);
    }
}
