//! Scalar values and logical data types.
//!
//! Rivus follows "execution-aware typing" (Master principle #7): a logical
//! `DataType` is a *hint for which execution lane* a column should ride, not a
//! rigid memory contract. The MVP collapses the numeric lanes onto `i64`/`f64`,
//! but the `DataType` enum is shaped so the SIMD / decimal / bignum lanes
//! described in `docs/design/06-type-system.md` can be added without churn.

use std::fmt;

/// A single scalar value. Used for literals, predicate evaluation and the
/// "current object" (`$_`) field access. Bulk data lives in [`crate::Column`].
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    /// Default SIMD integer lane.
    I64(i64),
    /// Default SIMD float lane.
    F64(f64),
    Str(String),
}

impl Value {
    pub fn dtype(&self) -> DataType {
        match self {
            Value::Null => DataType::Null,
            Value::Bool(_) => DataType::Bool,
            Value::I64(_) => DataType::I64,
            Value::F64(_) => DataType::F64,
            Value::Str(_) => DataType::Str,
        }
    }

    /// Best-effort numeric view for comparisons across the int/float lanes.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::I64(v) => Some(*v as f64),
            Value::F64(v) => Some(*v),
            Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, ""),
            Value::Bool(b) => write!(f, "{b}"),
            Value::I64(v) => write!(f, "{v}"),
            Value::F64(v) => write!(f, "{v}"),
            Value::Str(s) => write!(f, "{s}"),
        }
    }
}

/// Logical type = execution-lane hint. See design doc 06.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Null,
    Bool,
    /// Integer SIMD lane (i32/i64 collapsed to i64 in the MVP).
    I64,
    /// Float SIMD lane (f32/f64 collapsed to f64 in the MVP).
    F64,
    /// Stream-based text (see design doc 09 "Text is stream").
    Str,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            DataType::Null => "null",
            DataType::Bool => "bool",
            DataType::I64 => "i64",
            DataType::F64 => "f64",
            DataType::Str => "str",
        };
        f.write_str(s)
    }
}
