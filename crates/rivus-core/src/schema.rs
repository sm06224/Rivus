//! Structural schema for chunks.
//!
//! Schemas are structural (Master principle: gradual / structural typing).
//! Two chunks with the same field names + lanes are interchangeable regardless
//! of where they came from.

use crate::value::DataType;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub dtype: DataType,
}

impl Field {
    pub fn new(name: impl Into<String>, dtype: DataType) -> Self {
        Field {
            name: name.into(),
            dtype,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Schema { fields }
    }

    pub fn empty() -> Arc<Schema> {
        Arc::new(Schema { fields: vec![] })
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }

    pub fn field_names(&self) -> Vec<&str> {
        self.fields.iter().map(|f| f.name.as_str()).collect()
    }

    /// Project to a subset of fields, preserving the requested order.
    pub fn project(&self, names: &[String]) -> Option<Schema> {
        let mut fields = Vec::with_capacity(names.len());
        for n in names {
            let idx = self.index_of(n)?;
            fields.push(self.fields[idx].clone());
        }
        Some(Schema { fields })
    }
}
