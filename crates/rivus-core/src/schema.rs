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
    /// Static structural detail for a nested (`Struct`/`List`) field (§32 s3).
    /// `None` for a flat scalar lane. The `dtype` is the opaque lane marker
    /// (`Struct`/`List`, kept `Copy`); this carries the shape so `explain` /
    /// type-checks can show `user:{name:str}` / `tags:[str]` (design §06: the
    /// *structural* layer, distinct from the lane). Flat fields never set it, so
    /// existing schemas are unchanged.
    pub nested: Option<Nested>,
}

/// Nested structural detail carried by a [`Field`] whose lane is `Struct`/`List`
/// (§32 s3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Nested {
    /// A struct's ordered, named child fields.
    Struct(Vec<Field>),
    /// A list's element field (its `name` is conventionally `item`).
    List(Box<Field>),
}

impl Field {
    /// A flat (scalar-lane) field — the common case; no nested detail.
    pub fn new(name: impl Into<String>, dtype: DataType) -> Self {
        Field {
            name: name.into(),
            dtype,
            nested: None,
        }
    }

    /// A `Struct`-lane field carrying its named child fields (§32 s3).
    pub fn struct_(name: impl Into<String>, children: Vec<Field>) -> Self {
        Field {
            name: name.into(),
            dtype: DataType::Struct,
            nested: Some(Nested::Struct(children)),
        }
    }

    /// A `List`-lane field carrying its element field (§32 s3).
    pub fn list(name: impl Into<String>, element: Field) -> Self {
        Field {
            name: name.into(),
            dtype: DataType::List,
            nested: Some(Nested::List(Box::new(element))),
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
