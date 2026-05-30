//! Scalar expressions used inside transforms (filter predicates, projections).
//!
//! Expressions encode the access strategies from the syntax draft:
//! - `Col` / `$_.field`  → fast structural access
//! - `DeepCol` / `$_..field` → recursive traversal (slow path)
//! - `DynCol` / `item("field")` → dynamic resolution (slow path)
//!
//! Each carries an `access` tag so the optimizer / JIT can specialize the fast
//! path and fall back only where required (Master principle #7).

use rivus_core::{DataType, Value};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            CmpOp::Eq => "==",
            CmpOp::Ne => "!=",
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
        }
    }
}

/// Binary arithmetic operators for computed columns (`(age * 12)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl ArithOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            ArithOp::Add => "+",
            ArithOp::Sub => "-",
            ArithOp::Mul => "*",
            ArithOp::Div => "/",
            ArithOp::Mod => "%",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// `$_.field` — direct structural lookup.
    Fast,
    /// `$_..field` — recursive traversal.
    Deep,
    /// `item("field")` — dynamic resolution.
    Dynamic,
}

#[derive(Debug, Clone)]
pub enum Expr {
    /// Reference to a field of the current object, with an access strategy.
    Field {
        name: String,
        access: Access,
    },
    Literal(Value),
    Compare {
        left: Box<Expr>,
        op: CmpOp,
        right: Box<Expr>,
    },
    /// Logical AND of two predicates.
    And(Box<Expr>, Box<Expr>),
    /// Logical OR of two predicates.
    Or(Box<Expr>, Box<Expr>),
    /// Binary arithmetic (`left op right`) for computed columns.
    Arith {
        left: Box<Expr>,
        op: ArithOp,
        right: Box<Expr>,
    },
    /// Type cast `expr:type` — reinterpret a value as another lane (e.g. a
    /// string column compared numerically: `age:int >= 20`).
    Cast {
        expr: Box<Expr>,
        ty: DataType,
    },
}

impl Expr {
    pub fn field(name: impl Into<String>) -> Expr {
        Expr::Field {
            name: name.into(),
            access: Access::Fast,
        }
    }

    /// Source representation of the field accessor, for reversibility.
    fn field_src(name: &str, access: Access) -> String {
        match access {
            Access::Fast => format!("$_.{name}"),
            Access::Deep => format!("$_..{name}"),
            Access::Dynamic => format!("item(\"{name}\")"),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Field { name, access } => write!(f, "{}", Expr::field_src(name, *access)),
            Expr::Literal(Value::Str(s)) => write!(f, "\"{s}\""),
            Expr::Literal(v) => write!(f, "{v}"),
            Expr::Compare { left, op, right } => {
                write!(f, "{left} {} {right}", op.as_str())
            }
            Expr::And(a, b) => write!(f, "{a} and {b}"),
            Expr::Or(a, b) => write!(f, "{a} or {b}"),
            // Always parenthesized so the source round-trips and re-parses with
            // the same structure regardless of precedence.
            Expr::Arith { left, op, right } => write!(f, "({left} {} {right})", op.as_str()),
            Expr::Cast { expr, ty } => write!(f, "{expr}:{ty}"),
        }
    }
}
