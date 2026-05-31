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

/// Scalar functions callable in expressions: `upper(x)`, `substr(s, 0, 3)`, …
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Func {
    Upper,
    Lower,
    Len,
    Trim,
    Substr,
    Contains,
}

impl Func {
    pub fn parse(s: &str) -> Option<Func> {
        Some(match s {
            "upper" => Func::Upper,
            "lower" => Func::Lower,
            "len" => Func::Len,
            "trim" => Func::Trim,
            "substr" => Func::Substr,
            "contains" => Func::Contains,
            _ => return None,
        })
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Func::Upper => "upper",
            Func::Lower => "lower",
            Func::Len => "len",
            Func::Trim => "trim",
            Func::Substr => "substr",
            Func::Contains => "contains",
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
    /// Scalar function call `func(args…)` (string/util functions).
    Func {
        func: Func,
        args: Vec<Expr>,
    },
    /// `case when COND then VAL [when COND then VAL ...] [else VAL] end`. The
    /// first branch whose condition is truthy yields its value; if none match,
    /// `default` (the `else`) is used, or an empty string when absent. Row-wise.
    Case {
        branches: Vec<(Expr, Expr)>,
        default: Option<Box<Expr>>,
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
            Expr::Func { func, args } => {
                let a: Vec<String> = args.iter().map(|e| e.to_string()).collect();
                write!(f, "{}({})", func.as_str(), a.join(", "))
            }
            Expr::Case { branches, default } => {
                write!(f, "case")?;
                for (cond, val) in branches {
                    write!(f, " when {cond} then {val}")?;
                }
                if let Some(d) = default {
                    write!(f, " else {d}")?;
                }
                write!(f, " end")
            }
        }
    }
}
