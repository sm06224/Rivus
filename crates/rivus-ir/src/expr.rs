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
    StartsWith,
    EndsWith,
    /// SQL `LIKE` pattern: `%` = any run, `_` = any single char (case-sensitive).
    Like,
    /// Shell glob: `*` = any run, `?` = any single char, `[abc]`/`[a-z]`/`[!..]`
    /// character classes.
    Glob,
    /// Full regular expression (unanchored partial match). The IR always knows
    /// it (parse/`to_source` are std-only); evaluating it needs the runtime's
    /// off-by-default `regex` feature, else it raises a recoverable error.
    Regexp,
    /// `replace(s, from, to)` — replace every occurrence of a literal substring.
    Replace,
    /// `split_part(s, sep, n)` — the `n`-th field (1-based) after splitting `s`
    /// on the literal separator `sep`; empty string when out of range.
    SplitPart,
    /// `concat(a, b, …)` — concatenate all arguments as text (any arity).
    Concat,
    /// `abs(x)` — absolute value (numeric).
    Abs,
    /// `round(x)` — round to the nearest integer (ties away from zero).
    Round,
    /// `floor(x)` — largest integer ≤ x.
    Floor,
    /// `ceil(x)` — smallest integer ≥ x.
    Ceil,
    /// `coalesce(a, b, …)` — the first argument whose text is non-empty (any
    /// arity); empty string if all are empty. The SQL/pandas null-coalesce.
    Coalesce,
    /// Datetime field extractors (design 23): `year(ts)`/`month(ts)`/`day(ts)`/
    /// `hour(ts)`/`minute(ts)`/`second(ts)` — each returns an `i64`.
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    /// `trunc(ts, "day")` — truncate a datetime to a `year`/`month`/`day`/`hour`/
    /// `minute`/`second` boundary (the time-series group-by key); returns a
    /// datetime at the same unit. Design 23.
    Trunc,
    /// `format(ts, "yyyy-MM-dd")` — render a datetime as text. Design 23.
    Format,
    /// `weekday(x)` — ISO day-of-week of a date/datetime: `0 = Mon … 6 = Sun`
    /// (returns `i64`). #58.
    Weekday,
    /// `is_weekend(x)` — whether a date/datetime falls on Sat/Sun (`bool`). #58.
    IsWeekend,
    /// `date(x)` — the calendar `date` of a datetime (drops the time-of-day),
    /// returning the exact `date` lane. #58.
    Date,
    /// `time(x)` — the `time`-of-day of a datetime (drops the calendar date),
    /// returning the exact `time` lane. #58.
    Time,
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
            "starts_with" => Func::StartsWith,
            "ends_with" => Func::EndsWith,
            "like" => Func::Like,
            "glob" => Func::Glob,
            "regexp" | "regex" | "matches" => Func::Regexp,
            "replace" => Func::Replace,
            "split_part" => Func::SplitPart,
            "concat" => Func::Concat,
            "abs" => Func::Abs,
            "round" => Func::Round,
            "floor" => Func::Floor,
            "ceil" => Func::Ceil,
            "coalesce" => Func::Coalesce,
            "year" => Func::Year,
            "month" => Func::Month,
            "day" => Func::Day,
            "hour" => Func::Hour,
            "minute" => Func::Minute,
            "second" => Func::Second,
            "trunc" => Func::Trunc,
            "format" => Func::Format,
            "weekday" => Func::Weekday,
            "is_weekend" => Func::IsWeekend,
            "date" => Func::Date,
            "time" => Func::Time,
            _ => return None,
        })
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Func::Upper => "upper",
            Func::Lower => "lower",
            Func::Len => "len",
            Func::Substr => "substr",
            Func::Trim => "trim",
            Func::Contains => "contains",
            Func::StartsWith => "starts_with",
            Func::EndsWith => "ends_with",
            Func::Like => "like",
            Func::Glob => "glob",
            Func::Regexp => "regexp",
            Func::Replace => "replace",
            Func::SplitPart => "split_part",
            Func::Concat => "concat",
            Func::Abs => "abs",
            Func::Round => "round",
            Func::Floor => "floor",
            Func::Ceil => "ceil",
            Func::Coalesce => "coalesce",
            Func::Year => "year",
            Func::Month => "month",
            Func::Day => "day",
            Func::Hour => "hour",
            Func::Minute => "minute",
            Func::Second => "second",
            Func::Trunc => "trunc",
            Func::Format => "format",
            Func::Weekday => "weekday",
            Func::IsWeekend => "is_weekend",
            Func::Date => "date",
            Func::Time => "time",
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
