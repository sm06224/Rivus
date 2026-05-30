//! Row-wise expression evaluation for the MVP.
//!
//! This is the deliberately simple, always-correct interpreter. The optimizer
//! / JIT story (docs 08 + 09) replaces this hot path with vectorized, then
//! compiled, predicate kernels — but they must produce identical results to
//! this reference evaluator.
//!
//! Predicate evaluation has borrowed fast paths for the common `Field CMP
//! Literal` shape: a string comparison reads the arena column as `&str` and a
//! numeric comparison reads the lane directly, so neither allocates a `Value`
//! (in particular, no `String` per row for string-keyed filters). Anything that
//! doesn't fit the fast paths falls back to the owned-`Value` interpreter, so
//! results are identical.

use rivus_core::{Chunk, Column, DataType, StrColumn, Value};
use rivus_ir::{Access, ArithOp, CmpOp, Expr, Func};
use std::cmp::Ordering;

/// Apply a scalar function to argument values for one row.
fn call_func(func: Func, args: &[Expr], chunk: &Chunk, row: usize) -> Value {
    let arg = |i: usize| {
        args.get(i)
            .map(|e| eval(e, chunk, row))
            .unwrap_or(Value::Null)
    };
    match func {
        Func::Upper => Value::Str(arg(0).to_string().to_uppercase()),
        Func::Lower => Value::Str(arg(0).to_string().to_lowercase()),
        Func::Trim => Value::Str(arg(0).to_string().trim().to_string()),
        Func::Len => Value::I64(arg(0).to_string().chars().count() as i64),
        Func::Contains => {
            let hay = arg(0).to_string();
            let needle = arg(1).to_string();
            Value::Bool(hay.contains(&needle))
        }
        Func::Substr => {
            let s = arg(0).to_string();
            let start = arg(1).as_f64().unwrap_or(0.0) as usize;
            let take = args
                .get(2)
                .map(|e| eval(e, chunk, row).as_f64().unwrap_or(0.0) as usize)
                .unwrap_or(usize::MAX);
            let out: String = s.chars().skip(start).take(take).collect();
            Value::Str(out)
        }
    }
}

/// Coerce a value to an integer (truncating floats; parsing strings; bool→0/1).
fn to_i64(v: Value) -> i64 {
    match v {
        Value::I64(x) => x,
        Value::F64(x) => x as i64,
        Value::Bool(b) => b as i64,
        Value::Str(s) => s
            .trim()
            .parse::<i64>()
            .or_else(|_| s.trim().parse::<f64>().map(|f| f as i64))
            .unwrap_or(0),
        Value::Null => 0,
    }
}

/// Coerce a value to a float (parsing strings; bool→0/1).
fn to_f64(v: Value) -> f64 {
    match v {
        Value::I64(x) => x as f64,
        Value::F64(x) => x,
        Value::Bool(b) => b as i64 as f64,
        Value::Str(s) => s.trim().parse().unwrap_or(f64::NAN),
        Value::Null => f64::NAN,
    }
}

/// Coerce a value to a bool (`true`/nonzero/non-empty-numeric).
fn to_bool(v: Value) -> bool {
    match v {
        Value::Bool(b) => b,
        Value::I64(x) => x != 0,
        Value::F64(x) => x != 0.0,
        Value::Str(s) => s.trim().eq_ignore_ascii_case("true") || s.trim() == "1",
        Value::Null => false,
    }
}

/// Cast a value to a target lane.
fn cast_value(v: Value, ty: DataType) -> Value {
    match ty {
        DataType::I64 => Value::I64(to_i64(v)),
        DataType::F64 => Value::F64(to_f64(v)),
        DataType::Bool => Value::Bool(to_bool(v)),
        DataType::Str => Value::Str(v.to_string()),
        DataType::Null => Value::Null,
    }
}

/// Cast a whole column to a target lane (columnar path for computed columns).
fn cast_column(col: Column, ty: DataType) -> Column {
    let n = col.len();
    match ty {
        DataType::I64 => Column::I64((0..n).map(|i| to_i64(col.value_at(i))).collect()),
        DataType::F64 => Column::F64((0..n).map(|i| to_f64(col.value_at(i))).collect()),
        DataType::Bool => Column::Bool((0..n).map(|i| to_bool(col.value_at(i))).collect()),
        DataType::Str => {
            let mut s = StrColumn::with_capacity(n, n * 8);
            for i in 0..n {
                s.push(&col.value_at(i).to_string());
            }
            Column::Str(s)
        }
        DataType::Null => col,
    }
}

/// Evaluate an expression over a whole chunk, producing a column of `chunk.len`
/// rows (the columnar path used by computed-column projection). A `Field` is the
/// underlying column; a `Literal` is a constant column; arithmetic combines
/// numeric lanes; boolean-valued expressions become a `Bool` column.
pub fn eval_column(expr: &Expr, chunk: &Chunk) -> Column {
    match expr {
        Expr::Field { name, .. } => match chunk.column(name) {
            Some(c) => c.clone(),
            // Missing field → a NaN numeric lane (continue-first).
            None => Column::F64(vec![f64::NAN; chunk.len]),
        },
        Expr::Literal(v) => const_column(v, chunk.len),
        Expr::Cast { expr, ty } => cast_column(eval_column(expr, chunk), *ty),
        Expr::Arith { left, op, right } => eval_arith(left, *op, right, chunk),
        Expr::Func { func, args } => {
            let n = chunk.len;
            match func {
                Func::Len => Column::I64(
                    (0..n)
                        .map(|r| to_i64(call_func(*func, args, chunk, r)))
                        .collect(),
                ),
                Func::Contains => Column::Bool(
                    (0..n)
                        .map(|r| matches!(call_func(*func, args, chunk, r), Value::Bool(true)))
                        .collect(),
                ),
                _ => {
                    let mut s = StrColumn::with_capacity(n, n * 8);
                    for r in 0..n {
                        s.push(&call_func(*func, args, chunk, r).to_string());
                    }
                    Column::Str(s)
                }
            }
        }
        // Compare / And / Or are predicates → a boolean column.
        _ => {
            let v: Vec<bool> = (0..chunk.len)
                .map(|row| eval_predicate(expr, chunk, row))
                .collect();
            Column::Bool(v)
        }
    }
}

fn const_column(v: &Value, n: usize) -> Column {
    match v {
        Value::I64(x) => Column::I64(vec![*x; n]),
        Value::F64(x) => Column::F64(vec![*x; n]),
        Value::Bool(x) => Column::Bool(vec![*x; n]),
        Value::Str(s) => {
            let mut c = StrColumn::with_capacity(n, s.len() * n);
            for _ in 0..n {
                c.push(s);
            }
            Column::Str(c)
        }
        Value::Null => Column::F64(vec![f64::NAN; n]),
    }
}

/// A numeric f64 lane for an expression, plus whether it is an *integer* lane
/// (so `int op int` can stay integer). Strings are parsed best-effort ("text is
/// stream"): a non-numeric cell becomes NaN.
fn num_lane(e: &Expr, chunk: &Chunk) -> (Vec<f64>, bool) {
    match eval_column(e, chunk) {
        Column::I64(v) => (v.iter().map(|&x| x as f64).collect(), true),
        Column::Bool(v) => (v.iter().map(|&x| if x { 1.0 } else { 0.0 }).collect(), true),
        Column::F64(v) => (v, false),
        Column::Str(s) => {
            let lane = (0..s.len())
                .map(|i| s.get(i).trim().parse::<f64>().unwrap_or(f64::NAN))
                .collect();
            (lane, false)
        }
    }
}

fn eval_arith(left: &Expr, op: ArithOp, right: &Expr, chunk: &Chunk) -> Column {
    let (lf, li) = num_lane(left, chunk);
    let (rf, ri) = num_lane(right, chunk);
    let n = chunk.len;
    // Integer lane only when both sides are integers and the op preserves it
    // (division always yields a float, matching pandas/SQL `/` semantics).
    if li && ri && op != ArithOp::Div {
        let out: Vec<i64> = (0..n)
            .map(|i| {
                let a = lf[i] as i64;
                let b = rf[i] as i64;
                match op {
                    ArithOp::Add => a.wrapping_add(b),
                    ArithOp::Sub => a.wrapping_sub(b),
                    ArithOp::Mul => a.wrapping_mul(b),
                    ArithOp::Mod => {
                        if b != 0 {
                            a % b
                        } else {
                            0
                        }
                    }
                    ArithOp::Div => unreachable!(),
                }
            })
            .collect();
        Column::I64(out)
    } else {
        let out: Vec<f64> = (0..n)
            .map(|i| {
                let a = lf[i];
                let b = rf[i];
                match op {
                    ArithOp::Add => a + b,
                    ArithOp::Sub => a - b,
                    ArithOp::Mul => a * b,
                    ArithOp::Div => a / b,
                    ArithOp::Mod => a % b,
                }
            })
            .collect();
        Column::F64(out)
    }
}

/// Evaluate a predicate expression for a single row.
pub fn eval_predicate(expr: &Expr, chunk: &Chunk, row: usize) -> bool {
    match expr {
        Expr::Compare { left, op, right } => compare_fast(left, *op, right, chunk, row),
        Expr::And(a, b) => eval_predicate(a, chunk, row) && eval_predicate(b, chunk, row),
        Expr::Or(a, b) => eval_predicate(a, chunk, row) || eval_predicate(b, chunk, row),
        other => matches!(eval(other, chunk, row), Value::Bool(true)),
    }
}

pub fn eval(expr: &Expr, chunk: &Chunk, row: usize) -> Value {
    match expr {
        Expr::Literal(v) => v.clone(),
        Expr::Field { name, access } => eval_field(name, *access, chunk, row),
        Expr::Compare { left, op, right } => {
            Value::Bool(compare_fast(left, *op, right, chunk, row))
        }
        Expr::And(a, b) => {
            Value::Bool(eval_predicate(a, chunk, row) && eval_predicate(b, chunk, row))
        }
        Expr::Or(a, b) => {
            Value::Bool(eval_predicate(a, chunk, row) || eval_predicate(b, chunk, row))
        }
        Expr::Arith { left, op, right } => arith_value(left, *op, right, chunk, row),
        Expr::Cast { expr, ty } => cast_value(eval(expr, chunk, row), *ty),
        Expr::Func { func, args } => call_func(*func, args, chunk, row),
    }
}

/// Row-wise arithmetic, kept consistent with the columnar [`eval_arith`]:
/// integer lanes stay integer (except `/`), anything else is float, and a
/// non-numeric operand yields `Null` (continue-first).
fn arith_value(left: &Expr, op: ArithOp, right: &Expr, chunk: &Chunk, row: usize) -> Value {
    let lv = eval(left, chunk, row);
    let rv = eval(right, chunk, row);
    let (Some(a), Some(b)) = (lv.as_f64(), rv.as_f64()) else {
        return Value::Null;
    };
    let int = matches!(lv, Value::I64(_) | Value::Bool(_))
        && matches!(rv, Value::I64(_) | Value::Bool(_))
        && op != ArithOp::Div;
    if int {
        let (a, b) = (a as i64, b as i64);
        Value::I64(match op {
            ArithOp::Add => a.wrapping_add(b),
            ArithOp::Sub => a.wrapping_sub(b),
            ArithOp::Mul => a.wrapping_mul(b),
            ArithOp::Mod => {
                if b != 0 {
                    a % b
                } else {
                    0
                }
            }
            ArithOp::Div => unreachable!(),
        })
    } else {
        Value::F64(match op {
            ArithOp::Add => a + b,
            ArithOp::Sub => a - b,
            ArithOp::Mul => a * b,
            ArithOp::Div => a / b,
            ArithOp::Mod => a % b,
        })
    }
}

fn eval_field(name: &str, _access: Access, chunk: &Chunk, row: usize) -> Value {
    // MVP: Fast / Deep / Dynamic all resolve via the flat schema. The slow-path
    // access strategies (recursive `$_..`, dynamic `item(..)`) are recorded in
    // the IR so the optimizer can specialize them once nested chunks land.
    match chunk.column(name) {
        Some(col) => col.value_at(row),
        None => Value::Null,
    }
}

/// Compare two sub-expressions for a row, taking borrowed fast paths first.
fn compare_fast(left: &Expr, op: CmpOp, right: &Expr, chunk: &Chunk, row: usize) -> bool {
    // String fast path: no `String` allocation per side per row.
    if let (Some(a), Some(b)) = (as_str(left, chunk, row), as_str(right, chunk, row)) {
        return cmp_ord(a.partial_cmp(b), op);
    }
    // Numeric fast path (int/float/bool lanes), no allocation.
    if let (Some(a), Some(b)) = (as_num(left, chunk, row), as_num(right, chunk, row)) {
        return cmp_ord(a.partial_cmp(&b), op);
    }
    // General fallback for mixed / null operands: owned-Value comparison.
    let l = eval(left, chunk, row);
    let r = eval(right, chunk, row);
    compare(&l, op, &r)
}

/// Borrow a `&str` for a Field backed by a string column, or a string literal.
fn as_str<'a>(e: &'a Expr, chunk: &'a Chunk, row: usize) -> Option<&'a str> {
    match e {
        Expr::Literal(Value::Str(s)) => Some(s),
        Expr::Field { name, .. } => match chunk.column(name)? {
            Column::Str(s) => Some(s.get(row)),
            _ => None,
        },
        _ => None,
    }
}

/// Read a numeric value for a Field backed by a numeric/bool lane, or a numeric
/// literal — without materializing a `Value`.
fn as_num(e: &Expr, chunk: &Chunk, row: usize) -> Option<f64> {
    match e {
        Expr::Literal(v) => v.as_f64(),
        Expr::Field { name, .. } => match chunk.column(name)? {
            Column::I64(v) => Some(v[row] as f64),
            Column::F64(v) => Some(v[row]),
            Column::Bool(v) => Some(if v[row] { 1.0 } else { 0.0 }),
            Column::Str(_) => None,
        },
        _ => None,
    }
}

fn cmp_ord(ord: Option<Ordering>, op: CmpOp) -> bool {
    matches!(
        (ord, op),
        (Some(Ordering::Equal), CmpOp::Eq | CmpOp::Le | CmpOp::Ge)
            | (Some(Ordering::Less), CmpOp::Lt | CmpOp::Le | CmpOp::Ne)
            | (Some(Ordering::Greater), CmpOp::Gt | CmpOp::Ge | CmpOp::Ne)
    )
}

fn compare(l: &Value, op: CmpOp, r: &Value) -> bool {
    let ord = match (l, r) {
        (Value::Str(a), Value::Str(b)) => a.partial_cmp(b),
        _ => match (l.as_f64(), r.as_f64()) {
            (Some(a), Some(b)) => a.partial_cmp(&b),
            _ => {
                // Fall back to equality semantics for mixed/null operands.
                return match op {
                    CmpOp::Eq => l == r,
                    CmpOp::Ne => l != r,
                    _ => false,
                };
            }
        },
    };
    cmp_ord(ord, op)
}
