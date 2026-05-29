//! Row-wise expression evaluation for the MVP.
//!
//! This is the deliberately simple, always-correct interpreter. The optimizer
//! / JIT story (docs 08 + 09) replaces this hot path with vectorized, then
//! compiled, predicate kernels — but they must produce identical results to
//! this reference evaluator.

use rivus_core::{Chunk, Value};
use rivus_ir::{Access, CmpOp, Expr};

/// Evaluate a predicate expression for a single row.
pub fn eval_predicate(expr: &Expr, chunk: &Chunk, row: usize) -> bool {
    matches!(eval(expr, chunk, row), Value::Bool(true))
}

pub fn eval(expr: &Expr, chunk: &Chunk, row: usize) -> Value {
    match expr {
        Expr::Literal(v) => v.clone(),
        Expr::Field { name, access } => eval_field(name, *access, chunk, row),
        Expr::Compare { left, op, right } => {
            let l = eval(left, chunk, row);
            let r = eval(right, chunk, row);
            Value::Bool(compare(&l, *op, &r))
        }
        Expr::And(a, b) => {
            Value::Bool(eval_predicate(a, chunk, row) && eval_predicate(b, chunk, row))
        }
        Expr::Or(a, b) => {
            Value::Bool(eval_predicate(a, chunk, row) || eval_predicate(b, chunk, row))
        }
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

fn compare(l: &Value, op: CmpOp, r: &Value) -> bool {
    use std::cmp::Ordering;
    let ord = match (l, r) {
        (Value::Str(a), Value::Str(b)) => a.partial_cmp(b),
        _ => match (l.as_f64(), r.as_f64()) {
            (Some(a), Some(b)) => a.partial_cmp(&b),
            _ => {
                // Fall back to string-equality semantics for mixed/null.
                return match op {
                    CmpOp::Eq => l == r,
                    CmpOp::Ne => l != r,
                    _ => false,
                };
            }
        },
    };
    matches!(
        (ord, op),
        (Some(Ordering::Equal), CmpOp::Eq | CmpOp::Le | CmpOp::Ge)
            | (Some(Ordering::Less), CmpOp::Lt | CmpOp::Le | CmpOp::Ne)
            | (Some(Ordering::Greater), CmpOp::Gt | CmpOp::Ge | CmpOp::Ne)
    )
}
