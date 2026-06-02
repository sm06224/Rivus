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
        Func::StartsWith => {
            let hay = arg(0).to_string();
            let prefix = arg(1).to_string();
            Value::Bool(hay.starts_with(&prefix))
        }
        Func::EndsWith => {
            let hay = arg(0).to_string();
            let suffix = arg(1).to_string();
            Value::Bool(hay.ends_with(&suffix))
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
        Func::Like => {
            let hay = arg(0).to_string();
            let pat = arg(1).to_string();
            Value::Bool(like_match(&hay, &pat))
        }
        Func::Glob => {
            let hay = arg(0).to_string();
            let pat = arg(1).to_string();
            Value::Bool(glob_match(&hay, &pat))
        }
        Func::Regexp => {
            // Row-wise fallback (the columnar path in `eval_column` compiles the
            // pattern once). With the feature off this is always false.
            let hay = arg(0).to_string();
            let pat = arg(1).to_string();
            Value::Bool(regexp_match(&hay, &pat))
        }
        Func::Replace => {
            let s = arg(0).to_string();
            let from = arg(1).to_string();
            let to = arg(2).to_string();
            // An empty `from` would loop in `str::replace`'s sense of "between
            // every char"; keep it a no-op so the result is well-defined.
            let out = if from.is_empty() {
                s
            } else {
                s.replace(&from, &to)
            };
            Value::Str(out)
        }
        Func::SplitPart => {
            let s = arg(0).to_string();
            let sep = arg(1).to_string();
            // 1-based field index (DuckDB/awk convention); 0 or out-of-range → "".
            let n = arg(2).as_f64().unwrap_or(0.0) as i64;
            let out = if sep.is_empty() || n < 1 {
                String::new()
            } else {
                s.split(sep.as_str())
                    .nth((n - 1) as usize)
                    .unwrap_or("")
                    .to_string()
            };
            Value::Str(out)
        }
        Func::Concat => {
            let mut out = String::new();
            for i in 0..args.len() {
                out.push_str(&arg(i).to_string());
            }
            Value::Str(out)
        }
        Func::Abs => num_value(arg(0), f64::abs),
        Func::Round => num_value(arg(0), f64::round),
        Func::Floor => num_value(arg(0), f64::floor),
        Func::Ceil => num_value(arg(0), f64::ceil),
        Func::Coalesce => {
            // First argument whose text form is non-empty, kept as-is (preserves
            // its lane). Empty string if every argument is empty/null.
            for i in 0..args.len() {
                let v = arg(i);
                if !v.to_string().is_empty() {
                    return v;
                }
            }
            Value::Str(String::new())
        }
    }
}

/// Apply a unary numeric function to a value, coercing a numeric *string* (e.g.
/// from a `:str`-declared column) by parsing it. A non-numeric value yields
/// `Null` (continue-first). An integral result is returned as `I64` so an
/// integer-looking column stays integer-looking; otherwise `F64`.
fn num_value(v: Value, f: impl Fn(f64) -> f64) -> Value {
    let x = match v.as_f64() {
        Some(x) => x,
        None => match v {
            Value::Str(s) => match s.trim().parse::<f64>() {
                Ok(x) => x,
                Err(_) => return Value::Null,
            },
            _ => return Value::Null,
        },
    };
    let r = f(x);
    if r.is_finite() && r.fract() == 0.0 && r.abs() < 9.007_199_254_740_992e15 {
        Value::I64(r as i64)
    } else {
        Value::F64(r)
    }
}

/// Run `f` with the compiled regex for `pat`, caching it per thread so a
/// row-wise predicate (`|? regexp(col, "…")`) compiles the pattern once, not
/// once per row. An invalid pattern is cached as `None` (→ no match), keeping
/// continue-first semantics. The cache is keyed by the pattern string; flows
/// use a tiny number of distinct patterns, so it never grows unbounded in
/// practice.
#[cfg(feature = "regex")]
fn with_regex<R>(pat: &str, f: impl FnOnce(Option<&regex::Regex>) -> R) -> R {
    use std::cell::RefCell;
    use std::collections::HashMap;
    thread_local! {
        static CACHE: RefCell<HashMap<String, Option<regex::Regex>>> =
            RefCell::new(HashMap::new());
    }
    CACHE.with(|c| {
        let mut m = c.borrow_mut();
        let entry = m
            .entry(pat.to_string())
            .or_insert_with(|| regex::Regex::new(pat).ok());
        f(entry.as_ref())
    })
}

/// Does `text` contain a match for `pat` (unanchored)? Behind the off-by-default
/// `regex` feature; without it, always `false`. Uses the per-thread compiled-
/// regex cache so repeated calls with the same pattern don't recompile.
#[cfg(feature = "regex")]
fn regexp_match(text: &str, pat: &str) -> bool {
    with_regex(pat, |re| re.map(|r| r.is_match(text)).unwrap_or(false))
}

#[cfg(not(feature = "regex"))]
fn regexp_match(_text: &str, _pat: &str) -> bool {
    false
}

/// The literal pattern of `regexp(col, "lit")`, if arg 1 is a string literal
/// (the common case) — lets the columnar path compile the regex exactly once.
fn regex_literal(args: &[Expr]) -> Option<&str> {
    match args.get(1) {
        Some(Expr::Literal(Value::Str(s))) => Some(s.as_str()),
        _ => None,
    }
}

/// Columnar `regexp` with a literal pattern: compile once, test every row.
#[cfg(feature = "regex")]
fn regexp_column(args: &[Expr], chunk: &Chunk) -> Column {
    let pat = regex_literal(args).unwrap_or("");
    let col = eval_column(&args[0], chunk);
    with_regex(pat, |re| match re {
        Some(re) => Column::Bool(
            (0..chunk.len)
                .map(|r| re.is_match(&col.value_at(r).to_string()))
                .collect(),
        ),
        // Invalid pattern → all-false (continue-first: the run doesn't panic).
        None => Column::Bool(vec![false; chunk.len]),
    })
}

#[cfg(not(feature = "regex"))]
fn regexp_column(_args: &[Expr], chunk: &Chunk) -> Column {
    Column::Bool(vec![false; chunk.len])
}

/// SQL `LIKE`: `%` matches any run (including empty), `_` matches exactly one
/// char. Case-sensitive, no escape char (MVP). Linear-time backtracking with a
/// single restart pointer (the classic two-pointer wildcard match), so a
/// pathological pattern can't blow up.
fn like_match(text: &str, pat: &str) -> bool {
    wildcard_match(text, pat, '%', '_')
}

/// Shell glob over a single string: `*` any run, `?` any single char, plus
/// `[abc]` / `[a-z]` / `[!abc]` character classes. Case-sensitive.
fn glob_match(text: &str, pat: &str) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pat.chars().collect();
    glob_rec(&t, 0, &p, 0)
}

/// Two-pointer wildcard match where `star` is the any-run wildcard and `one`
/// the any-single wildcard. O(n·m) worst case but no recursion/backtracking
/// explosion: on a mismatch it backtracks only to just-after the last `star`,
/// advancing how much that star consumed by one (the canonical greedy algo).
fn wildcard_match(text: &str, pat: &str, star: char, one: char) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pat.chars().collect();
    let (mut ti, mut pi) = (0usize, 0usize);
    // `last_star` = pattern index of the most recent `star`; `consumed` = how
    // many text chars it currently absorbs (grows by one on each backtrack).
    let mut last_star: Option<usize> = None;
    let mut consumed = 0usize;
    while ti < t.len() {
        if pi < p.len() && (p[pi] == one || p[pi] == t[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == star {
            last_star = Some(pi);
            consumed = ti;
            pi += 1;
        } else if let Some(ls) = last_star {
            pi = ls + 1;
            consumed += 1;
            ti = consumed;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == star {
        pi += 1;
    }
    pi == p.len()
}

/// Recursive glob with `[...]` classes (rare enough that recursion is fine; `*`
/// still bounded because each `*` advances `ti` monotonically per call chain).
fn glob_rec(t: &[char], ti: usize, p: &[char], pi: usize) -> bool {
    if pi == p.len() {
        return ti == t.len();
    }
    match p[pi] {
        '*' => {
            // Match zero-or-more: try consuming none, then one more each step.
            for k in ti..=t.len() {
                if glob_rec(t, k, p, pi + 1) {
                    return true;
                }
            }
            false
        }
        '?' => ti < t.len() && glob_rec(t, ti + 1, p, pi + 1),
        '[' => {
            if ti >= t.len() {
                return false;
            }
            // Parse the class `[...]`: optional leading `!` negates.
            let mut j = pi + 1;
            let negate = j < p.len() && p[j] == '!';
            if negate {
                j += 1;
            }
            let mut matched = false;
            let class_start = j;
            while j < p.len() && (p[j] != ']' || j == class_start) {
                // Range `a-z` when a `-` sits between two chars inside the class.
                if j + 2 < p.len() && p[j + 1] == '-' && p[j + 2] != ']' {
                    if t[ti] >= p[j] && t[ti] <= p[j + 2] {
                        matched = true;
                    }
                    j += 3;
                } else {
                    if t[ti] == p[j] {
                        matched = true;
                    }
                    j += 1;
                }
            }
            // `j` is at the closing `]` (or end if malformed → no match).
            if j >= p.len() {
                return false;
            }
            if matched != negate {
                glob_rec(t, ti + 1, p, j + 1)
            } else {
                false
            }
        }
        c => ti < t.len() && t[ti] == c && glob_rec(t, ti + 1, p, pi + 1),
    }
}

/// Coerce a value to an integer (truncating floats; parsing strings; bool→0/1).
fn to_i64(v: Value) -> i64 {
    match v {
        Value::I64(x) => x,
        Value::F64(x) => x as i64,
        Value::Dec(d) => d.to_f64() as i64,
        Value::DateTime(t) => t.ticks,
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
        Value::Dec(d) => d.to_f64(),
        Value::DateTime(t) => t.ticks as f64,
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
        Value::Dec(d) => d.unscaled != 0,
        Value::DateTime(t) => t.ticks != 0,
        Value::Str(s) => s.trim().eq_ignore_ascii_case("true") || s.trim() == "1",
        Value::Null => false,
    }
}

/// Cast a value to a target lane.
fn cast_value(v: Value, ty: DataType) -> Value {
    match ty {
        DataType::I64 => Value::I64(to_i64(v)),
        DataType::F64 => Value::F64(to_f64(v)),
        // Cast to decimal at a fixed scale: route through the f64 view and
        // round-half-even to the target scale (the reader has an exact text
        // path; this covers computed casts). Design doc 21.
        DataType::Decimal { scale } => Value::Dec(f64_to_decimal(to_f64(v), scale)),
        // Cast to datetime treats the value as epoch ticks at the target unit
        // (the reader has an exact text path; this covers computed casts).
        DataType::DateTime { unit } => Value::DateTime(rivus_core::DateTime::new(to_i64(v), unit)),
        DataType::Bool => Value::Bool(to_bool(v)),
        DataType::Str => Value::Str(v.to_string()),
        DataType::Null => Value::Null,
    }
}

/// Build a `Decimal` at `scale` from an f64 (round-half-even on the last digit).
fn f64_to_decimal(x: f64, scale: u8) -> rivus_core::Decimal {
    if !x.is_finite() {
        return rivus_core::Decimal::new(0, scale);
    }
    let mut pow = 1.0f64;
    for _ in 0..scale {
        pow *= 10.0;
    }
    // `round_ties_even` gives banker's rounding, matching Decimal::rescale.
    let unscaled = (x * pow).round_ties_even() as i128;
    rivus_core::Decimal::new(unscaled, scale)
}

/// Exact comparison of a decimal cell (`u` unscaled at `scale`) against an exact
/// decimal literal `lit`, **shared by the vectorized kernel and the interpreter**
/// so the two stay byte-identical. The accounting contract (design 21) is that a
/// decimal comparison **never silently rounds** either operand: `Decimal`'s
/// ordering rescales both to the larger of the two scales and compares as `i128`
/// (falling back to the f64 view only if an i128 rescale overflows). This is why
/// the literal must reach here as its written decimal, not via `f64`.
#[inline]
pub(crate) fn dec_cmp(u: i128, scale: u8, op: CmpOp, lit: &rivus_core::Decimal) -> bool {
    cmp_ord(rivus_core::Decimal::new(u, scale).partial_cmp(lit), op)
}

/// Apply `op` to two unscaled `i128` values already at a common scale (the
/// kernel hoists the rescale out of its row loop, then calls this per cell).
#[inline]
pub(crate) fn dec_cmp_i128(a: i128, op: CmpOp, b: i128) -> bool {
    cmp_ord(a.partial_cmp(&b), op)
}

/// `10^n` as `i128`, or `None` if it overflows (`n > 38`). Used to lift a cell's
/// unscaled value to a common scale.
#[inline]
pub(crate) fn pow10_i128(n: u8) -> Option<i128> {
    let mut p: i128 = 1;
    for _ in 0..n {
        p = p.checked_mul(10)?;
    }
    Some(p)
}

/// Cast a whole column to a target lane (columnar path for computed columns).
pub(crate) fn cast_column(col: Column, ty: DataType) -> Column {
    let n = col.len();
    match ty {
        DataType::I64 => Column::I64((0..n).map(|i| to_i64(col.value_at(i))).collect()),
        DataType::F64 => Column::F64((0..n).map(|i| to_f64(col.value_at(i))).collect()),
        DataType::Decimal { scale } => {
            // Build the unscaled i128 lane per cell (design doc 21).
            let unscaled = (0..n)
                .map(|i| f64_to_decimal(to_f64(col.value_at(i)), scale).unscaled)
                .collect();
            Column::Dec(rivus_core::DecColumn { unscaled, scale })
        }
        DataType::DateTime { unit } => {
            let ticks = (0..n).map(|i| to_i64(col.value_at(i))).collect();
            Column::DateTime(rivus_core::DtColumn { ticks, unit })
        }
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
                // `regexp(col, "literal")` compiles the pattern once for the
                // whole chunk (per-row compilation is catastrophic — ~10× slower).
                Func::Regexp if regex_literal(args).is_some() => regexp_column(args, chunk),
                Func::Contains
                | Func::StartsWith
                | Func::EndsWith
                | Func::Like
                | Func::Glob
                | Func::Regexp => Column::Bool(
                    (0..n)
                        .map(|r| matches!(call_func(*func, args, chunk, r), Value::Bool(true)))
                        .collect(),
                ),
                // Numeric / coalesce funcs produce a value whose lane depends on
                // the data (e.g. `round` is integral, `coalesce` may be text),
                // so pick the narrowest fitting lane per chunk.
                Func::Abs | Func::Round | Func::Floor | Func::Ceil | Func::Coalesce => {
                    let vals: Vec<Value> =
                        (0..n).map(|r| call_func(*func, args, chunk, r)).collect();
                    column_from_values(vals)
                }
                _ => {
                    let mut s = StrColumn::with_capacity(n, n * 8);
                    for r in 0..n {
                        s.push(&call_func(*func, args, chunk, r).to_string());
                    }
                    Column::Str(s)
                }
            }
        }
        // `case … end` is row-wise; evaluate each row and pick a column lane
        // from the resulting values (all-int → I64, all-numeric → F64,
        // all-bool → Bool, otherwise Str — mirroring schema inference).
        Expr::Case { .. } => {
            let vals: Vec<Value> = (0..chunk.len).map(|r| eval(expr, chunk, r)).collect();
            column_from_values(vals)
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

/// Build a typed column from row values, choosing the narrowest lane that fits
/// (all-int → I64, all-numeric → F64, all-bool → Bool, else Str). Used by
/// row-wise expressions like `case` that don't have a native columnar form.
pub(crate) fn column_from_values(vals: Vec<Value>) -> Column {
    let all_int = vals
        .iter()
        .all(|v| matches!(v, Value::I64(_) | Value::Bool(_)));
    let all_num = vals
        .iter()
        .all(|v| matches!(v, Value::I64(_) | Value::F64(_) | Value::Bool(_)));
    let all_bool = !vals.is_empty() && vals.iter().all(|v| matches!(v, Value::Bool(_)));
    if all_bool {
        Column::Bool(
            vals.iter()
                .map(|v| matches!(v, Value::Bool(true)))
                .collect(),
        )
    } else if all_int {
        Column::I64(
            vals.iter()
                .map(|v| v.as_f64().unwrap_or(0.0) as i64)
                .collect(),
        )
    } else if all_num {
        Column::F64(vals.iter().map(|v| v.as_f64().unwrap_or(0.0)).collect())
    } else {
        let mut s = StrColumn::with_capacity(vals.len(), vals.len() * 8);
        for v in &vals {
            s.push(&v.to_string());
        }
        Column::Str(s)
    }
}

fn const_column(v: &Value, n: usize) -> Column {
    match v {
        Value::I64(x) => Column::I64(vec![*x; n]),
        Value::F64(x) => Column::F64(vec![*x; n]),
        Value::Dec(d) => Column::Dec(rivus_core::DecColumn {
            unscaled: vec![d.unscaled; n],
            scale: d.scale,
        }),
        Value::DateTime(t) => Column::DateTime(rivus_core::DtColumn {
            ticks: vec![t.ticks; n],
            unit: t.unit,
        }),
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
        Column::Dec(d) => {
            let pow = 10f64.powi(d.scale as i32);
            (d.unscaled.iter().map(|&u| u as f64 / pow).collect(), false)
        }
        // DateTime arithmetic operates on the raw integer tick lane (epoch ticks
        // at the column's unit); diffs/offsets stay integer.
        Column::DateTime(d) => (d.ticks.iter().map(|&t| t as f64).collect(), true),
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
        Expr::Case { branches, default } => {
            for (cond, val) in branches {
                if eval_predicate(cond, chunk, row) {
                    return eval(val, chunk, row);
                }
            }
            match default {
                Some(d) => eval(d, chunk, row),
                None => Value::Str(String::new()),
            }
        }
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
    // Exact decimal lane: a decimal column vs a numeric literal compares as i128
    // (matching the kernel), not via the lossy f64 view.
    if let Some(b) = dec_field_vs_literal(left, op, right, chunk, row) {
        return b;
    }
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

/// `decimal_column OP numeric_literal` (either operand order) → exact `i128`
/// comparison via [`dec_cmp`], matching the kernel. `None` when the operands are
/// not that shape (the caller then takes its usual numeric/string path).
fn dec_field_vs_literal(
    left: &Expr,
    op: CmpOp,
    right: &Expr,
    chunk: &Chunk,
    row: usize,
) -> Option<bool> {
    let dec_cell = |e: &Expr| -> Option<(i128, u8)> {
        if let Expr::Field { name, .. } = e {
            if let Some(Column::Dec(d)) = chunk.column(name) {
                return Some((d.unscaled[row], d.scale));
            }
        }
        None
    };
    // The literal as an *exact* decimal (written decimals are `Value::Dec`,
    // integers `Value::I64`); anything else is left to the numeric/f64 path.
    let lit = |e: &Expr| -> Option<rivus_core::Decimal> {
        match e {
            Expr::Literal(Value::Dec(d)) => Some(*d),
            Expr::Literal(Value::I64(n)) => Some(rivus_core::Decimal::new(*n as i128, 0)),
            Expr::Literal(Value::Bool(b)) => Some(rivus_core::Decimal::new(*b as i128, 0)),
            _ => None,
        }
    };
    if let (Some((u, s)), Some(r)) = (dec_cell(left), lit(right)) {
        return Some(dec_cmp(u, s, op, &r));
    }
    if let (Some(r), Some((u, s))) = (lit(left), dec_cell(right)) {
        // `literal OP decimal` == `decimal OP_reversed literal`.
        let rev = match op {
            CmpOp::Lt => CmpOp::Gt,
            CmpOp::Le => CmpOp::Ge,
            CmpOp::Gt => CmpOp::Lt,
            CmpOp::Ge => CmpOp::Le,
            CmpOp::Eq => CmpOp::Eq,
            CmpOp::Ne => CmpOp::Ne,
        };
        return Some(dec_cmp(u, s, rev, &r));
    }
    None
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
            Column::Dec(d) => Some(d.unscaled[row] as f64 / 10f64.powi(d.scale as i32)),
            Column::DateTime(d) => Some(d.ticks[row] as f64),
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

/// Datetime-aware comparison: when one operand is a `DateTime`, compare on the
/// exact integer-tick lane rather than the lossy f64 view (design 23).
///
/// * two datetimes → exact cross-unit instant order;
/// * datetime vs a text literal → parse the literal into the same lane (the
///   datetime's unit, auto-inferring its format) and compare instants
///   (`|? ts >= "260601000000"`). A literal matching no known format is not a
///   valid instant, so only `!=` holds (continue-first; no fatal).
///
/// Returns `None` when neither operand is a datetime (normal path applies), or
/// for datetime-vs-number (handled by the raw-tick f64 view downstream).
fn dt_cmp(l: &Value, op: CmpOp, r: &Value) -> Option<bool> {
    let parse = |s: &str, unit| rivus_core::DateTime::parse_auto(s, unit);
    match (l, r) {
        (Value::DateTime(a), Value::DateTime(b)) => Some(cmp_ord(a.partial_cmp(b), op)),
        (Value::DateTime(a), Value::Str(s)) => Some(match parse(s, a.unit) {
            Some(b) => cmp_ord(a.partial_cmp(&b), op),
            None => op == CmpOp::Ne,
        }),
        (Value::Str(s), Value::DateTime(b)) => Some(match parse(s, b.unit) {
            Some(a) => cmp_ord(a.partial_cmp(b), op),
            None => op == CmpOp::Ne,
        }),
        _ => None,
    }
}

fn compare(l: &Value, op: CmpOp, r: &Value) -> bool {
    if let Some(b) = dt_cmp(l, op, r) {
        return b;
    }
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

#[cfg(test)]
mod match_tests {
    use super::{glob_match, like_match};

    #[test]
    fn like_wildcards() {
        assert!(like_match("JP-1234", "JP-%"));
        assert!(like_match("JP-1234", "%234"));
        assert!(like_match("JP-1234", "%-%"));
        assert!(like_match("JP-1234", "__-____")); // 2 + dash + 4
        assert!(like_match("abc", "%")); // % matches everything
        assert!(like_match("", "%")); // including empty
        assert!(like_match("abc", "abc")); // literal
        assert!(!like_match("JP-1234", "US-%"));
        assert!(!like_match("ab", "abc"));
        assert!(like_match("abc", "ab_")); // `_` matches exactly the trailing c
    }

    #[test]
    fn like_underscore_is_exactly_one() {
        assert!(like_match("abc", "ab_"));
        assert!(!like_match("ab", "ab_")); // nothing for the _
        assert!(!like_match("abcd", "ab_")); // trailing d unmatched
    }

    #[test]
    fn glob_wildcards_and_classes() {
        assert!(glob_match("JP-0042", "[JD]*-00??"));
        assert!(glob_match("DE-0099", "[JD]*-00??"));
        assert!(!glob_match("US-0007", "[JD]*-00??")); // U not in [JD]
        assert!(glob_match("abc", "a?c"));
        assert!(glob_match("abc", "a*"));
        assert!(glob_match("a-z", "[a-z]-[a-z]"));
        assert!(!glob_match("A-z", "[a-z]-[a-z]")); // case-sensitive
        assert!(glob_match("x", "[!abc]")); // negated class
        assert!(!glob_match("a", "[!abc]"));
        assert!(glob_match("anything", "*"));
        assert!(glob_match("", "*"));
    }

    #[test]
    fn no_catastrophic_backtracking() {
        // A pathological LIKE that a naive recursive matcher chokes on must
        // still resolve quickly with the two-pointer algorithm.
        let text = "a".repeat(64);
        let pat = "%".repeat(50) + "b"; // never matches (no 'b')
        assert!(!like_match(&text, &pat));
    }
}

#[cfg(all(test, feature = "regex"))]
mod regex_tests {
    use super::regexp_match;

    #[test]
    fn regexp_partial_and_anchored() {
        assert!(regexp_match("JP-1234", r"^JP-\d{4}$"));
        assert!(regexp_match("xx JP-9 yy", r"JP-\d")); // unanchored partial
        assert!(!regexp_match("US-1234", r"^JP-\d{4}$"));
        assert!(regexp_match("abc", r"[a-c]+"));
        // Invalid pattern → false (continue-first, no panic).
        assert!(!regexp_match("abc", r"([unclosed"));
    }
}
