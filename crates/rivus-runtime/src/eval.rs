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

use rivus_core::{Chunk, Column, ColumnData, DataType, StrColumn, Validity, Value};
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
            let start = arg(1).as_f64().unwrap_or(1.0);
            let take = args
                .get(2)
                .map(|e| eval(e, chunk, row).as_f64().unwrap_or(0.0) as usize)
                .unwrap_or(usize::MAX);
            Value::Str(substr_1based(&s, start, take))
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
            // First **non-null** argument, kept as-is (preserves its lane). A
            // real empty string `""` is non-null, so it is now returned (design
            // 26 §26.2(c) / §26.7: rectified from "non-empty" to "non-null").
            // Every argument null → null.
            for i in 0..args.len() {
                let v = arg(i);
                if !v.is_null() {
                    return v;
                }
            }
            Value::Null
        }
        // Datetime field extractors (design 23). The argument is coerced to a
        // `DateTime` (a datetime cell as-is, or a text/epoch value parsed into
        // the lane); a non-datetime that won't coerce yields `Null`.
        Func::Year | Func::Month | Func::Day | Func::Hour | Func::Minute | Func::Second => {
            match as_datetime(arg(0)) {
                Some(dt) => {
                    let (y, mo, d, h, mi, se) = dt.fields();
                    Value::I64(match func {
                        Func::Year => y,
                        Func::Month => mo,
                        Func::Day => d,
                        Func::Hour => h,
                        Func::Minute => mi,
                        _ => se,
                    })
                }
                None => Value::Null,
            }
        }
        // `trunc(ts, "day")` → datetime truncated to the boundary (same unit).
        Func::Trunc => match as_datetime(arg(0)) {
            Some(dt) => Value::DateTime(dt.truncated(arg(1).to_string().trim())),
            None => Value::Null,
        },
        // `format(ts|dur, "fmt")` → text rendering. A duration renders human by
        // default, or ISO-8601 with `"iso"`/`"iso8601"`. A datetime uses the
        // strptime-style `fmt`. Anything else coerces to its text form (so
        // `format` is total / continue-first).
        Func::Format => {
            let a0 = arg(0);
            let fmt = arg(1).to_string();
            match a0 {
                Value::Duration(d) => Value::Str(
                    if fmt.eq_ignore_ascii_case("iso") || fmt.eq_ignore_ascii_case("iso8601") {
                        d.to_iso8601()
                    } else {
                        d.to_human()
                    },
                ),
                other => match as_datetime(other.clone()) {
                    Some(dt) => Value::Str(dt.format(&fmt)),
                    None => Value::Str(other.to_string()),
                },
            }
        }
        // Date extractors (#58): coerce to a calendar date (a date cell as-is, a
        // datetime's date part, or a parsed text), then derive. A value that
        // won't coerce yields `Null` (continue-first).
        Func::Weekday => match as_date(arg(0)) {
            Some(d) => Value::I64(d.weekday() as i64),
            None => Value::Null,
        },
        Func::IsWeekend => match as_date(arg(0)) {
            Some(d) => Value::Bool(d.weekday() >= 5),
            None => Value::Null,
        },
        Func::Date => match as_date(arg(0)) {
            Some(d) => Value::Date(d),
            None => Value::Null,
        },
        Func::Time => match as_time(arg(0)) {
            Some(t) => Value::Time(t),
            None => Value::Null,
        },
    }
}

/// Coerce a value to a [`TimeOfDay`] for `time(x)` (#58): a time cell as-is; a
/// datetime → its time-of-day part (ticks mod one day); `HH:mm:ss` text →
/// parsed; else a datetime auto-parse reduced to its time. Else `None`.
fn as_time(v: Value) -> Option<rivus_core::TimeOfDay> {
    use rivus_core::{TimeOfDay, TimeUnit};
    let from_dt = |dt: rivus_core::DateTime| {
        let per_day = dt.unit.per_sec() * 86_400;
        TimeOfDay::new(dt.ticks.rem_euclid(per_day), dt.unit)
    };
    match v {
        Value::Time(t) => Some(t),
        Value::DateTime(dt) => Some(from_dt(dt)),
        Value::Str(s) => TimeOfDay::parse_at(&s, TimeUnit::Sec)
            .or_else(|| as_datetime(Value::Str(s)).map(from_dt)),
        _ => None,
    }
}

/// Coerce a value to a calendar [`Date`] for the date functions (#58): a date
/// cell as-is; a datetime → its date part; ISO `yyyy-MM-dd` text → parsed; else
/// a text/epoch value is read through the datetime auto-parse and reduced to its
/// date. Anything else → `None` (continue-first).
fn as_date(v: Value) -> Option<rivus_core::Date> {
    match v {
        Value::Date(d) => Some(d),
        Value::DateTime(dt) => {
            let (y, mo, d, ..) = dt.fields();
            Some(rivus_core::Date::from_ymd(y, mo, d))
        }
        Value::Str(s) => rivus_core::Date::parse(&s).or_else(|| {
            as_datetime(Value::Str(s)).map(|dt| {
                let (y, mo, d, ..) = dt.fields();
                rivus_core::Date::from_ymd(y, mo, d)
            })
        }),
        _ => None,
    }
}

/// Coerce a value to a [`DateTime`] for the datetime functions (design 23): a
/// datetime cell is taken as-is; a text value is auto-parsed (second unit); an
/// integer is read as epoch seconds. Anything else → `None` (continue-first).
fn as_datetime(v: Value) -> Option<rivus_core::DateTime> {
    use rivus_core::{DateTime, TimeUnit};
    match v {
        Value::DateTime(dt) => Some(dt),
        Value::Str(s) => DateTime::parse_auto(&s, TimeUnit::Sec),
        Value::I64(n) => Some(DateTime::new(n, TimeUnit::Sec)),
        _ => None,
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
        Some(re) => Column::bool(
            (0..chunk.len)
                .map(|r| re.is_match(&col.value_at(r).to_string()))
                .collect(),
        ),
        // Invalid pattern → all-false (continue-first: the run doesn't panic).
        None => Column::bool(vec![false; chunk.len]),
    })
}

#[cfg(not(feature = "regex"))]
fn regexp_column(_args: &[Expr], chunk: &Chunk) -> Column {
    Column::bool(vec![false; chunk.len])
}

/// `substr(s, start, len)` with a **1-based** start (SQL / DuckDB convention):
/// `start == 1` is the first char and `start <= 1` clamps to the beginning
/// (lenient, so the old 0-based call `substr(s, 0, n)` still returns the prefix).
/// `#bugreport ③`. `take == usize::MAX` means "to the end" (no length given).
fn substr_1based(s: &str, start: f64, take: usize) -> String {
    let start1 = start as i64;
    let skip = if start1 <= 1 {
        0
    } else {
        (start1 - 1) as usize
    };
    s.chars().skip(skip).take(take).collect()
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
        Value::Duration(d) => d.ticks,
        Value::Date(d) => d.epoch_day as i64,
        Value::Time(t) => t.ticks,
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
        Value::Duration(d) => d.ticks as f64,
        Value::Date(d) => d.epoch_day as f64,
        Value::Time(t) => t.ticks as f64,
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
        Value::Duration(d) => d.ticks != 0,
        Value::Date(d) => d.epoch_day != 0,
        Value::Time(t) => t.ticks != 0,
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
        // Cast to duration treats the value as a raw tick span at the unit.
        DataType::Duration { unit } => Value::Duration(rivus_core::Duration::new(to_i64(v), unit)),
        // Cast to date treats the value as an epoch-day (i32).
        DataType::Date => Value::Date(rivus_core::Date::new(to_i64(v) as i32)),
        // Cast to time treats the value as ticks-since-midnight (MVP Sec).
        DataType::Time => Value::Time(rivus_core::TimeOfDay::new(
            to_i64(v),
            rivus_core::TimeUnit::Sec,
        )),
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
        DataType::I64 => Column::i64((0..n).map(|i| to_i64(col.value_at(i))).collect()),
        DataType::F64 => Column::f64((0..n).map(|i| to_f64(col.value_at(i))).collect()),
        DataType::Decimal { scale } => {
            // Build the unscaled i128 lane per cell (design doc 21).
            let unscaled = (0..n)
                .map(|i| f64_to_decimal(to_f64(col.value_at(i)), scale).unscaled)
                .collect();
            Column::dec(rivus_core::DecColumn { unscaled, scale })
        }
        DataType::DateTime { unit } => {
            let ticks = (0..n).map(|i| to_i64(col.value_at(i))).collect();
            Column::datetime(rivus_core::DtColumn { ticks, unit })
        }
        DataType::Duration { unit } => {
            let ticks = (0..n).map(|i| to_i64(col.value_at(i))).collect();
            Column::duration(rivus_core::DurColumn { ticks, unit })
        }
        DataType::Date => Column::date((0..n).map(|i| to_i64(col.value_at(i)) as i32).collect()),
        DataType::Time => Column::time((0..n).map(|i| to_i64(col.value_at(i))).collect()),
        DataType::Bool => Column::bool((0..n).map(|i| to_bool(col.value_at(i))).collect()),
        DataType::Str => {
            let mut s = StrColumn::with_capacity(n, n * 8);
            for i in 0..n {
                s.push(&col.value_at(i).to_string());
            }
            Column::str(s)
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
            None => Column::f64(vec![f64::NAN; chunk.len]),
        },
        Expr::Literal(v) => const_column(v, chunk.len),
        Expr::Cast { expr, ty } => cast_column(eval_column(expr, chunk), *ty),
        Expr::Arith { left, op, right } => eval_arith(left, *op, right, chunk),
        Expr::Func { func, args } => {
            let n = chunk.len;
            match func {
                // Integer-valued funcs: `len` and the datetime field extractors
                // (design 23) all yield an i64 lane.
                Func::Len
                | Func::Year
                | Func::Month
                | Func::Day
                | Func::Hour
                | Func::Minute
                | Func::Second
                | Func::Weekday => Column::i64(
                    (0..n)
                        .map(|r| to_i64(call_func(*func, args, chunk, r)))
                        .collect(),
                ),
                // `trunc` stays on the datetime lane (truncated ticks, same unit).
                Func::Trunc => {
                    let vals: Vec<Value> =
                        (0..n).map(|r| call_func(*func, args, chunk, r)).collect();
                    column_from_values(vals)
                }
                // `regexp(col, "literal")` compiles the pattern once for the
                // whole chunk (per-row compilation is catastrophic — ~10× slower).
                Func::Regexp if regex_literal(args).is_some() => regexp_column(args, chunk),
                Func::Contains
                | Func::StartsWith
                | Func::EndsWith
                | Func::Like
                | Func::Glob
                | Func::Regexp
                | Func::IsWeekend => Column::bool(
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
                // `date(x)`/`time(x)` → the exact date/time lane
                // (column_from_values keeps the all-Date / all-Time result). #58.
                Func::Date | Func::Time => {
                    let vals: Vec<Value> =
                        (0..n).map(|r| call_func(*func, args, chunk, r)).collect();
                    column_from_values(vals)
                }
                _ => {
                    let mut s = StrColumn::with_capacity(n, n * 8);
                    for r in 0..n {
                        s.push(&call_func(*func, args, chunk, r).to_string());
                    }
                    Column::str(s)
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
            Column::bool(v)
        }
    }
}

/// Build a typed column from row values, choosing the narrowest lane that fits
/// (all-int → I64, all-numeric → F64, all-bool → Bool, else Str). Used by
/// row-wise expressions like `case` that don't have a native columnar form.
pub(crate) fn column_from_values(vals: Vec<Value>) -> Column {
    // Null-aware (design 26): a `Value::Null` is *missing* — it does not force
    // the column to the string lane, and it carries validity = 0 (the backing
    // byte stays the lane default). Lane choice looks only at the non-null
    // values, so `int op int` with a null stays the I64 lane with a null hole.
    let bits: Vec<bool> = vals.iter().map(|v| !v.is_null()).collect();
    let validity = Validity::from_bits(&bits);
    let with = |data: ColumnData| Column::new(data, validity.clone());
    let first_present = vals.iter().find(|v| !v.is_null());
    let all = |pred: fn(&Value) -> bool| {
        first_present.is_some() && vals.iter().filter(|v| !v.is_null()).all(pred)
    };

    // All-datetime → keep the datetime lane (e.g. `trunc(ts, "day")`), carrying
    // the first non-null cell's unit (every cell shares it). Design 23.
    if let Some(Value::DateTime(first)) = first_present {
        if all(|v| matches!(v, Value::DateTime(_))) {
            let unit = first.unit;
            return with(ColumnData::DateTime(rivus_core::DtColumn {
                ticks: vals
                    .iter()
                    .map(|v| match v {
                        Value::DateTime(t) => t.ticks,
                        _ => 0,
                    })
                    .collect(),
                unit,
            }));
        }
    }
    // All-duration → keep the duration lane (e.g. `ts2 - ts1`). Design 23 / #57.
    if let Some(Value::Duration(first)) = first_present {
        if all(|v| matches!(v, Value::Duration(_))) {
            let unit = first.unit;
            return with(ColumnData::Duration(rivus_core::DurColumn {
                ticks: vals
                    .iter()
                    .map(|v| match v {
                        Value::Duration(d) => d.ticks,
                        _ => 0,
                    })
                    .collect(),
                unit,
            }));
        }
    }
    // All-date → keep the date lane (e.g. `date(ts)`). Integer epoch-day, #58.
    if matches!(first_present, Some(Value::Date(_))) && all(|v| matches!(v, Value::Date(_))) {
        return with(ColumnData::Date(
            vals.iter()
                .map(|v| match v {
                    Value::Date(d) => d.epoch_day,
                    _ => 0,
                })
                .collect(),
        ));
    }
    // All-time → keep the time-of-day lane (e.g. `time(ts)`). Integer ticks, #58.
    if matches!(first_present, Some(Value::Time(_))) && all(|v| matches!(v, Value::Time(_))) {
        return with(ColumnData::Time(
            vals.iter()
                .map(|v| match v {
                    Value::Time(t) => t.ticks,
                    _ => 0,
                })
                .collect(),
        ));
    }
    let all_bool = all(|v| matches!(v, Value::Bool(_)));
    let all_int = all(|v| matches!(v, Value::I64(_) | Value::Bool(_)));
    let all_num = all(|v| matches!(v, Value::I64(_) | Value::F64(_) | Value::Bool(_)));
    if all_bool {
        with(ColumnData::Bool(
            vals.iter()
                .map(|v| matches!(v, Value::Bool(true)))
                .collect(),
        ))
    } else if all_int {
        with(ColumnData::I64(
            vals.iter()
                .map(|v| v.as_f64().unwrap_or(0.0) as i64)
                .collect(),
        ))
    } else if all_num {
        with(ColumnData::F64(
            vals.iter().map(|v| v.as_f64().unwrap_or(0.0)).collect(),
        ))
    } else {
        // Mixed / all-null → string lane. A null still renders empty (validity
        // carries the null-ness), a real value keeps its text form.
        let mut s = StrColumn::with_capacity(vals.len(), vals.len() * 8);
        for v in &vals {
            s.push(&v.to_string());
        }
        with(ColumnData::Str(s))
    }
}

fn const_column(v: &Value, n: usize) -> Column {
    match v {
        Value::I64(x) => Column::i64(vec![*x; n]),
        Value::F64(x) => Column::f64(vec![*x; n]),
        Value::Dec(d) => Column::dec(rivus_core::DecColumn {
            unscaled: vec![d.unscaled; n],
            scale: d.scale,
        }),
        Value::DateTime(t) => Column::datetime(rivus_core::DtColumn {
            ticks: vec![t.ticks; n],
            unit: t.unit,
        }),
        Value::Duration(d) => Column::duration(rivus_core::DurColumn {
            ticks: vec![d.ticks; n],
            unit: d.unit,
        }),
        Value::Date(d) => Column::date(vec![d.epoch_day; n]),
        Value::Time(t) => Column::time(vec![t.ticks; n]),
        Value::Bool(x) => Column::bool(vec![*x; n]),
        Value::Str(s) => {
            let mut c = StrColumn::with_capacity(n, s.len() * n);
            for _ in 0..n {
                c.push(s);
            }
            Column::str(c)
        }
        // A constant `null` → an all-null column (validity = 0), not an
        // all-valid NaN column (design 26). Not reachable today — there is no
        // `null` literal in the syntax (§26.6) — but kept correct so a future
        // literal / constant-fold can't silently ship NaN-as-null.
        Value::Null => Column::new(
            ColumnData::F64(vec![0.0; n]),
            rivus_core::Validity::all_null(n),
        ),
    }
}

/// A numeric f64 lane for an already-evaluated column, plus whether it is an
/// *integer* lane (so `int op int` can stay integer). Strings are parsed
/// best-effort ("text is stream"): a non-numeric cell becomes NaN. The
/// arithmetic path inspects the column's lane for typed temporal ops (#57)
/// before falling back here.
fn col_num_lane(col: Column) -> (Vec<f64>, bool) {
    match col.into_data() {
        ColumnData::I64(v) => (v.iter().map(|&x| x as f64).collect(), true),
        ColumnData::Bool(v) => (v.iter().map(|&x| if x { 1.0 } else { 0.0 }).collect(), true),
        ColumnData::F64(v) => (v, false),
        ColumnData::Dec(d) => {
            let pow = 10f64.powi(d.scale as i32);
            (d.unscaled.iter().map(|&u| u as f64 / pow).collect(), false)
        }
        // DateTime arithmetic operates on the raw integer tick lane (epoch ticks
        // at the column's unit); diffs/offsets stay integer.
        ColumnData::DateTime(d) => (d.ticks.iter().map(|&t| t as f64).collect(), true),
        // Duration likewise rides the integer tick lane (#57).
        ColumnData::Duration(d) => (d.ticks.iter().map(|&t| t as f64).collect(), true),
        // Date arithmetic operates on the integer epoch-day lane (#58); diffs /
        // day-offsets stay integer.
        ColumnData::Date(v) => (v.iter().map(|&x| x as f64).collect(), true),
        // Time-of-day rides the integer tick lane too (#58).
        ColumnData::Time(v) => (v.iter().map(|&x| x as f64).collect(), true),
        ColumnData::Str(s) => {
            let lane = (0..s.len())
                .map(|i| s.get(i).trim().parse::<f64>().unwrap_or(f64::NAN))
                .collect();
            (lane, false)
        }
    }
}

/// Clamp an `i128` into `i64` (saturating) — used so a cross-unit lift or a
/// `Duration × n` that overflows degrades gracefully (continue-first) rather
/// than wrapping into a nonsense instant/span.
fn sat_i128(x: i128) -> i64 {
    x.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

/// The finer of two units (larger ticks-per-second), used as the common unit
/// when combining two temporal values.
fn finer_unit(a: rivus_core::TimeUnit, b: rivus_core::TimeUnit) -> rivus_core::TimeUnit {
    if a.per_sec() >= b.per_sec() {
        a
    } else {
        b
    }
}

/// Lift `ticks` from `from` to a finer-or-equal unit `to` (exact i128, saturated
/// back to i64).
fn lift_ticks(ticks: i64, from: rivus_core::TimeUnit, to: rivus_core::TimeUnit) -> i128 {
    let factor = (to.per_sec() / from.per_sec()) as i128;
    ticks as i128 * factor
}

/// Typed datetime/duration arithmetic (design 23 / #57), exact in `i64` ticks —
/// never the f64 lane. Returns `None` for any non-temporal combination (the
/// caller then takes the numeric path), keeping all other arithmetic unchanged.
///
/// `DateTime − DateTime → Duration`; `DateTime ± Duration → DateTime`;
/// `Duration ± Duration → Duration`; `Duration × int → Duration`;
/// `Duration ÷ Duration → f64 ratio`. Cross-unit operands lift to the finer
/// unit; an overflow saturates (continue-first).
fn temporal_op(l: &Value, op: ArithOp, r: &Value) -> Option<Value> {
    use rivus_core::{DateTime, Duration};
    match (l, r) {
        // instant − instant = span
        (Value::DateTime(a), Value::DateTime(b)) if op == ArithOp::Sub => {
            let u = finer_unit(a.unit, b.unit);
            let d = lift_ticks(a.ticks, a.unit, u) - lift_ticks(b.ticks, b.unit, u);
            Some(Value::Duration(Duration::new(sat_i128(d), u)))
        }
        // instant ± span = instant (Add is commutative with span + instant)
        (Value::DateTime(a), Value::Duration(s)) if matches!(op, ArithOp::Add | ArithOp::Sub) => {
            let u = finer_unit(a.unit, s.unit);
            let at = lift_ticks(a.ticks, a.unit, u);
            let st = lift_ticks(s.ticks, s.unit, u);
            let t = if op == ArithOp::Add { at + st } else { at - st };
            Some(Value::DateTime(DateTime::new(sat_i128(t), u)))
        }
        (Value::Duration(s), Value::DateTime(a)) if op == ArithOp::Add => {
            let u = finer_unit(a.unit, s.unit);
            let t = lift_ticks(a.ticks, a.unit, u) + lift_ticks(s.ticks, s.unit, u);
            Some(Value::DateTime(DateTime::new(sat_i128(t), u)))
        }
        // span ± span = span
        (Value::Duration(a), Value::Duration(b)) if matches!(op, ArithOp::Add | ArithOp::Sub) => {
            let u = finer_unit(a.unit, b.unit);
            let at = lift_ticks(a.ticks, a.unit, u);
            let bt = lift_ticks(b.ticks, b.unit, u);
            let t = if op == ArithOp::Add { at + bt } else { at - bt };
            Some(Value::Duration(Duration::new(sat_i128(t), u)))
        }
        // span ÷ span = dimensionless ratio (f64, at the common unit)
        (Value::Duration(a), Value::Duration(b)) if op == ArithOp::Div => {
            let u = finer_unit(a.unit, b.unit);
            let at = lift_ticks(a.ticks, a.unit, u) as f64;
            let bt = lift_ticks(b.ticks, b.unit, u) as f64;
            Some(Value::F64(at / bt))
        }
        // span × integer = span (either operand order)
        (Value::Duration(a), Value::I64(n)) | (Value::I64(n), Value::Duration(a))
            if op == ArithOp::Mul =>
        {
            Some(Value::Duration(Duration::new(
                sat_i128(a.ticks as i128 * *n as i128),
                a.unit,
            )))
        }
        _ => None,
    }
}

fn eval_arith(left: &Expr, op: ArithOp, right: &Expr, chunk: &Chunk) -> Column {
    let lc = eval_column(left, chunk);
    let rc = eval_column(right, chunk);
    let n = chunk.len;
    // Typed temporal arithmetic (exact i64; #57) when either side is a datetime
    // or duration lane. Peek row 0: if the combination isn't a temporal op (e.g.
    // datetime+datetime), fall through to the numeric path unchanged.
    let temporal = matches!(lc.data(), ColumnData::DateTime(_) | ColumnData::Duration(_))
        || matches!(rc.data(), ColumnData::DateTime(_) | ColumnData::Duration(_));
    if temporal && (n == 0 || temporal_op(&lc.value_at(0), op, &rc.value_at(0)).is_some()) {
        let vals: Vec<Value> = (0..n)
            .map(|i| temporal_op(&lc.value_at(i), op, &rc.value_at(i)).unwrap_or(Value::Null))
            .collect();
        return column_from_values(vals);
    }
    // Null propagation (design 26 §26.2(c)): a result row is null iff either
    // operand is null there. All-valid operands keep the zero-cost path.
    let out_validity = if lc.has_nulls() || rc.has_nulls() {
        Validity::from_bits(
            &(0..n)
                .map(|i| !lc.is_null(i) && !rc.is_null(i))
                .collect::<Vec<_>>(),
        )
    } else {
        Validity::all_valid()
    };
    let (lf, li) = col_num_lane(lc);
    let (rf, ri) = col_num_lane(rc);
    // Integer lane only when both sides are integers and the op preserves it
    // (division always yields a float, matching pandas/SQL `/` semantics).
    let data = if li && ri && op != ArithOp::Div {
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
        ColumnData::I64(out)
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
        ColumnData::F64(out)
    };
    Column::new(data, out_validity)
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
        // An unbound `$x` hole should have been bound before execution; if one
        // reaches eval it yields Null (continue-first, never a panic).
        Expr::Hole(_) => Value::Null,
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
    // Typed temporal arithmetic (exact i64; #57), consistent with the columnar
    // `eval_arith` so the two stay byte-identical.
    if let Some(v) = temporal_op(&lv, op, &rv) {
        return v;
    }
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
            if let Some(ColumnData::Dec(d)) = chunk.column(name).map(|c| c.data()) {
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
        Expr::Field { name, .. } => match chunk.column(name)?.data() {
            ColumnData::Str(s) => Some(s.get(row)),
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
        Expr::Field { name, .. } => match chunk.column(name)?.data() {
            ColumnData::I64(v) => Some(v[row] as f64),
            ColumnData::F64(v) => Some(v[row]),
            ColumnData::Dec(d) => Some(d.unscaled[row] as f64 / 10f64.powi(d.scale as i32)),
            // Datetime is *not* read through this f64 lane: ns ticks exceed 2^53
            // and `tick as f64` would silently lose precision. A datetime field
            // routes to the owned-`Value` path → `dt_cmp` (exact i64). Design 23 / #53.
            ColumnData::DateTime(_) => None,
            // Duration likewise stays off the f64 lane (exact i64; #57).
            ColumnData::Duration(_) => None,
            // Date routes to the exact Value path too (epoch-day compared as a
            // date, not a coerced float); #58.
            ColumnData::Date(_) => None,
            // Time-of-day routes to the exact Value path too (#58).
            ColumnData::Time(_) => None,
            ColumnData::Bool(v) => Some(if v[row] { 1.0 } else { 0.0 }),
            ColumnData::Str(_) => None,
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
/// exact integer-tick lane rather than the lossy f64 view (design 23 / #53).
///
/// * two datetimes → exact cross-unit instant order;
/// * datetime vs a text literal → parse the literal into the same lane (the
///   datetime's unit, auto-inferring its format) and compare instants
///   (`|? ts >= "260601000000"`). A literal matching no known format is not a
///   valid instant, so only `!=` holds (continue-first; no fatal).
/// * datetime vs an **integer** literal → the integer is a raw tick count at the
///   column's unit; compared exactly in `i64` (no `tick as f64` rounding, so it
///   is correct even for nanosecond ticks past 2^53).
///
/// Returns `None` when neither operand is a datetime (normal path applies), or
/// for datetime-vs-float (a nonsensical mix; the f64 view handles it downstream).
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
        // Integer literal = raw ticks at the column's unit → exact i64 order.
        (Value::DateTime(a), Value::I64(n)) => Some(cmp_ord(a.ticks.partial_cmp(n), op)),
        (Value::I64(n), Value::DateTime(b)) => Some(cmp_ord(n.partial_cmp(&b.ticks), op)),
        // Duration comparisons mirror datetime: exact i128 cross-unit order;
        // a text literal parses at the span's unit; an integer is raw ticks. #57.
        (Value::Duration(a), Value::Duration(b)) => Some(cmp_ord(a.partial_cmp(b), op)),
        (Value::Duration(a), Value::Str(s)) => {
            Some(match rivus_core::Duration::parse_at(s, a.unit) {
                Some(b) => cmp_ord(a.partial_cmp(&b), op),
                None => op == CmpOp::Ne,
            })
        }
        (Value::Str(s), Value::Duration(b)) => {
            Some(match rivus_core::Duration::parse_at(s, b.unit) {
                Some(a) => cmp_ord(a.partial_cmp(b), op),
                None => op == CmpOp::Ne,
            })
        }
        (Value::Duration(a), Value::I64(n)) => Some(cmp_ord(a.ticks.partial_cmp(n), op)),
        (Value::I64(n), Value::Duration(b)) => Some(cmp_ord(n.partial_cmp(&b.ticks), op)),
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
mod dt_cmp_tests {
    use super::{dt_cmp, temporal_op};
    use rivus_core::{DateTime, Duration, TimeUnit, Value};
    use rivus_ir::{ArithOp, CmpOp};

    /// `DateTime − DateTime → Duration` is exact i64 even at nanosecond ticks
    /// past 2^53 (the #57 headline: resolves the #53 f64 caveat). Also covers
    /// the rest of the type algebra (span ± span, instant ± span, span × int,
    /// span ÷ span).
    #[test]
    fn temporal_arithmetic_is_exact_and_typed() {
        let base = 1_700_000_000_000_000_000_i64; // ns, ≫ 2^53
        assert!(
            base as f64 == (base + 1) as f64,
            "precondition: f64 loses 1ns"
        );
        let a = Value::DateTime(DateTime::new(base + 1, TimeUnit::Nano));
        let b = Value::DateTime(DateTime::new(base, TimeUnit::Nano));
        // instant − instant = 1 ns span, exact (f64 would give 0).
        assert_eq!(
            temporal_op(&a, ArithOp::Sub, &b),
            Some(Value::Duration(Duration::new(1, TimeUnit::Nano)))
        );
        // instant + span = instant.
        let span = Value::Duration(Duration::new(5, TimeUnit::Nano));
        assert_eq!(
            temporal_op(&b, ArithOp::Add, &span),
            Some(Value::DateTime(DateTime::new(base + 5, TimeUnit::Nano)))
        );
        // span + span = span; span × int = span.
        let s1 = Value::Duration(Duration::new(90, TimeUnit::Sec));
        let s2 = Value::Duration(Duration::new(30, TimeUnit::Sec));
        assert_eq!(
            temporal_op(&s1, ArithOp::Add, &s2),
            Some(Value::Duration(Duration::new(120, TimeUnit::Sec)))
        );
        assert_eq!(
            temporal_op(&s1, ArithOp::Mul, &Value::I64(3)),
            Some(Value::Duration(Duration::new(270, TimeUnit::Sec)))
        );
        // span ÷ span = dimensionless f64 ratio.
        assert_eq!(temporal_op(&s1, ArithOp::Div, &s2), Some(Value::F64(3.0)));
        // Non-temporal combos defer to the numeric path.
        assert_eq!(temporal_op(&a, ArithOp::Add, &b), None); // instant+instant
        assert_eq!(
            temporal_op(&Value::I64(1), ArithOp::Add, &Value::I64(2)),
            None
        );
    }

    /// Duration comparison is exact at ns past 2^53 (adversarial), and a text
    /// literal parses at the span's unit.
    #[test]
    fn duration_compare_is_exact() {
        let base = 1_700_000_000_000_000_000_i64;
        let x = Value::Duration(Duration::new(base, TimeUnit::Nano));
        let y = Value::Duration(Duration::new(base + 1, TimeUnit::Nano));
        assert_eq!(dt_cmp(&x, CmpOp::Lt, &y), Some(true));
        assert_eq!(dt_cmp(&x, CmpOp::Eq, &y), Some(false));
        // Text literal in human form parses to the same lane.
        let h = Value::Duration(Duration::new(90, TimeUnit::Sec));
        assert_eq!(
            dt_cmp(&h, CmpOp::Eq, &Value::Str("00:01:30".into())),
            Some(true)
        );
        assert_eq!(
            dt_cmp(&h, CmpOp::Lt, &Value::Str("00:02:00".into())),
            Some(true)
        );
    }

    /// Two nanosecond instants 1 ns apart, both past 2^53, where `tick as f64`
    /// collapses them — the adversarial case from #53. `dt_cmp` must order them
    /// exactly (i64), never via the f64 view.
    #[test]
    fn nanosecond_compare_is_exact_past_2_pow_53() {
        let base = 1_700_000_000_000_000_000_i64; // ≈ 2023 in ns, ≫ 2^53
        assert!(
            base as f64 == (base + 1) as f64,
            "precondition: f64 loses 1ns"
        );

        let a = Value::DateTime(DateTime::new(base, TimeUnit::Nano));
        let b = Value::DateTime(DateTime::new(base + 1, TimeUnit::Nano));
        // Strict order is resolved (f64 would call them equal).
        assert_eq!(dt_cmp(&a, CmpOp::Lt, &b), Some(true));
        assert_eq!(dt_cmp(&a, CmpOp::Ge, &b), Some(false));
        assert_eq!(dt_cmp(&a, CmpOp::Eq, &b), Some(false));
        assert_eq!(dt_cmp(&a, CmpOp::Ne, &b), Some(true));

        // Integer literal = raw ticks at the column's unit, compared in i64.
        assert_eq!(dt_cmp(&a, CmpOp::Eq, &Value::I64(base)), Some(true));
        assert_eq!(dt_cmp(&a, CmpOp::Ge, &Value::I64(base + 1)), Some(false));
        assert_eq!(dt_cmp(&b, CmpOp::Gt, &Value::I64(base)), Some(true));
    }

    /// Cross-unit comparison stays exact (1 s == 1000 ms), and an unparseable
    /// text literal is continue-first (only `!=` holds).
    #[test]
    fn cross_unit_and_bad_literal() {
        let s = Value::DateTime(DateTime::new(1, TimeUnit::Sec));
        let ms = Value::DateTime(DateTime::new(1_000, TimeUnit::Milli));
        assert_eq!(dt_cmp(&s, CmpOp::Eq, &ms), Some(true));

        let bad = Value::Str("not-a-date".into());
        assert_eq!(dt_cmp(&s, CmpOp::Ge, &bad), Some(false));
        assert_eq!(dt_cmp(&s, CmpOp::Ne, &bad), Some(true));
    }
}

#[cfg(test)]
mod match_tests {
    use super::{glob_match, like_match, substr_1based};

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
    fn substr_is_one_based() {
        // 1-based: substr(s,1) is the first char (SQL/DuckDB). #bugreport ③.
        assert_eq!(substr_1based("hello", 1.0, 3), "hel");
        assert_eq!(substr_1based("hello", 2.0, 3), "ell");
        assert_eq!(substr_1based("hello", 3.0, usize::MAX), "llo");
        // Lenient: start <= 1 clamps to the beginning (old 0-based call survives).
        assert_eq!(substr_1based("hello", 0.0, 3), "hel");
        assert_eq!(substr_1based("hello", -5.0, 3), "hel");
        // Past the end → empty; full string with no length.
        assert_eq!(substr_1based("hello", 99.0, 3), "");
        assert_eq!(substr_1based("hello", 1.0, usize::MAX), "hello");
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
