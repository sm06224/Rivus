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
    /// Full regular expression (unanchored partial match) — the lowering of the
    /// `~` infix and `'…'` regex literal (§29.5-6 s4) as well as the
    /// `regexp()`/`regex()`/`matches()` calls. The IR always knows it
    /// (parse/`to_source` are std-only); evaluating it needs the runtime's
    /// off-by-default `regex` feature — a feature-less build **refuses the plan
    /// before running** (`PlanGraph::uses_regexp`, never-silent) instead of
    /// evaluating every test to false.
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
    /// `bucket(ts, dur)` — bucket a datetime into arbitrary dur boundaries (closed-open).
    Bucket,
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
            "bucket" => Func::Bucket,
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
            Func::Bucket => "bucket",
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
    /// `source.field` — a field of the chunk's origin [`rivus_core::Resource`]
    /// (design §28.6 provenance), resolved against chunk metadata rather than a
    /// data column. The `field` (`uri`/`scheme`) is *not* baked into the variant,
    /// so one generic accessor covers every Resource field and is reused by the
    /// discovery `Resource` column (slice 3).
    Source,
}

impl Access {
    /// Does this access resolve against a **data column** (the chunk's schema)?
    /// `Source` resolves against chunk provenance metadata instead, so the
    /// column-keyed fast paths must skip it (they would otherwise mistake the
    /// Resource field name for a real column).
    pub fn is_column(self) -> bool {
        !matches!(self, Access::Source)
    }
}

/// A **path expression** key (§32.3): a root column plus a chain of nested
/// access segments. Generalizes the bare-column keys of group / sort / distinct
/// / join (today `Vec<String>`) so a key can reach into structured (Struct /
/// List) values — `user.age`, `tags[0]`, `user.address.city`.
///
/// **The degenerate form is pinned.** A depth-0 path (no segments) *is* a bare
/// column name and must stay byte-identical: it resolves on the existing flat
/// fast path (`Schema::index_of`) and round-trips through `to_source` as the
/// plain name `country` — never `country()` / `path(country)` (§32.3, ratified
/// #161 ②). [`PathExpr::fmt`] prints just the root when there are no segments,
/// and the parser yields a bare path for a plain name, so the round-trip holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathExpr {
    pub root: String,
    pub segs: Vec<PathSeg>,
}

/// One step of a [`PathExpr`] after the root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSeg {
    /// `.name` — a struct field.
    Field(String),
    /// `[i]` — a list index (0-based).
    Index(u32),
}

impl PathExpr {
    /// A bare-column (depth-0) path — the degenerate form that preserves every
    /// existing behaviour byte-for-byte.
    pub fn bare(name: impl Into<String>) -> Self {
        PathExpr {
            root: name.into(),
            segs: Vec::new(),
        }
    }

    /// Is this the degenerate (bare-column) form? Such a path resolves on the
    /// flat fast path and round-trips as a plain name.
    pub fn is_bare(&self) -> bool {
        self.segs.is_empty()
    }

    /// The bare column name when this is the degenerate form, else `None`.
    pub fn as_bare(&self) -> Option<&str> {
        self.segs.is_empty().then_some(self.root.as_str())
    }

    /// The flat-column name this path resolves against today (§32 s2). A bare
    /// path is its own name (`country`), preserving the existing fast path
    /// byte-for-byte; a nested path has no flat column yet (nested resolution is
    /// s4 / structured data is s3), so it stringifies to its surface spelling
    /// (`user.age`), which simply doesn't match a flat column and is surfaced as
    /// missing — never-silent — until the nested lanes land.
    pub fn column_name(&self) -> String {
        match self.as_bare() {
            Some(name) => name.to_string(),
            None => self.to_string(),
        }
    }

    /// Parse a path from its surface spelling `root ('.' field | '[' int ']')*`.
    /// A plain `name` (no segments) is the degenerate form. Returns `None` on a
    /// malformed path (empty root / field, unterminated `[`, non-numeric index)
    /// — never-silent: the caller surfaces the error.
    pub fn parse(s: &str) -> Option<PathExpr> {
        let bytes = s.as_bytes();
        // root = leading run up to the first '.' or '['.
        let mut i = 0;
        while i < bytes.len() && bytes[i] != b'.' && bytes[i] != b'[' {
            i += 1;
        }
        if i == 0 {
            return None;
        }
        let root = s[..i].to_string();
        let rest = &s[i..];
        let rb = rest.as_bytes();
        let mut segs = Vec::new();
        let mut j = 0;
        while j < rb.len() {
            match rb[j] {
                b'.' => {
                    j += 1;
                    let start = j;
                    while j < rb.len() && rb[j] != b'.' && rb[j] != b'[' {
                        j += 1;
                    }
                    if j == start {
                        return None; // empty field name
                    }
                    segs.push(PathSeg::Field(rest[start..j].to_string()));
                }
                b'[' => {
                    j += 1;
                    let start = j;
                    while j < rb.len() && rb[j] != b']' {
                        j += 1;
                    }
                    if j >= rb.len() {
                        return None; // unterminated '['
                    }
                    let idx: u32 = rest[start..j].parse().ok()?;
                    segs.push(PathSeg::Index(idx));
                    j += 1; // skip ']'
                }
                _ => return None,
            }
        }
        Some(PathExpr { root, segs })
    }
}

impl fmt::Display for PathExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Degenerate form prints as the bare name (the round-trip pin); deeper
        // paths append `.field` / `[i]`.
        f.write_str(&self.root)?;
        for seg in &self.segs {
            match seg {
                PathSeg::Field(n) => write!(f, ".{n}")?,
                PathSeg::Index(i) => write!(f, "[{i}]")?,
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum Expr {
    /// Reference to a field of the current object, with an access strategy.
    Field {
        name: String,
        access: Access,
    },
    /// `$_[i]` — a **positional** column reference (§29.5-6 s4): the `i`-th
    /// column of the current row, 0-based, in schema order. For headerless /
    /// positionally-defined data. Out-of-range → null + counted (continue-first,
    /// never silent). Name-keyed optimizer rules (projection pushdown) must
    /// treat an expression containing one conservatively: positions shift when
    /// columns are pruned, so pruning is skipped.
    FieldAt(u32),
    /// `base.name` — a **union sub-view** (§29.3, s2): a zero-copy char slice
    /// `[start, end)` of fixed-width string column `base`, named `name`. Defined
    /// by a `col :string(W) :{ name@start..end … }` block (carried on
    /// `Op::ProjectExpr.views`); referenced in expression context with the same
    /// `.` accessor as `source.uri`. `start`/`end` are **character** offsets
    /// (half-open); evaluation borrows the slice and never copies the substring.
    SubView {
        base: String,
        name: String,
        start: u32,
        end: u32,
    },
    /// A **nested path** into a Struct/List column (§32 s4): `user.age`,
    /// `tags[0]`, `user.address.city`. The degenerate (no-segment) form never
    /// reaches here — a bare column is `Expr::Field` — so this variant always
    /// carries at least one `.field` / `[i]` step. Resolution walks the
    /// `ColumnData::{Struct,List}` lanes at the root column; a missing field /
    /// out-of-range index / type mismatch is a **typed null + counted** failure
    /// (continue-first, never silent). A `.` against a §29 fixed-width view base
    /// is dispatched to [`Expr::SubView`] at parse time, so `.` here is always a
    /// struct-field step.
    Path(PathExpr),
    Literal(Value),
    /// A `$x` **value hole** (§25.3): a named placeholder for a value, filled by
    /// a binding (`| clean min=0`) or a scope parameter. It is bound at the
    /// IR/value level — never by text interpolation — so an external binding can
    /// only ever supply a *value*, never inject flow structure (injection-safe,
    /// prepared-statement style). An unbound hole that reaches evaluation yields
    /// null and is **surfaced once** on the error stream by the runtime
    /// (`PlanGraph::unbound_holes`) — never silently dropped (continue-first).
    Hole(String),
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

    /// Return a copy with every `$x` hole bound from `bindings` replaced by its
    /// value literal (§25.3 prepared binding). Holes absent from `bindings` are
    /// left as holes. Binding happens structurally on the IR — the value is
    /// placed as a `Literal`, never spliced as source text — so a binding can
    /// never inject flow structure (injection-safe).
    pub fn bind_holes(&self, bindings: &std::collections::HashMap<String, Value>) -> Expr {
        match self {
            Expr::Hole(name) => match bindings.get(name) {
                Some(v) => Expr::Literal(v.clone()),
                None => Expr::Hole(name.clone()),
            },
            Expr::Field { .. }
            | Expr::FieldAt(_)
            | Expr::SubView { .. }
            | Expr::Path(_)
            | Expr::Literal(_) => self.clone(),
            Expr::Compare { left, op, right } => Expr::Compare {
                left: Box::new(left.bind_holes(bindings)),
                op: *op,
                right: Box::new(right.bind_holes(bindings)),
            },
            Expr::And(a, b) => Expr::And(
                Box::new(a.bind_holes(bindings)),
                Box::new(b.bind_holes(bindings)),
            ),
            Expr::Or(a, b) => Expr::Or(
                Box::new(a.bind_holes(bindings)),
                Box::new(b.bind_holes(bindings)),
            ),
            Expr::Arith { left, op, right } => Expr::Arith {
                left: Box::new(left.bind_holes(bindings)),
                op: *op,
                right: Box::new(right.bind_holes(bindings)),
            },
            Expr::Cast { expr, ty } => Expr::Cast {
                expr: Box::new(expr.bind_holes(bindings)),
                ty: *ty,
            },
            Expr::Func { func, args } => Expr::Func {
                func: *func,
                args: args.iter().map(|a| a.bind_holes(bindings)).collect(),
            },
            Expr::Case { branches, default } => Expr::Case {
                branches: branches
                    .iter()
                    .map(|(c, v)| (c.bind_holes(bindings), v.bind_holes(bindings)))
                    .collect(),
                default: default.as_ref().map(|d| Box::new(d.bind_holes(bindings))),
            },
        }
    }

    /// Collect the names of every `$x` hole in this expression (de-duplicated by
    /// the caller if needed), in left-to-right order.
    pub fn collect_holes(&self, out: &mut Vec<String>) {
        match self {
            Expr::Hole(name) => out.push(name.clone()),
            Expr::Field { .. }
            | Expr::FieldAt(_)
            | Expr::SubView { .. }
            | Expr::Path(_)
            | Expr::Literal(_) => {}
            Expr::Compare { left, right, .. } | Expr::Arith { left, right, .. } => {
                left.collect_holes(out);
                right.collect_holes(out);
            }
            Expr::And(a, b) | Expr::Or(a, b) => {
                a.collect_holes(out);
                b.collect_holes(out);
            }
            Expr::Cast { expr, .. } => expr.collect_holes(out),
            Expr::Func { args, .. } => args.iter().for_each(|a| a.collect_holes(out)),
            Expr::Case { branches, default } => {
                for (c, v) in branches {
                    c.collect_holes(out);
                    v.collect_holes(out);
                }
                if let Some(d) = default {
                    d.collect_holes(out);
                }
            }
        }
    }

    /// Does `f` hold for this expression or any sub-expression? The one generic
    /// walker for whole-tree predicates (`uses_regexp`, the optimizer's
    /// positional-reference guard), so structural walks don't multiply and
    /// drift when a variant is added.
    pub fn any(&self, f: &impl Fn(&Expr) -> bool) -> bool {
        if f(self) {
            return true;
        }
        match self {
            Expr::Field { .. }
            | Expr::FieldAt(_)
            | Expr::SubView { .. }
            | Expr::Path(_)
            | Expr::Literal(_)
            | Expr::Hole(_) => false,
            Expr::Compare { left, right, .. } | Expr::Arith { left, right, .. } => {
                left.any(f) || right.any(f)
            }
            Expr::And(a, b) | Expr::Or(a, b) => a.any(f) || b.any(f),
            Expr::Cast { expr, .. } => expr.any(f),
            Expr::Func { args, .. } => args.iter().any(|a| a.any(f)),
            Expr::Case { branches, default } => {
                branches.iter().any(|(c, v)| c.any(f) || v.any(f))
                    || default.as_ref().is_some_and(|d| d.any(f))
            }
        }
    }

    /// Does this expression contain a regex test (`~` / `regexp()`), i.e. need
    /// the runtime's off-by-default `regex` feature to evaluate? (§29.5-6 s4:
    /// a feature-less build refuses such a plan before running — never-silent.)
    pub fn uses_regexp(&self) -> bool {
        self.any(&|e| {
            matches!(
                e,
                Expr::Func {
                    func: Func::Regexp,
                    ..
                }
            )
        })
    }

    /// Escape a string for a `"…"` source literal so `to_source` round-trips a
    /// value containing quotes, backslashes or newlines (mirrors the lexer's
    /// `\n \t \" \\` unescaping). Surfaced by `$x` bindings of arbitrary strings.
    pub fn escape_string(s: &str) -> String {
        let mut out = String::with_capacity(s.len() + 2);
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\t' => out.push_str("\\t"),
                other => out.push(other),
            }
        }
        out
    }

    /// Source representation of the field accessor, for reversibility.
    fn field_src(name: &str, access: Access) -> String {
        match access {
            Access::Fast => format!("$_.{name}"),
            Access::Deep => format!("$_..{name}"),
            Access::Dynamic => format!("item(\"{name}\")"),
            // Provenance accessor (§28.6): `source.uri` / `source.scheme`. The
            // field is generic, so this round-trips any Resource field name.
            Access::Source => format!("source.{name}"),
        }
    }

    /// Will this expression render as the bare `~` infix (`lhs ~ 'pat'`, §29.5-6
    /// s4)? Such a rendering is **not an atom** — a `~` literal binds at the
    /// comparison level and is not parenthesized, so an operator/cast/`~` parent
    /// must wrap it in parens or `to_source` re-parses differently (IR
    /// reversibility). A `Regexp` lhs is excluded so a nested test keeps the
    /// call form (`regexp(a ~ 'x', "y")`), whose arg re-parses cleanly.
    fn renders_as_infix(e: &Expr) -> bool {
        matches!(e, Expr::Func { func: Func::Regexp, args }
        if matches!(args.as_slice(), [lhs, Expr::Literal(Value::Str(p))]
            if !p.contains('\'')
                && !matches!(
                    lhs,
                    Expr::Compare { .. }
                        | Expr::And(..)
                        | Expr::Or(..)
                        | Expr::Func { func: Func::Regexp, .. }
                )))
    }

    /// `to_source` of `e`, parenthesized when it would otherwise render as the
    /// bare `~` infix (so the result is an atom for the calling context).
    fn paren_if_infix(e: &Expr) -> String {
        if Expr::renders_as_infix(e) {
            format!("({e})")
        } else {
            e.to_string()
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Field { name, access } => write!(f, "{}", Expr::field_src(name, *access)),
            // `$_[i]` — positional column reference (§29.5-6 s4).
            Expr::FieldAt(i) => write!(f, "$_[{i}]"),
            // `base.name` — union sub-view accessor (§29.3, s2), same `.` form as
            // the `source.uri` provenance accessor.
            Expr::SubView { base, name, .. } => write!(f, "{base}.{name}"),
            // `user.age` / `tags[0]` — nested path (§32 s4). `PathExpr::fmt`
            // prints the root then each `.field` / `[i]` step.
            Expr::Path(p) => write!(f, "{p}"),
            Expr::Literal(Value::Str(s)) => write!(f, "\"{}\"", Expr::escape_string(s)),
            // A resource literal round-trips its uri **only** — `size`/`mtime` are
            // out of the determinism contract (§00 0.14), so they are never emitted.
            Expr::Literal(Value::Resource(r)) => {
                write!(f, "resource(\"{}\")", Expr::escape_string(r.uri()))
            }
            Expr::Literal(v) => write!(f, "{v}"),
            Expr::Hole(name) => write!(f, "${name}"),
            // Operands are parenthesized when they would render as the bare `~`
            // infix, which is not an atom (else `(a ~ 'x') == b` re-parses wrong).
            Expr::Compare { left, op, right } => {
                write!(
                    f,
                    "{} {} {}",
                    Expr::paren_if_infix(left),
                    op.as_str(),
                    Expr::paren_if_infix(right)
                )
            }
            Expr::And(a, b) => write!(f, "{a} and {b}"),
            Expr::Or(a, b) => write!(f, "{a} or {b}"),
            // Always parenthesized so the source round-trips and re-parses with
            // the same structure regardless of precedence.
            Expr::Arith { left, op, right } => write!(
                f,
                "({} {} {})",
                Expr::paren_if_infix(left),
                op.as_str(),
                Expr::paren_if_infix(right)
            ),
            Expr::Cast { expr, ty } => write!(f, "{}:{ty}", Expr::paren_if_infix(expr)),
            Expr::Func { func, args } => {
                // §29.5-6 s4: the canonical spelling of a literal-pattern regex
                // test is the `~` infix with a raw `'…'` regex literal (the old
                // `regexp(col, "p")` call converges here, like s1's `:` chain).
                // A pattern containing `'` has no raw spelling, a computed pattern
                // has no infix form, and a nested regex / bare-printed predicate
                // (Compare/And/Or) lhs would not re-parse as the `~` lhs — all
                // keep the call form (see `renders_as_infix`).
                if Expr::renders_as_infix(self) {
                    if let [lhs, Expr::Literal(Value::Str(p))] = args.as_slice() {
                        return write!(f, "{lhs} ~ '{p}'");
                    }
                }
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

/// A word that names a column type in surface syntax — every alias the
/// parser's `decl_type`/`finish_type` accept (`int`/`i64`/`integer`, …) plus
/// the structured types (`decimal`, `datetime`, …). The `:` definition chain
/// (design §29.2) resolves `col :word` with this predicate: a type word after
/// `:` always means a cast, never a rename target. `to_source` consults it
/// too, so an alias that collides with a type word is rendered in the
/// parenthesized `(col) as alias` escape-hatch form instead of a `:alias`
/// that would re-parse as a cast. Must stay in sync with the parser's type
/// tables (locked by the parser's `every_type_word_casts_in_a_colon_chain`).
pub fn is_type_word(w: &str) -> bool {
    matches!(
        w.to_ascii_lowercase().as_str(),
        "int"
            | "i64"
            | "integer"
            | "float"
            | "f64"
            | "double"
            | "str"
            | "string"
            | "text"
            | "bool"
            | "boolean"
            | "resource"
            | "decimal"
            | "datetime"
            | "duration"
            | "date"
            | "time"
    )
}

#[cfg(test)]
mod tests {
    use super::{Expr, PathExpr, PathSeg};
    use rivus_core::{Resource, Value};

    // §32.3 #161 ②: the degenerate (bare) path round-trips as the plain name —
    // never `name()` / `path(name)` — so existing keys stay byte-identical.
    #[test]
    fn bare_path_round_trips_as_plain_name() {
        let p = PathExpr::bare("country");
        assert!(p.is_bare());
        assert_eq!(p.as_bare(), Some("country"));
        assert_eq!(p.to_string(), "country");
        assert_eq!(PathExpr::parse("country"), Some(p));
    }

    #[test]
    fn nested_paths_parse_and_round_trip() {
        for s in ["user.age", "tags[0]", "user.address.city", "a[3].b[12]"] {
            let p = PathExpr::parse(s).unwrap_or_else(|| panic!("parse {s}"));
            assert!(!p.is_bare(), "{s} is not bare");
            assert_eq!(p.to_string(), s, "round-trip {s}");
        }
        let p = PathExpr::parse("user.age").unwrap();
        assert_eq!(p.root, "user");
        assert_eq!(p.segs, vec![PathSeg::Field("age".into())]);
        let t = PathExpr::parse("tags[0]").unwrap();
        assert_eq!(t.segs, vec![PathSeg::Index(0)]);
    }

    #[test]
    fn malformed_paths_are_rejected() {
        // Empty root, empty field, unterminated index, non-numeric index.
        for s in [".x", "a.", "a[", "a[x]", ""] {
            assert!(PathExpr::parse(s).is_none(), "should reject {s:?}");
        }
    }

    #[test]
    fn resource_literal_to_source_is_uri_only() {
        // `to_source` emits the uri only: `size`/`mtime` are out of the
        // determinism contract (§00 0.14) and must never reach the source.
        let with_meta = Expr::Literal(Value::Resource(Resource::with_meta(
            "s3://b/k",
            Some(1024),
            Some(42),
        )));
        assert_eq!(with_meta.to_string(), "resource(\"s3://b/k\")");
        // A uri with a quote is escaped just like a string literal.
        let quoted = Expr::Literal(Value::Resource(Resource::new("file:///a\"b.csv")));
        assert_eq!(quoted.to_string(), "resource(\"file:///a\\\"b.csv\")");
    }
}
