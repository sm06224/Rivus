//! Row transforms: filter · validate · take · sort · distinct · describe · dropna/fill · rename/drop/cast/reorder · project.
//!
//! Split out of the former monolithic `operators.rs` (design 26 §26.8.1,
//! mechanical move-only split; logic unchanged).

use super::*;

// -------------------------------------------------------------------- filter

pub(crate) struct Filter {
    pub(crate) pred: Expr,
    /// Cast failures inside the predicate (BUG-D §23.6) — surfaced once on finish.
    pub(crate) cast_fails: u64,
}

/// Surface predicate cast failures once on finish (never-silent, BUG-D §23.6):
/// a `Str` cast inside a `|?` predicate that won't parse → null (so the row's
/// comparison is false), counted and reported with the predicate for context.
/// Per-worker partials sum to the serial total in parallel (cf. `parse_failures`).
fn surface_pred_cast_fails(n: u64, preds: &[&Expr], ctx: &mut OpCtx) {
    if n > 0 {
        let txt = preds
            .iter()
            .map(|p| p.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        ctx.raise(
            ErrorEvent::new(
                Severity::Recoverable,
                ErrorScope::Item,
                format!(
                    "{n} value(s) failed to evaluate in `|? {txt}` (unparseable cast or \
                     division by zero); set to null"
                ),
            )
            .at_node(ctx.label.clone()),
        );
    }
}

impl Operator for Filter {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        // Vectorized numeric path when possible; else the row-wise interpreter.
        let keep = match kernel::compile(&[&self.pred], &chunk) {
            Some(plan) => kernel::run(&plan, &chunk),
            None => {
                let mut f = 0u64;
                let keep: Vec<usize> = (0..chunk.len)
                    .filter(|&row| eval::eval_predicate_acc(&self.pred, &chunk, row, &mut f))
                    .collect();
                self.cast_fails += f;
                keep
            }
        };
        if keep.is_empty() {
            return Vec::new();
        }
        if keep.len() == chunk.len {
            return vec![chunk];
        }
        vec![chunk.gather(&keep)]
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        surface_pred_cast_fails(self.cast_fails, &[&self.pred], ctx);
        Vec::new()
    }
}

// ------------------------------------------------------------------ validate

/// `|! pred warn|reject|halt` — a row contract (#83 §24). A row where `pred` is
/// false is non-conforming and disposed of per `disposition`, **always** surfaced
/// on the error stream (never silent). `warn`/`reject` accumulate one summary
/// emitted on finish (so the count is chunk-size independent); `halt` raises a
/// fatal immediately. Row disposal is stateless (selection-vector gather, #40).
pub(crate) struct Validate {
    pub(crate) pred: Expr,
    pub(crate) disposition: Disposition,
    pub(crate) fails: u64,
    pub(crate) sample: Option<String>,
    /// Cast failures inside the predicate (BUG-D §23.6) — surfaced once on finish.
    pub(crate) cast_fails: u64,
}

impl Validate {
    fn passing(&self, chunk: &Chunk, cast_fails: &mut u64) -> Vec<usize> {
        match kernel::compile(&[&self.pred], chunk) {
            Some(plan) => kernel::run(&plan, chunk),
            None => (0..chunk.len)
                .filter(|&row| eval::eval_predicate_acc(&self.pred, chunk, row, cast_fails))
                .collect(),
        }
    }
    /// Compact render of one row (≤4 fields) for the error-stream sample.
    fn render_row(chunk: &Chunk, row: usize) -> String {
        let mut s = String::new();
        for (c, f) in chunk.schema.fields.iter().enumerate().take(4) {
            if c > 0 {
                s.push_str(", ");
            }
            s.push_str(&f.name);
            s.push('=');
            s.push_str(&chunk.value(row, c).to_string());
        }
        s
    }
}

impl Operator for Validate {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        let mut cf = 0u64;
        let keep = self.passing(&chunk, &mut cf);
        self.cast_fails += cf;
        let n_fail = chunk.len - keep.len();
        if n_fail == 0 {
            return vec![chunk];
        }
        if self.sample.is_none() {
            let kept: std::collections::HashSet<usize> = keep.iter().copied().collect();
            if let Some(row) = (0..chunk.len).find(|r| !kept.contains(r)) {
                self.sample = Some(Self::render_row(&chunk, row));
            }
        }
        self.fails += n_fail as u64;
        match self.disposition {
            // Strict: surface a fatal now (the engine halts on it) and pass the
            // conforming rows seen so far downstream.
            Disposition::Halt => {
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Fatal,
                        ErrorScope::Item,
                        format!(
                            "{} row(s) failed `|! {}` (halt); e.g. {}",
                            self.fails,
                            self.pred,
                            self.sample.as_deref().unwrap_or("")
                        ),
                    )
                    .at_node(ctx.label.clone()),
                );
                if keep.is_empty() {
                    Vec::new()
                } else {
                    vec![chunk.gather(&keep)]
                }
            }
            // Keep every row; the summary is surfaced on finish.
            Disposition::Warn => vec![chunk],
            // Drop the non-conforming rows; the summary is surfaced on finish.
            Disposition::Reject => {
                if keep.is_empty() {
                    Vec::new()
                } else if keep.len() == chunk.len {
                    vec![chunk]
                } else {
                    vec![chunk.gather(&keep)]
                }
            }
        }
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        // A cast inside the predicate that failed to parse (BUG-D §23.6) is
        // surfaced too — separately from the validation summary.
        surface_pred_cast_fails(self.cast_fails, &[&self.pred], ctx);
        // warn/reject surface one Recoverable summary on exhaustion (never silent;
        // halt already raised its fatal). The count is chunk-size independent.
        if self.fails > 0 && !matches!(self.disposition, Disposition::Halt) {
            let verb = if matches!(self.disposition, Disposition::Reject) {
                "dropped"
            } else {
                "kept"
            };
            ctx.raise(
                ErrorEvent::new(
                    Severity::Recoverable,
                    ErrorScope::Item,
                    format!(
                        "{} row(s) failed `|! {}` ({}); {verb}; e.g. {}",
                        self.fails,
                        self.pred,
                        self.disposition.as_str(),
                        self.sample.as_deref().unwrap_or("")
                    ),
                )
                .at_node(ctx.label.clone()),
            );
        }
        Vec::new()
    }
}

// ---------------------------------------------------------------------- take

/// `take N` — forward at most `N` rows total, then drop everything else.
/// Stateful: `remaining` is the global budget, so results are independent of
/// `chunk_size` (a chunk straddling the limit is truncated to fit).
pub(crate) struct Take {
    pub(crate) remaining: usize,
}

impl Operator for Take {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.remaining == 0 {
            return Vec::new();
        }
        if chunk.len <= self.remaining {
            self.remaining -= chunk.len;
            return vec![chunk];
        }
        // Chunk overruns the budget: keep just the first `remaining` rows.
        let idx: Vec<usize> = (0..self.remaining).collect();
        self.remaining = 0;
        vec![chunk.gather(&idx)]
    }

    /// A filled `take` passes nothing further — the saturation signal that
    /// stops an **unbounded** source upstream (§28.12; bounded sources never
    /// consult it, so the bounded serial loop is unchanged).
    fn saturated(&self) -> bool {
        self.remaining == 0
    }
}

// ---------------------------------------------------------------------- sort

/// `sort KEY [desc]` — a blocking sort. Buffers every chunk, then on finish
/// concatenates them (in arrival = source order), stably sorts by the key
/// column, and emits one ordered chunk. Stable + concatenate-then-sort makes
/// the output independent of `chunk_size`; ties keep source order for both
/// ascending and descending.
pub(crate) struct Sort {
    keys: Vec<(PathExpr, bool)>,
    buf: Vec<Chunk>,
    emitted: bool,
}

impl Sort {
    pub(crate) fn new(keys: Vec<(PathExpr, bool)>) -> Self {
        Sort {
            keys,
            buf: Vec::new(),
            emitted: false,
        }
    }
}

/// A resolved per-key row comparator (PERF-G): compares two row indices of one
/// already-bound column lane, `(a, b) -> Ordering`.
type RowCmp<'a> = Box<dyn Fn(usize, usize) -> std::cmp::Ordering + 'a>;

/// Build a **monotyped row comparator** for one sort key, resolving the column's
/// lane `match` and its null state **once** (PERF-G) instead of on every
/// comparison. The returned closure does only the typed compare (plus a null
/// branch when, and only when, the column actually has nulls), so the
/// `idx.sort_by` inner loop is branch-light and cache-coherent. Byte-identical to
/// the old per-compare `cmp_rows`: same lane order, NaN→Equal, **nulls last**
/// (§26.2b), and uri order for the resource lane (§28.3).
fn make_cmp(col: &Column) -> RowCmp<'_> {
    use std::cmp::Ordering;
    // Wrap a lane's element comparison with the null rule (hoisted: the all-valid
    // column gets a closure with no null branch at all).
    fn wrap<'a, T, F>(col: &'a Column, v: &'a T, cmp: F) -> RowCmp<'a>
    where
        T: ?Sized + 'a,
        F: Fn(&T, usize, usize) -> Ordering + 'a,
    {
        if col.has_nulls() {
            // Null model §26.2b: null sorts greatest → nulls last on ascending
            // (the caller reverses for descending). null == null → Equal (stable).
            Box::new(move |a, b| match (col.is_null(a), col.is_null(b)) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => cmp(v, a, b),
            })
        } else {
            Box::new(move |a, b| cmp(v, a, b))
        }
    }
    match col.data() {
        ColumnData::Bool(v) => wrap(col, v, |v: &Vec<bool>, a, b| v[a].cmp(&v[b])),
        ColumnData::I64(v) => wrap(col, v, |v: &Vec<i64>, a, b| v[a].cmp(&v[b])),
        ColumnData::F64(v) => wrap(col, v, |v: &Vec<f64>, a, b| {
            v[a].partial_cmp(&v[b]).unwrap_or(Ordering::Equal)
        }),
        // One column shares a scale, so the unscaled i128 order is the exact value
        // order — no precision loss in the sort key (design doc 21).
        ColumnData::Dec(d) => wrap(col, &d.unscaled, |v: &Vec<i128>, a, b| v[a].cmp(&v[b])),
        // Shared unit → integer tick order is exact chronological order.
        ColumnData::DateTime(d) => wrap(col, &d.ticks, |v: &Vec<i64>, a, b| v[a].cmp(&v[b])),
        ColumnData::Duration(d) => wrap(col, &d.ticks, |v: &Vec<i64>, a, b| v[a].cmp(&v[b])),
        ColumnData::Date(v) => wrap(col, v, |v: &Vec<i32>, a, b| v[a].cmp(&v[b])),
        ColumnData::Time(v) => wrap(col, v, |v: &Vec<i64>, a, b| v[a].cmp(&v[b])),
        // Resource sorts by uri (the in-contract identity; §00 0.14) — byte order,
        // matching discovery's deterministic uri ordering (§28.3).
        // The dict lane compares the same bytes the plain arm compares.
        ColumnData::StrDict(v) => wrap(col, v, |v: &rivus_core::DictColumn, a, b| {
            v.get(a).cmp(v.get(b))
        }),
        ColumnData::Str(v) | ColumnData::Resource(v) => {
            wrap(col, v, |v: &StrColumn, a, b| v.get(a).cmp(v.get(b)))
        }
        // §32 s3a: a nested lane has no native sort key — order deterministically
        // by its `Value` text form. No flow yields a nested sort key today.
        ColumnData::Struct(_) | ColumnData::List(_) => wrap(col, col, |c: &Column, a, b| {
            c.value_at(a).to_string().cmp(&c.value_at(b).to_string())
        }),
    }
}

/// Decorate-sort one key (PERF-G follow-up). Extract the key into a **contiguous
/// `(key, idx)` array** and sort that, so the hot loop reads dense, cache-local
/// key bytes instead of chasing random rows through the full column on every one
/// of the ~`n·log n` comparisons (the dominant cost the bare `make_cmp` hoist
/// couldn't reach). Monomorphic in `K` (one instantiation per lane), so there is
/// no dyn call either.
///
/// **Byte-identical** to the `make_cmp` comparator path: same `slice::sort_by`
/// (stable), the same comparator return values for the same key values, and the
/// same initial `0..n` order, so the algorithm makes identical decisions and
/// yields the identical permutation — including NaN→Equal, the null rule, and
/// `desc` (which reverses the whole comparison, not the stable tie-break, so ties
/// keep ascending source order for both directions).
fn argsort_one<K>(
    n: usize,
    key: impl Fn(usize) -> K,
    has_nulls: bool,
    is_null: impl Fn(usize) -> bool,
    desc: bool,
    cmp: impl Fn(&K, &K) -> std::cmp::Ordering,
) -> Vec<usize> {
    use std::cmp::Ordering;
    if has_nulls {
        // Carry the null flag in the pair so the sort never touches the validity
        // bitmap. Null rule §26.2b: null is greatest (nulls last on ascending),
        // null == null → Equal (stable tie); `desc` reverses the whole compare.
        let mut pairs: Vec<(bool, K, usize)> = (0..n).map(|i| (is_null(i), key(i), i)).collect();
        pairs.sort_by(|a, b| {
            let o = match (a.0, b.0) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => cmp(&a.1, &b.1),
            };
            if desc {
                o.reverse()
            } else {
                o
            }
        });
        pairs.into_iter().map(|p| p.2).collect()
    } else {
        let mut pairs: Vec<(K, usize)> = (0..n).map(|i| (key(i), i)).collect();
        pairs.sort_by(|a, b| {
            let o = cmp(&a.0, &b.0);
            if desc {
                o.reverse()
            } else {
                o
            }
        });
        pairs.into_iter().map(|p| p.1).collect()
    }
}

/// Dispatch the lane once (PERF-G follow-up), then decorate-sort the key into a
/// contiguous monotyped array. Lane order matches `make_cmp` exactly (Resource
/// sorts by uri, §28.3).
fn argsort_single(col: &Column, desc: bool) -> Vec<usize> {
    use std::cmp::Ordering;
    let n = col.len();
    let nulls = col.has_nulls();
    match col.data() {
        ColumnData::Bool(v) => argsort_one(n, |i| v[i], nulls, |i| col.is_null(i), desc, bool::cmp),
        ColumnData::I64(v) => argsort_one(n, |i| v[i], nulls, |i| col.is_null(i), desc, i64::cmp),
        ColumnData::F64(v) => argsort_one(
            n,
            |i| v[i],
            nulls,
            |i| col.is_null(i),
            desc,
            |a: &f64, b: &f64| a.partial_cmp(b).unwrap_or(Ordering::Equal),
        ),
        ColumnData::Dec(d) => argsort_one(
            n,
            |i| d.unscaled[i],
            nulls,
            |i| col.is_null(i),
            desc,
            i128::cmp,
        ),
        ColumnData::DateTime(d) => {
            argsort_one(n, |i| d.ticks[i], nulls, |i| col.is_null(i), desc, i64::cmp)
        }
        ColumnData::Duration(d) => {
            argsort_one(n, |i| d.ticks[i], nulls, |i| col.is_null(i), desc, i64::cmp)
        }
        ColumnData::Date(v) => argsort_one(n, |i| v[i], nulls, |i| col.is_null(i), desc, i32::cmp),
        ColumnData::Time(v) => argsort_one(n, |i| v[i], nulls, |i| col.is_null(i), desc, i64::cmp),
        ColumnData::Str(v) | ColumnData::Resource(v) => argsort_one(
            n,
            |i| v.get(i),
            nulls,
            |i| col.is_null(i),
            desc,
            |a: &&str, b: &&str| a.cmp(b),
        ),
        ColumnData::StrDict(d) => argsort_one(
            n,
            |i| d.get(i),
            nulls,
            |i| col.is_null(i),
            desc,
            |a: &&str, b: &&str| a.cmp(b),
        ),
        // §32 s3a: a nested lane sorts by its `Value` text form (matches
        // `make_cmp`). Not reachable today (no nested sort key in a flow).
        ColumnData::Struct(_) | ColumnData::List(_) => argsort_one(
            n,
            |i| col.value_at(i).to_string(),
            nulls,
            |i| col.is_null(i),
            desc,
            |a: &String, b: &String| a.cmp(b),
        ),
    }
}

impl Operator for Sort {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        if !chunk.is_empty() {
            self.buf.push(chunk);
        }
        Vec::new() // blocking boundary: output on finish
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.emitted || self.buf.is_empty() {
            return Vec::new();
        }
        self.emitted = true;

        // Concatenate buffered chunks into one set of columns (source order).
        let mut iter = std::mem::take(&mut self.buf).into_iter();
        let first = iter.next().unwrap();
        let schema = first.schema.clone();
        let mut cols = first.columns;
        for c in iter {
            for (i, col) in c.columns.iter().enumerate() {
                cols[i].append(col);
            }
        }
        let total = cols.first().map(|c| c.len()).unwrap_or(0);

        // Resolve each sort key to (column index, descending). §32 s4b: a bare
        // key uses the flat fast path; a nested key (`user.age`) is materialized
        // into a derived column appended to `work` (past the original columns).
        // An unknown bare key warns once and is skipped (continue-first); if none
        // resolve the stream is emitted in source order. The derived columns are
        // only used to sort — the output gathers the original columns.
        let base = cols.len();
        let mut work = Chunk::new(0, schema.clone(), cols);
        let paths: Vec<PathExpr> = self.keys.iter().map(|(k, _)| k.clone()).collect();
        let mut nested_fails = 0u64;
        let resolved = eval::resolve_key_indices(&mut work, &paths, &mut nested_fails);
        let mut key_cols: Vec<(usize, bool)> = Vec::with_capacity(self.keys.len());
        for ((k, desc), idx) in self.keys.iter().zip(&resolved) {
            match idx {
                Some(ki) => key_cols.push((*ki, *desc)),
                None => ctx.raise(
                    ErrorEvent::new(
                        Severity::Warn,
                        ErrorScope::Chunk,
                        format!("sort: unknown key '{}' (ignored)", k.column_name()),
                    )
                    .at_node(ctx.label.clone()),
                ),
            }
        }
        let cols = &work.columns;

        let idx: Vec<usize> = if key_cols.len() == 1 {
            // Single key — the common case, and every sort benchmark. Decorate:
            // extract the key into a contiguous (key, idx) array and sort that
            // (cache-local, monomorphic, no dyn call). Byte-identical to the
            // comparator path below (PERF-G follow-up).
            let (ki, desc) = key_cols[0];
            argsort_single(&cols[ki], desc)
        } else if key_cols.is_empty() {
            (0..total).collect()
        } else {
            // Multi-key: resolve each key's lane + null state once (PERF-G), then
            // compare via the monotyped closures in the hot loop. (A composite
            // decorated key would need a memcomparable encoding per lane — kept as
            // a further follow-up so byte-identity stays certain here.)
            let mut idx: Vec<usize> = (0..total).collect();
            let cmps: Vec<(RowCmp, bool)> = key_cols
                .iter()
                .map(|&(ki, desc)| (make_cmp(&cols[ki]), desc))
                .collect();
            idx.sort_by(|&a, &b| {
                for (cmp, desc) in &cmps {
                    let o = cmp(a, b);
                    let o = if *desc { o.reverse() } else { o };
                    if o != std::cmp::Ordering::Equal {
                        return o;
                    }
                }
                std::cmp::Ordering::Equal
            });
            idx
        };

        // Gather only the original columns (drop any derived nested-key columns).
        let sorted: Vec<Column> = work.columns[..base]
            .iter()
            .map(|c| c.gather(&idx))
            .collect();
        super::surface_key_path_fails(nested_fails, "sort", ctx);
        vec![Chunk::new(ctx.fresh_id(), schema, sorted)]
    }
}

// ------------------------------------------------------------------ distinct

/// Append `row`'s value in column `ci` to a **grouping/dedup key**, tagging null
/// distinctly (null model §26.2b): every `null` folds to the same key (so a
/// `null` group-by key keeps its rows in one "null group", and `distinct` folds
/// duplicate nulls), yet a `null` never collides with a real value — not even a
/// real empty string. Present cells are written `\x01<text>`, a null as `\x00`.
pub(crate) fn push_group_key_field(key: &mut String, chunk: &Chunk, ci: usize, row: usize) {
    if chunk.columns[ci].is_null(row) {
        key.push('\u{0}');
    } else {
        key.push('\u{1}');
        match chunk.columns[ci].data() {
            // Bare string key: borrow the column's `&str` (zero allocation;
            // identical bytes to `Value::Str(_).to_string()`).
            ColumnData::Str(s) => key.push_str(s.get(row)),
            _ => {
                use std::fmt::Write;
                let _ = write!(key, "{}", chunk.value(row, ci));
            }
        }
    }
}

/// `distinct [keys...]` — keep the first occurrence of each distinct key,
/// dropping later duplicates. Streaming (emits surviving rows per chunk) but
/// stateful: a global seen-set spans chunks, so it runs serially. Output order
/// is first-occurrence order, independent of `chunk_size`.
pub(crate) struct Distinct {
    keys: Vec<PathExpr>,
    seen: std::collections::HashSet<String>,
    /// Nested key-path structural misses (§32.8③), surfaced once on finish.
    key_fails: u64,
}

impl Distinct {
    pub(crate) fn new(keys: Vec<PathExpr>) -> Self {
        Distinct {
            keys,
            seen: std::collections::HashSet::new(),
            key_fails: 0,
        }
    }
}

impl Operator for Distinct {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        let mut chunk = chunk;
        let base = chunk.columns.len();
        // Columns that form the dedup key: every column, or the resolved keys.
        // A nested key (§32 s4b) is materialized into a derived column appended
        // past `base`; an unknown *bare* key is skipped (continue-first), as
        // before. The derived columns are dropped before the chunk is emitted.
        let idxs: Vec<usize> = if self.keys.is_empty() {
            (0..base).collect()
        } else {
            let mut nested_fails = 0u64;
            let r = eval::resolve_key_indices(&mut chunk, &self.keys, &mut nested_fails)
                .into_iter()
                .flatten()
                .collect();
            self.key_fails += nested_fails;
            r
        };

        let mut keep = Vec::new();
        let mut key = String::new();
        for row in 0..chunk.len {
            key.clear();
            for (j, &ci) in idxs.iter().enumerate() {
                if j > 0 {
                    key.push('\u{1f}'); // unit separator: unlikely in data
                }
                push_group_key_field(&mut key, &chunk, ci, row);
            }
            if self.seen.insert(key.clone()) {
                keep.push(row);
            }
        }

        // Drop any derived nested-key columns so the output keeps only the
        // original columns (the schema never gained the derived ones).
        chunk.columns.truncate(base);
        if keep.is_empty() {
            return Vec::new();
        }
        if keep.len() == chunk.len {
            return vec![chunk];
        }
        vec![chunk.gather(&keep)]
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        super::surface_key_path_fails(self.key_fails, "distinct", ctx);
        Vec::new()
    }
}

// ------------------------------------------------------------------ describe

/// `describe` — a one-pass streaming summary: per input column, its type, row
/// count, and (for numeric columns) min / max / mean. Accumulates across chunks
/// and emits a single summary chunk on finish (one row per column). Stateful →
/// serial path. The summary is rendered as string cells for clean display.
#[derive(Default)]
pub(crate) struct Describe {
    names: Vec<String>,
    types: Vec<DataType>,
    count: u64,
    // Per-column numeric accumulators (used only for I64/F64 columns).
    n: Vec<u64>,
    sum: Vec<f64>,
    min: Vec<f64>,
    max: Vec<f64>,
    inited: bool,
    emitted: bool,
}

impl Describe {
    fn init(&mut self, chunk: &Chunk) {
        self.names = chunk
            .schema
            .field_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        self.types = chunk.columns.iter().map(|c| c.dtype()).collect();
        let k = self.names.len();
        self.n = vec![0; k];
        self.sum = vec![0.0; k];
        self.min = vec![f64::INFINITY; k];
        self.max = vec![f64::NEG_INFINITY; k];
        self.inited = true;
    }
}

impl Operator for Describe {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        if !self.inited {
            self.init(&chunk);
        }
        self.count += chunk.len as u64;
        for (ci, col) in chunk.columns.iter().enumerate() {
            let vals: &mut dyn Iterator<Item = f64> = match col.data() {
                ColumnData::I64(v) => &mut v.iter().map(|&x| x as f64),
                ColumnData::F64(v) => &mut v.iter().copied(),
                _ => continue, // non-numeric: only type + count are reported
            };
            for x in vals {
                self.n[ci] += 1;
                self.sum[ci] += x;
                self.min[ci] = self.min[ci].min(x);
                self.max[ci] = self.max[ci].max(x);
            }
        }
        Vec::new() // summary emitted on finish
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.emitted || !self.inited {
            return Vec::new();
        }
        self.emitted = true;

        let fmt = |x: f64| {
            if x.fract() == 0.0 && x.abs() < 1e15 {
                format!("{x:.0}")
            } else {
                format!("{x}")
            }
        };
        let mut column = StrColumn::default();
        let mut typ = StrColumn::default();
        let mut count = Vec::new();
        let mut min = StrColumn::default();
        let mut max = StrColumn::default();
        let mut mean = StrColumn::default();
        for (i, name) in self.names.iter().enumerate() {
            column.push(name);
            typ.push(&self.types[i].to_string());
            count.push(self.count as i64);
            if self.n[i] > 0 {
                min.push(&fmt(self.min[i]));
                max.push(&fmt(self.max[i]));
                mean.push(&fmt(self.sum[i] / self.n[i] as f64));
            } else {
                min.push("");
                max.push("");
                mean.push("");
            }
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("column", DataType::Str),
            Field::new("type", DataType::Str),
            Field::new("count", DataType::I64),
            Field::new("min", DataType::Str),
            Field::new("max", DataType::Str),
            Field::new("mean", DataType::Str),
        ]));
        let columns = vec![
            Column::str(column),
            Column::str(typ),
            Column::i64(count),
            Column::str(min),
            Column::str(max),
            Column::str(mean),
        ];
        vec![Chunk::new(ctx.fresh_id(), schema, columns)]
    }
}

// ------------------------------------------------------------ dropna / fill

/// `dropna [cols]` — drop rows that are **null** (missing) in any target column;
/// with no columns, in any column. Streaming and stateless. Null-aware (design
/// 26 §26.9): it drops a `null`, **not** a real empty string `""` or a real `0`
/// — those are present values. (Before the null model a blank numeric parsed to
/// `0` and dropna was blind to it; that is BUG-A, now fixed.)
pub(crate) struct DropNa {
    pub(crate) cols: Vec<String>,
    /// Rows dropped for a null (#204) — surfaced once on finish, like
    /// `validate … reject` reports its count: silently vanishing rows are the
    /// same never-silent debt whether a contract or a null dropped them.
    pub(crate) dropped: u64,
}

impl Operator for DropNa {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        let idxs: Vec<usize> = if self.cols.is_empty() {
            (0..chunk.columns.len()).collect()
        } else {
            self.cols
                .iter()
                .filter_map(|c| chunk.schema.index_of(c))
                .collect()
        };
        let keep: Vec<usize> = (0..chunk.len)
            .filter(|&r| !idxs.iter().any(|&ci| chunk.columns[ci].is_null(r)))
            .collect();
        self.dropped += (chunk.len - keep.len()) as u64;
        if keep.is_empty() {
            return Vec::new();
        }
        if keep.len() == chunk.len {
            return vec![chunk];
        }
        vec![chunk.gather(&keep)]
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.dropped > 0 {
            let cols = if self.cols.is_empty() {
                "any column".to_string()
            } else {
                format!("{:?}", self.cols)
            };
            ctx.raise(
                ErrorEvent::new(
                    Severity::Recoverable,
                    ErrorScope::Chunk,
                    format!(
                        "dropna: dropped {} row(s) with null(s) in {cols}",
                        self.dropped
                    ),
                )
                .at_node(ctx.label.clone()),
            );
        }
        Vec::new()
    }
}

/// `explode COL` / `unnest COL` (§32 s4c) — multiply rows over a `List` column.
/// Stateless per chunk: each input row yields one output row per list element,
/// with the other columns repeated and `COL` replaced by the element (lane =
/// the list's element type). An empty or null list contributes **zero** rows
/// (Arrow `UNNEST` / SQL); expansion order is the list's physical order, so the
/// result is byte-identical across serial / parallel / chunk-size. A non-list
/// (or unknown) column is a never-silent warning + pass-through (continue-first).
pub(crate) struct Explode {
    pub(crate) col: String,
}

impl Operator for Explode {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        let Some(ci) = chunk.schema.index_of(&self.col) else {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Warn,
                    ErrorScope::Chunk,
                    format!("explode: unknown column '{}' (passed through)", self.col),
                )
                .at_node(ctx.label.clone()),
            );
            return vec![chunk];
        };
        let ColumnData::List(list) = chunk.columns[ci].data() else {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Warn,
                    ErrorScope::Chunk,
                    format!(
                        "explode: column '{}' is not a list lane (passed through)",
                        self.col
                    ),
                )
                .at_node(ctx.label.clone()),
            );
            return vec![chunk];
        };
        // Parent-row map + element-index map, in the list's physical order. A
        // null list (validity 0) contributes nothing — its offsets span is empty.
        let mut row_map: Vec<usize> = Vec::new();
        let mut elem_idx: Vec<usize> = Vec::new();
        for r in 0..chunk.len {
            if chunk.columns[ci].is_null(r) {
                continue; // null list → zero rows
            }
            let (a, b) = (list.offsets[r] as usize, list.offsets[r + 1] as usize);
            for e in a..b {
                row_map.push(r);
                elem_idx.push(e);
            }
        }
        // The exploded column is the list's child, selected (and reordered to)
        // the element map — already the element lane.
        let exploded = Column::new(
            list.child.data().gather(&elem_idx),
            list.child.validity().gather(&elem_idx),
        );
        // Output field for the exploded column: the element field, renamed to the
        // column (carrying its nested detail); fall back to the child's lane.
        let exploded_field = match &chunk.schema.fields[ci].nested {
            Some(Nested::List(elem)) => {
                let mut e = (**elem).clone();
                e.name = self.col.clone();
                e
            }
            _ => Field::new(self.col.clone(), exploded.data().dtype()),
        };

        let fields: Vec<Field> = chunk
            .schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if i == ci {
                    exploded_field.clone()
                } else {
                    f.clone()
                }
            })
            .collect();
        let cols: Vec<Column> = chunk
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if i == ci {
                    exploded.clone()
                } else {
                    c.gather(&row_map)
                }
            })
            .collect();

        if row_map.is_empty() {
            return Vec::new(); // every list empty/null → no output rows
        }
        vec![Chunk::new(
            ctx.fresh_id(),
            Arc::new(Schema::new(fields)),
            cols,
        )]
    }
}

/// `fill col VALUE` — replace **null** (missing) cells of a column with `VALUE`,
/// on **any** lane (null model §26; numeric blanks are now null, no longer `0`,
/// so `fill price 0` works). Streaming, stateless. `VALUE` is coerced to the
/// column's lane; a real `0`/`""` is a present value and is left untouched. A
/// column with no nulls passes through unchanged (zero cost).
pub(crate) struct Fill {
    pub(crate) col: String,
    pub(crate) value: String,
}

/// Coerce the `fill` literal text into the column's lane (`Value::Null` if it
/// can't be represented there, leaving such rows null).
fn parse_fill(value: &str, dtype: DataType) -> Value {
    let t = value.trim();
    match dtype {
        DataType::Str => Value::Str(value.to_string()),
        // A resource fill literal is taken as the uri.
        DataType::Resource => Value::Resource(rivus_core::Resource::new(value.to_string())),
        DataType::I64 => t.parse::<i64>().map(Value::I64).unwrap_or(Value::Null),
        DataType::F64 => t.parse::<f64>().map(Value::F64).unwrap_or(Value::Null),
        DataType::Bool => match t {
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            _ => Value::Null,
        },
        DataType::Decimal { scale } => {
            rivus_core::Decimal::parse_scaled(t, scale).map_or(Value::Null, Value::Dec)
        }
        DataType::Date => rivus_core::Date::parse(t).map_or(Value::Null, Value::Date),
        DataType::Time => rivus_core::TimeOfDay::parse_at(t, rivus_core::TimeUnit::Sec)
            .map_or(Value::Null, Value::Time),
        // Datetime/duration need a format/unit to parse a literal; not supported
        // as a `fill` constant yet (those rows stay null). Tracked for a follow-up.
        DataType::DateTime { .. } | DataType::Duration { .. } | DataType::Null => Value::Null,
        // §32 s3a: no surface literal for a nested lane → those rows stay null.
        DataType::Struct | DataType::List => Value::Null,
    }
}

impl Operator for Fill {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        let Some(ci) = chunk.schema.index_of(&self.col) else {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Warn,
                    ErrorScope::Chunk,
                    format!("fill: unknown column '{}'", self.col),
                )
                .at_node(ctx.label.clone()),
            );
            return vec![chunk];
        };
        // No nulls → nothing to fill (zero-cost passthrough for all-valid data).
        if !chunk.columns[ci].has_nulls() {
            return vec![chunk];
        }
        let fill_val = parse_fill(&self.value, chunk.columns[ci].dtype());
        let col = &chunk.columns[ci];
        let vals: Vec<Value> = (0..chunk.len)
            .map(|r| {
                if col.is_null(r) {
                    fill_val.clone()
                } else {
                    col.value_at(r)
                }
            })
            .collect();
        let mut columns = chunk.columns.clone();
        columns[ci] = eval::column_from_values(vals);
        let mut out = Chunk::new(chunk.meta.id, chunk.schema.clone(), columns);
        out.meta = chunk.meta.clone();
        vec![out]
    }
}

/// Replace a text column's blank cells with the nearest non-empty value:
/// `ffill` carries the last seen value forward, `bfill` the next value back.
///
/// `ffill` is streaming — it carries one value across chunks and rewrites each
/// chunk in flight. `bfill` needs the *next* value, which may live in a later
/// chunk, so it buffers the stream and emits on `finish` (a pipeline-breaker
/// like `sort`). Both rewrite only a `Str` column; a numeric column is passed
/// through unchanged (its blanks already became `0` at parse time). Leading
/// blanks for `ffill` (and trailing blanks for `bfill`) have no neighbor to
/// borrow and stay empty.
pub(crate) struct FillDirectional {
    col: String,
    forward: bool,
    /// `ffill` state: the last non-empty value seen so far (carried across
    /// chunks). Unused for `bfill`.
    carry: Option<String>,
    /// `bfill` buffer: every chunk, replayed in a single backward pass on finish.
    buf: Vec<Chunk>,
    warned: bool,
}

impl FillDirectional {
    pub(crate) fn ffill(col: String) -> Self {
        FillDirectional {
            col,
            forward: true,
            carry: None,
            buf: Vec::new(),
            warned: false,
        }
    }
    pub(crate) fn bfill(col: String) -> Self {
        FillDirectional {
            col,
            forward: false,
            carry: None,
            buf: Vec::new(),
            warned: false,
        }
    }

    /// Warn once if the column is unknown or non-text; returns the column index
    /// when it's a fillable `Str` column.
    fn target(&mut self, chunk: &Chunk, ctx: &mut OpCtx) -> Option<usize> {
        let Some(ci) = chunk.schema.index_of(&self.col) else {
            if !self.warned {
                self.warned = true;
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Warn,
                        ErrorScope::Chunk,
                        format!("fill: unknown column '{}'", self.col),
                    )
                    .at_node(ctx.label.clone()),
                );
            }
            return None;
        };
        matches!(
            chunk.columns[ci].data(),
            ColumnData::Str(_) | ColumnData::StrDict(_)
        )
        .then_some(ci)
    }
}

impl Operator for FillDirectional {
    fn process(&mut self, _from: NodeId, mut chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        if !self.forward {
            // bfill: buffer; the next non-empty value may be in a later chunk.
            self.buf.push(chunk);
            return Vec::new();
        }
        let Some(ci) = self.target(&chunk, ctx) else {
            return vec![chunk];
        };
        // A dict lane decodes in place first (design/42): the fill REWRITES
        // cells, so the Str-shaped logic below must see the plain lane —
        // behavior identical by construction, representation dropped.
        chunk.columns[ci].undict();
        let ColumnData::Str(s) = chunk.columns[ci].data() else {
            return vec![chunk];
        };
        let mut filled = StrColumn::with_capacity(chunk.len, 0);
        for r in 0..chunk.len {
            let v = s.get(r);
            if v.is_empty() {
                match &self.carry {
                    Some(c) => filled.push(c),
                    None => filled.push(""),
                }
            } else {
                filled.push(v);
                self.carry = Some(v.to_string());
            }
        }
        let mut columns = chunk.columns.clone();
        columns[ci] = Column::str(filled);
        let mut out = Chunk::new(chunk.meta.id, chunk.schema.clone(), columns);
        out.meta = chunk.meta.clone();
        vec![out]
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.forward || self.buf.is_empty() {
            return Vec::new();
        }
        let chunks = std::mem::take(&mut self.buf);
        // Resolve the column once against the first chunk (schema is stable).
        let ci = match self.target(&chunks[0], ctx) {
            Some(ci) => ci,
            None => return chunks, // unknown or non-text → pass through unchanged
        };
        // One backward pass across all rows, carrying the next non-empty value.
        let mut next: Option<String> = None;
        let mut out = chunks;
        for chunk in out.iter_mut().rev() {
            chunk.columns[ci].undict();
            let ColumnData::Str(s) = chunk.columns[ci].data() else {
                continue;
            };
            let mut vals: Vec<String> = (0..chunk.len).map(|r| s.get(r).to_string()).collect();
            for v in vals.iter_mut().rev() {
                if v.is_empty() {
                    if let Some(n) = &next {
                        *v = n.clone();
                    }
                } else {
                    next = Some(v.clone());
                }
            }
            let mut filled = StrColumn::with_capacity(chunk.len, 0);
            for v in &vals {
                filled.push(v);
            }
            chunk.columns[ci] = Column::str(filled);
        }
        out
    }
}

/// `fill col mean|median` — replace blank cells of a text column with a
/// whole-column statistic of its non-empty **numeric** cells. Buffers the entire
/// stream (a pipeline-breaker like `sort`): the statistic needs every value, so
/// it can only be known on `finish`. Works on a `Str` column (declare `:str` so
/// blanks survive parsing); a numeric column has no blank cells (they became `0`
/// at parse time) and is passed through unchanged. Cells that don't parse as a
/// number are ignored when computing the statistic but kept as-is in the output.
pub(crate) struct FillStat {
    col: String,
    median: bool,
    buf: Vec<Chunk>,
    warned: bool,
}

impl FillStat {
    pub(crate) fn new(col: String, median: bool) -> Self {
        FillStat {
            col,
            median,
            buf: Vec::new(),
            warned: false,
        }
    }

    /// Linear-interpolated median (p50) of a sorted-in-place value set; mirrors
    /// the percentile aggregate so `fill median` and `|# median:` agree.
    fn median_of(mut v: Vec<f64>) -> f64 {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        if v.is_empty() {
            return 0.0;
        }
        if v.len() == 1 {
            return v[0];
        }
        let rank = 0.5 * (v.len() - 1) as f64;
        let lo = rank.floor() as usize;
        let hi = rank.ceil() as usize;
        let frac = rank - lo as f64;
        v[lo] + (v[hi] - v[lo]) * frac
    }

    /// Format the fill value without a trailing `.0` when it is integral, so an
    /// integer-looking column stays integer-looking after the fill.
    fn format_stat(x: f64) -> String {
        if x.fract() == 0.0 && x.abs() < 1e15 {
            format!("{}", x as i64)
        } else {
            format!("{x}")
        }
    }

    /// Fill the null cells of a NUMERIC column with the mean/median of its
    /// valid cells (#204). Lane rules: an F64 column fills in place; a Decimal
    /// column fills at its scale (half-even, like the reader); an I64 column
    /// fills in place when the statistic is integral, else the whole column
    /// widens to F64 (pandas `fillna(mean)` semantics — a fractional mean can't
    /// live in an integer lane). Deterministic and chunk-size independent (the
    /// statistic is computed over ALL buffered chunks before any fill).
    fn fill_numeric(&mut self, mut chunks: Vec<Chunk>, ci: usize, ctx: &mut OpCtx) -> Vec<Chunk> {
        let mut nums: Vec<f64> = Vec::new();
        let mut count = 0f64;
        let mut sum = 0f64;
        let mut holes = false;
        for c in &chunks {
            let col = &c.columns[ci];
            for r in 0..c.len {
                if col.is_null(r) {
                    holes = true;
                    continue;
                }
                if let Some(x) = col.value_at(r).as_f64() {
                    sum += x;
                    count += 1.0;
                    if self.median {
                        nums.push(x);
                    }
                }
            }
        }
        if !holes {
            return chunks; // nothing to fill — zero-cost pass-through
        }
        if count == 0.0 {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Warn,
                    ErrorScope::Chunk,
                    format!(
                        "fill {}: no numeric values to compute {}",
                        self.col,
                        if self.median { "median" } else { "mean" }
                    ),
                )
                .at_node(ctx.label.clone()),
            );
            return chunks;
        }
        let stat = if self.median {
            Self::median_of(nums)
        } else {
            sum / count
        };
        let int_ok = stat.fract() == 0.0 && stat.abs() < 9.2e18;
        for c in chunks.iter_mut() {
            let n = c.len;
            let col = &c.columns[ci];
            let new_col = match col.data() {
                ColumnData::F64(v) => {
                    let out: Vec<f64> = (0..n)
                        .map(|r| if col.is_null(r) { stat } else { v[r] })
                        .collect();
                    Column::f64(out)
                }
                ColumnData::Dec(d) => {
                    let filled = eval::f64_to_decimal_pub(stat, d.scale);
                    let unscaled: Vec<i128> = (0..n)
                        .map(|r| {
                            if col.is_null(r) {
                                filled.unscaled
                            } else {
                                d.unscaled[r]
                            }
                        })
                        .collect();
                    Column::dec(rivus_core::DecColumn {
                        unscaled,
                        scale: d.scale,
                    })
                }
                ColumnData::I64(v) if int_ok => {
                    let s = stat as i64;
                    let out: Vec<i64> = (0..n)
                        .map(|r| if col.is_null(r) { s } else { v[r] })
                        .collect();
                    Column::i64(out)
                }
                // Fractional statistic into an integer lane: widen to F64.
                ColumnData::I64(v) => {
                    let out: Vec<f64> = (0..n)
                        .map(|r| if col.is_null(r) { stat } else { v[r] as f64 })
                        .collect();
                    Column::f64(out)
                }
                _ => unreachable!("guarded by the lane match in finish"),
            };
            let dtype = new_col.data().dtype();
            c.columns[ci] = new_col;
            let mut fields = c.schema.fields.clone();
            fields[ci] = Field::new(self.col.clone(), dtype);
            c.schema = Arc::new(Schema::new(fields));
        }
        chunks
    }
}

impl Operator for FillStat {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        self.buf.push(chunk);
        Vec::new() // blocking: needs the whole column to know the statistic
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.buf.is_empty() {
            return Vec::new();
        }
        let mut chunks = std::mem::take(&mut self.buf);
        let Some(ci) = chunks[0].schema.index_of(&self.col) else {
            if !self.warned {
                self.warned = true;
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Warn,
                        ErrorScope::Chunk,
                        format!("fill: unknown column '{}'", self.col),
                    )
                    .at_node(ctx.label.clone()),
                );
            }
            return chunks;
        };
        // Numeric lane (#204): with the null model a blank numeric cell is
        // NULL (not the pre-null-model 0), so a numeric column CAN carry holes
        // — compute the statistic over the valid cells and fill the null ones.
        // (The old pass-through assumed blanks had parsed to 0; that premise
        // died with BUG-A, which made `fill … mean` a silent no-op here.)
        match chunks[0].columns[ci].data() {
            ColumnData::Str(_) | ColumnData::StrDict(_) => {}
            ColumnData::I64(_) | ColumnData::F64(_) | ColumnData::Dec(_) => {
                return self.fill_numeric(chunks, ci, ctx);
            }
            _ => {
                if !self.warned {
                    self.warned = true;
                    ctx.raise(
                        ErrorEvent::new(
                            Severity::Warn,
                            ErrorScope::Chunk,
                            format!(
                                "fill {} {}: unsupported column lane (numeric or string \
                                 expected); rows left as-is",
                                self.col,
                                if self.median { "median" } else { "mean" }
                            ),
                        )
                        .at_node(ctx.label.clone()),
                    );
                }
                return chunks;
            }
        }

        // Pass 1: collect every non-empty cell that parses as a number.
        let mut nums: Vec<f64> = Vec::new();
        let mut count = 0f64;
        let mut sum = 0f64;
        for c in &chunks {
            if let ColumnData::StrDict(d) = c.columns[ci].data() {
                // Read-only pass: peek through the dict without rewriting.
                for r in 0..c.len {
                    let cell = d.get(r).trim();
                    if cell.is_empty() {
                        continue;
                    }
                    if let Ok(v) = cell.parse::<f64>() {
                        count += 1.0;
                        sum += v;
                    }
                }
                continue;
            }
            if let ColumnData::Str(s) = c.columns[ci].data() {
                for r in 0..c.len {
                    let cell = s.get(r).trim();
                    if cell.is_empty() {
                        continue;
                    }
                    if let Ok(x) = cell.parse::<f64>() {
                        sum += x;
                        count += 1.0;
                        if self.median {
                            nums.push(x);
                        }
                    }
                }
            }
        }
        // No numeric cell to learn from → leave blanks as-is (warn once).
        if count == 0.0 {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Warn,
                    ErrorScope::Chunk,
                    format!(
                        "fill {}: no numeric values to compute {}",
                        self.col,
                        if self.median { "median" } else { "mean" }
                    ),
                )
                .at_node(ctx.label.clone()),
            );
            return chunks;
        }
        let stat = if self.median {
            Self::median_of(nums)
        } else {
            sum / count
        };
        let fill = Self::format_stat(stat);

        // Pass 2: rewrite blank cells with the formatted statistic.
        for c in chunks.iter_mut() {
            c.columns[ci].undict();
            let ColumnData::Str(s) = c.columns[ci].data() else {
                continue;
            };
            let mut filled = StrColumn::with_capacity(c.len, 0);
            for r in 0..c.len {
                let v = s.get(r);
                filled.push(if v.trim().is_empty() { &fill } else { v });
            }
            c.columns[ci] = Column::str(filled);
        }
        chunks
    }
}

/// `rename OLD NEW [OLD NEW ...]` — rename columns in place. Position, type and
/// values are untouched; only the field name changes. Unknown `OLD` names raise
/// a one-line warning and are skipped. Stateless and streaming.
pub(crate) struct Rename {
    pub(crate) pairs: Vec<(String, String)>,
}

impl Operator for Rename {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        let mut fields = chunk.schema.fields.clone();
        for (from, to) in &self.pairs {
            match chunk.schema.index_of(from) {
                Some(i) => fields[i] = Field::new(to.clone(), fields[i].dtype),
                None => ctx.raise(
                    ErrorEvent::new(
                        Severity::Warn,
                        ErrorScope::Chunk,
                        format!("rename: unknown column '{from}'"),
                    )
                    .at_node(ctx.label.clone())
                    .at_chunk(chunk.meta.id),
                ),
            }
        }
        let schema = Arc::new(Schema::new(fields));
        let mut out = Chunk::new(chunk.meta.id, schema, chunk.columns.clone());
        out.meta = chunk.meta.clone();
        vec![out]
    }
}

/// `drop COL [COL ...]` — remove the named columns, keeping the rest in order.
/// Unknown names are ignored (dropping a non-existent column is a no-op).
/// Stateless and streaming.
pub(crate) struct Drop {
    pub(crate) cols: Vec<String>,
}

impl Operator for Drop {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        let keep: Vec<usize> = (0..chunk.schema.fields.len())
            .filter(|&i| !self.cols.iter().any(|c| c == &chunk.schema.fields[i].name))
            .collect();
        if keep.len() == chunk.schema.fields.len() {
            return vec![chunk]; // nothing matched → unchanged
        }
        let fields: Vec<Field> = keep
            .iter()
            .map(|&i| chunk.schema.fields[i].clone())
            .collect();
        let columns: Vec<Column> = keep.iter().map(|&i| chunk.columns[i].clone()).collect();
        let schema = Arc::new(Schema::new(fields));
        let mut out = Chunk::new(chunk.meta.id, schema, columns);
        out.meta = chunk.meta.clone();
        vec![out]
    }
}

/// `cast COL:type [COL:type ...]` — re-type named columns in place (position and
/// name kept; the column's values are re-coerced through the cast lane, exactly
/// like an inline `(col:type)` projection). Unknown names warn once and are
/// skipped. Stateless and streaming.
pub(crate) struct Cast {
    pub(crate) casts: Vec<(String, DataType)>,
    /// Per-column count of non-null cells that failed a temporal parse (→ null);
    /// surfaced once on finish (never-silent, BUG-D §23.6).
    pub(crate) fails: std::collections::BTreeMap<String, (DataType, u64)>,
}

/// Surface accumulated cast-failure totals once on finish (never-silent, BUG-D
/// §23.6), one summary per column. The count is a total → chunk-size independent;
/// in the parallel path each worker surfaces its partition's partial and the
/// counts **sum** to the serial total (the same contract as the reader's
/// parse-failure summary / `validate` reject summary).
fn surface_cast_failures(
    fails: &std::collections::BTreeMap<String, (DataType, u64)>,
    ctx: &mut OpCtx,
) {
    for (col, (ty, n)) in fails {
        if *n > 0 {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Recoverable,
                    ErrorScope::Item,
                    format!(
                        "{n} value(s) in '{col}' could not be cast to {ty} (or hit a \
                     division by zero); set to null"
                    ),
                )
                .at_node(ctx.label.clone()),
            );
        }
    }
}

impl Operator for Cast {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        // Identity fast path: every cast resolves to a column that already
        // has the target type — pass the chunk through untouched. The old
        // form cloned EVERY column of EVERY chunk (a full data copy per
        // chunk; ~58ms/file of the 10M group feed) before the per-column
        // identity check inside `cast_column` could return it unchanged.
        if self.casts.iter().all(|(name, ty)| {
            chunk
                .schema
                .index_of(name)
                .is_some_and(|i| chunk.schema.fields[i].dtype == *ty)
        }) {
            return vec![chunk];
        }
        let mut fields = chunk.schema.fields.clone();
        // Move the columns out of the owned chunk: only actually-cast columns
        // are rebuilt; the rest transfer without a copy. Take-then-refill
        // keeps a repeated name re-casting the already-cast column, exactly
        // like the old sequential in-place form.
        let mut slots: Vec<Option<Column>> = chunk.columns.into_iter().map(Some).collect();
        let mut changed = false;
        for (name, ty) in &self.casts {
            match chunk.schema.index_of(name) {
                Some(i) => {
                    let mut f = 0u64;
                    let col = slots[i].take().expect("slot is always refilled");
                    slots[i] = Some(eval::cast_column(col, *ty, &mut f));
                    if f > 0 {
                        self.fails.entry(name.clone()).or_insert((*ty, 0)).1 += f;
                    }
                    fields[i] = Field::new(name.clone(), *ty);
                    changed = true;
                }
                None => ctx.raise(
                    ErrorEvent::new(
                        Severity::Warn,
                        ErrorScope::Chunk,
                        format!("cast: unknown column '{name}'"),
                    )
                    .at_node(ctx.label.clone())
                    .at_chunk(chunk.meta.id),
                ),
            }
        }
        let columns: Vec<Column> = slots
            .into_iter()
            .map(|s| s.expect("slot is always refilled"))
            .collect();
        let schema = if changed {
            Arc::new(Schema::new(fields))
        } else {
            chunk.schema.clone()
        };
        let mut out = Chunk::new(chunk.meta.id, schema, columns);
        out.meta = chunk.meta.clone();
        vec![out]
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        surface_cast_failures(&self.fails, ctx);
        Vec::new()
    }
}

/// `reorder COL [COL ...]` — move the named columns to the front in the given
/// order; the remaining columns follow in their original order. Unknown names
/// are ignored. Stateless, streaming, type/value preserving (a permutation).
pub(crate) struct Reorder {
    pub(crate) cols: Vec<String>,
}

impl Operator for Reorder {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        // Front: the named columns that exist, in request order (dedup so a
        // repeated name doesn't duplicate a column). Then every other column in
        // its original order.
        let mut order: Vec<usize> = Vec::with_capacity(chunk.schema.fields.len());
        for name in &self.cols {
            if let Some(i) = chunk.schema.index_of(name) {
                if !order.contains(&i) {
                    order.push(i);
                }
            }
        }
        for i in 0..chunk.schema.fields.len() {
            if !order.contains(&i) {
                order.push(i);
            }
        }
        // A no-op permutation (already in this order) passes through untouched.
        if order.iter().enumerate().all(|(pos, &i)| pos == i) {
            return vec![chunk];
        }
        let fields: Vec<Field> = order
            .iter()
            .map(|&i| chunk.schema.fields[i].clone())
            .collect();
        let columns: Vec<Column> = order.iter().map(|&i| chunk.columns[i].clone()).collect();
        let schema = Arc::new(Schema::new(fields));
        let mut out = Chunk::new(chunk.meta.id, schema, columns);
        out.meta = chunk.meta.clone();
        vec![out]
    }
}

// -------------------------------------------------------- computed projection

/// `|> field (expr) as alias …` — projection that can compute new columns.
/// Each item is evaluated columnar-style over the chunk (see `eval::eval_column`)
/// and emitted under its output name. Stateless and row-count preserving.
pub(crate) struct ProjectExpr {
    pub(crate) items: Vec<(Expr, String)>,
    /// Per-output-column count of cast failures within the item's expression
    /// (→ null); surfaced once on finish (never-silent, BUG-D §23.6).
    pub(crate) fails: std::collections::BTreeMap<String, (DataType, u64)>,
}

impl Operator for ProjectExpr {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        let mut fields = Vec::with_capacity(self.items.len());
        let mut cols = Vec::with_capacity(self.items.len());
        for (expr, alias) in &self.items {
            // Observe a bare reference to a missing column (continue-first). A
            // `source.<field>` accessor (Access::Source) reads provenance, not a
            // column, so it is never "unknown" here.
            if let Expr::Field { name, access } = expr {
                if access.is_column() && chunk.column(name).is_none() {
                    ctx.raise(
                        ErrorEvent::new(
                            Severity::Warn,
                            ErrorScope::Chunk,
                            format!("project: unknown field '{name}'"),
                        )
                        .at_node(ctx.label.clone())
                        .at_chunk(chunk.meta.id),
                    );
                }
            }
            let mut f = 0u64;
            let col = eval::eval_column(expr, &chunk, &mut f);
            if f > 0 {
                self.fails
                    .entry(alias.clone())
                    .or_insert((col.dtype(), 0))
                    .1 += f;
            }
            fields.push(Field::new(alias.clone(), col.dtype()));
            cols.push(col);
        }
        let schema = Arc::new(Schema::new(fields));
        let mut out = Chunk::new(chunk.meta.id, schema, cols);
        out.meta = chunk.meta.clone(); // preserve mode / telemetry
        vec![out]
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        surface_cast_failures(&self.fails, ctx);
        Vec::new()
    }
}

// ------------------------------------------------------------------- project

pub(crate) struct Project {
    pub(crate) fields: Vec<String>,
}

impl Operator for Project {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        match chunk.project(&self.fields) {
            Some(c) => vec![c],
            None => {
                // Missing field: warn and pass through unchanged (continue-first).
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Warn,
                        ErrorScope::Chunk,
                        format!("project: unknown field in {:?}", self.fields),
                    )
                    .at_node(ctx.label.clone())
                    .at_chunk(chunk.meta.id),
                );
                vec![chunk]
            }
        }
    }
}

// ------------------------------------------------------- fused filter+project

/// Optimizer-produced fusion of consecutive filters and an optional trailing
/// projection. Evaluates all predicates (AND) in one row scan, then gathers
/// **only the projected columns** at the surviving indices — a single gather
/// instead of filter-then-project's two, and unused columns are never copied.
pub(crate) struct FilterProject {
    pub(crate) preds: Vec<Expr>,
    pub(crate) fields: Option<Vec<String>>,
    /// Cast failures inside the predicates (BUG-D §23.6) — surfaced once on finish.
    pub(crate) cast_fails: u64,
}

impl Operator for FilterProject {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        // Vectorized numeric path when the whole conjunction compiles; else the
        // row-wise interpreter (must produce identical results).
        let pred_refs: Vec<&Expr> = self.preds.iter().collect();
        let keep = match kernel::compile(&pred_refs, &chunk) {
            Some(plan) => kernel::run(&plan, &chunk),
            None => {
                let mut f = 0u64;
                let keep: Vec<usize> = (0..chunk.len)
                    .filter(|&row| {
                        self.preds
                            .iter()
                            .all(|p| eval::eval_predicate_acc(p, &chunk, row, &mut f))
                    })
                    .collect();
                self.cast_fails += f;
                keep
            }
        };
        if keep.is_empty() {
            return Vec::new();
        }

        let Some(fields) = &self.fields else {
            // Pure fused filter (no projection).
            if keep.len() == chunk.len {
                return vec![chunk];
            }
            return vec![chunk.gather(&keep)];
        };

        // Gather only the projected columns at the surviving rows (one pass).
        let mut idx = Vec::with_capacity(fields.len());
        for f in fields {
            match chunk.schema.index_of(f) {
                Some(i) => idx.push(i),
                None => {
                    // Missing field: warn, fall back to keeping all columns.
                    ctx.raise(
                        ErrorEvent::new(
                            Severity::Warn,
                            ErrorScope::Chunk,
                            format!("fused project: unknown field in {fields:?}"),
                        )
                        .at_node(ctx.label.clone())
                        .at_chunk(chunk.meta.id),
                    );
                    return vec![chunk.gather(&keep)];
                }
            }
        }
        let columns: Vec<Column> = idx
            .iter()
            .map(|&i| chunk.columns[i].gather(&keep))
            .collect();
        let schema = Arc::new(Schema::new(
            idx.iter()
                .map(|&i| chunk.schema.fields[i].clone())
                .collect(),
        ));
        let mut out = Chunk::new(chunk.meta.id, schema, columns);
        out.meta = chunk.meta.clone(); // preserve provenance (id, mode, warnings)
        vec![out]
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        let refs: Vec<&Expr> = self.preds.iter().collect();
        surface_pred_cast_fails(self.cast_fails, &refs, ctx);
        Vec::new()
    }
}

// ------------------------------------------------------------- sessionize (§36.5)

/// Session windows (§36.5 / #60): append a `session` column carrying each
/// row's **session start** (the ts column's datetime lane — the same "window
/// start as key" shape as `bucket`/`hops`). A new session starts when the gap
/// from the previous row's ts (per `by` group) exceeds `gap`.
///
/// Stateful per group (`last ts` + `current start` only — bounded by group
/// cardinality, input-size independent), per-chunk emit (streaming), and
/// **order-dependent**: the engine keeps it on the serial path (like `ffill`).
/// Input is assumed time-ascending (#60 contract); a time regression still
/// sessionizes by the same rule (its gap is negative ⇒ same session) but is
/// **counted and surfaced once** on finish (never-silent).
pub(crate) struct Sessionize {
    pub(crate) ts: String,
    pub(crate) gap: String,
    pub(crate) by: Vec<String>,
    /// Composite `by` key (0x1F-joined, like GroupBy) → (last ts, session start).
    pub(crate) state: std::collections::BTreeMap<String, (i64, i64)>,
    pub(crate) regressions: u64,
    pub(crate) warned: bool,
}

impl Sessionize {
    fn warn_once(&mut self, ctx: &mut OpCtx, msg: String) {
        if !self.warned {
            self.warned = true;
            ctx.raise(
                ErrorEvent::new(Severity::Warn, ErrorScope::Chunk, msg).at_node(ctx.label.clone()),
            );
        }
    }
}

impl Operator for Sessionize {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        let Some(ci) = chunk.schema.index_of(&self.ts) else {
            self.warn_once(
                ctx,
                format!("sessionize: unknown column '{}' (passed through)", self.ts),
            );
            return vec![chunk];
        };
        let ColumnData::DateTime(d) = chunk.columns[ci].data() else {
            self.warn_once(
                ctx,
                format!(
                    "sessionize: column '{}' is not a datetime lane (passed through) — \
                     declare it, e.g. `({}:datetime)`",
                    self.ts, self.ts
                ),
            );
            return vec![chunk];
        };
        let unit = d.unit;
        let Some(gap) = rivus_core::Duration::parse_interval(&self.gap, unit).and_then(|g| {
            // Exact gap ticks at the ts unit (same contract as bucket/hops).
            let n = g.ticks as i128 * unit.per_sec() as i128;
            let per = g.unit.per_sec() as i128;
            if g.ticks <= 0 || n % per != 0 {
                None
            } else {
                i64::try_from(n / per).ok()
            }
        }) else {
            self.warn_once(
                ctx,
                format!(
                    "sessionize: gap \"{}\" is not a positive duration representable at the \
                     ts unit (passed through)",
                    self.gap
                ),
            );
            return vec![chunk];
        };
        // Resolve the `by` columns (missing ones warn once and group as "").
        let by_cols: Vec<String> = self.by.clone();
        let mut by_idx: Vec<Option<usize>> = Vec::with_capacity(by_cols.len());
        for c in &by_cols {
            let i = chunk.schema.index_of(c);
            if i.is_none() {
                self.warn_once(
                    ctx,
                    format!("sessionize: unknown `by` column '{c}' (grouped as empty)"),
                );
            }
            by_idx.push(i);
        }

        let ticks = &d.ticks;
        let mut starts: Vec<i64> = Vec::with_capacity(chunk.len);
        let mut valid: Vec<bool> = Vec::with_capacity(chunk.len);
        let mut key = String::new();
        for (r, &t) in ticks.iter().enumerate().take(chunk.len) {
            if chunk.columns[ci].is_null(r) {
                // A null ts can't be sessionized → null session cell (the row
                // itself flows on; continue-first, blank-cell convention).
                starts.push(0);
                valid.push(false);
                continue;
            }
            key.clear();
            for (k, idx) in by_idx.iter().enumerate() {
                if k > 0 {
                    key.push('\u{1f}');
                }
                if let Some(i) = idx {
                    if !chunk.columns[*i].is_null(r) {
                        key.push_str(&chunk.value(r, *i).to_string());
                    }
                }
            }
            let start = match self.state.get(key.as_str()) {
                Some(&(last, cur_start)) => {
                    if t < last {
                        self.regressions += 1;
                    }
                    // Strictly-greater-than-gap starts a new session; a gap of
                    // exactly `gap` continues it (closed threshold).
                    if t.saturating_sub(last) > gap {
                        t
                    } else {
                        cur_start
                    }
                }
                None => t,
            };
            self.state.insert(key.clone(), (t, start));
            starts.push(start);
            valid.push(true);
        }

        // Append the `session` column (suffix `_r` on collision, §27.1 rule).
        let name = if chunk.schema.index_of("session").is_some() {
            "session_r"
        } else {
            "session"
        };
        let mut fields = chunk.schema.fields.clone();
        fields.push(rivus_core::Field::new(name, chunk.schema.fields[ci].dtype));
        let mut columns = chunk.columns.clone();
        columns.push(Column::new(
            ColumnData::DateTime(rivus_core::DtColumn {
                ticks: starts,
                unit,
            }),
            rivus_core::Validity::from_bits(&valid),
        ));
        let mut out = Chunk::new(chunk.meta.id, Arc::new(Schema::new(fields)), columns);
        out.meta = chunk.meta.clone();
        vec![out]
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.regressions > 0 {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Recoverable,
                    ErrorScope::Chunk,
                    format!(
                        "sessionize: {} row(s) arrived out of time order (ts went backwards) — \
                         sessions assume an ascending '{}'; sort upstream for exact boundaries",
                        self.regressions, self.ts
                    ),
                )
                .at_node(ctx.label.clone()),
            );
        }
        Vec::new()
    }
}

// --------------------------------------------------------------- shift (#65)

/// Time-series shift/difference (`shift col lag|diff|pct_change [N] by … as
/// out`, #65): append `out` derived from a value `n` rows back **within the
/// same `by` group, in source order**. Stateful per group (a ring of the last
/// `n` values), streaming per-chunk emit, order-dependent → serial path (the
/// engine keeps it off the parallel path, like `ffill`/`sessionize`).
/// Chunk-size independent because the shift is defined in source order.
pub(crate) struct Shift {
    pub(crate) col: String,
    pub(crate) kind: rivus_ir::ShiftKind,
    pub(crate) n: usize,
    pub(crate) by: Vec<String>,
    pub(crate) out: String,
    /// Composite `by` key (0x1F-joined, like GroupBy) → ring of the last `n`
    /// source values (most recent at the back).
    pub(crate) state:
        std::collections::BTreeMap<String, std::collections::VecDeque<rivus_core::Value>>,
    pub(crate) warned: bool,
}

impl Shift {
    /// The output lane, mirrored exactly in `schema_prop` (§32.1): `lag` keeps
    /// the source lane; `diff` of a datetime → `Duration`, of an exact numeric
    /// lane → that lane, else `f64`; `pct_change` → `f64`.
    fn out_dtype(kind: rivus_ir::ShiftKind, src: rivus_core::DataType) -> rivus_core::DataType {
        use rivus_core::DataType as D;
        use rivus_ir::ShiftKind as K;
        match kind {
            K::Lag => src,
            K::Diff => match src {
                D::DateTime { unit } => D::Duration { unit },
                D::I64 => D::I64,
                D::Decimal { scale } => D::Decimal { scale },
                _ => D::F64,
            },
            K::PctChange => D::F64,
        }
    }

    /// `cur − prev`, kept in the exact lane where the lane is associative
    /// (datetime→Duration, i64, decimal); otherwise via f64. `Null` if either
    /// operand is null or the lanes are incompatible (continue-first).
    fn diff(cur: &rivus_core::Value, prev: &rivus_core::Value) -> rivus_core::Value {
        use rivus_core::Value as V;
        match (cur, prev) {
            (V::Null, _) | (_, V::Null) => V::Null,
            (V::DateTime(a), V::DateTime(b)) if a.unit == b.unit => {
                V::Duration(rivus_core::Duration::new(a.ticks - b.ticks, a.unit))
            }
            (V::I64(a), V::I64(b)) => V::I64(a - b),
            (V::Dec(a), V::Dec(b)) if a.scale == b.scale => {
                V::Dec(rivus_core::Decimal::new(a.unscaled - b.unscaled, a.scale))
            }
            _ => match (cur.as_f64(), prev.as_f64()) {
                (Some(a), Some(b)) => V::F64(a - b),
                _ => V::Null,
            },
        }
    }

    /// `(cur − prev)/prev` as f64; `Null` on null operands or a zero base.
    fn pct_change(cur: &rivus_core::Value, prev: &rivus_core::Value) -> rivus_core::Value {
        match (cur.as_f64(), prev.as_f64()) {
            (Some(a), Some(b)) if b != 0.0 => rivus_core::Value::F64((a - b) / b),
            _ => rivus_core::Value::Null,
        }
    }
}

impl Operator for Shift {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        let Some(ci) = chunk.schema.index_of(&self.col) else {
            if !self.warned {
                self.warned = true;
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Warn,
                        ErrorScope::Chunk,
                        format!("shift: unknown column '{}' (passed through)", self.col),
                    )
                    .at_node(ctx.label.clone()),
                );
            }
            return vec![chunk];
        };
        let src_dtype = chunk.schema.fields[ci].dtype;
        let out_dtype = Self::out_dtype(self.kind, src_dtype);

        // Resolve the `by` columns (missing ones warn once, group as empty).
        let by_cols = self.by.clone();
        let mut by_idx: Vec<Option<usize>> = Vec::with_capacity(by_cols.len());
        for c in &by_cols {
            let i = chunk.schema.index_of(c);
            if i.is_none() && !self.warned {
                self.warned = true;
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Warn,
                        ErrorScope::Chunk,
                        format!("shift: unknown `by` column '{c}' (grouped as empty)"),
                    )
                    .at_node(ctx.label.clone()),
                );
            }
            by_idx.push(i);
        }

        let mut out_vals: Vec<rivus_core::Value> = Vec::with_capacity(chunk.len);
        let mut key = String::new();
        for r in 0..chunk.len {
            key.clear();
            for (k, idx) in by_idx.iter().enumerate() {
                if k > 0 {
                    key.push('\u{1f}');
                }
                if let Some(i) = idx {
                    if !chunk.columns[*i].is_null(r) {
                        key.push_str(&chunk.value(r, *i).to_string());
                    }
                }
            }
            let cur = chunk.value(r, ci);
            let ring = self.state.entry(key.clone()).or_default();
            // The lagged value is the front of a full ring (row r − n).
            let lagged = if ring.len() == self.n {
                Some(ring.front().unwrap().clone())
            } else {
                None
            };
            use rivus_ir::ShiftKind as K;
            let v = match (lagged, self.kind) {
                (None, _) => rivus_core::Value::Null, // not enough history yet
                (Some(prev), K::Lag) => prev,
                (Some(prev), K::Diff) => Self::diff(&cur, &prev),
                (Some(prev), K::PctChange) => Self::pct_change(&cur, &prev),
            };
            out_vals.push(v);
            // Advance the ring with the current source value.
            ring.push_back(cur);
            if ring.len() > self.n {
                ring.pop_front();
            }
        }

        // Build the appended column in the schema-declared lane (preserved even
        // when a whole chunk is null, so the lane never silently degrades).
        let out_col = typed_column_for(&out_vals, out_dtype);
        let name = if chunk.schema.index_of(&self.out).is_some() {
            format!("{}_r", self.out)
        } else {
            self.out.clone()
        };
        let mut fields = chunk.schema.fields.clone();
        fields.push(rivus_core::Field::new(name, out_dtype));
        let mut columns = chunk.columns.clone();
        columns.push(out_col);
        let mut out = Chunk::new(chunk.meta.id, Arc::new(Schema::new(fields)), columns);
        out.meta = chunk.meta.clone();
        vec![out]
    }
}

/// Build a column of exactly `dtype` from row values (null-aware), preserving
/// the lane even when every value is null. Values whose lane doesn't match the
/// target become null (continue-first). Used by `shift` so the appended
/// column's static schema and its runtime lane agree byte-for-byte.
fn typed_column_for(vals: &[rivus_core::Value], dtype: rivus_core::DataType) -> Column {
    use rivus_core::{DataType as D, Value as V};
    let n = vals.len();
    let bits: Vec<bool> = vals.iter().map(|v| !v.is_null()).collect();
    let validity = rivus_core::Validity::from_bits(&bits);
    let data = match dtype {
        D::I64 => ColumnData::I64(
            vals.iter()
                .map(|v| if let V::I64(x) = v { *x } else { 0 })
                .collect(),
        ),
        D::F64 => ColumnData::F64(vals.iter().map(|v| v.as_f64().unwrap_or(0.0)).collect()),
        D::Bool => ColumnData::Bool(vals.iter().map(|v| matches!(v, V::Bool(true))).collect()),
        D::Decimal { scale } => ColumnData::Dec(rivus_core::DecColumn {
            unscaled: vals
                .iter()
                .map(|v| if let V::Dec(d) = v { d.unscaled } else { 0 })
                .collect(),
            scale,
        }),
        D::DateTime { unit } => ColumnData::DateTime(rivus_core::DtColumn {
            ticks: vals
                .iter()
                .map(|v| if let V::DateTime(t) = v { t.ticks } else { 0 })
                .collect(),
            unit,
        }),
        D::Duration { unit } => ColumnData::Duration(rivus_core::DurColumn {
            ticks: vals
                .iter()
                .map(|v| if let V::Duration(d) = v { d.ticks } else { 0 })
                .collect(),
            unit,
        }),
        D::Date => ColumnData::Date(
            vals.iter()
                .map(|v| if let V::Date(d) = v { d.epoch_day } else { 0 })
                .collect(),
        ),
        D::Time => ColumnData::Time(
            vals.iter()
                .map(|v| if let V::Time(t) = v { t.ticks } else { 0 })
                .collect(),
        ),
        // Text (and any lane not materialized above) rides the string lane; a
        // null renders empty via validity.
        _ => {
            let mut s = StrColumn::with_capacity(n, n * 8);
            for v in vals {
                s.push(&v.to_string());
            }
            ColumnData::Str(s)
        }
    };
    Column::new(data, validity)
}
