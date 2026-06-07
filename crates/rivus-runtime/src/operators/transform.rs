//! Row transforms: filter · validate · take · sort · distinct · describe · dropna/fill · rename/drop/cast/reorder · project.
//!
//! Split out of the former monolithic `operators.rs` (design 26 §26.8.1,
//! mechanical move-only split; logic unchanged).

use super::*;

// -------------------------------------------------------------------- filter

pub(crate) struct Filter {
    pub(crate) pred: Expr,
}

impl Operator for Filter {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        // Vectorized numeric path when possible; else the row-wise interpreter.
        let keep = match kernel::compile(&[&self.pred], &chunk) {
            Some(plan) => kernel::run(&plan, &chunk),
            None => (0..chunk.len)
                .filter(|&row| eval::eval_predicate(&self.pred, &chunk, row))
                .collect(),
        };
        if keep.is_empty() {
            return Vec::new();
        }
        if keep.len() == chunk.len {
            return vec![chunk];
        }
        vec![chunk.gather(&keep)]
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
}

impl Validate {
    fn passing(&self, chunk: &Chunk) -> Vec<usize> {
        match kernel::compile(&[&self.pred], chunk) {
            Some(plan) => kernel::run(&plan, chunk),
            None => (0..chunk.len)
                .filter(|&row| eval::eval_predicate(&self.pred, chunk, row))
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
        let keep = self.passing(&chunk);
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
}

// ---------------------------------------------------------------------- sort

/// `sort KEY [desc]` — a blocking sort. Buffers every chunk, then on finish
/// concatenates them (in arrival = source order), stably sorts by the key
/// column, and emits one ordered chunk. Stable + concatenate-then-sort makes
/// the output independent of `chunk_size`; ties keep source order for both
/// ascending and descending.
pub(crate) struct Sort {
    keys: Vec<(String, bool)>,
    buf: Vec<Chunk>,
    emitted: bool,
}

impl Sort {
    pub(crate) fn new(keys: Vec<(String, bool)>) -> Self {
        Sort {
            keys,
            buf: Vec::new(),
            emitted: false,
        }
    }
}

/// Compare two rows of one column for ordering (NaN treated as equal).
fn cmp_rows(col: &Column, a: usize, b: usize) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    // Null model §26.2b: a null sorts as the largest value → **nulls last** on an
    // ascending sort (and first on descending, since the caller reverses). null
    // == null is Equal (the stable sort then keeps source order). Gated by
    // has_nulls → zero cost for all-valid columns.
    if col.has_nulls() {
        match (col.is_null(a), col.is_null(b)) {
            (true, true) => return Ordering::Equal,
            (true, false) => return Ordering::Greater,
            (false, true) => return Ordering::Less,
            (false, false) => {}
        }
    }
    match col.data() {
        ColumnData::Bool(v) => v[a].cmp(&v[b]),
        ColumnData::I64(v) => v[a].cmp(&v[b]),
        ColumnData::F64(v) => v[a].partial_cmp(&v[b]).unwrap_or(Ordering::Equal),
        // One column shares a scale, so the unscaled i128 order is the exact
        // value order — no precision loss in the sort key (design doc 21).
        ColumnData::Dec(d) => d.unscaled[a].cmp(&d.unscaled[b]),
        // One column shares a unit, so the integer tick order is the exact
        // chronological order.
        ColumnData::DateTime(d) => d.ticks[a].cmp(&d.ticks[b]),
        // Duration shares a unit too → exact i64 magnitude order (#57).
        ColumnData::Duration(d) => d.ticks[a].cmp(&d.ticks[b]),
        // Date: epoch-day order is exact chronological order (#58).
        ColumnData::Date(v) => v[a].cmp(&v[b]),
        // Time-of-day: tick order is exact chronological order (#58).
        ColumnData::Time(v) => v[a].cmp(&v[b]),
        // Resource sorts by uri (the in-contract identity; §00 0.14) — byte
        // order, matching discovery's deterministic uri ordering (§28.3).
        ColumnData::Str(v) | ColumnData::Resource(v) => v.get(a).cmp(v.get(b)),
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

        // Resolve each sort key to (column index, descending). An unknown key
        // warns once and is skipped (continue-first); if none resolve the stream
        // is emitted in source order.
        let mut key_cols: Vec<(usize, bool)> = Vec::with_capacity(self.keys.len());
        for (k, desc) in &self.keys {
            match schema.index_of(k) {
                Some(ki) => key_cols.push((ki, *desc)),
                None => ctx.raise(
                    ErrorEvent::new(
                        Severity::Warn,
                        ErrorScope::Chunk,
                        format!("sort: unknown key '{k}' (ignored)"),
                    )
                    .at_node(ctx.label.clone()),
                ),
            }
        }

        let mut idx: Vec<usize> = (0..total).collect();
        if !key_cols.is_empty() {
            idx.sort_by(|&a, &b| {
                for &(ki, desc) in &key_cols {
                    let o = cmp_rows(&cols[ki], a, b);
                    let o = if desc { o.reverse() } else { o };
                    if o != std::cmp::Ordering::Equal {
                        return o;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        let sorted: Vec<Column> = cols.iter().map(|c| c.gather(&idx)).collect();
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
        key.push_str(&chunk.value(row, ci).to_string());
    }
}

/// `distinct [keys...]` — keep the first occurrence of each distinct key,
/// dropping later duplicates. Streaming (emits surviving rows per chunk) but
/// stateful: a global seen-set spans chunks, so it runs serially. Output order
/// is first-occurrence order, independent of `chunk_size`.
pub(crate) struct Distinct {
    keys: Vec<String>,
    seen: std::collections::HashSet<String>,
}

impl Distinct {
    pub(crate) fn new(keys: Vec<String>) -> Self {
        Distinct {
            keys,
            seen: std::collections::HashSet::new(),
        }
    }
}

impl Operator for Distinct {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        // Columns that form the dedup key: the named ones, or every column.
        let idxs: Vec<usize> = if self.keys.is_empty() {
            (0..chunk.columns.len()).collect()
        } else {
            self.keys
                .iter()
                .filter_map(|k| chunk.schema.index_of(k))
                .collect()
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

        if keep.is_empty() {
            return Vec::new();
        }
        if keep.len() == chunk.len {
            return vec![chunk];
        }
        vec![chunk.gather(&keep)]
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
        if keep.is_empty() {
            return Vec::new();
        }
        if keep.len() == chunk.len {
            return vec![chunk];
        }
        vec![chunk.gather(&keep)]
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
        matches!(chunk.columns[ci].data(), ColumnData::Str(_)).then_some(ci)
    }
}

impl Operator for FillDirectional {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        if !self.forward {
            // bfill: buffer; the next non-empty value may be in a later chunk.
            self.buf.push(chunk);
            return Vec::new();
        }
        let Some(ci) = self.target(&chunk, ctx) else {
            return vec![chunk];
        };
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
        // Numeric column → no blanks to fill (parsed to 0 already); pass through.
        if !matches!(chunks[0].columns[ci].data(), ColumnData::Str(_)) {
            return chunks;
        }

        // Pass 1: collect every non-empty cell that parses as a number.
        let mut nums: Vec<f64> = Vec::new();
        let mut count = 0f64;
        let mut sum = 0f64;
        for c in &chunks {
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
}

impl Operator for Cast {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        let mut fields = chunk.schema.fields.clone();
        let mut columns = chunk.columns.clone();
        let mut changed = false;
        for (name, ty) in &self.casts {
            match chunk.schema.index_of(name) {
                Some(i) => {
                    columns[i] = eval::cast_column(columns[i].clone(), *ty);
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
        if !changed {
            return vec![chunk];
        }
        let schema = Arc::new(Schema::new(fields));
        let mut out = Chunk::new(chunk.meta.id, schema, columns);
        out.meta = chunk.meta.clone();
        vec![out]
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
            let col = eval::eval_column(expr, &chunk);
            fields.push(Field::new(alias.clone(), col.dtype()));
            cols.push(col);
        }
        let schema = Arc::new(Schema::new(fields));
        let mut out = Chunk::new(chunk.meta.id, schema, cols);
        out.meta = chunk.meta.clone(); // preserve mode / telemetry
        vec![out]
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
}

impl Operator for FilterProject {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        // Vectorized numeric path when the whole conjunction compiles; else the
        // row-wise interpreter (must produce identical results).
        let pred_refs: Vec<&Expr> = self.preds.iter().collect();
        let keep = match kernel::compile(&pred_refs, &chunk) {
            Some(plan) => kernel::run(&plan, &chunk),
            None => (0..chunk.len)
                .filter(|&row| {
                    self.preds
                        .iter()
                        .all(|p| eval::eval_predicate(p, &chunk, row))
                })
                .collect(),
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
}
