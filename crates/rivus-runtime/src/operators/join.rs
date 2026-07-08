//! Hash join operator (inner / left / right / full, composite keys).
//!
//! Split out of the former monolithic `operators.rs` (design 26 §26.8.1,
//! mechanical move-only split; logic unchanged).

use super::*;

// ----------------------------------------------------------------------- join

/// Inner hash join `A & B on lkey:rkey`. Buffers both inputs (a blocking,
/// serial pipeline-breaker like sort/group), builds a hash map of the right
/// side keyed by `right_key`, then probes with the left side. The output is the
/// left columns followed by the right columns (minus the join key); a name that
/// collides with a left column is suffixed `_r`. Keys compare by string value,
/// so `30` (i64) and `"30"` (str) match — convenient for loosely-typed CSV.
pub(crate) struct Join {
    left_keys: Vec<PathExpr>,
    right_keys: Vec<PathExpr>,
    kind: JoinKind,
    left_id: NodeId,
    left_buf: Vec<Chunk>,
    right_buf: Vec<Chunk>,
}

impl Join {
    pub(crate) fn new(
        left_keys: Vec<PathExpr>,
        right_keys: Vec<PathExpr>,
        kind: JoinKind,
        left_id: NodeId,
    ) -> Self {
        Join {
            left_keys,
            right_keys,
            kind,
            left_id,
            left_buf: Vec::new(),
            right_buf: Vec::new(),
        }
    }
}

/// A row's composite join key: the values at `idxs` joined by the ASCII unit
/// separator (`0x1F`, which can't appear in a parsed CSV field), so distinct key
/// tuples never collide.
/// Build the composite hash key for `row`, or `None` if **any** key part is
/// null. A null join key matches nothing (design 26 §26.2a / SQL `NULL`-join
/// semantics): the row is unmatched — dropped on an inner join, null-padded on
/// an outer join — so a null key never folds rows together (which would inflate
/// the output count vs DuckDB).
fn join_key_at(chunk: &Chunk, idxs: &[usize], row: usize) -> Option<String> {
    let mut s = String::new();
    for (n, &ci) in idxs.iter().enumerate() {
        if chunk.columns[ci].is_null(row) {
            return None;
        }
        if n > 0 {
            s.push('\u{1f}');
        }
        s.push_str(&chunk.value(row, ci).to_string());
    }
    Some(s)
}

/// Like [`join_key_at`] but appends the composite key into a **reused** buffer
/// instead of allocating a fresh `String` per call — the probe side calls this
/// once per row, so avoiding the per-row heap allocation (and the `Value` box a
/// bare column would otherwise pay) is decisive on a large probe. Returns
/// `false` (buffer left as-is) if any key part is null (a null key matches
/// nothing, as in [`join_key_at`]). The bytes written are **identical** to
/// [`join_key_at`]'s string for the same row (a str part borrows the column
/// directly; any other lane falls back to the same `Value::to_string` form), so
/// the key — and therefore every match — is byte-for-byte unchanged.
fn fill_join_key(chunk: &Chunk, idxs: &[usize], row: usize, buf: &mut String) -> bool {
    use std::fmt::Write;
    for (n, &ci) in idxs.iter().enumerate() {
        if chunk.columns[ci].is_null(row) {
            return false;
        }
        if n > 0 {
            buf.push('\u{1f}');
        }
        match chunk.columns[ci].data() {
            // Bare string key: borrow the column's `&str` (zero allocation,
            // identical bytes to `Value::Str(_).to_string()`).
            ColumnData::Str(s) => buf.push_str(s.get(row)),
            // Any other lane keeps the exact `Value::to_string` form (still into
            // the reused buffer) so the key bytes are unchanged.
            _ => {
                let _ = write!(buf, "{}", chunk.value(row, ci));
            }
        }
    }
    true
}

/// Concatenate buffered chunks (sharing a schema) into one.
fn concat_chunks(bufs: Vec<Chunk>) -> Option<Chunk> {
    let mut it = bufs.into_iter();
    let first = it.next()?;
    let schema = first.schema.clone();
    let mut cols = first.columns;
    for c in it {
        for (i, col) in c.columns.iter().enumerate() {
            cols[i].append(col);
        }
    }
    Some(Chunk::new(0, schema, cols))
}

impl Join {
    /// Emit one side unchanged (its own schema) — used when the other side has
    /// no rows at all and this join kind keeps the present side.
    fn pass_through(&self, ctx: &mut OpCtx, side: &Chunk) -> Chunk {
        let idx: Vec<usize> = (0..side.len).collect();
        let cols: Vec<Column> = side.columns.iter().map(|c| c.gather(&idx)).collect();
        Chunk::new(ctx.fresh_id(), side.schema.clone(), cols)
    }
}

impl Operator for Join {
    fn process(&mut self, from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        if from == self.left_id {
            self.left_buf.push(chunk);
        } else {
            self.right_buf.push(chunk);
        }
        Vec::new() // blocking: join emitted on finish
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        let left = concat_chunks(std::mem::take(&mut self.left_buf));
        let right = concat_chunks(std::mem::take(&mut self.right_buf));

        // One side entirely absent (no chunks). With no schema to pad against we
        // can only emit the *present* side, and only when this kind keeps it.
        let (left, right) = match (left, right) {
            (Some(l), Some(r)) => (l, r),
            (Some(l), None) => {
                return if self.kind.keeps_left() {
                    vec![self.pass_through(ctx, &l)]
                } else {
                    Vec::new()
                };
            }
            (None, Some(r)) => {
                return if self.kind.keeps_right() {
                    vec![self.pass_through(ctx, &r)]
                } else {
                    Vec::new()
                };
            }
            (None, None) => return Vec::new(),
        };

        let warn = |ctx: &mut OpCtx, side: &str, key: &str| {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Warn,
                    ErrorScope::Branch,
                    format!("join: unknown {side} key '{key}'"),
                )
                .at_node(ctx.label.clone()),
            );
        };
        // Resolve each key column on both sides (composite key, in key order).
        // §32 s4b: a bare key uses the flat fast path; a nested key (`user.id`)
        // is materialized into a derived column appended past the original
        // columns (`base_left` / `base_right`), used only to build the join key —
        // the output gathers only the original columns.
        // Right derived key columns never reach the output (it is built from
        // `right.schema.fields`), so only the left base count must be tracked.
        let (mut left, mut right) = (left, right);
        let base_left = left.columns.len();
        let mut key_fails = 0u64;
        let lresolved = eval::resolve_key_indices(&mut left, &self.left_keys, &mut key_fails);
        let rresolved = eval::resolve_key_indices(&mut right, &self.right_keys, &mut key_fails);
        let mut lk = Vec::with_capacity(self.left_keys.len());
        for (k, idx) in self.left_keys.iter().zip(&lresolved) {
            match idx {
                Some(i) => lk.push(*i),
                None => {
                    warn(ctx, "left", &k.column_name());
                    return Vec::new();
                }
            }
        }
        let mut rk = Vec::with_capacity(self.right_keys.len());
        for (k, idx) in self.right_keys.iter().zip(&rresolved) {
            match idx {
                Some(i) => rk.push(*i),
                None => {
                    warn(ctx, "right", &k.column_name());
                    return Vec::new();
                }
            }
        }

        // Build the hash table on the right side, then probe with the left.
        // Each output row is a `(Option<left>, Option<right>)` pair: an unmatched
        // left row (left/full) has `None` on the right and pads the right columns
        // with defaults; an unmatched right row (right/full) has `None` on the
        // left and pads the left columns — except the join-key columns, which
        // take the right key so the key is never lost.
        let mut table: HashMap<String, Vec<usize>> = HashMap::new();
        for ri in 0..right.len {
            // A null-key right row is never inserted, so it matches nothing; it
            // still surfaces as an unmatched row for right/full joins below.
            if let Some(k) = join_key_at(&right, &rk, ri) {
                table.entry(k).or_default().push(ri);
            }
        }
        let mut right_matched = vec![false; right.len];
        let mut lidx: Vec<Option<usize>> = Vec::new();
        let mut ridx: Vec<Option<usize>> = Vec::new();
        let keeps_left = self.kind.keeps_left();
        // Probe with a REUSED key buffer: the old path allocated a fresh heap
        // `String` (and boxed a `Value`) for every left row, which dominated the
        // cost on a large probe side (a 3M-row × 20-row left join measured 6.7s —
        // the per-row allocation, not the hash match). `String: Borrow<str>` lets
        // us look the buffer up without owning a key. Byte-identical: `keybuf`
        // holds the same bytes `join_key_at` would have returned.
        let mut keybuf = String::new();
        for li in 0..left.len {
            keybuf.clear();
            // A null-key left row matches nothing (no table lookup at all).
            let matched = if fill_join_key(&left, &lk, li, &mut keybuf) {
                table.get(keybuf.as_str())
            } else {
                None
            };
            match matched {
                Some(rs) => {
                    for &ri in rs {
                        right_matched[ri] = true;
                        lidx.push(Some(li));
                        ridx.push(Some(ri));
                    }
                }
                None if keeps_left => {
                    lidx.push(Some(li));
                    ridx.push(None);
                }
                None => {}
            }
        }
        // Right/full: append the right rows that no left row matched.
        if self.kind.keeps_right() {
            for (ri, matched) in right_matched.iter().enumerate() {
                if !*matched {
                    lidx.push(None);
                    ridx.push(Some(ri));
                }
            }
        }

        // Output schema: left fields, then right fields except the join keys
        // (collisions suffixed `_r`). The right key columns are dropped (the
        // left key column carries the value).
        let mut fields = left.schema.fields.clone();
        let mut right_cols = Vec::new();
        for (ci, f) in right.schema.fields.iter().enumerate() {
            if rk.contains(&ci) {
                continue;
            }
            let name = if left.schema.index_of(&f.name).is_some() {
                format!("{}_r", f.name)
            } else {
                f.name.clone()
            };
            fields.push(Field::new(name, f.dtype));
            right_cols.push(ci);
        }

        // Left columns: gather by `lidx`. A join-key column borrows the matching
        // right key when the left side is absent (key-preservation for
        // right/full joins); a non-key left column pads with the type default.
        // Gather only the original left columns (skip any derived nested-key
        // columns appended past `base_left`).
        let mut out: Vec<Column> = Vec::with_capacity(fields.len());
        for (ci, col) in left.columns[..base_left].iter().enumerate() {
            match lk.iter().position(|&k| k == ci) {
                Some(kpos) => {
                    out.push(join_key_column(col, &lidx, &ridx, &right.columns[rk[kpos]]))
                }
                None => out.push(col.gather_opt(&lidx)),
            }
        }
        for &ci in &right_cols {
            out.push(right.columns[ci].gather_opt(&ridx));
        }
        super::surface_key_path_fails(key_fails, "join", ctx);
        vec![Chunk::new(
            ctx.fresh_id(),
            Arc::new(Schema::new(fields)),
            out,
        )]
    }
}

/// Build the output join-key column. For a matched/left-present row it takes the
/// left key (`lidx`); for an unmatched-right row (`lidx == None`) it takes the
/// right key (`ridx`), so a right/full join never drops the key value. Falls
/// back to the left column's lane, widening to text only if the right key's
/// string form can't be represented there.
fn join_key_column(
    left_key: &Column,
    lidx: &[Option<usize>],
    ridx: &[Option<usize>],
    right_key: &Column,
) -> Column {
    // Fast path: every row has a left value → a plain gather_opt suffices.
    if lidx.iter().all(|o| o.is_some()) {
        return left_key.gather_opt(lidx);
    }
    // Mixed: assemble values, taking the right key when the left is absent.
    let vals: Vec<rivus_core::Value> = lidx
        .iter()
        .zip(ridx)
        .map(|(l, r)| match (l, r) {
            (Some(i), _) => left_key.value_at(*i),
            (None, Some(j)) => right_key.value_at(*j),
            (None, None) => rivus_core::Value::Str(String::new()),
        })
        .collect();
    eval::column_from_values(vals)
}
