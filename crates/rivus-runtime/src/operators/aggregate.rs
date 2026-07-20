//! Group-by aggregation: GroupBy operator, per-group AggAcc, parallel merge.
//!
//! Split out of the former monolithic `operators.rs` (design 26 §26.8.1,
//! mechanical move-only split; logic unchanged).

use super::*;

// ------------------------------------------------------------------- group by

/// Running accumulator for one aggregate within one group. Carries the
/// aggregate's `func` so it only maintains the state that function needs
/// (numeric moments, a distinct set, or first/last cells).
#[derive(Clone)]
pub(crate) struct AggAcc {
    func: AggFunc,
    sum: f64,
    sum_sq: f64,
    min: f64,
    max: f64,
    n: i64,
    /// Number of **non-null** values observed — `COUNT(col)` for `AggFunc::Count`
    /// (design 26 §26.2d). Counts every lane (unlike `n`, which is numeric-only).
    non_null: i64,
    first: Option<String>,
    last: Option<String>,
    distinct: std::collections::HashSet<String>,
    /// Buffered numeric values, only for percentile aggregates (`Pct`). Bounded
    /// by group cardinality, so percentiles are pipeline-breakers like sort.
    values: Vec<f64>,
    /// Exact decimal accumulation (design 21 §21.5): set once a `Value::Dec` is
    /// observed (a decimal column shares one scale). `sum`/`min`/`max` are kept in
    /// `i128` so the result is exact and order-independent — the property that
    /// lets a decimal `sum`/`avg` parallelize byte-identically (#41). `overflow`
    /// degrades that aggregate to the f64 lane (continue-first; §21.7).
    /// Exact integer lane (#41 slice 6): while every numeric observation is an
    /// `I64`, the sum ALSO accumulates in `i128` (exact, associative). At output
    /// a Sum/Avg over an all-integer column uses `int_sum as f64` — the
    /// correctly-rounded value, identical however the input was partitioned —
    /// which is what makes an I64 `sum`/`avg` safe on the parallel
    /// partition→merge paths. Serial uses the same accumulator, so
    /// serial == parallel bit-identically by construction. Any non-integer
    /// numeric observation drops the lane (`int_only = false` → the plain f64
    /// path, exactly the old behavior).
    int_only: bool,
    int_sum: i128,
    int_overflow: bool,
    dec_scale: Option<u8>,
    dec_sum: i128,
    dec_min: i128,
    dec_max: i128,
    dec_overflow: bool,
    /// Exact datetime lane (design 23 / #53): set once a `Value::DateTime` is
    /// observed (a column shares one unit). `min`/`max` are kept as exact `i64`
    /// ticks — never `tick as f64` — so they are correct at nanosecond
    /// resolution (ticks past 2^53) and the result keeps the `DateTime` type.
    dt_unit: Option<TimeUnit>,
    dt_min: i64,
    dt_max: i64,
    /// Exact duration lane (design 23 / #57): set once a `Value::Duration` is
    /// observed (a column shares one unit). Unlike an instant, a span's
    /// `sum`/`avg` are meaningful — and, being integer, exact and associative,
    /// so they parallelize byte-identically. `sum` accumulates in `i128`
    /// (overflow → f64 fallback, continue-first); `min`/`max` stay `i64`.
    dur_unit: Option<TimeUnit>,
    dur_sum: i128,
    dur_min: i64,
    dur_max: i64,
    dur_overflow: bool,
    /// Exact date lane (#58): set once a `Value::Date` is observed. `min`/`max`
    /// are kept as the i32 epoch-day, so they are exact + associative (parallel
    /// byte-identical) and the result keeps the `Date` type (renders yyyy-MM-dd).
    has_date: bool,
    date_min: i32,
    date_max: i32,
    /// Exact time-of-day lane (#58): i64 ticks-since-midnight min/max.
    has_time: bool,
    time_min: i64,
    time_max: i64,
    /// `array_agg` buffer (§32 / #172): the group's **non-null** values in
    /// observe (source) order. The parallel merge appends a later partition's
    /// buffer after this one (source order), so the list — element order included
    /// — is byte-identical serial vs parallel. Bounded by group size.
    arr: Vec<Value>,
}

/// Extra fractional digits an exact decimal `avg` carries beyond the input scale
/// (the exact `sum/count` quotient is rounded half-even to this scale; §21.5).
const DEC_AVG_EXTRA: u8 = 6;

/// Integer division `num / den` (with `den > 0`) rounded **half-to-even** — the
/// deterministic rounding the exact decimal `avg` shares with the reader, so the
/// quotient is identical regardless of how the (exact) `sum` and `count` were
/// accumulated (serial or parallel partition→merge). `|r|*2` can't overflow:
/// `|r| < den` and `den` is a row count.
fn div_round_half_even(num: i128, den: i128) -> i128 {
    debug_assert!(den > 0);
    let q = num / den;
    let r = num % den;
    let twice = r.abs() * 2;
    // Round up (toward num's sign) when past the half, or exactly at the half with
    // an odd quotient (half-to-even); otherwise keep the truncated quotient.
    if twice > den || (twice == den && q % 2 != 0) {
        q + num.signum()
    } else {
        q
    }
}

impl AggAcc {
    pub(crate) fn new(func: AggFunc) -> Self {
        AggAcc {
            func,
            sum: 0.0,
            sum_sq: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            n: 0,
            non_null: 0,
            first: None,
            last: None,
            distinct: std::collections::HashSet::new(),
            values: Vec::new(),
            int_only: true,
            int_sum: 0,
            int_overflow: false,
            dec_scale: None,
            dec_sum: 0,
            dec_min: i128::MAX,
            dec_max: i128::MIN,
            dec_overflow: false,
            dt_unit: None,
            dt_min: i64::MAX,
            dt_max: i64::MIN,
            dur_unit: None,
            dur_sum: 0,
            dur_min: i64::MAX,
            dur_max: i64::MIN,
            dur_overflow: false,
            has_date: false,
            date_min: i32::MAX,
            date_max: i32::MIN,
            has_time: false,
            time_min: i64::MAX,
            time_max: i64::MIN,
            arr: Vec::new(),
        }
    }

    /// Observe one cell value for this aggregate. Numeric aggregates ignore
    /// non-numeric cells; first/last/count_distinct ignore empty cells.
    pub(crate) fn observe(&mut self, v: &Value) {
        match self.func {
            AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max | AggFunc::Std => {
                if let Some(x) = v.as_f64() {
                    self.sum += x;
                    self.sum_sq += x * x;
                    self.min = self.min.min(x);
                    self.max = self.max.max(x);
                    self.n += 1;
                    // Exact integer lane: accumulate i128 alongside the f64
                    // moments while the column stays all-integer.
                    if let Value::I64(i) = v {
                        if !self.int_overflow {
                            match self.int_sum.checked_add(*i as i128) {
                                Some(s) => self.int_sum = s,
                                None => self.int_overflow = true,
                            }
                        }
                    } else {
                        self.int_only = false;
                    }
                    // Exact decimal lane: accumulate the unscaled i128 in parallel
                    // with the f64 moments (the f64 side still backs `std` and the
                    // overflow fallback). A column shares one scale.
                    if let Value::Dec(d) = v {
                        let s = *self.dec_scale.get_or_insert(d.scale);
                        // Same-column values share the scale; rescale defensively.
                        let u = if d.scale == s {
                            Some(d.unscaled)
                        } else {
                            d.rescale(s).map(|r| r.unscaled)
                        };
                        match u.and_then(|u| self.dec_sum.checked_add(u).map(|s| (u, s))) {
                            Some((u, sum)) => {
                                self.dec_sum = sum;
                                self.dec_min = self.dec_min.min(u);
                                self.dec_max = self.dec_max.max(u);
                            }
                            None => self.dec_overflow = true,
                        }
                    }
                    // Exact datetime lane: keep min/max as i64 ticks (design 23 /
                    // #53). A column shares one unit. min/max are associative →
                    // byte-identical in parallel; sum/avg stay on the f64 side
                    // (not meaningful instants; not parallel-safe — engine gates).
                    if let Value::DateTime(t) = v {
                        self.dt_unit.get_or_insert(t.unit);
                        self.dt_min = self.dt_min.min(t.ticks);
                        self.dt_max = self.dt_max.max(t.ticks);
                    }
                    // Exact duration lane: sum (i128), min/max (i64). A column
                    // shares one unit. All associative → parallel byte-identical
                    // (and, unlike instants, sum/avg are meaningful). #57.
                    if let Value::Duration(d) = v {
                        self.dur_unit.get_or_insert(d.unit);
                        match self.dur_sum.checked_add(d.ticks as i128) {
                            Some(s) => self.dur_sum = s,
                            None => self.dur_overflow = true,
                        }
                        self.dur_min = self.dur_min.min(d.ticks);
                        self.dur_max = self.dur_max.max(d.ticks);
                    }
                    // Exact date lane: keep min/max as the i32 epoch-day (#58).
                    // Associative → byte-identical in parallel; the result keeps
                    // the Date type instead of degrading to a raw f64/int.
                    if let Value::Date(d) = v {
                        self.has_date = true;
                        self.date_min = self.date_min.min(d.epoch_day);
                        self.date_max = self.date_max.max(d.epoch_day);
                    }
                    // Exact time-of-day lane: i64 tick min/max (#58).
                    if let Value::Time(t) = v {
                        self.has_time = true;
                        self.time_min = self.time_min.min(t.ticks);
                        self.time_max = self.time_max.max(t.ticks);
                    }
                }
            }
            // COUNT(col): the number of non-null values (design 26 §26.2d).
            AggFunc::Count => {
                if !v.is_null() {
                    self.non_null += 1;
                }
            }
            // Distinct **non-null** values: a null is not a distinct value, but a
            // real empty string is (rectified from "non-empty" to "non-null").
            AggFunc::CountDistinct => {
                if !v.is_null() {
                    self.distinct.insert(v.to_string());
                }
            }
            // First/last **non-null** value (rectified from "non-empty": a real
            // empty string is now a value, a null is skipped). §26.2d.
            AggFunc::First => {
                if self.first.is_none() && !v.is_null() {
                    self.first = Some(v.to_string());
                }
            }
            AggFunc::Last => {
                if !v.is_null() {
                    self.last = Some(v.to_string());
                }
            }
            AggFunc::Pct(_) => {
                if let Some(x) = v.as_f64() {
                    self.values.push(x);
                }
            }
            // array_agg: the group's non-null values, in source order (§32 / #172).
            AggFunc::ArrayAgg => {
                if !v.is_null() {
                    self.arr.push(v.clone());
                }
            }
        }
    }

    /// Fold another partial accumulator (covering a *later* run of source rows)
    /// into this one — the deterministic merge that lets a group-by run on
    /// per-partition workers and recombine in **source order** (#41). `other`
    /// must be the same `func` and follow `self` in source order (so `first`
    /// keeps the earliest and `last` the latest). Exact lanes (i128 decimal sum,
    /// counts, min/max, buffered percentile values) merge byte-identically; the
    /// f64 moments are folded too but a *parallel* group-by is only enabled when
    /// no aggregate depends on f64 associativity (the engine gates that).
    pub(crate) fn merge(&mut self, other: &AggAcc) {
        self.sum += other.sum;
        self.sum_sq += other.sum_sq;
        // Exact integer lane: associative i128 (order-independent merge).
        self.int_only &= other.int_only;
        self.int_overflow |= other.int_overflow;
        if !self.int_overflow {
            match self.int_sum.checked_add(other.int_sum) {
                Some(s) => self.int_sum = s,
                None => self.int_overflow = true,
            }
        }
        self.n += other.n;
        self.non_null += other.non_null;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        // Exact decimal lane (associative i128); a column shares one scale.
        if let Some(os) = other.dec_scale {
            let scale = *self.dec_scale.get_or_insert(os);
            let ou = if os == scale {
                Some(other.dec_sum)
            } else {
                None
            };
            match ou.and_then(|ou| self.dec_sum.checked_add(ou)) {
                Some(s) => self.dec_sum = s,
                None => self.dec_overflow = true,
            }
            self.dec_min = self.dec_min.min(other.dec_min);
            self.dec_max = self.dec_max.max(other.dec_max);
        }
        self.dec_overflow |= other.dec_overflow;
        // Exact datetime lane (associative i64); a column shares one unit.
        if let Some(ou) = other.dt_unit {
            self.dt_unit.get_or_insert(ou);
            self.dt_min = self.dt_min.min(other.dt_min);
            self.dt_max = self.dt_max.max(other.dt_max);
        }
        // Exact duration lane (associative i128 sum / i64 min/max). #57.
        if let Some(ou) = other.dur_unit {
            self.dur_unit.get_or_insert(ou);
            match self.dur_sum.checked_add(other.dur_sum) {
                Some(s) => self.dur_sum = s,
                None => self.dur_overflow = true,
            }
            self.dur_min = self.dur_min.min(other.dur_min);
            self.dur_max = self.dur_max.max(other.dur_max);
        }
        self.dur_overflow |= other.dur_overflow;
        // Exact date lane (associative i32 min/max). #58.
        if other.has_date {
            self.has_date = true;
            self.date_min = self.date_min.min(other.date_min);
            self.date_max = self.date_max.max(other.date_max);
        }
        // Exact time-of-day lane (associative i64 min/max). #58.
        if other.has_time {
            self.has_time = true;
            self.time_min = self.time_min.min(other.time_min);
            self.time_max = self.time_max.max(other.time_max);
        }
        for s in &other.distinct {
            self.distinct.insert(s.clone());
        }
        // Source order: `self` precedes `other`, so the earliest non-empty
        // `first` and the latest non-empty `last` win.
        if self.first.is_none() {
            self.first = other.first.clone();
        }
        if other.last.is_some() {
            self.last = other.last.clone();
        }
        self.values.extend_from_slice(&other.values);
        // array_agg: `self` precedes `other` in source order, so appending keeps
        // the list — and its element order — deterministic across partitions.
        self.arr.extend(other.arr.iter().cloned());
    }

    /// Numeric aggregate value (sum/avg/min/max/std). `0.0` for an empty group.
    pub(crate) fn num_value(&self) -> f64 {
        // All-integer Sum/Avg: the exact i128 sum, correctly rounded to f64 —
        // partition-order independent (the f64 running sum is not, past 2^53).
        let exact_int = self.int_only && !self.int_overflow && self.n > 0;
        match self.func {
            AggFunc::Sum if exact_int => self.int_sum as f64,
            AggFunc::Sum => self.sum,
            AggFunc::Avg if exact_int => self.int_sum as f64 / self.n as f64,
            AggFunc::Avg => {
                if self.n > 0 {
                    self.sum / self.n as f64
                } else {
                    0.0
                }
            }
            AggFunc::Min => {
                if self.n > 0 {
                    self.min
                } else {
                    0.0
                }
            }
            AggFunc::Max => {
                if self.n > 0 {
                    self.max
                } else {
                    0.0
                }
            }
            // ddof=1 sample std needs ≥2 values; otherwise it falls to `_ => 0.0`.
            AggFunc::Std if self.n > 1 => {
                // Sample standard deviation (ddof=1): √((Σx² − Σx·mean)/(n−1)).
                let mean = self.sum / self.n as f64;
                let var = (self.sum_sq - self.sum * mean) / (self.n as f64 - 1.0);
                var.max(0.0).sqrt()
            }
            AggFunc::Pct(p) => self.percentile(p),
            _ => 0.0,
        }
    }

    /// Linear-interpolated percentile of the buffered values (numpy/pandas
    /// default: rank = p/100·(n−1), interpolate between the two nearest order
    /// statistics). `0.0` for an empty group. Sorts a clone, so the accumulator
    /// stays reusable; the buffer is bounded by group cardinality.
    fn percentile(&self, p: u8) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }
        let mut v = self.values.clone();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        if v.len() == 1 {
            return v[0];
        }
        let rank = (p as f64 / 100.0) * (v.len() - 1) as f64;
        let lo = rank.floor() as usize;
        let hi = rank.ceil() as usize;
        let frac = rank - lo as f64;
        v[lo] + (v[hi] - v[lo]) * frac
    }

    /// The decimal scale this accumulator saw, i.e. **lane membership** for the
    /// exact-decimal lane — `Some` even after an i128 overflow. `dec_value()`
    /// conflates "not a decimal lane" with "overflowed" (both `None`); the
    /// overflow surfacing in `GroupBy::finish` (#202) needs them apart so an
    /// overflowed decimal column can stay Decimal (null cells) instead of
    /// silently degrading to f64.
    pub(crate) fn dec_scale(&self) -> Option<u8> {
        self.dec_scale
    }

    /// Exact decimal result for `sum`/`min`/`max`/`avg` on a decimal column, or
    /// `None` when this aggregate isn't an exact-decimal one (then the caller uses
    /// the f64 `num_value`). `avg` rounds the exact `sum/count` quotient half-even
    /// to `scale + DEC_AVG_EXTRA`; an i128 overflow leaves it to the f64 fallback.
    pub(crate) fn dec_value(&self) -> Option<rivus_core::Decimal> {
        let scale = self.dec_scale?;
        if self.dec_overflow {
            return None;
        }
        match self.func {
            AggFunc::Sum => Some(rivus_core::Decimal::new(self.dec_sum, scale)),
            AggFunc::Min if self.n > 0 => Some(rivus_core::Decimal::new(self.dec_min, scale)),
            AggFunc::Max if self.n > 0 => Some(rivus_core::Decimal::new(self.dec_max, scale)),
            AggFunc::Avg if self.n > 0 => {
                let out_scale = scale.saturating_add(DEC_AVG_EXTRA);
                let mut factor: i128 = 1;
                for _ in 0..(out_scale - scale) {
                    factor = factor.checked_mul(10)?;
                }
                let num = self.dec_sum.checked_mul(factor)?;
                Some(rivus_core::Decimal::new(
                    div_round_half_even(num, self.n as i128),
                    out_scale,
                ))
            }
            _ => None,
        }
    }

    /// Exact datetime result for `min`/`max` on a datetime column, or `None`
    /// when this aggregate isn't an exact-datetime `min`/`max` (then the caller
    /// uses the f64 `num_value`). Keeps the `i64` ticks and the column's unit, so
    /// the result is exact at any resolution and stays the `DateTime` type. #53.
    pub(crate) fn dt_value(&self) -> Option<DateTime> {
        let unit = self.dt_unit?;
        match self.func {
            AggFunc::Min if self.n > 0 => Some(DateTime::new(self.dt_min, unit)),
            AggFunc::Max if self.n > 0 => Some(DateTime::new(self.dt_max, unit)),
            _ => None,
        }
    }

    /// Exact date result for `min`/`max` on a date column, or `None` otherwise.
    /// Keeps the i32 epoch-day so the result is exact and stays the `Date` type
    /// (renders `yyyy-MM-dd` instead of a raw integer). #58.
    pub(crate) fn date_value(&self) -> Option<rivus_core::Date> {
        if !self.has_date {
            return None;
        }
        match self.func {
            AggFunc::Min if self.n > 0 => Some(rivus_core::Date::new(self.date_min)),
            AggFunc::Max if self.n > 0 => Some(rivus_core::Date::new(self.date_max)),
            _ => None,
        }
    }

    /// Exact time-of-day result for `min`/`max` on a time column, keeping the
    /// i64 ticks so the result stays the `Time` type (renders HH:mm:ss). #58.
    pub(crate) fn time_value(&self) -> Option<rivus_core::TimeOfDay> {
        if !self.has_time {
            return None;
        }
        match self.func {
            AggFunc::Min if self.n > 0 => Some(rivus_core::TimeOfDay::new(
                self.time_min,
                rivus_core::TimeUnit::Sec,
            )),
            AggFunc::Max if self.n > 0 => Some(rivus_core::TimeOfDay::new(
                self.time_max,
                rivus_core::TimeUnit::Sec,
            )),
            _ => None,
        }
    }

    /// Exact duration result for `sum`/`avg`/`min`/`max` on a duration column,
    /// or `None` otherwise (caller uses f64). `sum` is the exact i128 total,
    /// `avg` rounds `sum/count` half-to-even (a whole tick count, like the
    /// decimal avg), `min`/`max` are the i64 extremes — all exact and
    /// type-preserving. An i128 sum overflow falls back to f64. #57.
    pub(crate) fn dur_value(&self) -> Option<rivus_core::Duration> {
        let unit = self.dur_unit?;
        if self.dur_overflow {
            return None;
        }
        let mk = |t: i128| {
            rivus_core::Duration::new(t.clamp(i64::MIN as i128, i64::MAX as i128) as i64, unit)
        };
        match self.func {
            AggFunc::Sum => Some(mk(self.dur_sum)),
            AggFunc::Min if self.n > 0 => Some(rivus_core::Duration::new(self.dur_min, unit)),
            AggFunc::Max if self.n > 0 => Some(rivus_core::Duration::new(self.dur_max, unit)),
            AggFunc::Avg if self.n > 0 => {
                Some(mk(div_round_half_even(self.dur_sum, self.n as i128)))
            }
            _ => None,
        }
    }

    pub(crate) fn distinct_count(&self) -> i64 {
        self.distinct.len() as i64
    }
    /// `COUNT(col)` — the number of non-null values observed (§26.2d).
    pub(crate) fn count_value(&self) -> i64 {
        self.non_null
    }
    /// First/last non-null cell, or `None` for an all-null group (§26.2d) — the
    /// caller renders that as a null in the output column.
    pub(crate) fn first_opt(&self) -> Option<&str> {
        self.first.as_deref()
    }
    pub(crate) fn last_opt(&self) -> Option<&str> {
        self.last.as_deref()
    }
    /// The `array_agg` buffer: the group's non-null values in source order.
    pub(crate) fn arr_values(&self) -> &[Value] {
        &self.arr
    }
    #[cfg(test)]
    pub(crate) fn first_str(&self) -> &str {
        self.first.as_deref().unwrap_or("")
    }
    #[cfg(test)]
    pub(crate) fn last_str(&self) -> &str {
        self.last.as_deref().unwrap_or("")
    }
}

pub(crate) struct GroupState {
    /// The group's key values, one per group key (in key order). Stored so the
    /// output can emit one column per key (the map key is a packed composite).
    key_parts: Vec<String>,
    count: i64,
    accs: Vec<AggAcc>,
}

pub(crate) struct GroupBy {
    keys: Vec<PathExpr>,
    aggs: Vec<(AggFunc, String)>,
    /// The canonical, **sorted** group store: everything emitted (finish) or
    /// merged reads this, so output row order is the composite-key order — the
    /// same whether groups arrived via a serial stream or a parallel merge.
    groups: BTreeMap<String, GroupState>,
    /// Row-hot accumulation happens here instead: one Fx hash lookup per row,
    /// where the `BTreeMap` costs O(log g) *string comparisons* per row twice
    /// (~93ms/file on the 10M group standard). [`GroupBy::seal`] drains it
    /// into `groups` before anything is observed, so hash-map iteration order
    /// never reaches the output — byte-identical by construction.
    scratch: std::collections::HashMap<String, GroupState, crate::fxhash::FxBuild>,
    emitted: bool,
    /// Nested key-path structural misses (§32.8③), accumulated across chunks and
    /// surfaced once on finish (never-silent, not per-chunk spam).
    key_fails: u64,
}

impl GroupBy {
    pub(crate) fn new(keys: Vec<PathExpr>, aggs: Vec<(AggFunc, String)>) -> Self {
        GroupBy {
            keys,
            aggs,
            groups: BTreeMap::new(),
            scratch: std::collections::HashMap::default(),
            emitted: false,
            key_fails: 0,
        }
    }

    /// Row-level observation for the fused worker loop (design/41 Stage A):
    /// the engine hands the finished composite key, a lazy key-parts renderer
    /// (run only when the group is first seen), and one optional `Value` per
    /// aggregate (`None` = the agg's column is absent for this row — exactly
    /// a `agg_idx` miss in [`GroupBy::process`], which observes nothing).
    /// State handling is [`GroupBy::process`]'s inner loop verbatim — same
    /// scratch map, same `GroupState`, same `AggAcc::observe` — so a fused
    /// stream is byte-identical to feeding whole chunks.
    pub(crate) fn observe_row(
        &mut self,
        composite: &str,
        parts: impl FnOnce() -> Vec<String>,
        vals: &[Option<Value>],
    ) {
        match self.scratch.get_mut(composite) {
            Some(state) => {
                state.count += 1;
                for (j, v) in vals.iter().enumerate() {
                    if let Some(v) = v {
                        state.accs[j].observe(v);
                    }
                }
            }
            None => {
                let mut state = GroupState {
                    key_parts: parts(),
                    count: 1,
                    accs: self.aggs.iter().map(|(f, _)| AggAcc::new(*f)).collect(),
                };
                for (j, v) in vals.iter().enumerate() {
                    if let Some(v) = v {
                        state.accs[j].observe(v);
                    }
                }
                self.scratch.insert(composite.to_string(), state);
            }
        }
    }

    /// The aggregate count (arity of the `vals` slice `observe_row` expects).
    pub(crate) fn agg_count(&self) -> usize {
        self.aggs.len()
    }

    /// Fold the row-hot scratch into the sorted canonical store. Called before
    /// every read of `groups` (finish, merge). In the real lifecycles (process*
    /// → finish; process* → merge_from) every scratch entry is a pure move —
    /// but if a group somehow exists on both sides, fold scratch (the later
    /// observations) into it in stream order rather than silently replacing.
    fn seal(&mut self) {
        for (key, state) in self.scratch.drain() {
            match self.groups.get_mut(&key) {
                Some(s) => {
                    s.count += state.count;
                    for (a, oa) in s.accs.iter_mut().zip(state.accs.iter()) {
                        a.merge(oa);
                    }
                }
                None => {
                    self.groups.insert(key, state);
                }
            }
        }
    }

    /// Fold a *later* partition's partial group state into this one (the
    /// deterministic, source-ordered merge for parallel group-by; #41). Groups
    /// present only in `other` are appended (BTreeMap keeps key order, so the
    /// output row order is identical to a serial run); shared groups merge their
    /// counts and per-aggregate accumulators via [`AggAcc::merge`]. `other` must
    /// have the same keys and aggregates and follow `self` in source order.
    pub(crate) fn merge_from(&mut self, mut other: GroupBy) {
        // Both sides fold their row-hot scratch into the sorted store first,
        // so the merge below sees complete, canonical group states.
        self.seal();
        other.seal();
        // Nested key-path misses (§32.8③) sum across merged partitions, like the
        // per-group counts, so the finish-time surface is the true total.
        self.key_fails += other.key_fails;
        for (key, ostate) in other.groups {
            match self.groups.get_mut(&key) {
                Some(s) => {
                    s.count += ostate.count;
                    for (a, oa) in s.accs.iter_mut().zip(ostate.accs.iter()) {
                        a.merge(oa);
                    }
                }
                None => {
                    self.groups.insert(key, ostate);
                }
            }
        }
    }
}

/// Whether a group-by over these aggregates is **byte-identical** under a
/// partition→merge (parallel) execution, given the resolved type of each
/// aggregated column (#41). `min`/`max`/`count`/`count_distinct`/`first`/`last`/
/// percentile are always safe (associative or buffered+sorted); `sum`/`avg` are
/// safe only on an exact lane (decimal — i128 associative); `std` and `sum`/`avg`
/// on f64/integer columns are NOT (f64 addition is non-associative; integer sum
/// rides the f64 accumulator) and keep the serial path.
pub(crate) fn group_parallel_safe(
    aggs: &[(AggFunc, String)],
    col_type: impl Fn(&str) -> Option<DataType>,
) -> bool {
    aggs.iter().all(|(f, col)| match f {
        // COUNT(col) is a non-null tally → associative (counts sum) → safe.
        AggFunc::Count
        | AggFunc::Min
        | AggFunc::Max
        | AggFunc::CountDistinct
        | AggFunc::First
        | AggFunc::Last
        // array_agg concatenates in source order on merge (like first/last), so
        // the partition→merge result — element order included — is byte-identical
        // to serial. Integer/order-based, never the f64 #41 trap.
        | AggFunc::ArrayAgg
        | AggFunc::Pct(_) => true,
        // Exact integer lanes (decimal i128, duration i64) make sum/avg
        // associative → parallel byte-identical; f64 sum/avg are not. #57.
        // I64 joins the exact club via the int lane (i128 accumulation with the
        // correctly-rounded f64 output — see `AggAcc::num_value`).
        AggFunc::Sum | AggFunc::Avg => {
            matches!(
                col_type(col),
                Some(DataType::I64 | DataType::Decimal { .. } | DataType::Duration { .. })
            )
        }
        AggFunc::Std => false,
    })
}

/// Build a `GroupBy` operator from a `GroupBy` op (for the parallel scheduler,
/// which needs the concrete type to merge per-worker state). `None` for any
/// other op.
pub(crate) fn new_group(op: &Op) -> Option<GroupBy> {
    match op {
        // §32 s4b: `PathExpr` keys — a bare key resolves on the flat fast path, a
        // nested key is materialized at resolution time (`resolve_key_indices`).
        Op::GroupBy { keys, aggs } => Some(GroupBy::new(keys.clone(), aggs.clone())),
        _ => None,
    }
}

impl Operator for GroupBy {
    fn process(&mut self, _from: NodeId, chunk: Chunk, ctx: &mut OpCtx) -> Vec<Chunk> {
        // Resolve every group-key column index (§32 s4b): a bare key on the flat
        // fast path, a nested key materialized into a derived column appended to
        // the chunk. An unknown *bare* key warns once and drops the chunk
        // (continue-first — a later, well-formed chunk still aggregates).
        let mut chunk = chunk;
        let mut nested_fails = 0u64;
        let resolved = eval::resolve_key_indices(&mut chunk, &self.keys, &mut nested_fails);
        self.key_fails += nested_fails;
        let mut key_idx = Vec::with_capacity(self.keys.len());
        for (k, idx) in self.keys.iter().zip(&resolved) {
            match idx {
                Some(i) => key_idx.push(*i),
                None => {
                    ctx.raise(
                        ErrorEvent::new(
                            Severity::Warn,
                            ErrorScope::Chunk,
                            format!("group: unknown key '{}'", k.column_name()),
                        )
                        .at_node(ctx.label.clone()),
                    );
                    return Vec::new();
                }
            }
        }
        // Resolve aggregate column indices once per chunk.
        let agg_idx: Vec<Option<usize>> = self
            .aggs
            .iter()
            .map(|(_, c)| chunk.schema.index_of(c))
            .collect();
        // The aggregate funcs, copied out so the group-insert closure doesn't
        // borrow `self.aggs` while `self.groups` is mutably borrowed.
        let funcs: Vec<AggFunc> = self.aggs.iter().map(|(f, _)| *f).collect();

        // Composite map key built into a REUSED buffer across rows: the previous
        // form allocated a fresh `String` per row AND a throwaway `Vec<String>`
        // (`parts`) per row — but only *new* groups need `parts`, so 3M rows over
        // 16 groups wasted ~3M allocations each. Now the key reuses one buffer and
        // `parts` is rendered only when a group is first inserted. Byte-identical.
        let mut composite = String::new();
        for row in 0..chunk.len {
            // Dedup composite tags null distinctly so a `null` key folds into one
            // group and never collides with a real value (§26.2b).
            composite.clear();
            for (j, &i) in key_idx.iter().enumerate() {
                if j > 0 {
                    composite.push('\u{1f}');
                }
                push_group_key_field(&mut composite, &chunk, i, row);
            }
            // Hot path: an existing group is ONE Fx-hash lookup, no allocation
            // (`String: Borrow<str>` looks the buffer up directly). Only a new
            // group allocates: its output key parts, its owned map key, and
            // the accumulators.
            match self.scratch.get_mut(composite.as_str()) {
                Some(state) => {
                    state.count += 1;
                    for (j, idx) in agg_idx.iter().enumerate() {
                        if let Some(ci) = idx {
                            let v = chunk.value(row, *ci);
                            state.accs[j].observe(&v);
                        }
                    }
                }
                None => {
                    let parts: Vec<String> = key_idx
                        .iter()
                        .map(|&i| chunk.value(row, i).to_string())
                        .collect();
                    let mut state = GroupState {
                        key_parts: parts,
                        count: 1,
                        accs: funcs.iter().map(|f| AggAcc::new(*f)).collect(),
                    };
                    for (j, idx) in agg_idx.iter().enumerate() {
                        if let Some(ci) = idx {
                            let v = chunk.value(row, *ci);
                            state.accs[j].observe(&v);
                        }
                    }
                    self.scratch.insert(composite.clone(), state);
                }
            }
        }
        Vec::new() // group is a materializing boundary; output on finish
    }

    fn finish(&mut self, ctx: &mut OpCtx) -> Vec<Chunk> {
        if self.emitted {
            return Vec::new();
        }
        self.emitted = true;
        // Fold the row-hot scratch into the sorted store: every emit below
        // iterates `groups` in composite-key order, exactly as before.
        self.seal();
        super::surface_key_path_fails(self.key_fails, "group", ctx);

        // One Str column per group key (values pulled from each group's stored
        // key parts), then the count, then the aggregate columns.
        let mut fields: Vec<Field> = self
            .keys
            .iter()
            .map(|k| Field::new(k.column_name(), DataType::Str))
            .collect();
        fields.push(Field::new("count", DataType::I64));

        let mut columns: Vec<Column> = Vec::with_capacity(self.keys.len() + 1 + self.aggs.len());
        for ki in 0..self.keys.len() {
            let col: StrColumn = self
                .groups
                .values()
                .map(|s| s.key_parts[ki].as_str())
                .collect();
            columns.push(Column::str(col));
        }
        let counts: Vec<i64> = self.groups.values().map(|s| s.count).collect();
        columns.push(Column::i64(counts));

        for (j, (func, col)) in self.aggs.iter().enumerate() {
            let name = format!("{}_{}", func.label(), col);
            let (dtype, column) = match func {
                // COUNT(col): the per-group non-null tally (§26.2d).
                AggFunc::Count => (
                    DataType::I64,
                    Column::i64(
                        self.groups
                            .values()
                            .map(|s| s.accs[j].count_value())
                            .collect(),
                    ),
                ),
                AggFunc::CountDistinct => (
                    DataType::I64,
                    Column::i64(
                        self.groups
                            .values()
                            .map(|s| s.accs[j].distinct_count())
                            .collect(),
                    ),
                ),
                AggFunc::First | AggFunc::Last => {
                    // An all-null group has no first/last non-null value → the
                    // output cell is null (§26.2d), carried by the validity bitmap.
                    let mut sc = StrColumn::default();
                    let mut valid = Vec::with_capacity(self.groups.len());
                    for s in self.groups.values() {
                        let cell = if matches!(func, AggFunc::First) {
                            s.accs[j].first_opt()
                        } else {
                            s.accs[j].last_opt()
                        };
                        sc.push(cell.unwrap_or(""));
                        valid.push(cell.is_some());
                    }
                    (
                        DataType::Str,
                        Column::new(ColumnData::Str(sc), Validity::from_bits(&valid)),
                    )
                }
                // array_agg → a List lane (§32 / #172): one child column over all
                // groups' flattened non-null values (group order), with per-group
                // offsets. Source-ordered (observe + merge), so the list and its
                // element order are byte-identical serial vs parallel. The list is
                // always a real value — an empty group is `[]` (validity 1), never
                // null (the inverse of `explode`, whose empty list yields 0 rows).
                AggFunc::ArrayAgg => {
                    let mut offsets: Vec<i32> = Vec::with_capacity(self.groups.len() + 1);
                    offsets.push(0);
                    let mut flat: Vec<Value> = Vec::new();
                    let mut acc = 0i32;
                    for s in self.groups.values() {
                        let items = s.accs[j].arr_values();
                        acc += items.len() as i32;
                        flat.extend(items.iter().cloned());
                        offsets.push(acc);
                    }
                    let child = eval::column_from_values(flat);
                    let elem = Field::new("item", child.data().dtype());
                    let list = Column::new(
                        ColumnData::List(rivus_core::ListColumn {
                            offsets,
                            child: Box::new(child),
                        }),
                        Validity::all_valid(),
                    );
                    fields.push(Field::list(name, elem));
                    columns.push(list);
                    continue;
                }
                // sum/avg/min/max/std/pct. On a decimal column these stay exact
                // (i128) when every group produced an exact result; if any group
                // overflowed i128 the whole column degrades to f64 (continue-first,
                // §21.7) so the column stays one uniform type.
                _ => {
                    // Exact date min/max → keep the Date lane (i32 epoch-day),
                    // never a raw f64/int column. #58.
                    let date_ok = matches!(func, AggFunc::Min | AggFunc::Max)
                        && !self.groups.is_empty()
                        && self
                            .groups
                            .values()
                            .all(|s| s.accs[j].date_value().is_some());
                    if date_ok {
                        let epoch_days = self
                            .groups
                            .values()
                            .map(|s| s.accs[j].date_value().unwrap().epoch_day)
                            .collect();
                        fields.push(Field::new(name, DataType::Date));
                        columns.push(Column::date(epoch_days));
                        continue;
                    }
                    // Exact time-of-day min/max → keep the Time lane (i64 ticks),
                    // never a raw int. #58.
                    let time_ok = matches!(func, AggFunc::Min | AggFunc::Max)
                        && !self.groups.is_empty()
                        && self
                            .groups
                            .values()
                            .all(|s| s.accs[j].time_value().is_some());
                    if time_ok {
                        let ticks = self
                            .groups
                            .values()
                            .map(|s| s.accs[j].time_value().unwrap().ticks)
                            .collect();
                        fields.push(Field::new(name, DataType::Time));
                        columns.push(Column::time(ticks));
                        continue;
                    }
                    // Exact datetime min/max → keep the DateTime lane (i64 ticks,
                    // same unit), never an f64 column. #53.
                    let dt_ok = matches!(func, AggFunc::Min | AggFunc::Max)
                        && !self.groups.is_empty()
                        && self.groups.values().all(|s| s.accs[j].dt_value().is_some());
                    if dt_ok {
                        let dts: Vec<DateTime> = self
                            .groups
                            .values()
                            .map(|s| s.accs[j].dt_value().unwrap())
                            .collect();
                        let unit = dts[0].unit;
                        let ticks = dts.iter().map(|d| d.ticks).collect();
                        fields.push(Field::new(name, DataType::DateTime { unit }));
                        columns.push(Column::datetime(DtColumn { ticks, unit }));
                        continue;
                    }
                    // Exact duration sum/avg/min/max → keep the Duration lane
                    // (i128 sum / i64 extremes), never an f64 column. #57.
                    let dur_ok = matches!(
                        func,
                        AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max
                    ) && !self.groups.is_empty()
                        && self
                            .groups
                            .values()
                            .all(|s| s.accs[j].dur_value().is_some());
                    if dur_ok {
                        let durs: Vec<rivus_core::Duration> = self
                            .groups
                            .values()
                            .map(|s| s.accs[j].dur_value().unwrap())
                            .collect();
                        let unit = durs[0].unit;
                        let ticks = durs.iter().map(|d| d.ticks).collect();
                        fields.push(Field::new(name, DataType::Duration { unit }));
                        columns.push(Column::duration(rivus_core::DurColumn { ticks, unit }));
                        continue;
                    }
                    // Exact decimal lane (sum/avg/min/max). Exactness is the
                    // lane's whole contract (#202): a group whose i128
                    // accumulator overflowed must NOT silently degrade the
                    // column to f64 — that is a wrong money total with no
                    // warning. Instead the column stays Decimal, the overflowed
                    // cells become null (validity 0, continue-first), and the
                    // loss is surfaced once on the error stream (never-silent).
                    let is_dec_lane = matches!(
                        func,
                        AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max
                    ) && !self.groups.is_empty()
                        && self
                            .groups
                            .values()
                            .all(|s| s.accs[j].dec_scale().is_some());
                    if is_dec_lane {
                        let vals: Vec<Option<rivus_core::Decimal>> = self
                            .groups
                            .values()
                            .map(|s| s.accs[j].dec_value())
                            .collect();
                        let overflowed = vals.iter().filter(|v| v.is_none()).count();
                        if overflowed > 0 {
                            ctx.raise(
                                ErrorEvent::new(
                                    Severity::Recoverable,
                                    ErrorScope::Chunk,
                                    format!(
                                        "decimal {} of column '{col}' overflowed i128 in \
                                         {overflowed} group(s); those cells are null — the \
                                         exact lane cannot represent the value",
                                        func.label()
                                    ),
                                )
                                .at_node(ctx.label.clone()),
                            );
                        }
                        // All groups share the column's scale (sum/min/max) or
                        // scale+extra (avg), so the output scale is uniform;
                        // take it from any exact cell, else (all overflowed)
                        // from the accumulator's base scale — display-only then,
                        // since every cell is null.
                        let scale = vals
                            .iter()
                            .flatten()
                            .next()
                            .map(|d| d.scale)
                            .or_else(|| {
                                self.groups
                                    .values()
                                    .next()
                                    .and_then(|s| s.accs[j].dec_scale())
                            })
                            .unwrap_or(0);
                        let unscaled: Vec<i128> =
                            vals.iter().map(|v| v.map_or(0, |d| d.unscaled)).collect();
                        let valid: Vec<bool> = vals.iter().map(|v| v.is_some()).collect();
                        fields.push(Field::new(name, DataType::Decimal { scale }));
                        columns.push(Column::new(
                            ColumnData::Dec(rivus_core::DecColumn { unscaled, scale }),
                            Validity::from_bits(&valid),
                        ));
                        continue;
                    }
                    {
                        (
                            DataType::F64,
                            Column::f64(
                                self.groups
                                    .values()
                                    .map(|s| s.accs[j].num_value())
                                    .collect(),
                            ),
                        )
                    }
                }
            };
            fields.push(Field::new(name, dtype));
            columns.push(column);
        }

        let id = ctx.fresh_id();
        vec![Chunk::new(id, Arc::new(Schema::new(fields)), columns)]
    }
}

// ---------------------------------------------------------------------- merge

/// Identity forwarder. Used for `+` merge (n inputs, one output) and as the
/// structural pass-through at a `->` branch point.
pub(crate) struct Merge;

impl Operator for Merge {
    fn process(&mut self, _from: NodeId, chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        vec![chunk]
    }
}
