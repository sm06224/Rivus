//! Scalar values and logical data types.
//!
//! Rivus follows "execution-aware typing" (Master principle #7): a logical
//! `DataType` is a *hint for which execution lane* a column should ride, not a
//! rigid memory contract. The MVP collapses the numeric lanes onto `i64`/`f64`,
//! but the `DataType` enum is shaped so the SIMD / decimal / bignum lanes
//! described in `docs/design/06-type-system.md` can be added without churn.

use std::fmt;

/// An exact base-10 fixed-point value: `unscaled × 10^(−scale)`. The building
/// block of the **decimal lane** (`docs/design/21-exact-decimal.md`): because
/// the value is an integer (`i128`) plus a fixed scale, addition is *exact and
/// associative*, so a parallel partition-then-merge reduction reproduces a
/// serial fold byte-for-byte — the property f64 cannot give. Opt-in (`--exact`
/// / `:decimal`); the default numeric lanes stay `i64`/`f64`.
///
/// Equality is **numeric** (`1` == `1.00`) and agrees with [`PartialOrd`], so the
/// two are never contradictory. Within one column every value shares a scale, so
/// this only matters for cross-scale scalar comparisons.
#[derive(Debug, Clone, Copy)]
pub struct Decimal {
    /// The value scaled to an integer: `1234` with `scale=2` means `12.34`.
    pub unscaled: i128,
    /// Number of fractional digits (the power of ten the value is divided by).
    pub scale: u8,
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(std::cmp::Ordering::Equal)
    }
}

impl Decimal {
    pub fn new(unscaled: i128, scale: u8) -> Self {
        Decimal { unscaled, scale }
    }

    /// `10^scale` as `i128` (the divisor that maps `unscaled` to the real value).
    fn pow10(scale: u8) -> i128 {
        let mut p: i128 = 1;
        for _ in 0..scale {
            p *= 10;
        }
        p
    }

    /// Re-express this value at a different `scale`, rounding half-to-even when
    /// reducing precision. Used to align operands and to round division results
    /// deterministically (§21.5). Returns `None` only on `i128` overflow when
    /// scaling *up* (caller falls back / warns, continue-first).
    pub fn rescale(&self, target: u8) -> Option<Decimal> {
        use std::cmp::Ordering;
        match target.cmp(&self.scale) {
            Ordering::Equal => Some(*self),
            Ordering::Greater => {
                // More fractional digits: multiply (exact, may overflow).
                let factor = Self::pow10(target - self.scale);
                self.unscaled
                    .checked_mul(factor)
                    .map(|u| Decimal::new(u, target))
            }
            Ordering::Less => {
                // Fewer digits: divide and round half-to-even on the remainder.
                let factor = Self::pow10(self.scale - target);
                let q = self.unscaled / factor;
                let r = self.unscaled % factor;
                let half = factor / 2;
                let ar = r.abs();
                let rounded = match ar.cmp(&half) {
                    Ordering::Less => q,
                    Ordering::Greater => q + self.unscaled.signum(),
                    Ordering::Equal => {
                        // Exactly half: round to even (banker's rounding).
                        if q % 2 == 0 {
                            q
                        } else {
                            q + self.unscaled.signum()
                        }
                    }
                };
                Some(Decimal::new(rounded, target))
            }
        }
    }

    /// Exact sum of two decimals (operands aligned to the larger scale). `None`
    /// on `i128` overflow (the caller degrades to f64 + warning, continue-first).
    pub fn checked_add(&self, other: &Decimal) -> Option<Decimal> {
        let scale = self.scale.max(other.scale);
        let a = self.rescale(scale)?;
        let b = other.rescale(scale)?;
        a.unscaled
            .checked_add(b.unscaled)
            .map(|u| Decimal::new(u, scale))
    }

    /// Numeric view for cross-lane comparison / fallback (lossy for huge values,
    /// exact for the magnitudes decimal targets).
    pub fn to_f64(&self) -> f64 {
        self.unscaled as f64 / Self::pow10(self.scale) as f64
    }

    /// Parse decimal **text** (`"12.34"`, `"-0.5"`, `"100"`, `"+3.0"`, `".5"`,
    /// `"5."`) into a `Decimal` at exactly `scale` fractional digits — **exact,
    /// never via `f64`**. Excess fractional digits are rounded half-to-even (the
    /// same deterministic rule as [`rescale`](Self::rescale), which this reuses),
    /// fewer are zero-padded. Returns `None` on malformed input (non-digit, empty,
    /// multiple dots, exponent) or `i128` overflow, so the reader can route a bad
    /// cell to the error stream (continue-first) instead of panicking.
    pub fn parse_scaled(s: &str, scale: u8) -> Option<Decimal> {
        let (neg, rest) = match s.as_bytes().first() {
            Some(b'-') => (true, &s[1..]),
            Some(b'+') => (false, &s[1..]),
            _ => (false, s),
        };
        let (int_part, frac_part) = match rest.split_once('.') {
            Some((i, f)) => (i, f),
            None => (rest, ""),
        };
        // At least one digit overall; every remaining byte must be an ASCII digit.
        if int_part.is_empty() && frac_part.is_empty() {
            return None;
        }
        if !int_part.bytes().all(|b| b.is_ascii_digit())
            || !frac_part.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        // The text's own scale is its fractional-digit count; build the unscaled
        // magnitude exactly, then round to the requested scale.
        let natural_scale = u8::try_from(frac_part.len()).ok()?;
        let mut mag: i128 = 0;
        for &b in int_part.as_bytes().iter().chain(frac_part.as_bytes()) {
            mag = mag.checked_mul(10)?.checked_add((b - b'0') as i128)?;
        }
        let unscaled = if neg { -mag } else { mag };
        Decimal::new(unscaled, natural_scale).rescale(scale)
    }

    /// Count the fractional digits in decimal text (`"12.5"` → 1, `"7"` → 0),
    /// or `None` if the text is not a valid decimal. Used by the reader's
    /// auto-scale pass to pick a column scale = the max fractional width seen.
    pub fn fractional_digits(s: &str) -> Option<u8> {
        let rest = match s.as_bytes().first() {
            Some(b'-') | Some(b'+') => &s[1..],
            _ => s,
        };
        let (int_part, frac_part) = match rest.split_once('.') {
            Some((i, f)) => (i, f),
            None => (rest, ""),
        };
        if int_part.is_empty() && frac_part.is_empty() {
            return None;
        }
        if !int_part.bytes().all(|b| b.is_ascii_digit())
            || !frac_part.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        u8::try_from(frac_part.len()).ok()
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Compare on a common scale without losing precision: scale both up to
        // the larger scale (exact unless i128 overflows, then fall back to f64).
        let scale = self.scale.max(other.scale);
        match (self.rescale(scale), other.rescale(scale)) {
            (Some(a), Some(b)) => a.unscaled.partial_cmp(&b.unscaled),
            _ => self.to_f64().partial_cmp(&other.to_f64()),
        }
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.scale == 0 {
            return write!(f, "{}", self.unscaled);
        }
        let div = Self::pow10(self.scale);
        let sign = if self.unscaled < 0 { "-" } else { "" };
        let abs = self.unscaled.unsigned_abs();
        let int = abs / div as u128;
        let frac = abs % div as u128;
        // Zero-pad the fraction to exactly `scale` digits.
        write!(
            f,
            "{sign}{int}.{frac:0>width$}",
            width = self.scale as usize
        )
    }
}

/// Resolution of the datetime lane's epoch `ticks` (design 23). Default `Sec`
/// (a `yyMMddhhmmss` timestamp is second-precision).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeUnit {
    Sec,
    Milli,
    Micro,
    Nano,
}

impl TimeUnit {
    /// Ticks per second.
    pub fn per_sec(self) -> i64 {
        match self {
            TimeUnit::Sec => 1,
            TimeUnit::Milli => 1_000,
            TimeUnit::Micro => 1_000_000,
            TimeUnit::Nano => 1_000_000_000,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            TimeUnit::Sec => "s",
            TimeUnit::Milli => "ms",
            TimeUnit::Micro => "us",
            TimeUnit::Nano => "ns",
        }
    }
    pub fn parse(s: &str) -> Option<TimeUnit> {
        match s {
            "s" | "sec" => Some(TimeUnit::Sec),
            "ms" | "milli" => Some(TimeUnit::Milli),
            "us" | "micro" => Some(TimeUnit::Micro),
            "ns" | "nano" => Some(TimeUnit::Nano),
            _ => None,
        }
    }
}

/// Days since 1970-01-01 for a proleptic-Gregorian civil date (Howard Hinnant's
/// algorithm; std-only, exact for the full i64 range).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Inverse of [`days_from_civil`]: `(year, month, day)` from days-since-epoch.
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// A timestamp on the **datetime lane** (design 23): integer `ticks` since the
/// Unix epoch in `unit` resolution (UTC; no timezone yet). The integer form is
/// exact and associative — like the decimal lane — so `min`/`max`/`count`/`first`/
/// `last` parallelize byte-identically. Comparison is across units exact (i128).
#[derive(Debug, Clone, Copy)]
pub struct DateTime {
    pub ticks: i64,
    pub unit: TimeUnit,
}

impl PartialEq for DateTime {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(std::cmp::Ordering::Equal)
    }
}

impl PartialOrd for DateTime {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Compare absolute instants regardless of unit (cross-multiply in i128).
        let a = self.ticks as i128 * other.unit.per_sec() as i128;
        let b = other.ticks as i128 * self.unit.per_sec() as i128;
        a.partial_cmp(&b)
    }
}

impl DateTime {
    /// Common fixed-width / ISO timestamp formats, tried in order, for
    /// auto-inferring a bare `:datetime` column or a datetime *literal* in a
    /// predicate (design 23). Shared by the reader and the comparison path so
    /// the two agree on what a given text means.
    pub const AUTO_FORMATS: &'static [&'static str] = &[
        "yyyy-MM-ddTHH:mm:ss",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd",
        "yyyyMMddHHmmss",
        "yyMMddHHmmss",
        "yyyyMMdd",
    ];

    pub fn new(ticks: i64, unit: TimeUnit) -> Self {
        DateTime { ticks, unit }
    }

    /// Parse `s` by trying each [`AUTO_FORMATS`] entry in order (first match
    /// wins), at `unit`. `None` if none match (continue-first at the call site).
    ///
    /// [`AUTO_FORMATS`]: DateTime::AUTO_FORMATS
    pub fn parse_auto(s: &str, unit: TimeUnit) -> Option<DateTime> {
        let s = s.trim();
        Self::AUTO_FORMATS
            .iter()
            .find_map(|f| Self::parse_with_format(s, f, unit))
    }

    /// `(year, month, day, hour, minute, second)` in UTC (whole-second part).
    pub fn fields(&self) -> (i64, i64, i64, i64, i64, i64) {
        let secs = self.ticks.div_euclid(self.unit.per_sec());
        let day = secs.div_euclid(86400);
        let sod = secs.rem_euclid(86400);
        let (y, m, d) = civil_from_days(day);
        (y, m, d, sod / 3600, (sod / 60) % 60, sod % 60)
    }

    /// Truncate to a calendar/clock boundary (`year`/`month`/`day`/`hour`/
    /// `minute`/`second`), returning a `DateTime` at the same `unit` — the
    /// time-series group-by key (design 23). Integer math, so byte-identical
    /// across execution strategies. An unknown field truncates to the second.
    pub fn truncated(&self, field: &str) -> DateTime {
        let (y, mo, d, h, mi, se) = self.fields();
        let (y, mo, d, h, mi, se) = match field {
            "year" => (y, 1, 1, 0, 0, 0),
            "month" => (y, mo, 1, 0, 0, 0),
            "day" => (y, mo, d, 0, 0, 0),
            "hour" => (y, mo, d, h, 0, 0),
            "minute" => (y, mo, d, h, mi, 0),
            _ => (y, mo, d, h, mi, se), // "second" / unknown → whole second
        };
        let secs = days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + se;
        DateTime::new(secs * self.unit.per_sec(), self.unit)
    }

    /// Parse `s` with a `strptime`-style `fmt` (tokens `yyyy`/`yy`/`MM`/`dd`/`HH`
    /// or `hh`/`mm`/`ss`; any other char is a literal that must match) into a
    /// `DateTime` at `unit`. `None` on any mismatch (the reader routes a bad cell
    /// to the error stream / epoch-0, continue-first).
    pub fn parse_with_format(s: &str, fmt: &str, unit: TimeUnit) -> Option<DateTime> {
        let (sb, fb) = (s.as_bytes(), fmt.as_bytes());
        let (mut si, mut fi) = (0usize, 0usize);
        let (mut y, mut mo, mut d, mut h, mut mi, mut se) = (1970i64, 1i64, 1i64, 0i64, 0i64, 0i64);
        // Read `n` ASCII digits from `s` at `si`, advancing it.
        let read = |sb: &[u8], si: &mut usize, n: usize| -> Option<i64> {
            let mut v = 0i64;
            for _ in 0..n {
                let b = *sb.get(*si)?;
                if !b.is_ascii_digit() {
                    return None;
                }
                v = v * 10 + (b - b'0') as i64;
                *si += 1;
            }
            Some(v)
        };
        while fi < fb.len() {
            let rest = &fmt[fi..];
            if let Some(tok) = ["yyyy", "yy", "MM", "dd", "HH", "hh", "mm", "ss"]
                .into_iter()
                .find(|t| rest.starts_with(t))
            {
                let n = tok.len();
                let v = read(sb, &mut si, n)?;
                match tok {
                    "yyyy" => y = v,
                    // Fixed two-digit-year pivot (design 23): 00–68 → 20xx,
                    // 69–99 → 19xx (the POSIX/Unix convention; deterministic).
                    "yy" => y = if v <= 68 { 2000 + v } else { 1900 + v },
                    "MM" => mo = v,
                    "dd" => d = v,
                    "HH" | "hh" => h = v,
                    "mm" => mi = v,
                    "ss" => se = v,
                    _ => unreachable!(),
                }
                fi += n;
            } else {
                // Literal byte must match.
                if sb.get(si) != fb.get(fi) {
                    return None;
                }
                si += 1;
                fi += 1;
            }
        }
        if si != sb.len() || !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
            return None;
        }
        let secs = days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + se;
        Some(DateTime::new(secs * unit.per_sec(), unit))
    }

    /// Render with a `strftime`-style `fmt` (same tokens as `parse_with_format`).
    pub fn format(&self, fmt: &str) -> String {
        let (y, mo, d, h, mi, se) = self.fields();
        let fb = fmt.as_bytes();
        let mut out = String::with_capacity(fmt.len() + 8);
        let mut fi = 0usize;
        while fi < fb.len() {
            let rest = &fmt[fi..];
            if let Some(tok) = ["yyyy", "yy", "MM", "dd", "HH", "hh", "mm", "ss"]
                .into_iter()
                .find(|t| rest.starts_with(t))
            {
                use std::fmt::Write as _;
                match tok {
                    "yyyy" => {
                        let _ = write!(out, "{y:04}");
                    }
                    "yy" => {
                        let _ = write!(out, "{:02}", y.rem_euclid(100));
                    }
                    "MM" => {
                        let _ = write!(out, "{mo:02}");
                    }
                    "dd" => {
                        let _ = write!(out, "{d:02}");
                    }
                    "HH" | "hh" => {
                        let _ = write!(out, "{h:02}");
                    }
                    "mm" => {
                        let _ = write!(out, "{mi:02}");
                    }
                    "ss" => {
                        let _ = write!(out, "{se:02}");
                    }
                    _ => unreachable!(),
                }
                fi += tok.len();
            } else {
                out.push(fb[fi] as char);
                fi += 1;
            }
        }
        out
    }
}

impl fmt::Display for DateTime {
    /// Default ISO-8601 (`yyyy-MM-ddTHH:mm:ss`); sub-second ticks are truncated to
    /// the whole second in this rendering.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.format("yyyy-MM-ddTHH:mm:ss"))
    }
}

/// A signed time span (design 23 / #57): an integer count of `ticks` at a
/// `TimeUnit` — the result of `DateTime − DateTime`. Kept distinct from
/// `DateTime` (an instant) because their algebra differs: a duration's
/// `sum`/`avg` are meaningful and, being integer, **exact and associative**
/// (parallel byte-identical), whereas an instant's are not. Never routed
/// through f64 (ns ticks exceed 2^53; #53).
#[derive(Debug, Clone, Copy)]
pub struct Duration {
    pub ticks: i64,
    pub unit: TimeUnit,
}

impl PartialEq for Duration {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(std::cmp::Ordering::Equal)
    }
}

impl PartialOrd for Duration {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Compare magnitudes regardless of unit (cross-multiply in i128, exact).
        let a = self.ticks as i128 * other.unit.per_sec() as i128;
        let b = other.ticks as i128 * self.unit.per_sec() as i128;
        a.partial_cmp(&b)
    }
}

impl Duration {
    pub fn new(ticks: i64, unit: TimeUnit) -> Self {
        Duration { ticks, unit }
    }

    /// `(negative, days, hours, minutes, seconds, sub)` — the broken-down
    /// components for rendering, where `sub` is the leftover sub-second ticks at
    /// this unit. Uses i128 magnitude so `i64::MIN` doesn't overflow on negate.
    fn parts(&self) -> (bool, i64, i64, i64, i64, i64) {
        let per = self.unit.per_sec() as i128;
        let total = self.ticks as i128;
        let neg = total < 0;
        let mag = total.unsigned_abs();
        let sub = (mag % per as u128) as i64;
        let secs = (mag / per as u128) as i64;
        let days = secs / 86_400;
        let rem = secs % 86_400;
        (neg, days, rem / 3600, (rem % 3600) / 60, rem % 60, sub)
    }

    /// Number of fractional digits this unit renders (`0` for seconds).
    fn sub_digits(unit: TimeUnit) -> usize {
        match unit {
            TimeUnit::Sec => 0,
            TimeUnit::Milli => 3,
            TimeUnit::Micro => 6,
            TimeUnit::Nano => 9,
        }
    }

    /// Human-readable form `[-][Nd ]HH:MM:SS[.frac]` (the `Display` form, and
    /// what [`parse_at`] round-trips). `frac` width matches the unit.
    ///
    /// [`parse_at`]: Duration::parse_at
    pub fn to_human(&self) -> String {
        use std::fmt::Write as _;
        let (neg, days, h, m, s, sub) = self.parts();
        let mut out = String::new();
        if neg {
            out.push('-');
        }
        if days > 0 {
            let _ = write!(out, "{days}d ");
        }
        let _ = write!(out, "{h:02}:{m:02}:{s:02}");
        let w = Self::sub_digits(self.unit);
        if w > 0 && sub > 0 {
            let _ = write!(out, ".{sub:0w$}");
        }
        out
    }

    /// ISO-8601 duration form `PT#H#M#S` (days folded into hours; seconds carry
    /// the sub-second fraction). Sign-prefixed for negative spans.
    pub fn to_iso8601(&self) -> String {
        use std::fmt::Write as _;
        let (neg, days, h, m, s, sub) = self.parts();
        let mut out = String::new();
        if neg {
            out.push('-');
        }
        out.push_str("PT");
        let hours = days * 24 + h;
        if hours != 0 {
            let _ = write!(out, "{hours}H");
        }
        if m != 0 {
            let _ = write!(out, "{m}M");
        }
        // Always emit seconds when nothing else was written (so `PT0S` is valid).
        let w = Self::sub_digits(self.unit);
        if s != 0 || sub != 0 || (hours == 0 && m == 0) {
            if w > 0 && sub > 0 {
                let frac = format!("{sub:0w$}");
                let _ = write!(out, "{s}.{}S", frac.trim_end_matches('0'));
            } else {
                let _ = write!(out, "{s}S");
            }
        }
        out
    }

    /// Parse the human form `[-][Nd ]HH:MM:SS[.frac]` at `unit` into exact ticks.
    /// `None` on any malformed field (continue-first at the call site).
    pub fn parse_at(s: &str, unit: TimeUnit) -> Option<Duration> {
        let s = s.trim();
        let (neg, rest) = match s.strip_prefix('-') {
            Some(r) => (true, r),
            None => (false, s),
        };
        // Optional `Nd ` day prefix.
        let (days, hms) = match rest.split_once("d ") {
            Some((d, r)) => (d.trim().parse::<i64>().ok()?, r),
            None => (0, rest),
        };
        let (clock, frac) = match hms.split_once('.') {
            Some((c, f)) => (c, Some(f)),
            None => (hms, None),
        };
        let mut it = clock.split(':');
        let h: i64 = it.next()?.parse().ok()?;
        let m: i64 = it.next()?.parse().ok()?;
        let sec: i64 = it.next()?.parse().ok()?;
        if it.next().is_some() || !(0..60).contains(&m) || !(0..60).contains(&sec) {
            return None;
        }
        let per = unit.per_sec();
        let whole = (((days * 24 + h) * 60 + m) * 60 + sec).checked_mul(per)?;
        // Fractional sub-second: pad/truncate to the unit's digit count.
        let sub = match frac {
            None => 0,
            Some(f) => {
                if !f.bytes().all(|b| b.is_ascii_digit()) {
                    return None;
                }
                let w = Self::sub_digits(unit);
                let mut digits = String::with_capacity(w);
                for i in 0..w {
                    digits.push(f.as_bytes().get(i).map_or('0', |&b| b as char));
                }
                digits.parse::<i64>().unwrap_or(0)
            }
        };
        let ticks = whole.checked_add(sub)?;
        Some(Duration::new(if neg { -ticks } else { ticks }, unit))
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_human())
    }
}

/// A single scalar value. Used for literals, predicate evaluation and the
/// "current object" (`$_`) field access. Bulk data lives in [`crate::Column`].
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    /// Default SIMD integer lane.
    I64(i64),
    /// Default SIMD float lane.
    F64(f64),
    /// Exact fixed-point lane (opt-in; design doc 21).
    Dec(Decimal),
    /// Datetime lane (epoch ticks; design doc 23).
    DateTime(DateTime),
    /// Duration lane (signed tick span; design 23 / #57).
    Duration(Duration),
    Str(String),
}

impl Value {
    pub fn dtype(&self) -> DataType {
        match self {
            Value::Null => DataType::Null,
            Value::Bool(_) => DataType::Bool,
            Value::I64(_) => DataType::I64,
            Value::F64(_) => DataType::F64,
            Value::Dec(d) => DataType::Decimal { scale: d.scale },
            Value::DateTime(t) => DataType::DateTime { unit: t.unit },
            Value::Duration(d) => DataType::Duration { unit: d.unit },
            Value::Str(_) => DataType::Str,
        }
    }

    /// Best-effort numeric view for comparisons across the int/float lanes.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::I64(v) => Some(*v as f64),
            Value::F64(v) => Some(*v),
            Value::Dec(d) => Some(d.to_f64()),
            Value::DateTime(t) => Some(t.ticks as f64),
            Value::Duration(d) => Some(d.ticks as f64),
            Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, ""),
            Value::Bool(b) => write!(f, "{b}"),
            Value::I64(v) => write!(f, "{v}"),
            Value::F64(v) => write!(f, "{v}"),
            Value::Dec(d) => write!(f, "{d}"),
            Value::DateTime(t) => write!(f, "{t}"),
            Value::Duration(d) => write!(f, "{d}"),
            Value::Str(s) => write!(f, "{s}"),
        }
    }
}

/// Logical type = execution-lane hint. See design doc 06.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Null,
    Bool,
    /// Integer SIMD lane (i32/i64 collapsed to i64 in the MVP).
    I64,
    /// Float SIMD lane (f32/f64 collapsed to f64 in the MVP).
    F64,
    /// Exact fixed-point lane with a fixed fractional scale (design doc 21).
    Decimal {
        scale: u8,
    },
    /// Datetime lane: epoch ticks at a fixed `unit` (design doc 23).
    DateTime {
        unit: TimeUnit,
    },
    /// Duration lane: signed tick span at a fixed `unit` (design 23 / #57).
    Duration {
        unit: TimeUnit,
    },
    /// Stream-based text (see design doc 09 "Text is stream").
    Str,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Null => f.write_str("null"),
            DataType::Bool => f.write_str("bool"),
            DataType::I64 => f.write_str("i64"),
            DataType::F64 => f.write_str("f64"),
            DataType::Decimal { scale } => write!(f, "decimal({scale})"),
            // The unit is omitted (always `Sec` in the MVP) so the annotation
            // round-trips as the bare `datetime` the parser accepts; an explicit
            // `:datetime("fmt")` is rendered from `OpenCsv.dt_formats`, not here.
            DataType::DateTime { .. } => f.write_str("datetime"),
            // Unit omitted (Sec in the MVP) so `:duration` round-trips bare.
            DataType::Duration { .. } => f.write_str("duration"),
            DataType::Str => f.write_str("str"),
        }
    }
}

#[cfg(test)]
mod decimal_tests {
    use super::*;

    #[test]
    fn display_pads_fraction() {
        assert_eq!(Decimal::new(1234, 2).to_string(), "12.34");
        assert_eq!(Decimal::new(5, 2).to_string(), "0.05");
        assert_eq!(Decimal::new(-5, 2).to_string(), "-0.05");
        assert_eq!(Decimal::new(100, 0).to_string(), "100");
        assert_eq!(Decimal::new(-1000, 3).to_string(), "-1.000");
    }

    #[test]
    fn rescale_round_half_even() {
        // 12.345 → scale 2: exactly half (…45 with even/odd last kept digit).
        assert_eq!(
            Decimal::new(12345, 3).rescale(2).unwrap(),
            Decimal::new(1234, 2)
        ); // 4 even → stays
        assert_eq!(
            Decimal::new(12355, 3).rescale(2).unwrap(),
            Decimal::new(1236, 2)
        ); // 5 odd → up
           // Non-half rounds normally.
        assert_eq!(
            Decimal::new(12346, 3).rescale(2).unwrap(),
            Decimal::new(1235, 2)
        );
        assert_eq!(
            Decimal::new(12344, 3).rescale(2).unwrap(),
            Decimal::new(1234, 2)
        );
        // Negative half-to-even rounds toward even magnitude.
        assert_eq!(
            Decimal::new(-12345, 3).rescale(2).unwrap(),
            Decimal::new(-1234, 2)
        );
        assert_eq!(
            Decimal::new(-12355, 3).rescale(2).unwrap(),
            Decimal::new(-1236, 2)
        );
        // Scale up is exact.
        assert_eq!(
            Decimal::new(1234, 2).rescale(4).unwrap(),
            Decimal::new(123400, 4)
        );
    }

    #[test]
    fn add_is_exact_and_associative_regardless_of_order() {
        // 0.1 + 0.2 == 0.3 exactly (the canonical f64 failure).
        let a = Decimal::new(1, 1);
        let b = Decimal::new(2, 1);
        assert_eq!(a.checked_add(&b).unwrap(), Decimal::new(3, 1));

        // Mixed scales align to the larger; sum is associativity-free: any
        // grouping of the same values yields the identical unscaled integer.
        let vals = [
            Decimal::new(115, 2),   // 1.15
            Decimal::new(2, 1),     // 0.2
            Decimal::new(33333, 4), // 3.3333
            Decimal::new(-7, 0),    // -7
        ];
        let fold = |order: &[usize]| {
            let mut acc = Decimal::new(0, 0);
            for &i in order {
                acc = acc.checked_add(&vals[i]).unwrap();
            }
            acc
        };
        let left = fold(&[0, 1, 2, 3]);
        let other = fold(&[3, 1, 0, 2]); // different order
                                         // Same scale and same unscaled integer → byte-identical decimal.
        assert_eq!(left.rescale(4).unwrap(), other.rescale(4).unwrap());
    }

    #[test]
    fn ordering_compares_across_scales() {
        assert!(Decimal::new(120, 2) > Decimal::new(1, 0)); // 1.20 > 1
        assert!(Decimal::new(1, 0) == Decimal::new(100, 2)); // 1 == 1.00 (via cmp)
        assert_eq!(
            Decimal::new(1, 0).partial_cmp(&Decimal::new(100, 2)),
            Some(std::cmp::Ordering::Equal)
        );
        assert!(Decimal::new(-5, 1) < Decimal::new(0, 0));
    }

    #[test]
    fn value_dtype_and_f64_view() {
        let v = Value::Dec(Decimal::new(1250, 2));
        assert_eq!(v.dtype(), DataType::Decimal { scale: 2 });
        assert_eq!(v.as_f64(), Some(12.5));
        assert_eq!(v.to_string(), "12.50");
    }

    #[test]
    fn parse_scaled_is_exact_and_rounds_half_even() {
        // Exact at the target scale.
        assert_eq!(
            Decimal::parse_scaled("12.34", 2),
            Some(Decimal::new(1234, 2))
        );
        assert_eq!(
            Decimal::parse_scaled("100", 2),
            Some(Decimal::new(10000, 2))
        );
        assert_eq!(Decimal::parse_scaled("-0.5", 2), Some(Decimal::new(-50, 2)));
        assert_eq!(Decimal::parse_scaled("+3.0", 1), Some(Decimal::new(30, 1)));
        assert_eq!(Decimal::parse_scaled(".5", 2), Some(Decimal::new(50, 2)));
        assert_eq!(Decimal::parse_scaled("5.", 0), Some(Decimal::new(5, 0)));
        // Excess fractional digits round half-to-even.
        assert_eq!(
            Decimal::parse_scaled("12.345", 2),
            Some(Decimal::new(1234, 2))
        ); // 4 even
        assert_eq!(
            Decimal::parse_scaled("12.355", 2),
            Some(Decimal::new(1236, 2))
        ); // 5 odd→up
        assert_eq!(
            Decimal::parse_scaled("12.344", 2),
            Some(Decimal::new(1234, 2))
        );
        assert_eq!(
            Decimal::parse_scaled("-12.345", 2),
            Some(Decimal::new(-1234, 2))
        );
        // The canonical f64 trap is exact here: 0.1 and 0.2 at scale 1.
        let a = Decimal::parse_scaled("0.1", 1).unwrap();
        let b = Decimal::parse_scaled("0.2", 1).unwrap();
        assert_eq!(
            a.checked_add(&b).unwrap(),
            Decimal::parse_scaled("0.3", 1).unwrap()
        );
        // Malformed → None (so the reader can route to the error stream).
        for bad in ["", "+", "-", "abc", "1.2.3", "1e5", "1 ", " 1", "1,2", "."] {
            assert_eq!(
                Decimal::parse_scaled(bad, 2),
                None,
                "expected None for {bad:?}"
            );
        }
    }

    #[test]
    fn fractional_digits_counts_or_rejects() {
        assert_eq!(Decimal::fractional_digits("12.5"), Some(1));
        assert_eq!(Decimal::fractional_digits("7"), Some(0));
        assert_eq!(Decimal::fractional_digits("-3.1400"), Some(4));
        assert_eq!(Decimal::fractional_digits(".5"), Some(1));
        assert_eq!(Decimal::fractional_digits("5."), Some(0));
        assert_eq!(Decimal::fractional_digits("abc"), None);
        assert_eq!(Decimal::fractional_digits(""), None);
        assert_eq!(Decimal::fractional_digits("1.2.3"), None);
    }

    #[test]
    fn overflow_is_reported_not_panicked() {
        let big = Decimal::new(i128::MAX, 0);
        assert!(big.checked_add(&Decimal::new(1, 0)).is_none());
        // Rescaling up past i128 range also reports None (caller degrades).
        assert!(Decimal::new(i128::MAX / 5, 0).rescale(2).is_none());
    }

    #[test]
    fn datetime_parse_format_roundtrips() {
        // Epoch itself.
        let dt = DateTime::parse_with_format(
            "1970-01-01T00:00:00",
            "yyyy-MM-ddTHH:mm:ss",
            TimeUnit::Sec,
        )
        .unwrap();
        assert_eq!(dt.ticks, 0);
        // A known instant: 2020-02-29 12:34:56 UTC (leap day) = 1582979696 s.
        let dt = DateTime::parse_with_format(
            "2020-02-29 12:34:56",
            "yyyy-MM-dd HH:mm:ss",
            TimeUnit::Sec,
        )
        .unwrap();
        assert_eq!(dt.ticks, 1_582_979_696);
        // Round-trips back to the same text.
        assert_eq!(dt.format("yyyy-MM-dd HH:mm:ss"), "2020-02-29 12:34:56");
        // Default Display is ISO-8601 with a `T`.
        assert_eq!(dt.to_string(), "2020-02-29T12:34:56");
        // Two-digit year token + compact format.
        let c = DateTime::parse_with_format("210304050607", "yyMMddHHmmss", TimeUnit::Sec).unwrap();
        assert_eq!(c.format("yyyy-MM-dd HH:mm:ss"), "2021-03-04 05:06:07");
    }

    #[test]
    fn datetime_rejects_invalid_and_partial() {
        // Month/day out of range.
        assert!(DateTime::parse_with_format("2020-13-01", "yyyy-MM-dd", TimeUnit::Sec).is_none());
        assert!(DateTime::parse_with_format("2020-00-10", "yyyy-MM-dd", TimeUnit::Sec).is_none());
        assert!(DateTime::parse_with_format("2020-01-32", "yyyy-MM-dd", TimeUnit::Sec).is_none());
        // Literal mismatch / trailing input.
        assert!(DateTime::parse_with_format("2020/01/01", "yyyy-MM-dd", TimeUnit::Sec).is_none());
        assert!(DateTime::parse_with_format("2020-01-01x", "yyyy-MM-dd", TimeUnit::Sec).is_none());
        // Non-digit where a digit is required.
        assert!(DateTime::parse_with_format("20-0a-01", "yy-MM-dd", TimeUnit::Sec).is_none());
    }

    #[test]
    fn datetime_compares_across_units_exactly() {
        // 1 second in millis vs 1 second in seconds are equal instants.
        let a = DateTime::new(1_000, TimeUnit::Milli);
        let b = DateTime::new(1, TimeUnit::Sec);
        assert_eq!(a, b);
        assert!(DateTime::new(999, TimeUnit::Milli) < b);
        assert!(DateTime::new(1_001, TimeUnit::Milli) > b);
        // Sub-second resolution preserved in ticks but truncated in Display.
        let m = DateTime::new(1_500, TimeUnit::Milli);
        assert_eq!(m.to_string(), "1970-01-01T00:00:01");
    }

    #[test]
    fn duration_human_format_and_roundtrip() {
        // 3d 02:15:00 in seconds = 3*86400 + 2*3600 + 15*60 = 267300 s.
        let d = Duration::new(267_300, TimeUnit::Sec);
        assert_eq!(d.to_string(), "3d 02:15:00");
        assert_eq!(d.to_iso8601(), "PT74H15M");
        // Round-trips back to the same ticks (and a sub-day span has no `Nd `).
        assert_eq!(Duration::parse_at("3d 02:15:00", TimeUnit::Sec), Some(d));
        let hms = Duration::new(2 * 3600 + 15 * 60, TimeUnit::Sec);
        assert_eq!(hms.to_string(), "02:15:00");
        assert_eq!(Duration::parse_at("02:15:00", TimeUnit::Sec), Some(hms));

        // Negative span keeps the sign through Display ↔ parse.
        let neg = Duration::new(-90, TimeUnit::Sec);
        assert_eq!(neg.to_string(), "-00:01:30");
        assert_eq!(Duration::parse_at("-00:01:30", TimeUnit::Sec), Some(neg));

        // Sub-second fraction at the unit's precision (millis → 3 digits).
        let ms = Duration::new(1_500, TimeUnit::Milli);
        assert_eq!(ms.to_string(), "00:00:01.500");
        assert_eq!(
            Duration::parse_at("00:00:01.500", TimeUnit::Milli),
            Some(ms)
        );
    }

    #[test]
    fn duration_compares_across_units_exactly() {
        // 1000 ms == 1 s as durations; ordering is exact (i128 lift).
        assert_eq!(
            Duration::new(1_000, TimeUnit::Milli),
            Duration::new(1, TimeUnit::Sec)
        );
        assert!(Duration::new(999, TimeUnit::Milli) < Duration::new(1, TimeUnit::Sec));
        // Nanosecond ticks past 2^53 stay distinct (f64 would collapse them).
        let base = 1_700_000_000_000_000_000_i64;
        assert!(base as f64 == (base + 1) as f64);
        assert!(Duration::new(base, TimeUnit::Nano) < Duration::new(base + 1, TimeUnit::Nano));
    }
}
