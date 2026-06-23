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
        // SWAR fast path (#71): ≤18 total digits fit `u64` with no overflow, so
        // skip the per-digit `checked_*` (which never trips in this range). The
        // digits are already validated above; process int then frac as one
        // stream. Byte-identical to the scalar loop — same magnitude, and the
        // checked loop also never returns `None` for ≤18 digits.
        let mag: i128 = if int_part.len() + frac_part.len() <= 18 {
            let acc = crate::numparse::accumulate_digits_u64(int_part.as_bytes(), 0);
            crate::numparse::accumulate_digits_u64(frac_part.as_bytes(), acc) as i128
        } else {
            let mut mag: i128 = 0;
            for &b in int_part.as_bytes().iter().chain(frac_part.as_bytes()) {
                mag = mag.checked_mul(10)?.checked_add((b - b'0') as i128)?;
            }
            mag
        };
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

/// Split a trailing ISO-8601 timezone (`Z`/`z` or `±HH:mm`) off a datetime
/// string, returning the remainder and the offset in seconds (UTC = local −
/// offset). No timezone → offset `0`. #93.
fn strip_iso_zone(s: &str) -> (&str, i64) {
    let b = s.as_bytes();
    let n = b.len();
    if n > 0 && (b[n - 1] == b'Z' || b[n - 1] == b'z') {
        return (&s[..n - 1], 0);
    }
    // `±HH:mm` at the very end: sign at n-6, `:` at n-3, the four HHmm digits.
    if n >= 6 {
        let c = b[n - 6];
        if (c == b'+' || c == b'-')
            && b[n - 3] == b':'
            && b[n - 5].is_ascii_digit()
            && b[n - 4].is_ascii_digit()
            && b[n - 2].is_ascii_digit()
            && b[n - 1].is_ascii_digit()
        {
            let hh = (b[n - 5] - b'0') as i64 * 10 + (b[n - 4] - b'0') as i64;
            let mm = (b[n - 2] - b'0') as i64 * 10 + (b[n - 1] - b'0') as i64;
            let mag = hh * 3600 + mm * 60;
            return (&s[..n - 6], if c == b'-' { -mag } else { mag });
        }
    }
    (s, 0)
}

/// Drop a fractional-seconds suffix (`.digits`) — the MVP truncates to the
/// second (after the timezone is already stripped, a `.` is unambiguously the
/// fraction since the date/time formats use `-`/`:`, never `.`). #93.
fn strip_fraction(s: &str) -> &str {
    match s.rfind('.') {
        Some(i) if i + 1 < s.len() && s[i + 1..].bytes().all(|c| c.is_ascii_digit()) => &s[..i],
        _ => s,
    }
}

/// Timezone-abbreviation table (§29 s3 / issue #140, ratified **(a) std-only**):
/// each entry is an **unambiguous alias for one fixed offset** — never a
/// DST-rule conversion (named zones like `Asia/Tokyo` are out of scope; IANA
/// tzdata would make results depend on an external, versioned dataset).
/// Core per the ruling: `UTC`/`GMT`/`JST`; plus `MST`/`HST`, which are
/// unambiguous in wild data (MST's other use, Sonora, is the same −7; HST is
/// Hawaii only). The criterion is **"is the abbreviation ambiguous in wild
/// cells?"**, not "is it an IANA zone name" — so `EST` is out (Australian
/// Eastern Standard Time is +10; tzdata 2017a renamed the Australian
/// abbreviations to `AEST` etc. precisely over this clash — silently applying
/// −5 would be a 15-hour skew). **Deliberately excluded as ambiguous**
/// (never-silent: a cell carrying one fails its format and is counted, never
/// guessed): `CST` (US Central / China / Cuba), `IST` (India / Israel /
/// Ireland), `BST` (British Summer / Bangladesh), `PST` (US Pacific /
/// Philippine), `EST` (US Eastern / Australian Eastern), `AST`, `CDT`
/// (US / Cuba), and the other DST-pair names.
const TZ_ABBREV: &[(&str, i64)] = &[
    ("UTC", 0),
    ("GMT", 0),
    ("JST", 9 * 3600),
    ("MST", -7 * 3600),
    ("HST", -10 * 3600),
];

/// Split a trailing timezone **abbreviation** (`… JST`: uppercase, separated by
/// exactly one space) off a datetime string, returning the remainder and the
/// fixed offset in seconds (UTC = local − offset). Only [`TZ_ABBREV`] entries
/// match; anything else (ambiguous `CST`, lowercase `jst`, unknown words) is
/// left in place, so the cell fails its format never-silently instead of being
/// silently guessed.
fn strip_zone_abbrev(s: &str) -> Option<(&str, i64)> {
    let (name, off) = TZ_ABBREV
        .iter()
        .find(|(name, _)| s.ends_with(name))
        .copied()?;
    let base = s[..s.len() - name.len()].strip_suffix(' ')?;
    Some((base, off))
}

/// Weekday-name table for the `ddd` format token (§29 s3), Monday-first like
/// ISO ([`Date::weekday`]). English short names (the default locale).
const DDD_EN: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
/// Japanese single-kanji weekday names (`[ja-jp]`), Monday-first.
const DDD_JA: [&str; 7] = ["月", "火", "水", "木", "金", "土", "日"];

/// Locale for locale-sensitive format tokens (`ddd`), selected by a leading
/// `[tag]` on the format string (e.g. `"[ja-jp]yyyy年MM月dd日(ddd)"`). The
/// tables are **std-only** consts (§29.5-5: zero dependencies for locale).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DtLocale {
    En,
    Ja,
}

impl DtLocale {
    fn ddd(self) -> &'static [&'static str; 7] {
        match self {
            DtLocale::En => &DDD_EN,
            DtLocale::Ja => &DDD_JA,
        }
    }
}

/// Split a leading `[tag]` locale off a format string. An unknown tag is left
/// in place (it would only ever match itself as literal bytes); program-level
/// format strings are gated by [`DateTime::validate_format`] at declaration, so
/// a typo'd tag is a parse error there, never a silent literal.
fn split_locale(fmt: &str) -> (DtLocale, &str) {
    if let Some(rest) = fmt.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            let tag = &rest[..end];
            if tag.eq_ignore_ascii_case("ja-jp") {
                return (DtLocale::Ja, &rest[end + 1..]);
            }
            if tag.eq_ignore_ascii_case("en-us") {
                return (DtLocale::En, &rest[end + 1..]);
            }
        }
    }
    (DtLocale::En, fmt)
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
        // Stateless callers (scalar casts, comparison coercion) start every
        // cell from the canonical order; column-level loops thread a `hint`
        // through `parse_auto_sticky` for the move-to-front fast path.
        let mut hint = 0;
        Self::parse_auto_sticky(s, unit, &mut hint)
    }

    /// Like [`parse_auto`] but tries `AUTO_FORMATS[*hint]` **first** — the
    /// format that matched the previous cell of this column — then the rest in
    /// canonical order, updating `*hint` to whichever index matched. For a
    /// uniform non-ISO column (e.g. `yyMMddHHmmss`) every cell after the first
    /// parses in a single attempt instead of re-paying the failed trials for
    /// the leading ISO formats; the dominant real-world case is non-ISO, so
    /// this is a constant cost on every datetime flow today (see #135).
    ///
    /// **Byte-identical to [`parse_auto`].** [`AUTO_FORMATS`] is mutually
    /// disjoint — separators and full-consumption digit counts make at most one
    /// entry match any input (pinned by the `auto_formats_disjoint` test) — so
    /// reordering the trial cannot change *which* format matches, only how many
    /// attempts it takes. On a miss it still scans every format (full
    /// fallback), so a mixed-format column degrades to the canonical behaviour,
    /// never to wrong or dropped values. Callers hold one `hint` per column per
    /// worker (never shared across threads), so serial == parallel is preserved.
    ///
    /// [`AUTO_FORMATS`]: DateTime::AUTO_FORMATS
    pub fn parse_auto_sticky(s: &str, unit: TimeUnit, hint: &mut usize) -> Option<DateTime> {
        let s = s.trim();
        // #93: accept ISO variants the base formats don't encode — a trailing
        // timezone (`Z` or `±HH:mm`, normalised to UTC) and fractional seconds
        // (truncated to the column's unit; the MVP `Sec` lane drops them; full
        // sub-second preservation pairs with the datetime-unit work in #58).
        let (base, offset_secs) = Self::normalize_iso(s);
        let n = Self::AUTO_FORMATS.len();
        let h = (*hint).min(n - 1);
        std::iter::once(h)
            .chain((0..n).filter(move |&i| i != h))
            .find_map(|i| {
                Self::parse_with_format(base, Self::AUTO_FORMATS[i], unit).map(|dt| (i, dt))
            })
            .map(|(i, dt)| {
                *hint = i;
                DateTime::new(dt.ticks - offset_secs * unit.per_sec(), unit)
            })
    }

    /// Strip a trailing timezone (`Z` / `±HH:mm` — #93 — or an unambiguous
    /// abbreviation like ` JST` — §29 s3 / #140) and fractional-second suffix
    /// from a datetime string, returning the bare `…HH:mm:ss` text and the UTC
    /// offset in seconds (UTC = local − offset). Shared by `parse_auto` and the
    /// reader's `DtSpec` so both accept the same variants.
    pub fn normalize_iso(s: &str) -> (&str, i64) {
        let (base, off) = Self::strip_zone(s);
        (strip_fraction(base), off)
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

    /// Buckets this datetime to an arbitrary duration width `dur` relative to the
    /// Unix epoch, returning a `DateTime` at the same `unit`. Resolves to the left
    /// boundary (closed-open `[start, start+dur)`), handling negative ticks (before
    /// epoch) correctly by performing integer floor division. Returns `None` if
    /// `dur` is non-positive or if `dur`'s ticks cannot be exactly represented in
    /// `self.unit` without fractional truncation.
    pub fn bucketed(&self, dur: Duration) -> Option<DateTime> {
        if dur.ticks <= 0 {
            return None;
        }
        let self_per_sec = self.unit.per_sec() as i128;
        let dur_per_sec = dur.unit.per_sec() as i128;

        // Convert dur to self's unit: dur_ticks = dur.ticks * self_unit / dur_unit
        let dur_ticks_self_128 = dur.ticks as i128 * self_per_sec;
        if dur_ticks_self_128 % dur_per_sec != 0 {
            return None; // Cannot represent dur exactly in self's unit without truncation
        }
        let dur_ticks = (dur_ticks_self_128 / dur_per_sec) as i64;

        let mut q = self.ticks / dur_ticks;
        let r = self.ticks % dur_ticks;
        if r < 0 {
            q -= 1;
        }
        Some(DateTime::new(q * dur_ticks, self.unit))
    }


    /// Parse `s` with a `strptime`-style `fmt` (tokens `yyyy`/`yy`/`MM`/`dd`/
    /// `ddd`/`HH` or `hh`/`mm`/`ss`/`n…n`; any other char is a literal that must
    /// match) into a `DateTime` at `unit`. A leading `[ja-jp]`/`[en-us]` tag
    /// selects the `ddd` weekday table (§29 s3). `ddd` is **validated**: a cell
    /// whose weekday name contradicts its civil date is a mismatch — corrupt
    /// data is never silently accepted. An `n…n` run of length k reads exactly
    /// k fractional-second digits, scaled to `unit` ticks. `None` on any
    /// mismatch (the reader routes a bad cell to the error stream / epoch-0,
    /// continue-first).
    pub fn parse_with_format(s: &str, fmt: &str, unit: TimeUnit) -> Option<DateTime> {
        let (locale, fmt) = split_locale(fmt);
        let (sb, fb) = (s.as_bytes(), fmt.as_bytes());
        let (mut si, mut fi) = (0usize, 0usize);
        let (mut y, mut mo, mut d, mut h, mut mi, mut se) = (1970i64, 1i64, 1i64, 0i64, 0i64, 0i64);
        let mut claimed_dow: Option<u8> = None;
        let mut frac: Option<(i64, u32)> = None;
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
            // Byte slice, never `&fmt[fi..]`: literal matching advances `fi`
            // one byte at a time, so inside a multi-byte literal (e.g. `年`)
            // `fi` is not a char boundary and a str slice would panic.
            let rest = &fb[fi..];
            // `ddd` must be tried before the token table — `dd` is its prefix
            // and would otherwise win, leaving a stray `d` literal.
            if rest.starts_with(b"ddd") {
                let names = locale.ddd();
                let idx = names
                    .iter()
                    .position(|n| sb[si..].starts_with(n.as_bytes()))?;
                claimed_dow = Some(idx as u8);
                si += names[idx].len();
                fi += 3;
            } else if let Some(tok) = ["yyyy", "yy", "MM", "dd", "HH", "hh", "mm", "ss"]
                .into_iter()
                .find(|t| rest.starts_with(t.as_bytes()))
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
            } else if fb[fi] == b'n' {
                // `n…n` sub-second run (§29 s3): k tokens read exactly k digits.
                // `validate_format` caps runs at 9 for program-level strings;
                // cap defensively here too (10^k must fit the scaling math).
                let k = rest.iter().take_while(|&&b| b == b'n').count();
                if k > 9 {
                    return None;
                }
                let v = read(sb, &mut si, k)?;
                frac = Some((v, k as u32));
                fi += k;
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
        let day = days_from_civil(y, mo, d);
        if let Some(c) = claimed_dow {
            // `ddd` is a checked claim, not decoration: 1970-01-01 was Thursday
            // (=3), same convention as `Date::weekday` (0 = Mon … 6 = Sun).
            if ((day + 3).rem_euclid(7)) as u8 != c {
                return None;
            }
        }
        let mut ticks = (day * 86400 + h * 3600 + mi * 60 + se) * unit.per_sec();
        if let Some((v, k)) = frac {
            // k digits → ticks at `unit` (i128 intermediate: v < 10^9, per_sec
            // ≤ 10^9). Digits finer than the unit truncate deterministically.
            ticks += ((v as i128 * unit.per_sec() as i128) / 10i128.pow(k)) as i64;
        }
        Some(DateTime::new(ticks, unit))
    }

    /// Render with a `strftime`-style `fmt` (same tokens as `parse_with_format`,
    /// including `ddd` weekday names — locale via a leading `[ja-jp]` tag — and
    /// `n…n` sub-second digits, §29 s3).
    pub fn format(&self, fmt: &str) -> String {
        let (locale, fmt) = split_locale(fmt);
        let (y, mo, d, h, mi, se) = self.fields();
        let fb = fmt.as_bytes();
        let mut out = String::with_capacity(fmt.len() + 8);
        let mut fi = 0usize;
        while fi < fb.len() {
            let rest = &fmt[fi..];
            // `ddd` before the token table (`dd` is its prefix).
            if rest.starts_with("ddd") {
                let day = self.ticks.div_euclid(self.unit.per_sec()).div_euclid(86400);
                out.push_str(locale.ddd()[((day + 3).rem_euclid(7)) as usize]);
                fi += 3;
            } else if let Some(tok) = ["yyyy", "yy", "MM", "dd", "HH", "hh", "mm", "ss"]
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
            } else if fb[fi] == b'n' {
                // `n…n` (§29 s3): the fraction of a second at this value's unit,
                // rendered to exactly k digits (coarser units pad with zeros).
                use std::fmt::Write as _;
                let k = rest.bytes().take_while(|&b| b == b'n').count().min(9);
                let ps = self.unit.per_sec() as i128;
                let f = self.ticks.rem_euclid(self.unit.per_sec()) as i128;
                let v = f * 10i128.pow(k as u32) / ps;
                let _ = write!(out, "{v:0width$}", width = k);
                fi += k;
            } else {
                // Copy the literal char whole — a multi-byte literal (e.g. `年`
                // in a `[ja-jp]` format) must not be pushed byte-by-byte.
                let ch = fmt[fi..].chars().next().unwrap_or('\u{fffd}');
                out.push(ch);
                fi += ch.len_utf8();
            }
        }
        out
    }

    /// Validate a format string at **declaration** time (§29 s3, never-silent):
    /// a leading `[tag]` must be a known locale (`[ja-jp]`/`[en-us]`), and at
    /// most one `n…n` sub-second run of 1..=9 digits is allowed. Cell-level
    /// mismatches stay per-cell (continue-first); this catches *program*
    /// mistakes — a typo'd tag must not silently become a literal.
    pub fn validate_format(fmt: &str) -> Result<(), String> {
        if let Some(rest) = fmt.strip_prefix('[') {
            match rest.find(']') {
                Some(end) => {
                    let tag = &rest[..end];
                    if !tag.eq_ignore_ascii_case("ja-jp") && !tag.eq_ignore_ascii_case("en-us") {
                        return Err(format!(
                            "unknown locale tag '[{tag}]' in datetime format \
                             (supported: [ja-jp], [en-us])"
                        ));
                    }
                }
                None => {
                    return Err(
                        "unclosed locale tag '[' at the start of a datetime format".to_string()
                    )
                }
            }
        }
        let (_, body) = split_locale(fmt);
        let b = body.as_bytes();
        let (mut i, mut runs) = (0usize, 0usize);
        while i < b.len() {
            if b[i] == b'n' {
                let k = b[i..].iter().take_while(|&&c| c == b'n').count();
                if k > 9 {
                    return Err(format!(
                        "sub-second run of {k} `n` is too long (1..=9 digits: \
                         nnn = milliseconds … nnnnnnnnn = nanoseconds)"
                    ));
                }
                runs += 1;
                i += k;
            } else {
                i += 1;
            }
        }
        if runs > 1 {
            return Err(
                "a datetime format may contain at most one `n…n` sub-second run".to_string(),
            );
        }
        Ok(())
    }

    /// The tick resolution a format needs (§29 s3): its `n…n` sub-second run
    /// decides — none → `Sec`, 1–3 digits → `Milli`, 4–6 → `Micro`, 7–9 →
    /// `Nano`. The reader stores the column at this unit, so every declared
    /// digit is preserved exactly (integer ticks — no float rounding).
    pub fn unit_for_format(fmt: &str) -> TimeUnit {
        let (_, body) = split_locale(fmt);
        let b = body.as_bytes();
        let (mut i, mut longest) = (0usize, 0usize);
        while i < b.len() {
            if b[i] == b'n' {
                let k = b[i..].iter().take_while(|&&c| c == b'n').count();
                longest = longest.max(k);
                i += k;
            } else {
                i += 1;
            }
        }
        match longest {
            0 => TimeUnit::Sec,
            1..=3 => TimeUnit::Milli,
            4..=6 => TimeUnit::Micro,
            _ => TimeUnit::Nano,
        }
    }

    /// Does this format consume sub-second digits itself (an `n…n` run)? The
    /// reader must then *not* pre-strip the cell's fraction (#93 normalization)
    /// — the format's own run reads it. Locale-tag aware (`[en-us]` contains an
    /// `n` that must not count).
    pub fn format_has_subsec(fmt: &str) -> bool {
        split_locale(fmt).1.bytes().any(|b| b == b'n')
    }

    /// Strip only a trailing zone — an ISO `Z`/`±HH:mm` (#93) or an unambiguous
    /// abbreviation from [`TZ_ABBREV`] (` JST`, §29 s3 / #140) — keeping any
    /// fractional seconds, for specs whose format consumes the fraction itself.
    pub fn strip_zone(s: &str) -> (&str, i64) {
        let s = s.trim();
        if let Some(hit) = strip_zone_abbrev(s) {
            return hit;
        }
        strip_iso_zone(s)
    }
}

impl fmt::Display for DateTime {
    /// Default ISO-8601 (`yyyy-MM-ddTHH:mm:ss`). A sub-second lane (`Milli`/
    /// `Micro`/`Nano`, §29 s3) appends its full-width fraction (`.SSS` /
    /// `.SSSSSS` / `.SSSSSSSSS`) so declared precision is never silently
    /// dropped on output; the `Sec` rendering is unchanged (no fraction).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.format("yyyy-MM-ddTHH:mm:ss"))?;
        let ps = self.unit.per_sec();
        if ps > 1 {
            let width = match self.unit {
                TimeUnit::Milli => 3,
                TimeUnit::Micro => 6,
                TimeUnit::Nano => 9,
                TimeUnit::Sec => unreachable!(),
            };
            write!(f, ".{:0width$}", self.ticks.rem_euclid(ps), width = width)?;
        }
        Ok(())
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

    /// Parses an interval string like `15m`, `6h`, `1d`, `500ms`, `00:15:00` at `unit` into exact ticks.
    /// Returns `None` on any malformed input or if the interval cannot be represented exactly in `unit`.
    pub fn parse_interval(s: &str, unit: TimeUnit) -> Option<Duration> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }

        // Try the standard human parse_at first (e.g. 00:15:00)
        if let Some(d) = Self::parse_at(s, unit) {
            return Some(d);
        }

        // Parse suffix form
        let neg = s.starts_with('-');
        let rest = if neg { &s[1..] } else { s };

        // Split digits and suffix
        let first_alpha = rest.find(|c: char| c.is_alphabetic())?;
        let (digits, suffix) = rest.split_at(first_alpha);
        let val: i64 = digits.parse().ok()?;

        let suffix = suffix.trim().to_ascii_lowercase();
        let mult: i64 = match suffix.as_str() {
            "ns" | "nano" | "nanos" | "nanosecond" | "nanoseconds" => {
                let ticks = if neg { -val } else { val };
                return Self::convert_ticks(ticks, TimeUnit::Nano, unit);
            }
            "us" | "micro" | "micros" | "microsecond" | "microseconds" => {
                let ticks = if neg { -val } else { val };
                return Self::convert_ticks(ticks, TimeUnit::Micro, unit);
            }
            "ms" | "milli" | "millis" | "millisecond" | "milliseconds" => {
                let ticks = if neg { -val } else { val };
                return Self::convert_ticks(ticks, TimeUnit::Milli, unit);
            }
            "s" | "sec" | "secs" | "second" | "seconds" => 1,
            "m" | "min" | "mins" | "minute" | "minutes" => 60,
            "h" | "hour" | "hours" => 3600,
            "d" | "day" | "days" => 86400,
            _ => return None,
        };

        // Multiply by target unit ticks per second
        let total_secs = val.checked_mul(mult)?;
        let total_ticks = total_secs.checked_mul(unit.per_sec())?;
        let ticks = if neg { -total_ticks } else { total_ticks };
        Some(Duration::new(ticks, unit))
    }

    fn convert_ticks(ticks: i64, from: TimeUnit, to: TimeUnit) -> Option<Duration> {
        let from_per_sec = from.per_sec() as i128;
        let to_per_sec = to.per_sec() as i128;
        let ticks_to_128 = ticks as i128 * to_per_sec;
        if ticks_to_128 % from_per_sec != 0 {
            return None; // Fractional ticks cannot be represented without truncation
        }
        let ticks_to = (ticks_to_128 / from_per_sec) as i64;
        Some(Duration::new(ticks_to, to))
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_human())
    }
}

/// A calendar date with **no** time-of-day: exact `i32` days since the Unix
/// epoch (1970-01-01 = day 0). Integer representation → exact and associative
/// (like the decimal/datetime lanes), and it carries no `unit` (a date has no
/// sub-day resolution). Renders / parses as ISO `yyyy-MM-dd`.
/// (#58, Epic #56 — building the time-series subtypes on `DateTime`/`Duration`.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date {
    /// Days since 1970-01-01 (proleptic Gregorian).
    pub epoch_day: i32,
}

impl Date {
    pub fn new(epoch_day: i32) -> Date {
        Date { epoch_day }
    }

    /// Build from a civil `(year, month, day)`. The epoch-day is held in `i32`,
    /// which spans roughly years ±5.8 million around 1970 — far beyond any real
    /// calendar use. A date outside that range would truncate; debug builds
    /// assert against it so a silent wrap never slips through unnoticed.
    pub fn from_ymd(y: i64, m: i64, d: i64) -> Date {
        let ed = days_from_civil(y, m, d);
        debug_assert!(
            i32::try_from(ed).is_ok(),
            "Date epoch-day {ed} out of i32 range (year {y})"
        );
        Date {
            epoch_day: ed as i32,
        }
    }

    /// The civil `(year, month, day)`.
    pub fn ymd(&self) -> (i64, i64, i64) {
        civil_from_days(self.epoch_day as i64)
    }

    /// Day of week, `0 = Monday … 6 = Sunday` (ISO). Exact integer arithmetic.
    pub fn weekday(&self) -> u8 {
        // 1970-01-01 was a Thursday (=3 in Mon..Sun). rem_euclid keeps it in 0..6
        // for negative epoch-days too.
        (((self.epoch_day as i64) + 3).rem_euclid(7)) as u8
    }

    /// Parse a strict ISO `yyyy-MM-dd` date. `None` for any malformed or
    /// out-of-range date (e.g. `2024-02-30`), validated by a civil round-trip so
    /// a nonexistent day never silently maps to a nearby one (never-silent).
    pub fn parse(s: &str) -> Option<Date> {
        let b = s.trim().as_bytes();
        if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
            return None;
        }
        let num = |lo: usize, hi: usize| -> Option<i64> {
            let mut n = 0i64;
            for &c in &b[lo..hi] {
                if !c.is_ascii_digit() {
                    return None;
                }
                n = n * 10 + (c - b'0') as i64;
            }
            Some(n)
        };
        let (y, m, d) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
        if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
            return None;
        }
        let date = Date::from_ymd(y, m, d);
        if date.ymd() != (y, m, d) {
            return None; // impossible civil date (e.g. 2024-02-30)
        }
        Some(date)
    }
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (y, m, d) = self.ymd();
        write!(f, "{y:04}-{m:02}-{d:02}")
    }
}

#[cfg(test)]
mod date_tests {
    use super::Date;

    #[test]
    fn parse_format_roundtrips_and_is_exact() {
        for s in ["1970-01-01", "2000-02-29", "2024-06-03", "1999-12-31"] {
            let d = Date::parse(s).expect("valid date");
            assert_eq!(d.to_string(), s, "round-trip {s}");
            // from_ymd ⇄ ymd is exact
            let (y, m, dd) = d.ymd();
            assert_eq!(Date::from_ymd(y, m, dd), d);
        }
        // epoch anchor
        assert_eq!(Date::parse("1970-01-01").unwrap().epoch_day, 0);
    }

    #[test]
    fn rejects_invalid_and_partial() {
        for s in [
            "2024-02-30", // nonexistent day
            "2023-02-29", // not a leap year
            "2024-13-01", // bad month
            "2024-00-10", // zero month
            "2024-6-3",   // not zero-padded / wrong length
            "2024/06/03", // wrong separator
            "notadate",
            "",
        ] {
            assert!(Date::parse(s).is_none(), "must reject {s:?}");
        }
    }

    #[test]
    fn weekday_is_correct() {
        // 2024-06-03 is a Monday (=0), 2024-06-09 is a Sunday (=6).
        assert_eq!(Date::parse("2024-06-03").unwrap().weekday(), 0);
        assert_eq!(Date::parse("2024-06-09").unwrap().weekday(), 6);
        assert_eq!(Date::parse("1970-01-01").unwrap().weekday(), 3); // Thursday
    }
}

/// A **time-of-day** with no calendar date: exact `i64` ticks since midnight at
/// a fixed `unit` (`Sec` in the MVP). Renders / parses as `HH:mm:ss[.frac]`,
/// bounded to a single day. Integer → exact and associative like the other
/// temporal lanes. (#58, Epic #56.)
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TimeOfDay {
    /// Ticks since midnight at `unit` resolution (`0 .. ticks_per_day`).
    pub ticks: i64,
    pub unit: TimeUnit,
}

/// Sub-second decimal digits carried by a `unit` (`Sec`→0 … `Nano`→9).
fn unit_sub_digits(unit: TimeUnit) -> usize {
    match unit {
        TimeUnit::Sec => 0,
        TimeUnit::Milli => 3,
        TimeUnit::Micro => 6,
        TimeUnit::Nano => 9,
    }
}

impl TimeOfDay {
    pub fn new(ticks: i64, unit: TimeUnit) -> TimeOfDay {
        TimeOfDay { ticks, unit }
    }

    /// `(hour, minute, second, sub_second_ticks)`.
    pub fn parts(&self) -> (i64, i64, i64, i64) {
        let per = self.unit.per_sec();
        let total_s = self.ticks.div_euclid(per);
        let sub = self.ticks.rem_euclid(per);
        (total_s / 3600, (total_s / 60) % 60, total_s % 60, sub)
    }

    /// Parse `HH:mm:ss[.frac]` at `unit`. `None` for a malformed or out-of-range
    /// time (hour `0..23`, minute/second `0..59`) — never-silent, so a bad time
    /// never silently maps to a nearby one.
    pub fn parse_at(s: &str, unit: TimeUnit) -> Option<TimeOfDay> {
        let s = s.trim();
        let (clock, frac) = match s.split_once('.') {
            Some((c, f)) => (c, Some(f)),
            None => (s, None),
        };
        let mut it = clock.split(':');
        let h: i64 = it.next()?.parse().ok()?;
        let m: i64 = it.next()?.parse().ok()?;
        let sec: i64 = it.next()?.parse().ok()?;
        if it.next().is_some()
            || !(0..24).contains(&h)
            || !(0..60).contains(&m)
            || !(0..60).contains(&sec)
        {
            return None;
        }
        let per = unit.per_sec();
        let whole = ((h * 60 + m) * 60 + sec) * per;
        let sub = match frac {
            None => 0,
            Some(f) => {
                if f.is_empty() || !f.bytes().all(|b| b.is_ascii_digit()) {
                    return None;
                }
                // Pad / truncate the fraction to the unit's digit width.
                let w = unit_sub_digits(unit);
                let mut digits = String::with_capacity(w);
                for i in 0..w {
                    digits.push(f.as_bytes().get(i).map_or('0', |&b| b as char));
                }
                digits.parse::<i64>().unwrap_or(0)
            }
        };
        Some(TimeOfDay::new(whole + sub, unit))
    }
}

impl fmt::Display for TimeOfDay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (h, m, s, sub) = self.parts();
        write!(f, "{h:02}:{m:02}:{s:02}")?;
        let w = unit_sub_digits(self.unit);
        if w > 0 && sub != 0 {
            write!(f, ".{sub:0w$}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod timeofday_tests {
    use super::{DataType, Resource, TimeOfDay, TimeUnit, Value};

    #[test]
    fn parse_format_roundtrips_and_is_exact() {
        for s in ["00:00:00", "09:05:07", "23:59:59", "12:30:00"] {
            let t = TimeOfDay::parse_at(s, TimeUnit::Sec).expect("valid time");
            assert_eq!(t.to_string(), s, "round-trip {s}");
        }
        assert_eq!(
            TimeOfDay::parse_at("01:02:03", TimeUnit::Sec)
                .unwrap()
                .ticks,
            3723
        );
    }

    #[test]
    fn rejects_invalid() {
        // Out-of-range fields and structurally-wrong text are rejected;
        // non-zero-padded fields (`1:2:3`) are valid and canonicalize on Display.
        for s in [
            "24:00:00", "12:60:00", "12:00:60", "12:00", "noon", "12:00:0x", "",
        ] {
            assert!(
                TimeOfDay::parse_at(s, TimeUnit::Sec).is_none(),
                "must reject {s:?}"
            );
        }
        assert_eq!(
            TimeOfDay::parse_at("1:2:3", TimeUnit::Sec)
                .unwrap()
                .to_string(),
            "01:02:03",
            "lenient parse canonicalizes on Display"
        );
    }

    #[test]
    fn sub_second_milli_roundtrips() {
        let t = TimeOfDay::parse_at("00:00:01.250", TimeUnit::Milli).unwrap();
        assert_eq!(t.ticks, 1_250);
        assert_eq!(t.to_string(), "00:00:01.250");
    }

    #[test]
    fn resource_value_uri_identity_and_lane() {
        let r = Resource::new("file:///data/a.csv");
        // The value carries the Resource lane and renders its uri.
        assert_eq!(Value::Resource(r.clone()).dtype(), DataType::Resource);
        assert_eq!(Value::Resource(r.clone()).to_string(), "file:///data/a.csv");
        assert_eq!(DataType::Resource.to_string(), "resource");
        // A resource has no numeric view.
        assert_eq!(Value::Resource(r).as_f64(), None);
    }

    #[test]
    fn resource_equality_is_uri_only_meta_out_of_contract() {
        // size/mtime are out of the determinism contract (§00 0.14): two handles
        // to the same uri are equal regardless of them.
        let bare = Resource::new("s3://b/k");
        let with_meta = Resource::with_meta("s3://b/k", Some(123), Some(456));
        assert_eq!(bare, with_meta, "equality must ignore size/mtime");
        assert_eq!(with_meta.size(), Some(123));
        assert_eq!(with_meta.mtime(), Some(456));
        assert_ne!(bare, Resource::new("s3://b/other"));
    }
}

/// A handle to an I/O resource (design §28.1): the first-class `Resource` value.
///
/// Its **identity is the `uri`** (the scheme is a pure function of it) — the
/// in-contract, deterministic part (§00 0.14). `size`/`mtime` are optional,
/// discovery-filled metadata that are **outside the determinism contract**: they
/// take no part in equality (and, later, ordering / hashing / `to_source`), so
/// byte-identity and reproducibility depend only on the uri. Bulk handles live
/// on the `Resource` column lane ([`crate::ColumnData::Resource`], uri-backed).
#[derive(Debug, Clone)]
pub struct Resource {
    uri: String,
    /// Discovery-filled byte size, if known (out of the determinism contract).
    size: Option<u64>,
    /// Discovery-filled modification time as epoch ticks, if known (out of the
    /// determinism contract).
    mtime: Option<i64>,
}

impl Resource {
    /// A handle to `uri` with no metadata (the common case: a literal / a single
    /// `open` target). Metadata is filled later by discovery (slice 3).
    pub fn new(uri: impl Into<String>) -> Self {
        Resource {
            uri: uri.into(),
            size: None,
            mtime: None,
        }
    }

    /// A handle carrying discovery metadata (out-of-contract `size`/`mtime`).
    pub fn with_meta(uri: impl Into<String>, size: Option<u64>, mtime: Option<i64>) -> Self {
        Resource {
            uri: uri.into(),
            size,
            mtime,
        }
    }

    /// The uri — the in-contract identity.
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Discovery-filled byte size, if known (out of the determinism contract).
    pub fn size(&self) -> Option<u64> {
        self.size
    }

    /// Discovery-filled modification time (epoch ticks), if known (out of the
    /// determinism contract).
    pub fn mtime(&self) -> Option<i64> {
        self.mtime
    }
}

/// Equality is **uri-only**: `size`/`mtime` are out-of-contract metadata (§00
/// 0.14), so two handles to the same uri are equal regardless of them. This is
/// what keeps `Resource` deterministic and byte-identical across runs.
impl PartialEq for Resource {
    fn eq(&self, other: &Self) -> bool {
        self.uri == other.uri
    }
}

impl fmt::Display for Resource {
    /// Renders the uri (the in-contract identity).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.uri)
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
    /// Calendar date lane (i32 epoch-day, no time-of-day; #58).
    Date(Date),
    /// Time-of-day lane (i64 ticks since midnight; #58).
    Time(TimeOfDay),
    Str(String),
    /// I/O resource handle lane (design §28.1).
    Resource(Resource),
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
            Value::Date(_) => DataType::Date,
            Value::Time(_) => DataType::Time,
            Value::Str(_) => DataType::Str,
            Value::Resource(_) => DataType::Resource,
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
            Value::Date(d) => Some(d.epoch_day as f64),
            Value::Time(t) => Some(t.ticks as f64),
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
            Value::Date(d) => write!(f, "{d}"),
            Value::Time(t) => write!(f, "{t}"),
            Value::Str(s) => write!(f, "{s}"),
            Value::Resource(r) => write!(f, "{r}"),
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
    /// Calendar date lane: i32 epoch-day, no time-of-day (#58).
    Date,
    /// Time-of-day lane: i64 ticks since midnight, no date (#58, MVP `Sec`).
    Time,
    /// Stream-based text (see design doc 09 "Text is stream").
    Str,
    /// I/O resource handle lane (design §28.1): a uri-identified handle. The
    /// meta (`size`/`mtime`) is out of the determinism contract (§00 0.14).
    Resource,
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
            DataType::Date => f.write_str("date"),
            DataType::Time => f.write_str("time"),
            DataType::Str => f.write_str("str"),
            DataType::Resource => f.write_str("resource"),
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

    /// The SWAR fast path (≤18 digits) in `parse_scaled` must be byte-identical
    /// to the scalar checked-i128 loop. Reference replicates the scalar magnitude
    /// build; sweep widths around the 8/18-digit boundaries, signs, dot
    /// positions, and target scales. #71.
    #[test]
    fn swar_decimal_parse_matches_scalar() {
        // Independent scalar reference: same parse, but always the checked loop.
        fn reference(s: &str, scale: u8) -> Option<Decimal> {
            let (neg, rest) = match s.as_bytes().first() {
                Some(b'-') => (true, &s[1..]),
                Some(b'+') => (false, &s[1..]),
                _ => (false, s),
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
            let natural_scale = u8::try_from(frac_part.len()).ok()?;
            let mut mag: i128 = 0;
            for &b in int_part.as_bytes().iter().chain(frac_part.as_bytes()) {
                mag = mag.checked_mul(10)?.checked_add((b - b'0') as i128)?;
            }
            let unscaled = if neg { -mag } else { mag };
            Decimal::new(unscaled, natural_scale).rescale(scale)
        }

        let mut cases: Vec<String> = Vec::new();
        let signs = ["", "-", "+"];
        for &sign in &signs {
            for ilen in 0..=20usize {
                for flen in 0..=20usize {
                    if ilen + flen == 0 {
                        continue;
                    }
                    let ints = "1234567890".repeat(3);
                    let fracs = "9876543210".repeat(3);
                    let mut s = String::from(sign);
                    s.push_str(&ints[..ilen]);
                    s.push('.');
                    s.push_str(&fracs[..flen]);
                    cases.push(s);
                }
            }
        }
        // Plus the explicit edge forms and a few malformed inputs.
        for extra in [
            "0", ".5", "5.", "100", "00100", "-0.0", "+0", "1.2.3", "", "-", "abc", "1e5",
        ] {
            cases.push(extra.to_string());
        }
        for s in &cases {
            for scale in [0u8, 1, 2, 6, 18] {
                assert_eq!(
                    Decimal::parse_scaled(s, scale),
                    reference(s, scale),
                    "decimal parse mismatch on {s:?} scale={scale}"
                );
            }
        }
    }

    /// Micro-benchmark (ignored; run with
    /// `cargo test -p rivus-core --release --lib bench_decimal_parse -- --ignored --nocapture`):
    /// decimal magnitude build, scalar checked-i128 loop vs the SWAR fast path. #71.
    #[test]
    #[ignore]
    fn bench_decimal_parse() {
        use std::time::Instant;
        // Scalar reference parser (the pre-#71 magnitude loop).
        fn scalar(s: &str, scale: u8) -> Option<Decimal> {
            let (neg, rest) = match s.as_bytes().first() {
                Some(b'-') => (true, &s[1..]),
                Some(b'+') => (false, &s[1..]),
                _ => (false, s),
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
            let natural_scale = u8::try_from(frac_part.len()).ok()?;
            let mut mag: i128 = 0;
            for &b in int_part.as_bytes().iter().chain(frac_part.as_bytes()) {
                mag = mag.checked_mul(10)?.checked_add((b - b'0') as i128)?;
            }
            let unscaled = if neg { -mag } else { mag };
            Decimal::new(unscaled, natural_scale).rescale(scale)
        }

        let samples: Vec<String> = (0..1024u64)
            .map(|i| {
                format!(
                    "{}.{:02}",
                    i.wrapping_mul(2_654_435_761) % 1_000_000,
                    i % 100
                )
            })
            .collect();
        let bytes: usize = samples.iter().map(|s| s.len()).sum();
        let reps = 4000usize;

        let t = Instant::now();
        let mut a = 0i128;
        for _ in 0..reps {
            for s in &samples {
                a = a.wrapping_add(scalar(s, 4).map_or(0, |d| d.unscaled));
            }
        }
        let scal = t.elapsed();
        let t = Instant::now();
        let mut b = 0i128;
        for _ in 0..reps {
            for s in &samples {
                b = b.wrapping_add(Decimal::parse_scaled(s, 4).map_or(0, |d| d.unscaled));
            }
        }
        let swar = t.elapsed();
        assert_eq!(a, b);
        let mbps = |d: std::time::Duration| (bytes * reps) as f64 / d.as_secs_f64() / 1e6;
        println!(
            "\n[#71 decimal-parse | short ~8-digit] {} samples × {reps} reps",
            samples.len()
        );
        println!("  scalar checked: {:?}  {:.0} MB/s", scal, mbps(scal));
        println!(
            "  SWAR fast:      {:?}  {:.0} MB/s  ({:.2}x scalar)",
            swar,
            mbps(swar),
            scal.as_secs_f64() / swar.as_secs_f64()
        );

        // Wide regime (16-digit integer part): the 8-digit SWAR block dominates.
        let wide: Vec<String> = (0..1024u64)
            .map(|i| format!("{}.{:04}", 1_000_000_000_000u64 + i, i % 10000))
            .collect();
        let wbytes: usize = wide.iter().map(|s| s.len()).sum();
        let t = Instant::now();
        let mut c = 0i128;
        for _ in 0..reps {
            for s in &wide {
                c = c.wrapping_add(scalar(s, 4).map_or(0, |d| d.unscaled));
            }
        }
        let wscal = t.elapsed();
        let t = Instant::now();
        let mut d = 0i128;
        for _ in 0..reps {
            for s in &wide {
                d = d.wrapping_add(Decimal::parse_scaled(s, 4).map_or(0, |d| d.unscaled));
            }
        }
        let wswar = t.elapsed();
        assert_eq!(c, d);
        let wmbps = |x: std::time::Duration| (wbytes * reps) as f64 / x.as_secs_f64() / 1e6;
        println!("[#71 decimal-parse | wide 16-digit]");
        println!("  scalar checked: {:?}  {:.0} MB/s", wscal, wmbps(wscal));
        println!(
            "  SWAR fast:      {:?}  {:.0} MB/s  ({:.2}x scalar)",
            wswar,
            wmbps(wswar),
            wscal.as_secs_f64() / wswar.as_secs_f64()
        );
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
    fn ddd_weekday_token_parses_validates_and_formats() {
        // 2026-06-10 is a Wednesday (anchored from the weekday test: 2024-06-03
        // is a Monday, +737 days ≡ +2 mod 7).
        let dt =
            DateTime::parse_with_format("Wed 2026-06-10", "ddd yyyy-MM-dd", TimeUnit::Sec).unwrap();
        assert_eq!(dt.format("ddd yyyy-MM-dd"), "Wed 2026-06-10");
        // `ddd` is validated, not decoration: a weekday name that contradicts
        // the civil date is a mismatch (never-silent at the call site).
        assert!(
            DateTime::parse_with_format("Mon 2026-06-10", "ddd yyyy-MM-dd", TimeUnit::Sec)
                .is_none()
        );
        // `[ja-jp]`: single-kanji weekday + multi-byte literals, both ways.
        let fmt = "[ja-jp]yyyy年MM月dd日(ddd)";
        let dt = DateTime::parse_with_format("2026年06月10日(水)", fmt, TimeUnit::Sec).unwrap();
        assert_eq!(dt.format(fmt), "2026年06月10日(水)");
        assert!(DateTime::parse_with_format("2026年06月10日(月)", fmt, TimeUnit::Sec).is_none());
        // A multi-byte literal must neither mangle the output nor panic the
        // parse when the cell diverges mid-literal.
        assert!(DateTime::parse_with_format("2026年06月10x(水)", fmt, TimeUnit::Sec).is_none());
    }

    #[test]
    fn subsecond_run_parses_scales_and_renders() {
        // 6-digit run at the matching Micro unit: every digit preserved.
        let fmt = "yyyy-MM-dd HH:mm:ss.nnnnnn";
        let dt = DateTime::parse_with_format("2026-06-10 12:00:00.123456", fmt, TimeUnit::Micro)
            .unwrap();
        assert_eq!(dt.ticks.rem_euclid(1_000_000), 123_456);
        assert_eq!(dt.format(fmt), "2026-06-10 12:00:00.123456");
        // Display appends the unit-width fraction (precision is never silently
        // dropped on output); the Sec rendering is unchanged elsewhere.
        assert_eq!(dt.to_string(), "2026-06-10T12:00:00.123456");
        // 3-digit run at Milli.
        let dt = DateTime::parse_with_format(
            "2026-06-10 12:00:00.250",
            "yyyy-MM-dd HH:mm:ss.nnn",
            TimeUnit::Milli,
        )
        .unwrap();
        assert_eq!(dt.ticks.rem_euclid(1_000), 250);
        // Wrong digit count for the run is a mismatch, not a guess.
        assert!(DateTime::parse_with_format(
            "2026-06-10 12:00:00.25",
            "yyyy-MM-dd HH:mm:ss.nnn",
            TimeUnit::Milli
        )
        .is_none());
        // Pre-epoch fraction composes with floored seconds (rem_euclid).
        let dt = DateTime::parse_with_format(
            "1969-12-31 23:59:59.500",
            "yyyy-MM-dd HH:mm:ss.nnn",
            TimeUnit::Milli,
        )
        .unwrap();
        assert_eq!(dt.ticks, -500);
        assert_eq!(dt.to_string(), "1969-12-31T23:59:59.500");
    }

    #[test]
    fn format_validation_and_unit_derivation() {
        // Program-level mistakes are declaration errors (never-silent).
        assert!(DateTime::validate_format("[xx-yy]yyyy").is_err());
        assert!(DateTime::validate_format("[ja-jp").is_err());
        assert!(DateTime::validate_format("mm.nnn ss.nnn").is_err());
        assert!(DateTime::validate_format("ss.nnnnnnnnnn").is_err()); // 10 digits
        assert!(DateTime::validate_format("[ja-jp]yyyy年(ddd) HH:mm:ss.nnnnnn").is_ok());
        assert!(DateTime::validate_format("[EN-US]yyyy").is_ok()); // case-insensitive
                                                                   // The sub-second run decides the column's tick unit.
        assert_eq!(DateTime::unit_for_format("HH:mm:ss"), TimeUnit::Sec);
        assert_eq!(DateTime::unit_for_format("ss.n"), TimeUnit::Milli);
        assert_eq!(DateTime::unit_for_format("ss.nnn"), TimeUnit::Milli);
        assert_eq!(DateTime::unit_for_format("ss.nnnnnn"), TimeUnit::Micro);
        assert_eq!(DateTime::unit_for_format("ss.nnnnnnnnn"), TimeUnit::Nano);
        // The `n` inside the `[en-us]` tag itself must not count as a run.
        assert_eq!(DateTime::unit_for_format("[en-us]HH:mm:ss"), TimeUnit::Sec);
        assert!(!DateTime::format_has_subsec("[en-us]HH:mm:ss"));
        assert!(DateTime::format_has_subsec("[en-us]HH:mm:ss.nnn"));
    }

    #[test]
    fn tz_abbreviations_normalize_unambiguous_only() {
        // §29 s3 / #140 (a): a trailing unambiguous abbreviation is a fixed
        // offset — 21:00 JST is 12:00 UTC. Never a DST-rule conversion.
        let utc = DateTime::parse_auto("2026-06-10T12:00:00Z", TimeUnit::Sec).unwrap();
        let jst = DateTime::parse_auto("2026-06-10 21:00:00 JST", TimeUnit::Sec).unwrap();
        assert_eq!(jst, utc);
        // Every table entry applies exactly its fixed offset (UTC = local − off).
        for (abbr, off) in [
            ("UTC", 0i64),
            ("GMT", 0),
            ("JST", 9 * 3600),
            ("MST", -7 * 3600),
            ("HST", -10 * 3600),
        ] {
            let got = DateTime::parse_auto(&format!("2026-06-10 12:00:00 {abbr}"), TimeUnit::Sec)
                .unwrap();
            assert_eq!(got.ticks, utc.ticks - off, "{abbr} offset wrong");
        }
        // Ambiguous (CST/IST/…, and EST: US −5 vs Australian +10), lowercase,
        // and unknown abbreviations never strip — the cell fails its formats
        // (counted at the caller, design 23), never silently guessed.
        for bad in [
            "2026-06-10 12:00:00 CST",
            "2026-06-10 12:00:00 IST",
            "2026-06-10 12:00:00 EST",
            "2026-06-10 12:00:00 jst",
            "2026-06-10 12:00:00 XYZ",
        ] {
            assert!(
                DateTime::parse_auto(bad, TimeUnit::Sec).is_none(),
                "must not guess: {bad}"
            );
        }
        // ISO order `…ss.frac ABBR`: both strip on the auto path…
        assert_eq!(
            DateTime::parse_auto("2026-06-10 21:00:00.5 JST", TimeUnit::Sec).unwrap(),
            utc
        );
        // …while `strip_zone` keeps the fraction for `n…n` specs.
        assert_eq!(
            DateTime::strip_zone("2026-06-10 21:00:00.123 JST"),
            ("2026-06-10 21:00:00.123", 9 * 3600)
        );
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
    fn datetime_iso_zone_and_fraction_normalize() {
        // #93: a trailing timezone normalises to UTC and a fractional second is
        // truncated (MVP Sec lane). Oracle: exact rendered UTC instant.
        let at = |s: &str| DateTime::parse_auto(s, TimeUnit::Sec).unwrap().to_string();
        assert_eq!(at("2024-06-03T14:30:00Z"), "2024-06-03T14:30:00"); // Z = UTC
        assert_eq!(at("2024-06-03T14:30:00.5"), "2024-06-03T14:30:00"); // frac truncated
        assert_eq!(at("2024-06-03T14:30:00.123456"), "2024-06-03T14:30:00");
        assert_eq!(at("2024-06-03T14:30:00+09:00"), "2024-06-03T05:30:00"); // −9h → UTC
        assert_eq!(at("2024-06-03T14:30:00-05:00"), "2024-06-03T19:30:00"); // +5h → UTC
        assert_eq!(at("2024-06-03T14:30:00.5Z"), "2024-06-03T14:30:00"); // frac + Z
                                                                         // The plain and date-only forms are untouched by the normalisation.
        assert_eq!(at("2024-06-03T14:30:00"), "2024-06-03T14:30:00");
        assert_eq!(at("2024-06-03"), "2024-06-03T00:00:00");
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

    /// Inputs exercising every `AUTO_FORMATS` shape plus adversarial
    /// near-misses, shared by the move-to-front pins below (#135).
    fn auto_format_corpus() -> Vec<String> {
        let mut v: Vec<String> = [
            // One valid sample per AUTO_FORMATS entry (all six shapes).
            "2024-06-03T14:30:00",
            "2024-06-03 14:30:00",
            "2024-06-03",
            "20240603143000",
            "240603143000",
            "20240603",
            // ISO variants normalised before the trial (#93).
            "2024-06-03T14:30:00Z",
            "2024-06-03T14:30:00.123",
            "2024-06-03T14:30:00+09:00",
            "  2024-06-03T14:30:00  ", // surrounding whitespace
            // Two-digit-year pivot boundary (00–68 → 20xx, 69–99 → 19xx).
            "680101000000",
            "690101000000",
            // Near-misses / garbage → None for every format.
            "",
            "not-a-date",
            "2024-13-03", // month out of range
            "2024-06-32", // day out of range
            "2024/06/03", // wrong separator
            "99999999",   // 8 digits, invalid MMdd
            "12",
            "2024-06-03T14:30", // truncated
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        // A deterministic sweep of pure-digit strings at each length the formats
        // care about (6/8/10/12/14), to surface any cross-format ambiguity.
        for len in [6usize, 8, 10, 12, 14] {
            for seed in [0u64, 1, 7, 13, 42] {
                let mut s = String::new();
                let mut x = seed.wrapping_mul(2_654_435_761).wrapping_add(len as u64);
                for _ in 0..len {
                    x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                    s.push((b'0' + ((x >> 33) % 10) as u8) as char);
                }
                v.push(s);
            }
        }
        v
    }

    #[test]
    fn auto_formats_disjoint() {
        // Byte-identity of the move-to-front trial (#135) rests on this: at most
        // one AUTO_FORMATS entry may match any input, so reordering the trial
        // cannot change which format wins. If a future format overlaps an
        // existing one this fails loudly — keep AUTO_FORMATS mutually disjoint
        // (separators + full-consumption digit counts; design 23).
        for unit in [TimeUnit::Sec, TimeUnit::Milli] {
            for s in auto_format_corpus() {
                let (base, _) = DateTime::normalize_iso(s.trim());
                let matches = DateTime::AUTO_FORMATS
                    .iter()
                    .filter(|f| DateTime::parse_with_format(base, f, unit).is_some())
                    .count();
                assert!(
                    matches <= 1,
                    "ambiguous input {s:?} matched {matches} formats"
                );
            }
        }
    }

    #[test]
    fn parse_auto_sticky_byte_identical() {
        // The move-to-front parse (#135) must equal the canonical first-match
        // parse for EVERY starting hint — the sticky-vs-not and serial==parallel
        // byte-identity guarantee. Starting hints past the end exercise the
        // defensive clamp; on a match the hint must land in range so the next
        // cell's fast path is valid.
        let n = DateTime::AUTO_FORMATS.len();
        for unit in [TimeUnit::Sec, TimeUnit::Milli] {
            for s in auto_format_corpus() {
                let canonical = DateTime::parse_auto(&s, unit);
                for start in 0..=n + 3 {
                    let mut hint = start;
                    let got = DateTime::parse_auto_sticky(&s, unit, &mut hint);
                    assert_eq!(got, canonical, "input {s:?} start-hint {start}");
                    if got.is_some() {
                        assert!(hint < n, "hint {hint} out of range for {s:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn datetime_compares_across_units_exactly() {
        // 1 second in millis vs 1 second in seconds are equal instants.
        let a = DateTime::new(1_000, TimeUnit::Milli);
        let b = DateTime::new(1, TimeUnit::Sec);
        assert_eq!(a, b);
        assert!(DateTime::new(999, TimeUnit::Milli) < b);
        assert!(DateTime::new(1_001, TimeUnit::Milli) > b);
        // Sub-second resolution is preserved in ticks AND rendered by Display
        // at the unit's full width (§29 s3 — precision is never silently
        // dropped on output; before s3 no user program could construct a
        // unit > Sec lane, so this changes no existing flow's bytes).
        let m = DateTime::new(1_500, TimeUnit::Milli);
        assert_eq!(m.to_string(), "1970-01-01T00:00:01.500");
        assert_eq!(
            DateTime::new(1_000_000, TimeUnit::Micro).to_string(),
            "1970-01-01T00:00:01.000000"
        );
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
