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
            Value::Str(_) => DataType::Str,
        }
    }

    /// Best-effort numeric view for comparisons across the int/float lanes.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::I64(v) => Some(*v as f64),
            Value::F64(v) => Some(*v),
            Value::Dec(d) => Some(d.to_f64()),
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
    fn overflow_is_reported_not_panicked() {
        let big = Decimal::new(i128::MAX, 0);
        assert!(big.checked_add(&Decimal::new(1, 0)).is_none());
        // Rescaling up past i128 range also reports None (caller degrades).
        assert!(Decimal::new(i128::MAX / 5, 0).rescale(2).is_none());
    }
}
