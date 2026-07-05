//! Fast, allocation-free numeric **formatting** (the write-side twin of
//! [`crate::numparse`]). The sink profile on 1 GB shows `save` as the second
//! cost after parse, and inside it `std::fmt` machinery dominates: a single
//! f64 column costs more to format than two i64 columns (measured; see
//! `docs/BENCHMARKS.md` "sink numeric formatting").
//!
//! Byte-identity is the contract: every fast path here emits **exactly** the
//! bytes `format!("{}")` would, or refuses (`false`) so the caller falls back
//! to `std::fmt`. Nothing may silently diverge from the std rendering.
//!
//! - Integers: the classic two-digits-per-step LUT (itoa-style), trivially
//!   identical to std's decimal rendering.
//! - Floats: a *short fixed-decimal* fast path. `Display` for f64 in Rust is
//!   the shortest round-trip decimal, rendered positionally (never `1e20`
//!   notation — probed and pinned in tests). For |v| ≤ 2^53 we search the
//!   smallest fraction width `k` whose nearest candidate `m = round(v·10^k)`
//!   **exactly** round-trips: with `m` and `10^k` both exactly representable,
//!   `(m as f64) / (10^k as f64)` is the correctly-rounded real quotient, so
//!   `q == v` is an exact decimal→binary round-trip test, not a heuristic.
//!   Anything ambiguous (a neighbor also round-trips, a trailing zero from a
//!   misrounded product, magnitude past 2^53, non-finite) → `false` → std.

/// `b"00".."99"` as 200 bytes: the two-digit lookup table.
static DIGIT_PAIRS: &[u8; 200] = b"\
0001020304050607080910111213141516171819\
2021222324252627282930313233343536373839\
4041424344454647484950515253545556575859\
6061626364656667686970717273747576777879\
8081828384858687888990919293949596979899";

/// Append `v`'s decimal digits to `buf` — byte-identical to `format!("{v}")`.
pub fn push_u64(buf: &mut String, v: u64) {
    let mut tmp = [0u8; 20];
    let n = write_u64_digits(v, &mut tmp);
    // The buffer holds ASCII digits only.
    buf.push_str(std::str::from_utf8(&tmp[20 - n..]).expect("ascii digits"));
}

/// Append `v` in decimal — byte-identical to `format!("{v}")`, including
/// `i64::MIN` (negated on the unsigned side, so no overflow).
pub fn push_i64(buf: &mut String, v: i64) {
    if v < 0 {
        buf.push('-');
    }
    push_u64(buf, v.unsigned_abs());
}

/// Write `v`'s digits right-aligned into `tmp`, returning the digit count.
/// Two digits per step via [`DIGIT_PAIRS`].
fn write_u64_digits(mut v: u64, tmp: &mut [u8; 20]) -> usize {
    let mut pos = 20;
    while v >= 100 {
        let pair = ((v % 100) as usize) * 2;
        v /= 100;
        pos -= 2;
        tmp[pos] = DIGIT_PAIRS[pair];
        tmp[pos + 1] = DIGIT_PAIRS[pair + 1];
    }
    if v >= 10 {
        let pair = (v as usize) * 2;
        pos -= 2;
        tmp[pos] = DIGIT_PAIRS[pair];
        tmp[pos + 1] = DIGIT_PAIRS[pair + 1];
    } else {
        pos -= 1;
        tmp[pos] = b'0' + v as u8;
    }
    20 - pos
}

/// 2^53 — the largest magnitude where every integer is exactly representable
/// and the shortest decimal of an integral f64 is provably the integer itself
/// (ulp ≤ 1 ⇒ the rounding interval holds no shorter decimal).
const EXACT_LIMIT: f64 = 9_007_199_254_740_992.0;

/// Append `v` exactly as `format!("{v}")` would and return `true`, or return
/// `false` (buffer untouched) when the value is outside the provably-exact
/// fast path and the caller must use `std::fmt`.
///
/// Covers the data-file common case: integers riding an f64 lane and short
/// fixed decimals ("93.46", money, measurements). The k-loop finds the
/// *smallest* fraction width that round-trips, which for a fixed magnitude is
/// exactly the shortest-significant-digits rendering std produces; a
/// same-width neighbor that also round-trips means the shortest form is not
/// unique — that ambiguity (std's tie policy) is not replicated, it is
/// **bailed on**, keeping the contract constructive.
pub fn push_f64(buf: &mut String, v: f64) -> bool {
    if !v.is_finite() {
        return false;
    }
    let a = v.abs();
    if a > EXACT_LIMIT {
        return false;
    }
    let mut pow = 1.0f64; // 10^k, exact for every k reached (≤ 22)
    for k in 0..=17u32 {
        let scaled = a * pow;
        if scaled > EXACT_LIMIT {
            // Beyond 2^53 the candidate integer is no longer exact.
            return false;
        }
        let m = scaled.round();
        // Exact round-trip: m and pow are exactly representable, so the
        // division is the correctly-rounded value of the real m/10^k.
        if m / pow == a {
            // A neighbor that also round-trips ⇒ two shortest candidates ⇒
            // std's tie choice would decide; refuse rather than guess.
            if (m - 1.0) / pow == a || (m + 1.0) / pow == a {
                return false;
            }
            // A trailing zero can only appear here if the k-1 product
            // misrounded past its own candidate; the shorter form exists, so
            // this rendering would not be shortest. Refuse (std handles it).
            if k > 0 && (m / 10.0).trunc() * 10.0 == m {
                return false;
            }
            if v.is_sign_negative() {
                buf.push('-'); // covers -0.0 → "-0", matching std
            }
            let mut tmp = [0u8; 20];
            let n = write_u64_digits(m as u64, &mut tmp);
            let digits = &tmp[20 - n..];
            let k = k as usize;
            if k == 0 {
                buf.push_str(std::str::from_utf8(digits).expect("ascii"));
            } else if n > k {
                // 9346, k=2 → "93.46"
                buf.push_str(std::str::from_utf8(&digits[..n - k]).expect("ascii"));
                buf.push('.');
                buf.push_str(std::str::from_utf8(&digits[n - k..]).expect("ascii"));
            } else {
                // 5, k=3 → "0.005"
                buf.push_str("0.");
                for _ in 0..k - n {
                    buf.push('0');
                }
                buf.push_str(std::str::from_utf8(digits).expect("ascii"));
            }
            return true;
        }
        pow *= 10.0;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fast_i64(v: i64) -> String {
        let mut s = String::new();
        push_i64(&mut s, v);
        s
    }

    fn fast_f64(v: f64) -> Option<String> {
        let mut s = String::new();
        push_f64(&mut s, v).then_some(s)
    }

    #[test]
    fn i64_matches_std_everywhere() {
        for v in [
            0,
            1,
            -1,
            9,
            10,
            99,
            100,
            101,
            -100,
            12345,
            -987654321,
            i64::MAX,
            i64::MIN,
            i64::MIN + 1,
        ] {
            assert_eq!(fast_i64(v), format!("{v}"), "i64 {v}");
        }
        // Dense sweep over digit-length boundaries.
        for p in 0..19u32 {
            let b = 10i64.pow(p);
            for d in -2..=2i64 {
                let v = b.saturating_add(d);
                assert_eq!(fast_i64(v), format!("{v}"));
                assert_eq!(fast_i64(-v), format!("{}", -v));
            }
        }
    }

    #[test]
    fn u64_matches_std_everywhere() {
        for v in [0u64, 7, 42, 100, 65535, u64::MAX] {
            let mut s = String::new();
            push_u64(&mut s, v);
            assert_eq!(s, format!("{v}"));
        }
    }

    #[test]
    fn f64_fast_path_matches_std_on_structured_grid() {
        // Every m/10^k grid point the fast path is built for must match std —
        // and any value it *accepts* must match std byte-for-byte.
        for k in 0..=6u32 {
            let pow = 10f64.powi(k as i32);
            for m in (-20_000..=20_000i64).step_by(7) {
                let v = (m as f64) / pow;
                if let Some(fast) = fast_f64(v) {
                    assert_eq!(fast, format!("{v}"), "m={m} k={k}");
                }
            }
        }
    }

    #[test]
    fn f64_pinned_cases_match_std() {
        // Pins the probed std policy: positional rendering, no ".0" on
        // integral floats, "-0", huge/tiny handled by the std fallback.
        for v in [
            0.0,
            -0.0,
            2.0,
            -2.0,
            93.46,
            -93.46,
            0.5,
            0.0001,
            123456.789,
            1e15,
            9007199254740992.0, // 2^53: integral, at the limit
        ] {
            if let Some(fast) = fast_f64(v) {
                assert_eq!(fast, format!("{v}"), "v={v}");
            }
        }
        // Outside the exact window the fast path must refuse, never guess.
        for v in [
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
            1e20,
            -1e20,
            f64::MAX,
            5e-324,
        ] {
            assert!(fast_f64(v).is_none(), "must fall back to std for {v}");
        }
    }

    #[test]
    fn f64_random_bit_patterns_match_std_when_accepted() {
        // SplitMix64 over raw bit patterns: whatever the fast path accepts
        // must equal std. (Most random doubles have long mantissas and are
        // refused — that refusal is the correctness mechanism.)
        let mut x = 0x9E3779B97F4A7C15u64;
        let mut accepted = 0u32;
        for _ in 0..2_000_000u32 {
            x = x.wrapping_add(0x9E3779B97F4A7C15);
            let mut z = x;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
            z ^= z >> 31;
            let v = f64::from_bits(z);
            if let Some(fast) = fast_f64(v) {
                assert_eq!(fast, format!("{v}"), "bits={z:#x}");
                accepted += 1;
            }
        }
        // And random *short decimals* (the actual data-file shape): these are
        // the cases the fast path exists for, so nearly all must be accepted.
        let mut hits = 0u32;
        for i in 0..500_000u32 {
            x = x.wrapping_add(0x9E3779B97F4A7C15);
            let m = (x >> 40) as i64 - (1 << 23); // ±8.4M
            let k = (i % 7) as i32;
            let v = (m as f64) / 10f64.powi(k);
            match fast_f64(v) {
                Some(fast) => {
                    assert_eq!(fast, format!("{v}"), "m={m} k={k}");
                    hits += 1;
                }
                None => {
                    // A refusal is allowed (ambiguity bail) but must be rare.
                }
            }
        }
        assert!(
            hits > 490_000,
            "short decimals should ride the fast path: {hits}/500000 (random-bits accepted: {accepted})"
        );
    }
}
