//! SWAR (SIMD-within-a-register) digit primitives shared across the runtime's
//! numeric lanes (#71). Pure `u64` arithmetic — **no `core::arch`, no `unsafe`,
//! dependency-zero** — and host-endian independent (words are read
//! little-endian so byte `i` of the input maps to lane `i`). These convert
//! eight ASCII digits per step, the building block for the exact integer
//! (`i64`) and decimal (`i128`) parsers; both keep their results **exact** (no
//! `f64`) and **byte-identical** to a scalar/std parse on every input.

/// True iff all 8 bytes of the little-endian `word` are ASCII `'0'..='9'`
/// (Lemire's branch-free range check). Gates [`parse_8_digits`], which assumes
/// valid digits.
#[inline(always)]
pub fn is_eight_digits(word: u64) -> bool {
    ((word & 0xF0F0_F0F0_F0F0_F0F0)
        | (((word.wrapping_add(0x0606_0606_0606_0606)) & 0xF0F0_F0F0_F0F0_F0F0) >> 4))
        == 0x3333_3333_3333_3333
}

/// Parse exactly 8 ASCII digits (little-endian `word`, byte 0 = leftmost digit)
/// into their value via SWAR pairwise horizontal sums — no per-digit multiply
/// or branch. The caller must guarantee [`is_eight_digits`] (else junk).
#[inline(always)]
pub fn parse_8_digits(word: u64) -> u64 {
    let mut v = word - 0x3030_3030_3030_3030;
    v = (v * 10 + (v >> 8)) & 0x00FF_00FF_00FF_00FF;
    v = (v * 100 + (v >> 16)) & 0x0000_FFFF_0000_FFFF;
    v = (v * 10000 + (v >> 32)) & 0x0000_0000_FFFF_FFFF;
    v
}

/// Accumulate the digit bytes of `digits` (every byte known to be ASCII
/// `'0'..='9'`) into `acc` as `acc·10^len + value`, using 8-digit SWAR blocks
/// plus a scalar tail. The caller guarantees the running result fits `u64`
/// (≤ 18 total digits across chained calls) — the exact-magnitude fast path for
/// the integer/decimal parsers. Returns the updated accumulator.
#[inline]
pub fn accumulate_digits_u64(digits: &[u8], mut acc: u64) -> u64 {
    let n = digits.len();
    let mut i = 0;
    while i + 8 <= n {
        // SAFETY-free: slice bounds checked by the loop guard.
        let word = u64::from_le_bytes(digits[i..i + 8].try_into().unwrap());
        acc = acc * 100_000_000 + parse_8_digits(word);
        i += 8;
    }
    while i < n {
        acc = acc * 10 + (digits[i] - b'0') as u64;
        i += 1;
    }
    acc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_eight_digits_matches_scalar() {
        // Exhaustively probe every byte value in one lane, with the rest digits.
        for b in 0u16..=255 {
            let b = b as u8;
            for lane in 0..8 {
                let mut buf = *b"00000000";
                buf[lane] = b;
                let word = u64::from_le_bytes(buf);
                let want = buf.iter().all(|c| c.is_ascii_digit());
                assert_eq!(is_eight_digits(word), want, "byte={b:#x} lane={lane}");
            }
        }
    }

    #[test]
    fn parse_8_digits_matches_scalar() {
        for sample in ["00000000", "12345678", "90000001", "00000009", "99999999"] {
            let word = u64::from_le_bytes(sample.as_bytes().try_into().unwrap());
            let want: u64 = sample.parse().unwrap();
            assert_eq!(parse_8_digits(word), want, "{sample}");
        }
    }

    #[test]
    fn accumulate_matches_scalar() {
        for s in ["", "7", "42", "12345678", "100000000", "999999999999999999"] {
            let want: u64 = if s.is_empty() { 0 } else { s.parse().unwrap() };
            assert_eq!(accumulate_digits_u64(s.as_bytes(), 0), want, "{s}");
        }
        // Chained (mimics int_part then frac_part of a decimal).
        assert_eq!(
            accumulate_digits_u64(b"678", accumulate_digits_u64(b"12345", 0)),
            12345678
        );
    }
}
