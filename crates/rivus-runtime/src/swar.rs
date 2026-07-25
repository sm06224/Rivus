//! Shared byte-search primitives: SWAR (SIMD-within-a-register, 8 bytes/step,
//! std-only, host-endian independent) with an AVX2 dispatch (32 bytes/step)
//! where the host supports it. Extracted from the CSV splitter (#71) so the
//! JSONL scanner shares one vetted implementation instead of growing a twin.
//!
//! Every function is **byte-identical to its scalar loop by construction**:
//! same first/last match position, no acceptance change — the callers' scanned
//! language is untouched, only the stride changes.

pub(crate) const SWAR_LO: u64 = 0x0101_0101_0101_0101;
pub(crate) const SWAR_HI: u64 = 0x8080_8080_8080_8080;
pub(crate) const SWAR_LO7: u64 = 0x7F7F_7F7F_7F7F_7F7F; // !SWAR_HI

/// Broadcast byte `b` into every lane of a u64.
#[inline(always)]
pub(crate) fn swar_splat(b: u8) -> u64 {
    SWAR_LO.wrapping_mul(b as u64)
}

/// For each byte of `word` equal to the byte broadcast in `splat`, set that
/// lane's high bit (`0x80`) and clear the rest — **exactly one bit per match,
/// with no cross-byte contamination**, so `trailing_zeros() >> 3` yields the
/// matching byte index and `m &= m - 1` advances to the next.
///
/// The naive `(x - LO) & ~x & HI` zero-byte trick is only reliable as a
/// *boolean* ("any match?"); its per-byte bits are corrupted by subtraction
/// borrows (a zero byte followed by a `0x01` lane false-positives), which makes
/// it wrong for *locating* matches. This borrow-free variant is exact:
/// `(b & 0x7F) + 0x7F` stays ≤ `0xFE`, so no carry crosses a byte boundary.
#[inline(always)]
pub(crate) fn swar_eq_mask(word: u64, splat: u64) -> u64 {
    let t = word ^ splat; // 0x00 lanes where the byte matches
                          // 0x80 per lane iff that lane is non-zero (carry-free), then flip so 0x80
                          // marks the matching (zero) lanes.
    let nonzero = ((t & SWAR_LO7).wrapping_add(SWAR_LO7) | t) & SWAR_HI;
    nonzero ^ SWAR_HI
}

/// First position `>= from` of `needle` in `bytes`, or `None`. The vectorized
/// `memchr`: AVX2 (32 B/step) when the host has it, else SWAR (8 B/step),
/// scalar tail — all returning the identical index a scalar
/// `iter().position()` would.
#[inline]
pub(crate) fn find_byte(bytes: &[u8], from: usize, needle: u8) -> Option<usize> {
    #[cfg(target_arch = "x86_64")]
    {
        // `is_x86_feature_detected!` memoizes, so this is a cheap cached branch.
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: only called after confirming the CPU supports AVX2.
            return unsafe { find_byte_avx2(bytes, from, needle) };
        }
    }
    find_byte_swar(bytes, from, needle)
}

/// First position `>= from` of `a` OR `b` in `bytes`, or `None` — the string
/// scanner's "closing quote or first escape" probe, one pass instead of two.
#[inline]
pub(crate) fn find_either(bytes: &[u8], from: usize, a: u8, b: u8) -> Option<usize> {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: only called after confirming the CPU supports AVX2.
            return unsafe { find_either_avx2(bytes, from, a, b) };
        }
    }
    find_either_swar(bytes, from, a, b)
}

/// LAST position of `needle` in `bytes`, or `None` — the vectorized
/// `iter().rposition()`, scanning words from the tail (`63 - leading_zeros`
/// picks the highest matching lane).
#[inline]
pub(crate) fn rfind_byte(bytes: &[u8], needle: u8) -> Option<usize> {
    let n = bytes.len();
    let splat = swar_splat(needle);
    let mut i = n;
    // Scalar tail first (the unaligned remainder at the END of the slice).
    while i > 0 && !i.is_multiple_of(8) {
        i -= 1;
        if bytes[i] == needle {
            return Some(i);
        }
    }
    while i >= 8 {
        i -= 8;
        let word = u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        let m = swar_eq_mask(word, splat);
        if m != 0 {
            return Some(i + (63 - m.leading_zeros() as usize) / 8);
        }
    }
    None
}

fn find_byte_swar(bytes: &[u8], from: usize, needle: u8) -> Option<usize> {
    let n = bytes.len();
    let splat = swar_splat(needle);
    let mut i = from;
    while i + 8 <= n {
        let word = u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        let m = swar_eq_mask(word, splat);
        if m != 0 {
            return Some(i + (m.trailing_zeros() as usize >> 3));
        }
        i += 8;
    }
    bytes[i..n].iter().position(|&b| b == needle).map(|p| i + p)
}

fn find_either_swar(bytes: &[u8], from: usize, a: u8, b: u8) -> Option<usize> {
    let n = bytes.len();
    let asplat = swar_splat(a);
    let bsplat = swar_splat(b);
    let mut i = from;
    while i + 8 <= n {
        let word = u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        let m = swar_eq_mask(word, asplat) | swar_eq_mask(word, bsplat);
        if m != 0 {
            return Some(i + (m.trailing_zeros() as usize >> 3));
        }
        i += 8;
    }
    bytes[i..n]
        .iter()
        .position(|&x| x == a || x == b)
        .map(|p| i + p)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn find_byte_avx2(bytes: &[u8], from: usize, needle: u8) -> Option<usize> {
    use std::arch::x86_64::*;
    let n = bytes.len();
    let nvec = _mm256_set1_epi8(needle as i8);
    let mut i = from;
    while i + 32 <= n {
        let chunk = _mm256_loadu_si256(bytes.as_ptr().add(i) as *const __m256i);
        let mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, nvec)) as u32;
        if mask != 0 {
            return Some(i + mask.trailing_zeros() as usize);
        }
        i += 32;
    }
    find_byte_swar(bytes, i, needle)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn find_either_avx2(bytes: &[u8], from: usize, a: u8, b: u8) -> Option<usize> {
    use std::arch::x86_64::*;
    let n = bytes.len();
    let avec = _mm256_set1_epi8(a as i8);
    let bvec = _mm256_set1_epi8(b as i8);
    let mut i = from;
    while i + 32 <= n {
        let chunk = _mm256_loadu_si256(bytes.as_ptr().add(i) as *const __m256i);
        let mask = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, avec))
            | _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, bvec))) as u32;
        if mask != 0 {
            return Some(i + mask.trailing_zeros() as usize);
        }
        i += 32;
    }
    find_either_swar(bytes, i, a, b)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every search agrees with the scalar oracle on every position/length
    /// combination around the 8- and 32-byte strides — the exact boundaries a
    /// stride bug would corrupt.
    #[test]
    fn finds_agree_with_scalar_oracle_across_stride_boundaries() {
        let lens = [0usize, 1, 7, 8, 9, 15, 16, 31, 32, 33, 63, 64, 65, 100];
        for &len in &lens {
            for hit in 0..len {
                let mut v = vec![b'x'; len];
                v[hit] = b'\n';
                for from in 0..=hit {
                    assert_eq!(
                        find_byte(&v, from, b'\n'),
                        Some(hit),
                        "find len={len} hit={hit} from={from}"
                    );
                }
                assert_eq!(find_byte(&v, hit + 1, b'\n'), None);
                assert_eq!(
                    rfind_byte(&v, b'\n'),
                    Some(hit),
                    "rfind len={len} hit={hit}"
                );
            }
            let v = vec![b'x'; len];
            assert_eq!(find_byte(&v, 0, b'\n'), None);
            assert_eq!(rfind_byte(&v, b'\n'), None);
        }
    }

    #[test]
    fn rfind_picks_the_last_of_many() {
        let mut v = vec![b'x'; 70];
        v[3] = b'\n';
        v[40] = b'\n';
        v[66] = b'\n';
        assert_eq!(rfind_byte(&v, b'\n'), Some(66));
        v[69] = b'\n';
        assert_eq!(rfind_byte(&v, b'\n'), Some(69));
    }

    #[test]
    fn find_either_returns_the_first_of_both_needles() {
        for (s, want) in [
            (r#"plain"tail"#, Some(5)),
            (r#"esc\aped"quote"#, Some(3)),
            ("neither at all............................", None),
            (r#"x"#, None),
            (r#"""#, Some(0)),
            (r#"\"#, Some(0)),
        ] {
            assert_eq!(
                find_either(s.as_bytes(), 0, b'"', b'\\'),
                want,
                "input {s:?}"
            );
            // Oracle parity.
            assert_eq!(
                find_either(s.as_bytes(), 0, b'"', b'\\'),
                s.bytes().position(|c| c == b'"' || c == b'\\'),
                "oracle mismatch for {s:?}"
            );
        }
        // A long escape-free run crosses both strides before the hit.
        let long = format!("{}\"", "a".repeat(75));
        assert_eq!(find_either(long.as_bytes(), 0, b'"', b'\\'), Some(75));
    }
}
