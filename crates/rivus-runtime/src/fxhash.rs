//! In-tree Fx-style hasher for the row-hot hash paths (group-by scratch,
//! broadcast-probe table).
//!
//! `std`'s default `HashMap` hasher (SipHash-1-3) is DoS-resistant but costs
//! several times more than needed for short, internally-generated keys that an
//! attacker never controls end-to-end (composite group keys, join key cells).
//! This is the classic multiply-rotate word hash used by rustc ("FxHash"),
//! re-implemented here in ~30 lines so the default build's supply chain grows
//! by nothing (CLAUDE.md policy v2 allows the crate, but there is nothing to
//! vet if the code is this small and std-only).
//!
//! Properties that matter to Rivus:
//! - **Deterministic**: no random seed, no per-process state — the same bytes
//!   hash the same everywhere. (Not that it may matter: every consumer must
//!   keep hash-iteration order out of observable output; see below.)
//! - **Byte-identity is preserved by construction at the call sites**, never
//!   by the hasher: the group-by drains its scratch map into the sorted
//!   canonical `BTreeMap` before anything is emitted, and the broadcast probe
//!   emits in left-row order — in both, map iteration order is unobservable.
//!
//! Do NOT use for anything attacker-facing or persisted.

use std::hash::{BuildHasherDefault, Hasher};

/// Multiply-rotate word hasher (rustc's FxHasher shape, 64-bit lanes).
#[derive(Default)]
pub(crate) struct FxHasher {
    hash: u64,
}

/// `BuildHasher` plug for `HashMap`/`HashSet` type parameters.
pub(crate) type FxBuild = BuildHasherDefault<FxHasher>;

const K: u64 = 0x51_7c_c1_b7_27_22_0a_95;

impl FxHasher {
    #[inline(always)]
    fn add(&mut self, word: u64) {
        self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(K);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut b = bytes;
        while b.len() >= 8 {
            self.add(u64::from_le_bytes(b[..8].try_into().expect("8-byte chunk")));
            b = &b[8..];
        }
        if b.len() >= 4 {
            self.add(u64::from(u32::from_le_bytes(
                b[..4].try_into().expect("4-byte chunk"),
            )));
            b = &b[4..];
        }
        for &byte in b {
            self.add(u64::from(byte));
        }
    }

    #[inline]
    fn write_u8(&mut self, n: u8) {
        self.add(u64::from(n));
    }

    #[inline]
    fn write_u64(&mut self, n: u64) {
        self.add(n);
    }

    #[inline]
    fn write_usize(&mut self, n: usize) {
        self.add(n as u64);
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::BuildHasher;

    /// Determinism: same bytes → same hash, across hasher instances (no seed).
    #[test]
    fn deterministic() {
        let b = FxBuild::default();
        let h1 = b.hash_one("country\u{1f}category");
        let h2 = FxBuild::default().hash_one("country\u{1f}category");
        assert_eq!(h1, h2);
        assert_ne!(h1, b.hash_one("country\u{1f}categorz"));
    }

    /// A `HashMap<String, _, FxBuild>` behaves like the std one for the
    /// borrow-based lookups the hot paths use (`&str` against `String` keys).
    #[test]
    fn str_borrow_lookup() {
        let mut m: std::collections::HashMap<String, i32, FxBuild> =
            std::collections::HashMap::default();
        m.insert("r0".to_string(), 1);
        assert_eq!(m.get("r0"), Some(&1));
        assert_eq!(m.get("r1"), None);
    }
}
