//! Deterministic test/benchmark data generation.
//!
//! Hidden from the public API (`#[doc(hidden)]`) but shared by benches and
//! stress tests so the "large / error-heavy / mixed" workloads are defined in
//! exactly one place and are byte-for-byte reproducible (seeded, no `rand`).

/// Tiny SplitMix64 PRNG — deterministic, dependency-free.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed.wrapping_add(0x9E37_79B9_7F4A_7C15))
    }
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// Uniform in `[0, n)`.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }
}

const COUNTRIES: [&str; 5] = ["JP", "US", "DE", "FR", "BR"];
const NAMES: [&str; 8] = ["aki", "ben", "cho", "dee", "eri", "fum", "gen", "hina"];

fn name(i: usize) -> String {
    format!("{}{}", NAMES[i % NAMES.len()], i)
}

/// Header shared by the clean / error-heavy generators.
pub const HEADER: &str = "id,name,age,score,country,active";

/// `rows` well-formed records: `id,name,age,score,country,active`.
pub fn clean(rows: usize, seed: u64) -> String {
    let mut rng = Rng::new(seed);
    let mut s = String::with_capacity(rows * 32);
    s.push_str(HEADER);
    s.push('\n');
    for i in 0..rows {
        let age = rng.below(90);
        let score = (rng.below(10_000) as f64) / 100.0;
        let country = COUNTRIES[rng.below(COUNTRIES.len() as u64) as usize];
        let active = rng.below(2) == 1;
        s.push_str(&format!(
            "{i},{},{age},{score},{country},{active}\n",
            name(i)
        ));
    }
    s
}

/// `rows` records where ~`bad_ratio` (0.0–1.0) of lines are malformed:
/// either the wrong arity, or an unparseable `age`. Exercises the
/// continue-first skip path and per-source recoverable error.
pub fn error_heavy(rows: usize, bad_ratio: f64, seed: u64) -> String {
    let mut rng = Rng::new(seed);
    let threshold = (bad_ratio.clamp(0.0, 1.0) * 1_000_000.0) as u64;
    let mut s = String::with_capacity(rows * 32);
    s.push_str(HEADER);
    s.push('\n');
    for i in 0..rows {
        if rng.below(1_000_000) < threshold {
            // Malformed: drop columns so the arity no longer matches the header.
            match rng.below(2) {
                0 => s.push_str(&format!("{i},{}\n", name(i))), // too few fields
                _ => s.push_str("###garbage row###\n"),         // single junk field
            }
            continue;
        }
        let age = rng.below(90);
        let score = (rng.below(10_000) as f64) / 100.0;
        let country = COUNTRIES[rng.below(COUNTRIES.len() as u64) as usize];
        let active = rng.below(2) == 1;
        s.push_str(&format!(
            "{i},{},{age},{score},{country},{active}\n",
            name(i)
        ));
    }
    s
}

/// Mixed-type column: `value` is mostly integers but ~`mix_ratio` of cells are
/// floats or text, so type inference falls back to `Str` and the predicate runs
/// on the string lane. Exercises graceful type degradation (Master principle 7).
pub fn mixed_types(rows: usize, mix_ratio: f64, seed: u64) -> String {
    let mut rng = Rng::new(seed);
    let threshold = (mix_ratio.clamp(0.0, 1.0) * 1_000_000.0) as u64;
    let mut s = String::with_capacity(rows * 16);
    s.push_str("id,value\n");
    for i in 0..rows {
        if rng.below(1_000_000) < threshold {
            match rng.below(2) {
                0 => s.push_str(&format!("{i},{}.5\n", rng.below(100))), // float
                _ => s.push_str(&format!("{i},N/A\n")),                  // text
            }
        } else {
            s.push_str(&format!("{i},{}\n", rng.below(100)));
        }
    }
    s
}

/// Rivus layout for [`bin_clean`]: packed little-endian `i32 id, i32 age,
/// f64 score, u8 active` (record size = 17 bytes).
pub const BIN_LAYOUT: &str = "id:i32 age:i32 score:f64 active:u8";

/// `rows` fixed-width binary records matching [`BIN_LAYOUT`] (a C struct dump).
/// The PRNG draws per row are `age = below(90)`, `score = below(10_000)`,
/// `active = below(2)` — `id` is the row index and consumes no randomness.
pub fn bin_clean(rows: usize, seed: u64) -> Vec<u8> {
    let mut rng = Rng::new(seed);
    let mut out = Vec::with_capacity(rows * 17);
    for i in 0..rows {
        out.extend_from_slice(&(i as i32).to_le_bytes());
        let age = rng.below(90) as i32;
        out.extend_from_slice(&age.to_le_bytes());
        let score = (rng.below(10_000) as f64) / 100.0;
        out.extend_from_slice(&score.to_le_bytes());
        out.push(if rng.below(2) == 1 { 1u8 } else { 0u8 });
    }
    out
}

/// `rows` JSON Lines objects with the same logical fields as [`clean`]:
/// `{"id":..,"name":"..","age":..,"score":..,"country":"..","active":..}`.
/// PRNG draws per row match [`clean`]: `age=below(90)`, `score=below(10_000)`,
/// `country=below(5)`, `active=below(2)`.
pub fn jsonl_clean(rows: usize, seed: u64) -> String {
    let mut rng = Rng::new(seed);
    let mut s = String::with_capacity(rows * 64);
    for i in 0..rows {
        let age = rng.below(90);
        let score = (rng.below(10_000) as f64) / 100.0;
        let country = COUNTRIES[rng.below(COUNTRIES.len() as u64) as usize];
        let active = rng.below(2) == 1;
        s.push_str(&format!(
            "{{\"id\":{i},\"name\":\"{}\",\"age\":{age},\"score\":{score},\"country\":\"{country}\",\"active\":{active}}}\n",
            name(i)
        ));
    }
    s
}

/// Like [`write_temp`] but for raw bytes (binary fixtures).
pub fn write_temp_bytes(tag: &str, bytes: &[u8]) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut path = std::env::temp_dir();
    path.push(format!("rivus_{tag}_{}_{n}.bin", std::process::id()));
    std::fs::write(&path, bytes).expect("write temp data");
    path
}

/// Write `text` to a uniquely-named temp file and return its path. The caller
/// owns cleanup; benches/tests delete it when done.
pub fn write_temp(tag: &str, text: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut path = std::env::temp_dir();
    path.push(format!("rivus_{tag}_{}_{n}.csv", std::process::id()));
    std::fs::write(&path, text).expect("write temp data");
    path
}
