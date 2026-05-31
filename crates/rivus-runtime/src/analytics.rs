//! Execution-environment analytics (Epic #30, Pillar C — issue #33).
//!
//! A small, std-only probe of the host that the autotuner uses to choose an
//! execution strategy. No third-party crates: CPU count comes from `std`,
//! available RAM is read from the OS the cheap way per platform, and both can be
//! overridden with env vars for deterministic tests (`RIVUS_CPUS`,
//! `RIVUS_RAM_BYTES`).
//!
//! ## What the strategy actually trades (measured, not asserted)
//!
//! The benchmark that drove this design (288 MB CSV, filter+project+save, 4
//! cpus, warm cache; see `docs/BENCHMARKS.md`):
//!
//! | reader                          | wall  |
//! |---------------------------------|-------|
//! | parallel byte-range (bounded)   | 1.09s |
//! | serial two-pass (bounded)       | 3.44s |
//!
//! The parallel byte-range reader is **both** the fastest and bounded-memory
//! (each worker streams its range and writes an ordered part file). So the real,
//! measured adaptive decision is **serial vs parallel**, driven by CPU count and
//! input size — not a memory-vs-speed reader swap. `--memory low` is the opt-in
//! "force single-thread" floor; the default autotunes.

/// A snapshot of the host's resources the autotuner reads.
#[derive(Debug, Clone, Copy)]
pub struct Analytics {
    /// Best estimate of *available* RAM in bytes (conservative; 0 if unknown).
    pub ram_bytes: u64,
    /// Logical CPUs available to the process.
    pub cpus: usize,
}

impl Analytics {
    /// Probe the environment. Honors `RIVUS_RAM_BYTES` / `RIVUS_CPUS` overrides
    /// (for deterministic tests) before falling back to OS probes.
    pub fn probe() -> Analytics {
        let ram_bytes = env_u64("RIVUS_RAM_BYTES").unwrap_or_else(available_ram_bytes);
        let cpus = env_u64("RIVUS_CPUS")
            .map(|n| n.max(1) as usize)
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1)
            });
        Analytics { ram_bytes, cpus }
    }
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok()?.trim().parse().ok()
}

/// Available RAM in bytes, best-effort per OS. Returns 0 when it can't be
/// determined (callers then treat memory as unknown and stay conservative).
fn available_ram_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        linux_available_ram().unwrap_or(0)
    }
    #[cfg(not(target_os = "linux"))]
    {
        // macOS (`sysctl`) and Windows (`GlobalMemoryStatusEx`) probes can be
        // added with `cfg` + minimal FFI (no crate) later; until then, unknown.
        0
    }
}

/// Parse `MemAvailable` (preferred) or `MemFree` from `/proc/meminfo` (kB → B).
#[cfg(target_os = "linux")]
fn linux_available_ram() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    let pick = |key: &str| -> Option<u64> {
        text.lines().find_map(|l| {
            let rest = l.strip_prefix(key)?;
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            Some(kb * 1024)
        })
    };
    pick("MemAvailable:").or_else(|| pick("MemFree:"))
}

/// Which execution strategy to use (Epic #30 / Pillar C). Both strategies return
/// **byte-identical results** (the parallel reader confirms the global schema
/// before any chunk and concatenates parts in source order — see
/// `streaming_parallel_matches_serial`); they trade CPU for wall-time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    /// Single-thread, bounded-memory streaming reader. The lowest-resource floor.
    Serial,
    /// Byte-range parallel reader across all logical CPUs. Faster on large
    /// inputs and still bounded-memory; the engine's eligibility gates (single
    /// file source, no stateful operators) can still send it back to serial.
    Parallel,
}

/// User-facing memory/speed preference (`--memory low|auto|fast`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MemoryPref {
    /// Force `Serial` — guaranteed single-thread, lowest resource use.
    Low,
    /// Autotune from CPU count + input size (the default). Parallel only pays off
    /// past a threshold, so small inputs stay serial.
    #[default]
    Auto,
    /// Prefer `Parallel` aggressively (a lower size threshold than `Auto`).
    Fast,
}

impl MemoryPref {
    pub fn parse(s: &str) -> Option<MemoryPref> {
        match s.to_ascii_lowercase().as_str() {
            "low" => Some(MemoryPref::Low),
            "auto" => Some(MemoryPref::Auto),
            "fast" => Some(MemoryPref::Fast),
            _ => None,
        }
    }

    fn word(self) -> &'static str {
        match self {
            MemoryPref::Low => "memory=low",
            MemoryPref::Auto => "memory=auto",
            MemoryPref::Fast => "memory=fast",
        }
    }
}

/// Choose a strategy from the preference, the host probe, the input size and the
/// parallel size threshold (`min_parallel_bytes`, supplied by the engine so the
/// decision and the engine's reader agree exactly). `Serial` is the floor:
/// `Parallel` is selected only with ≥2 CPUs and a large-enough (or
/// unknown-size, e.g. stdin) input. Returns the strategy and a one-line
/// rationale for telemetry (Observability §13: surface the decision).
pub fn choose_strategy(
    pref: MemoryPref,
    env: &Analytics,
    input_bytes: Option<u64>,
    min_parallel_bytes: u64,
) -> (Strategy, String) {
    match pref {
        MemoryPref::Low => (
            Strategy::Serial,
            "memory=low: forced serial (single-thread, bounded)".into(),
        ),
        MemoryPref::Auto | MemoryPref::Fast => {
            if env.cpus < 2 {
                return (
                    Strategy::Serial,
                    format!("{}: {} cpu → serial", pref.word(), env.cpus),
                );
            }
            match input_bytes {
                Some(n) if n >= min_parallel_bytes => (
                    Strategy::Parallel,
                    format!(
                        "{}: {} B ≥ {} B, {} cpus → parallel",
                        pref.word(),
                        n,
                        min_parallel_bytes,
                        env.cpus
                    ),
                ),
                Some(n) => (
                    Strategy::Serial,
                    format!(
                        "{}: {} B < {} B threshold → serial",
                        pref.word(),
                        n,
                        min_parallel_bytes
                    ),
                ),
                // Unknown size (e.g. stdin): defer to the engine's eligibility
                // gates rather than refusing parallelism outright.
                None => (
                    Strategy::Parallel,
                    format!(
                        "{}: unknown size, {} cpus → parallel",
                        pref.word(),
                        env.cpus
                    ),
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overrides_are_honored() {
        std::env::set_var("RIVUS_RAM_BYTES", "8000000000");
        std::env::set_var("RIVUS_CPUS", "4");
        let a = Analytics::probe();
        assert_eq!(a.ram_bytes, 8_000_000_000);
        assert_eq!(a.cpus, 4);
        std::env::remove_var("RIVUS_RAM_BYTES");
        std::env::remove_var("RIVUS_CPUS");
    }

    #[test]
    fn strategy_respects_cpus_size_and_floor() {
        let quad = Analytics {
            ram_bytes: 8_000_000_000,
            cpus: 4,
        };
        let min = 8 << 20; // 8 MiB
                           // low: always serial regardless of size/cpus.
        assert_eq!(
            choose_strategy(MemoryPref::Low, &quad, Some(1 << 30), min).0,
            Strategy::Serial
        );
        // auto/fast: large input + multicore → parallel.
        assert_eq!(
            choose_strategy(MemoryPref::Auto, &quad, Some(64 << 20), min).0,
            Strategy::Parallel
        );
        // auto/fast: small input → serial (parallel wouldn't pay off).
        assert_eq!(
            choose_strategy(MemoryPref::Fast, &quad, Some(1 << 10), min).0,
            Strategy::Serial
        );
        // unknown size (stdin) → parallel, defer to engine eligibility.
        assert_eq!(
            choose_strategy(MemoryPref::Auto, &quad, None, min).0,
            Strategy::Parallel
        );
        // single cpu → serial even for a huge file.
        let uni = Analytics {
            ram_bytes: 8_000_000_000,
            cpus: 1,
        };
        assert_eq!(
            choose_strategy(MemoryPref::Fast, &uni, Some(1 << 30), min).0,
            Strategy::Serial
        );
    }

    #[test]
    fn fast_threshold_is_more_aggressive_than_auto() {
        let quad = Analytics {
            ram_bytes: 8_000_000_000,
            cpus: 4,
        };
        let n = Some(2 << 20); // 2 MiB
                               // Auto threshold 8 MiB → serial; Fast threshold 1 MiB → parallel.
        assert_eq!(
            choose_strategy(MemoryPref::Auto, &quad, n, 8 << 20).0,
            Strategy::Serial
        );
        assert_eq!(
            choose_strategy(MemoryPref::Fast, &quad, n, 1 << 20).0,
            Strategy::Parallel
        );
    }
}
