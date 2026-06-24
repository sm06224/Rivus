//! §34.3 — explicit CPU budget / core affinity for the transport (issue #174).
//!
//! **The thesis (§34.0).** Rivus saturates CPU with SIMD on the *data plane*;
//! the transport's crypto (WireGuard / QUIC / TLS) also uses SIMD, so on a
//! distributed node **transport CPU competes with Rivus SIMD for the same
//! cores**. The #173 benchmark made this quantitative: a 200 k-row distributed
//! job is **689 ms** of which the wire is **< 1 %** — the cost is flow execution
//! and crypto, not bandwidth. So the lever is *controlling the transport's CPU
//! footprint* (keep it off the data-plane cores), not making the wire faster.
//!
//! **What this module does.** Pin the calling (transport/crypto/I-O) thread to a
//! bounded core set read from the environment, so it cannot steal cycles from the
//! SIMD data plane:
//!
//! ```text
//!   RIVUS_NET_TRANSPORT_CORES=0,1      Transport (crypto, I/O)
//!   RIVUS_NET_TELEMETRY_CORES=…        Telemetry  (falls back to transport set)
//!   RIVUS_NET_CONTROL_CORES=…          Control    (falls back to transport set)
//!   (everything else)                  Data processing (Rivus SIMD)
//! ```
//!
//! Cores accept a comma list with inclusive ranges: `0,1,4-6`.
//!
//! **Invariants.** Affinity is a *performance/ops* knob, **not data** — it must
//! never change a single output byte (§0.14, exactly like the `watch` queue
//! budget). Pinning is enforced only behind `feature = "cpubudget"` on Linux
//! (`sched_setaffinity`); the **API is always present** (a no-op that reports
//! `Unsupported` off-Linux / without the feature) so callers stay `cfg`-free, and
//! the default / `net` builds compile dep-free and unchanged.
//!
//! Pre-implementation status: the std worker's accept loop is pinned to the
//! `Transport` set. The finer Telemetry/Control split and pinning the QUIC
//! tokio worker threads are design-gated (§34.3, post-ratification).

use std::fmt;

/// Which logical transport role a thread is doing — selects the core set
/// (§34.1's Control / Data / Telemetry separation, here applied to *placement*).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Crypto + socket I/O (the heaviest, most likely to contend with SIMD).
    Transport,
    /// Telemetry/event narration.
    Telemetry,
    /// Control-plane (lifecycle, credit).
    Control,
}

impl Role {
    /// The environment variable naming this role's core set.
    pub fn env_var(self) -> &'static str {
        match self {
            Role::Transport => "RIVUS_NET_TRANSPORT_CORES",
            Role::Telemetry => "RIVUS_NET_TELEMETRY_CORES",
            Role::Control => "RIVUS_NET_CONTROL_CORES",
        }
    }
}

/// The outcome of a pin request — narratable on the telemetry channel, and
/// inspectable in tests. Never an `Err` that halts: affinity is best-effort
/// (continue-first), so a failure to pin degrades to "scheduler decides".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PinOutcome {
    /// Pinned the current thread to these cores.
    Pinned(Vec<usize>),
    /// No budget configured for this role (empty/unset) — leave the scheduler
    /// alone. This is the *default* and is not a problem.
    NoBudget,
    /// The platform/feature cannot pin (off-Linux, or `cpubudget` not built).
    Unsupported,
    /// The syscall was attempted and failed (e.g. a core id that is offline).
    Failed(String),
}

impl fmt::Display for PinOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PinOutcome::Pinned(c) => write!(f, "pinned to cores {c:?}"),
            PinOutcome::NoBudget => write!(f, "no cpu budget set (scheduler decides)"),
            PinOutcome::Unsupported => write!(f, "cpu affinity unsupported on this build/OS"),
            PinOutcome::Failed(e) => write!(f, "cpu affinity failed: {e}"),
        }
    }
}

/// Parse a core spec like `0,1,4-6` into a sorted, de-duplicated core list.
/// Tolerant of whitespace; silently drops empty fields and malformed ranges
/// (best-effort ops knob, never fatal). Public for testing.
pub fn parse_cores(spec: &str) -> Vec<usize> {
    let mut out: Vec<usize> = Vec::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((a, b)) = part.split_once('-') {
            if let (Ok(a), Ok(b)) = (a.trim().parse::<usize>(), b.trim().parse::<usize>()) {
                for c in a..=b {
                    out.push(c);
                }
            }
        } else if let Ok(c) = part.parse::<usize>() {
            out.push(c);
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// The transport CPU budget, parsed once from the environment.
#[derive(Clone, Debug, Default)]
pub struct CpuBudget {
    transport: Vec<usize>,
    telemetry: Vec<usize>,
    control: Vec<usize>,
}

impl CpuBudget {
    /// Read `RIVUS_NET_{TRANSPORT,TELEMETRY,CONTROL}_CORES`. Telemetry/Control
    /// fall back to the transport set when unset (a single knob is the common
    /// case). All-unset → an empty budget (every `cores_for` is empty → no-op).
    pub fn from_env() -> Self {
        let transport = std::env::var(Role::Transport.env_var())
            .map(|s| parse_cores(&s))
            .unwrap_or_default();
        let telemetry = std::env::var(Role::Telemetry.env_var())
            .map(|s| parse_cores(&s))
            .unwrap_or_else(|_| transport.clone());
        let control = std::env::var(Role::Control.env_var())
            .map(|s| parse_cores(&s))
            .unwrap_or_else(|_| transport.clone());
        CpuBudget {
            transport,
            telemetry,
            control,
        }
    }

    /// The core set budgeted for `role` (possibly empty → no pinning).
    pub fn cores_for(&self, role: Role) -> &[usize] {
        match role {
            Role::Transport => &self.transport,
            Role::Telemetry => &self.telemetry,
            Role::Control => &self.control,
        }
    }

    /// True if no role has any cores — the universal default (no env set).
    pub fn is_empty(&self) -> bool {
        self.transport.is_empty() && self.telemetry.is_empty() && self.control.is_empty()
    }
}

/// Pin the **current** thread to the env-budgeted core set for `role`.
/// Best-effort; returns the outcome to narrate (never panics, never halts).
pub fn pin_current_thread(role: Role) -> PinOutcome {
    let budget = CpuBudget::from_env();
    pin_current_thread_to(budget.cores_for(role))
}

/// Pin the **current** thread to exactly `cores`. Empty → `NoBudget`. On a
/// non-Linux or non-`cpubudget` build this is a no-op returning `Unsupported`.
///
/// Affinity changes *where* work runs, never *what* it computes — the result is
/// byte-identical (§0.14). Test-friendly: takes the cores explicitly so a test
/// need not mutate process-global env.
pub fn pin_current_thread_to(cores: &[usize]) -> PinOutcome {
    if cores.is_empty() {
        return PinOutcome::NoBudget;
    }
    pin_impl(cores)
}

#[cfg(all(feature = "cpubudget", target_os = "linux"))]
fn pin_impl(cores: &[usize]) -> PinOutcome {
    // CPU_SET on an index >= CPU_SETSIZE is undefined behaviour; clamp defensively
    // (CPU_SETSIZE is 1024 on glibc). Out-of-range ids are dropped, not fatal.
    const CPU_SETSIZE: usize = 1024;
    let valid: Vec<usize> = cores.iter().copied().filter(|&c| c < CPU_SETSIZE).collect();
    if valid.is_empty() {
        return PinOutcome::Failed(format!("no core id < {CPU_SETSIZE} in {cores:?}"));
    }
    // SAFETY: `set` is zero-initialised then populated only with in-range ids via
    // the libc CPU_* macros; `sched_setaffinity(0, …)` targets the calling thread.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        for &c in &valid {
            libc::CPU_SET(c, &mut set);
        }
        let rc = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        if rc != 0 {
            PinOutcome::Failed(std::io::Error::last_os_error().to_string())
        } else {
            PinOutcome::Pinned(valid)
        }
    }
}

#[cfg(not(all(feature = "cpubudget", target_os = "linux")))]
fn pin_impl(_cores: &[usize]) -> PinOutcome {
    PinOutcome::Unsupported
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cores_handles_lists_ranges_and_junk() {
        assert_eq!(parse_cores("0,1,2"), vec![0, 1, 2]);
        assert_eq!(parse_cores("4-6"), vec![4, 5, 6]);
        assert_eq!(parse_cores(" 0, 4-6 ,1 "), vec![0, 1, 4, 5, 6]);
        assert_eq!(parse_cores("2,2,1-1,1"), vec![1, 2]); // dedup + sort
        assert_eq!(parse_cores(""), Vec::<usize>::new());
        assert_eq!(parse_cores(",,x,9-"), Vec::<usize>::new()); // junk dropped
    }

    #[test]
    fn empty_budget_is_noop() {
        assert_eq!(pin_current_thread_to(&[]), PinOutcome::NoBudget);
        assert!(CpuBudget::default().is_empty());
    }

    #[test]
    fn outcome_renders() {
        assert_eq!(
            PinOutcome::Pinned(vec![0, 1]).to_string(),
            "pinned to cores [0, 1]"
        );
        assert_eq!(
            PinOutcome::NoBudget.to_string(),
            "no cpu budget set (scheduler decides)"
        );
    }

    /// **Byte-identity under affinity (§0.14, the #174 acceptance pin).** Running
    /// a flow with the current thread pinned to a core must produce the *exact
    /// same bytes* as running it unpinned — affinity is placement, not data.
    #[cfg(feature = "cpubudget")]
    #[test]
    fn affinity_does_not_change_output() {
        use crate::{run, RunOptions};
        let path = std::env::temp_dir().join(format!("rivus_aff_{}.csv", std::process::id()));
        let mut csv = String::from("name,age\n");
        for i in 0..500u32 {
            csv.push_str(&format!("user{i},{}\n", 10 + (i % 70)));
        }
        std::fs::write(&path, csv).unwrap();
        let src = format!(
            "R:\n open {}\n |? age >= 40\n |> name age\n;",
            path.display()
        );
        let src = src.as_str();

        let render = || -> Vec<u8> {
            let g = rivus_parser::parse(src).unwrap();
            let (g, _) = rivus_optimizer::optimize(g);
            let res = run(&g, RunOptions::default()).unwrap();
            let mut out = String::new();
            for o in &res.outputs {
                for c in &o.chunks {
                    for r in 0..c.len {
                        for i in 0..c.schema.fields.len() {
                            out.push_str(&c.value(r, i).to_string());
                            out.push(',');
                        }
                        out.push('\n');
                    }
                }
            }
            out.into_bytes()
        };

        let unpinned = render();
        // Pin to core 0 (present on every machine) and re-render.
        let outcome = pin_current_thread_to(&[0]);
        // On Linux+cpubudget this pins; elsewhere it is Unsupported — either way
        // the output must be identical.
        assert!(
            matches!(outcome, PinOutcome::Pinned(_) | PinOutcome::Unsupported),
            "unexpected pin outcome: {outcome}"
        );
        let pinned = render();
        assert_eq!(unpinned, pinned, "affinity must not change output bytes");
        std::fs::remove_file(&path).ok();
    }
}
