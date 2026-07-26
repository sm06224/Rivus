//! Shared test-environment policy (S2 — test honesty).
//!
//! Several guards can only prove what they claim when the environment provides
//! something: more than one CPU (so a *parallel* path actually engages), or the
//! `duckdb` CLI (so the parity diff is a real diff). On a machine that cannot
//! provide it, the honest thing is to say so and skip — and that is what a
//! developer laptop or a single-core runner should keep doing.
//!
//! The failure mode this module closes: **in CI the same skip is indistinguish-
//! able from a pass.** #253 made those guards assert that they had actually
//! fired ("発動 assert 無しのガードは腐る"), but the assert itself sits behind the
//! very environment check that disappears on a small runner, so a 1-CPU runner
//! would report a green tick for activation proofs nobody ran.
//!
//! So a strict flag turns "cannot run it here" into a hard failure. There are
//! **two**, because a CI job can only demand what it actually provides:
//!
//! * `RIVUS_TEST_REQUIRE_ACTIVATION=1` — parallel engagement. Set by the main
//!   `fmt · clippy · test` job, which first verifies the runner has ≥2 CPUs.
//! * `RIVUS_TEST_REQUIRE_DUCKDB=1` — the DuckDB live parity diff. Set only by
//!   the parity job, which installs the pinned CLI. The main job does not have
//!   `duckdb` and must not pretend otherwise.
//!
//! Local runs leave both unset and keep the explicit skip message.

/// The env var whose presence makes a missing capability fatal.
#[allow(dead_code)]
pub const REQUIRE_ACTIVATION: &str = "RIVUS_TEST_REQUIRE_ACTIVATION";
#[allow(dead_code)]
pub const REQUIRE_DUCKDB: &str = "RIVUS_TEST_REQUIRE_DUCKDB";

/// Decide what to do when a guard cannot exercise what it is meant to prove.
///
/// Returns `false` (caller should skip) when the requirement is unmet and the
/// matching strict flag is off. **Panics** when the flag is set, so an
/// environment-shaped hole can never masquerade as a passing guard in CI.
///
/// `what` names the missing capability, `consequence` says what goes unproven.
#[allow(dead_code)] // not every test target uses every helper
pub fn skip_unless(flag: &str, what: &str, consequence: &str) -> bool {
    if std::env::var_os(flag).is_some() {
        panic!(
            "{flag}=1 but {what} — {consequence}. \
             This environment cannot prove the guard, and a silent skip here \
             would report a green tick for a check nobody ran. Run on a host \
             that satisfies it, or unset the variable to skip explicitly."
        );
    }
    eprintln!("skipping: {what} ({consequence})");
    false
}

/// The activation-family wrapper (`RIVUS_TEST_REQUIRE_ACTIVATION`).
#[allow(dead_code)]
pub fn skip_unless_required(what: &str, consequence: &str) -> bool {
    skip_unless(REQUIRE_ACTIVATION, what, consequence)
}

/// The DuckDB-parity wrapper (`RIVUS_TEST_REQUIRE_DUCKDB`) — a separate flag so
/// the main test job, which has no `duckdb`, never demands it.
#[allow(dead_code)]
pub fn skip_unless_duckdb_required(what: &str, consequence: &str) -> bool {
    skip_unless(REQUIRE_DUCKDB, what, consequence)
}

/// `true` when the host has enough CPUs for a parallel path to engage.
#[allow(dead_code)]
pub fn has_parallelism() -> bool {
    std::thread::available_parallelism().map_or(1, |t| t.get()) >= 2
}

/// Guard entry point for the parallel-activation family: `true` → the caller
/// may proceed, `false` → skip (only reachable when the strict flag is off).
#[allow(dead_code)]
pub fn require_parallel_host() -> bool {
    has_parallelism()
        || skip_unless_required(
            "single-core runner cannot exercise the parallel path",
            "the activation assert for the parallel/fused/dict path goes unproven",
        )
}
