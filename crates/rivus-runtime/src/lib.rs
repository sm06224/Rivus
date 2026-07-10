//! `rivus-runtime` — the chunk execution engine.
//!
//! Given a [`rivus_ir::PlanGraph`], [`run`] executes it with a single-threaded,
//! chunk-granular, continue-first scheduler and returns the captured outputs,
//! the full error stream, the final runtime mode and per-node telemetry. See
//! `docs/design/02-execution-model.md` and `05-scheduler.md`.

mod analytics;
mod codec;
mod csv;
mod discovery;
mod engine;
mod eval;
mod fxhash;
mod jsonl;
mod kernel;
// §33 networking transport (feature `net`): a std-only HTTP/1.1 GET client and a
// TCP subscribe dial — `open "http://…"` (bounded GET) and `subscribe "tcp://…"`
// (unbounded feed). The default build does not compile it (zero-dep invariant).
// Distributed execution (`serve` / `run --on`) is a later slice on this feature.
#[cfg(feature = "net")]
mod net;
// §33 / §17 protected-channel distributed execution (feature `net`): ship the IR
// to a remote worker over a trusted channel (kernel-WireGuard-bound posture) and
// run it on the same chunk engine — byte-identical to a local run
// (`interpret == distribute`, §0.5). `serve` / `run --on rivus://…`.
#[cfg(feature = "net")]
pub mod distributed;
// §34.3 transport CPU-budget / core affinity (feature `net`). A no-op shim today
// (the API is always present so callers stay cfg-free); the actual
// `sched_setaffinity` syscall is gated behind a later `cpubudget` feature (+libc,
// Linux) and proven by a benchmark first. Dep-zero — pulls no `libc`.
#[cfg(feature = "net")]
pub mod cpu_budget;
mod operators;
// SUPPLY-CHAIN selected adapter (read-only slice): Apache Parquet input behind
// the off-by-default `parquet` feature. The default build does not compile it
// (zero-dep invariant); a feature-less run refuses the plan pre-run.
#[cfg(feature = "parquet")]
mod parquet_read;
mod route;
mod telemetry;
mod transport;

#[doc(hidden)]
pub mod gendata;

pub use analytics::{choose_strategy, Analytics, MemoryPref, Strategy};
pub use engine::{
    plan_validate, run, run_with_progress, Output, ProgressHook, RunOptions, RunResult,
};
pub use telemetry::{NodeSnapshot, NodeTelemetry, RuntimeSnapshot, WorkerTelemetry};

#[cfg(test)]
mod tests {
    use super::*;
    use rivus_core::Value;
    use std::io::Write;

    fn write_temp(name: &str, body: &str) -> String {
        let mut path = std::env::temp_dir();
        path.push(format!("rivus_test_{name}_{}.csv", std::process::id()));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn filter_project_pipeline() {
        let csv = write_temp("users", "name,age\naki,30\nben,15\ncho,40\n");
        let src = format!("Users:\n    open {csv}\n    |? age >= 20\n    |> name\n;");
        let graph = rivus_parser::parse(&src).unwrap();
        let res = run(
            &graph,
            RunOptions {
                chunk_size: 2,
                ..Default::default()
            },
        )
        .unwrap();

        let out = &res.outputs[0];
        let names: Vec<Value> = out
            .chunks
            .iter()
            .flat_map(|c| (0..c.len).map(|r| c.value(r, 0)))
            .collect();
        assert_eq!(
            names,
            vec![Value::Str("aki".into()), Value::Str("cho".into())]
        );
        assert_eq!(out.label.as_deref(), Some("Users"));
    }

    #[test]
    fn branch_and_merge_preserves_all_rows() {
        let csv = write_temp("split", "name,age\naki,30\nben,15\ncho,40\ndee,10\n");
        let src = format!(
            "\
Users:
    open {csv}
    -> Adults:
        |? age >= 20
    ;
    -> Minors:
        |? age < 20
    ;
;
Merged:
    Adults + Minors
;"
        );
        let graph = rivus_parser::parse(&src).unwrap();
        let res = run(
            &graph,
            RunOptions {
                chunk_size: 4,
                ..Default::default()
            },
        )
        .unwrap();
        let merged = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("Merged"))
            .unwrap();
        let rows: u64 = merged.chunks.iter().map(|c| c.len as u64).sum();
        assert_eq!(rows, 4); // 2 adults + 2 minors
    }

    #[test]
    fn malformed_rows_continue() {
        // Row "bad" has too few columns: it must be skipped, not crash.
        let csv = write_temp("bad", "name,age\naki,30\nbad\ncho,40\n");
        let src = format!("Users:\n    open {csv}\n    |? age >= 20\n;");
        let graph = rivus_parser::parse(&src).unwrap();
        let res = run(&graph, RunOptions::default()).unwrap();
        assert!(res.errors.iter().any(|e| e.message.contains("malformed")));
        // aki + cho survive.
        assert_eq!(res.total_rows_out(), 2);
    }
}
