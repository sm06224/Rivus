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
mod jsonl;
mod kernel;
// §33 / §17 protected-channel distributed execution (feature `net`): ship the IR
// as the deployment artifact to a remote worker over a capability-gated channel.
// std-only (kernel-WireGuard-bound posture; QUIC is the feature-gated alt).
#[cfg(feature = "net")]
pub mod distributed;
// §28.12.5-3: the QUIC transport — the feature-gated alternative to kernel
// WireGuard (heavy deps: quinn/tokio/rustls/ring). Off by default.
#[cfg(feature = "quic")]
pub mod distributed_quic;
// §34.3 CPU budget / core affinity for the transport (feature `cpubudget`,
// Linux-first). A pre-implementation: pin the transport/crypto threads to a
// bounded core set so they cannot steal SIMD cycles from the data plane. The
// API is always present (no-op without the feature / off-Linux) so callers stay
// cfg-free; only the syscall path is feature-gated.
pub mod cpu_budget;
// §33 networking transport (feature `net`): a std-only HTTP/1.1 GET client and a
// TCP subscribe dial. The default build does not compile it (zero-dep invariant).
#[cfg(feature = "net")]
mod net;
mod operators;
mod route;
mod telemetry;
mod transport;

#[doc(hidden)]
pub mod gendata;

pub use analytics::{choose_strategy, Analytics, MemoryPref, Strategy};
pub use engine::{run, run_with_progress, Output, ProgressHook, RunOptions, RunResult};
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
