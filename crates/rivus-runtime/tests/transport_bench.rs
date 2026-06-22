//! Transport micro-benchmarks (loopback) — distributed-execution round-trip
//! latency and throughput for the std (kernel-WireGuard-bound) path and, when
//! built with `--features quic`, the QUIC alternative. `#[ignore]`d (opt-in):
//!
//!   cargo test -p rivus-runtime --features net  --test transport_bench -- --ignored --nocapture
//!   cargo test -p rivus-runtime --features quic --test transport_bench -- --ignored --nocapture
//!
//! Numbers land in `docs/BENCHMARKS.md`. These measure the *whole* distributed
//! round-trip (ship IR → parse+optimize+run on the worker → credit-streamed
//! result back), not just raw socket throughput — that is the figure that
//! matters for "execute a flow on a remote worker".
#![cfg(feature = "net")]

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use rivus_runtime::distributed::{self, Handler, LinkConfig};
use rivus_runtime::{run, RunOptions};

fn handler() -> Handler {
    Arc::new(|src: &str| {
        let g = rivus_parser::parse(src).map_err(|e| format!("{e:?}"))?;
        let (g, _) = rivus_optimizer::optimize(g);
        let res = run(&g, RunOptions::default()).map_err(|e| format!("{e:?}"))?;
        // Render the first scope's rows as CSV bytes (the streamed result).
        let mut out = String::new();
        for o in &res.outputs {
            for c in &o.chunks {
                for r in 0..c.len {
                    let row: Vec<String> = (0..c.schema.fields.len())
                        .map(|i| c.value(r, i).to_string())
                        .collect();
                    out.push_str(&row.join(","));
                    out.push('\n');
                }
            }
        }
        Ok(out.into_bytes())
    })
}

/// Write a temp CSV with `rows` rows and return its path.
fn big_csv(rows: usize, tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("rivus_bench_{tag}_{}.csv", std::process::id()));
    let mut s = String::from("name,age,country\n");
    for i in 0..rows {
        s.push_str(&format!(
            "user{i},{},{}\n",
            18 + (i % 60),
            if i % 2 == 0 { "JP" } else { "US" }
        ));
    }
    std::fs::write(&p, s).unwrap();
    p
}

/// Spawn a one-shot std worker on an ephemeral loopback port; return its addr.
fn spawn_std_worker() -> (String, thread::JoinHandle<()>) {
    let (addr, listener) = distributed::bind_ephemeral().unwrap();
    let h = handler();
    let cfg = LinkConfig::default();
    let jh = thread::spawn(move || {
        // Serve a fixed number of jobs sequentially.
        for _ in 0..10_000 {
            let cfg = cfg.clone();
            let h = h.clone();
            if distributed::serve_on(&listener, &cfg, h).is_err() {
                break;
            }
        }
    });
    (addr, jh)
}

#[test]
#[ignore = "benchmark — run with --ignored --nocapture"]
fn bench_std_distributed_latency() {
    // Small-result round-trip latency (ship a tiny flow, get a few rows back).
    let path = big_csv(100, "lat");
    let src = format!(
        "R:\n open {}\n |? age >= 40\n |> name age\n;",
        path.display()
    );
    let (addr, _jh) = spawn_std_worker();
    let cfg = LinkConfig::default();

    // Warm up.
    for _ in 0..5 {
        let _ = distributed::run_remote(&addr, &cfg, &src).unwrap();
    }
    let iters = 200usize;
    let t0 = Instant::now();
    let mut bytes = 0usize;
    for _ in 0..iters {
        let r = distributed::run_remote(&addr, &cfg, &src).unwrap();
        bytes += r.len();
    }
    let el = t0.elapsed();
    println!(
        "[bench] std distributed round-trip latency: {iters} iters, {:.3} ms/iter, \
         {} result bytes/iter",
        el.as_secs_f64() * 1e3 / iters as f64,
        bytes / iters
    );
    std::fs::remove_file(&path).ok();
}

#[test]
#[ignore = "benchmark — run with --ignored --nocapture"]
fn bench_std_transport_throughput() {
    // PURE transport throughput: the worker returns a fixed pre-built buffer (no
    // flow execution), so this isolates the credit-streamed channel's MB/s from
    // the cost of running a flow. (The end-to-end distributed cost is dominated
    // by the worker's flow execution + render, measured separately.)
    let payload = 64 * 1024 * 1024usize; // 64 MiB
    let buf = Arc::new(vec![0u8; payload]);
    let (addr, listener) = distributed::bind_ephemeral().unwrap();
    let buf2 = buf.clone();
    let h: Handler = Arc::new(move |_src: &str| Ok((*buf2).clone()));
    let cfg = LinkConfig::default();
    thread::spawn(move || {
        for _ in 0..20 {
            if distributed::serve_on(&listener, &cfg, h.clone()).is_err() {
                break;
            }
        }
    });

    let cfg = LinkConfig::default();
    let _ = distributed::run_remote(&addr, &cfg, "x").unwrap(); // warm
    let iters = 8usize;
    let t0 = Instant::now();
    let mut bytes = 0usize;
    for _ in 0..iters {
        bytes += distributed::run_remote(&addr, &cfg, "x").unwrap().len();
    }
    let el = t0.elapsed();
    let mbps = (bytes as f64 / 1e6) / el.as_secs_f64();
    println!(
        "[bench] std transport throughput: {} MiB/iter × {iters}, {:.1} ms/iter, {:.0} MB/s",
        payload / (1024 * 1024),
        el.as_secs_f64() * 1e3 / iters as f64,
        mbps
    );
}

#[test]
#[ignore = "benchmark — run with --ignored --nocapture"]
fn bench_std_distributed_endtoend() {
    // End-to-end distributed cost: ship a passthrough over a big CSV; the worker
    // parses + runs the flow and streams the rendered result (this is dominated
    // by flow execution + render, not transport — contrast the transport bench).
    let rows = 200_000;
    let path = big_csv(rows, "e2e");
    let src = format!("R:\n open {}\n |> name age country\n;", path.display());
    let (addr, _jh) = spawn_std_worker();
    let cfg = LinkConfig::default();

    let _ = distributed::run_remote(&addr, &cfg, &src).unwrap(); // warm
    let iters = 10usize;
    let t0 = Instant::now();
    let mut bytes = 0usize;
    for _ in 0..iters {
        bytes += distributed::run_remote(&addr, &cfg, &src).unwrap().len();
    }
    let el = t0.elapsed();
    println!(
        "[bench] std distributed end-to-end (flow+transfer): {rows} rows → {} MB result, \
         {:.1} ms/iter",
        bytes / iters / 1_000_000,
        el.as_secs_f64() * 1e3 / iters as f64,
    );
    std::fs::remove_file(&path).ok();
}

#[cfg(feature = "quic")]
#[test]
#[ignore = "benchmark — run with --features quic --ignored --nocapture"]
fn bench_quic_distributed_latency() {
    use rivus_runtime::distributed_quic::{quic_run_remote, quic_worker, QuicConfig};
    let path = big_csv(100, "qlat");
    let src = format!(
        "R:\n open {}\n |? age >= 40\n |> name age\n;",
        path.display()
    );
    let worker = quic_worker("127.0.0.1:0", QuicConfig::default()).unwrap();
    let addr = worker.addr().to_string();
    let h = handler();
    thread::spawn(move || {
        let _ = worker.serve(h, |_| {});
    });
    thread::sleep(std::time::Duration::from_millis(1000)); // endpoint readiness
    let cfg = QuicConfig::default();
    for _ in 0..3 {
        let _ = quic_run_remote(&addr, &cfg, &src).unwrap();
    }
    let iters = 20usize;
    let t0 = Instant::now();
    let mut bytes = 0usize;
    for _ in 0..iters {
        bytes += quic_run_remote(&addr, &cfg, &src).unwrap().len();
    }
    let el = t0.elapsed();
    println!(
        "[bench] QUIC distributed per-call (new conn+handshake+cert each): {iters} iters, {:.3} ms/iter, \
         {} result bytes/iter",
        el.as_secs_f64() * 1e3 / iters as f64,
        bytes / iters
    );
    std::fs::remove_file(&path).ok();
}
