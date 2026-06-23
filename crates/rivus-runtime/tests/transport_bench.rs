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
fn bench_std_session_reuse_vs_per_call() {
    // §34.4 s2': quantify the connection-reuse win — a Session running N jobs over
    // ONE connection vs N fresh per-call connections (each re-connects + re-HELLOs).
    let path = big_csv(100, "sess");
    let src = format!(
        "R:\n open {}\n |? age >= 40\n |> name age\n;",
        path.display()
    );
    let iters = 300usize;

    // (a) per-call: a worker that serves one job per connection.
    let (addr, _jh) = spawn_std_worker();
    let cfg = LinkConfig::default();
    for _ in 0..5 {
        let _ = distributed::run_remote(&addr, &cfg, &src).unwrap();
    }
    let t0 = Instant::now();
    for _ in 0..iters {
        let _ = distributed::run_remote(&addr, &cfg, &src).unwrap();
    }
    let per_call = t0.elapsed().as_secs_f64() * 1e3 / iters as f64;

    // (b) session: one worker connection, N jobs reused over it.
    let (addr2, listener2) = distributed::bind_ephemeral().unwrap();
    let h = handler();
    let cfg_w = LinkConfig::default();
    let worker = thread::spawn(move || distributed::serve_on(&listener2, &cfg_w, h));
    let mut session = distributed::Session::connect(&addr2, &LinkConfig::default()).unwrap();
    for _ in 0..5 {
        let _ = session.run(&src).unwrap();
    }
    let t1 = Instant::now();
    for _ in 0..iters {
        let _ = session.run(&src).unwrap();
    }
    let sess = t1.elapsed().as_secs_f64() * 1e3 / iters as f64;
    drop(session);
    let _ = worker.join();

    println!(
        "[bench] std reuse: per-call {per_call:.3} ms/job vs session {sess:.3} ms/job \
         ({:.1}× faster reused, {iters} jobs)",
        per_call / sess
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
    use rivus_runtime::distributed_quic::{quic_run_remote, quic_worker, QuicConfig, QuicSession};
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
    let iters = 20usize;

    // (a) per-call: a fresh connection (TLS handshake + cert mint) every job.
    for _ in 0..3 {
        let _ = quic_run_remote(&addr, &cfg, &src).unwrap();
    }
    let t0 = Instant::now();
    for _ in 0..iters {
        let _ = quic_run_remote(&addr, &cfg, &src).unwrap();
    }
    let per_call = t0.elapsed().as_secs_f64() * 1e3 / iters as f64;

    // (b) session: one connection (one handshake), N jobs each a new bidi stream.
    let session = QuicSession::connect(&addr, &cfg).unwrap();
    for _ in 0..3 {
        let _ = session.run(&src).unwrap();
    }
    let t1 = Instant::now();
    for _ in 0..iters {
        let _ = session.run(&src).unwrap();
    }
    let sess = t1.elapsed().as_secs_f64() * 1e3 / iters as f64;

    println!(
        "[bench] QUIC reuse: per-call {per_call:.3} ms/job (new conn+TLS+cert) vs \
         session {sess:.3} ms/job (reused conn, new stream) ({:.1}× faster reused, {iters} jobs)",
        per_call / sess
    );
    std::fs::remove_file(&path).ok();
}

#[cfg(feature = "cpubudget")]
#[test]
#[ignore = "benchmark — run with --features cpubudget --ignored --nocapture"]
fn bench_cpubudget_affinity_protects_data_plane() {
    // §34.3 (#174) acceptance: SIMD data-plane throughput **while a transfer runs**,
    // on a shared core vs a pinned core. The wire is < 1 % of a distributed job
    // (#173); the contention is *crypto CPU*. So we model the transport as a busy
    // CPU loop (its crypto cost) and measure how many data-plane work units the
    // SIMD lane completes in a fixed wall — once with the OS free to co-schedule
    // transport onto the data cores (unpinned), once with transport confined to
    // core 0 and the data lane pinned to the rest (the §34.3 budget).
    use rivus_runtime::cpu_budget::pin_current_thread_to;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Duration;

    let ncpu = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2);
    if ncpu < 2 {
        eprintln!("[bench] cpubudget: needs >= 2 cores (have {ncpu}); skipping");
        return;
    }
    let data_cores: Vec<usize> = (1..ncpu).collect(); // core 0 reserved for transport

    // A CPU-bound integer reduction — a stand-in for the SIMD data plane. Integer
    // (associative → byte-identical), and the xor-feedback defeats const-folding.
    fn data_work() -> u64 {
        let mut acc = 0u64;
        for i in 0..1_500_000u64 {
            acc = acc.wrapping_add(i ^ (acc >> 1));
        }
        acc
    }
    // Busy CPU loop modelling transport crypto contending for cores.
    fn crypto_noise(stop: &AtomicBool) {
        let mut x = 1u64;
        while !stop.load(Ordering::Relaxed) {
            for i in 0..1_000_000u64 {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(i);
            }
            std::hint::black_box(x);
        }
    }

    let run_phase = |pinned: bool| -> u64 {
        let stop = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicU64::new(0));
        // One transport-noise thread per data core (enough to oversubscribe).
        let mut noise = Vec::new();
        for _ in &data_cores {
            let stop2 = stop.clone();
            noise.push(thread::spawn(move || {
                if pinned {
                    let _ = pin_current_thread_to(&[0]);
                }
                crypto_noise(&stop2);
            }));
        }
        // The data lane: one worker per data core, pinned there in the budget case.
        let deadline = Instant::now() + Duration::from_millis(1500);
        let mut workers = Vec::new();
        for &core in &data_cores {
            let done2 = done.clone();
            workers.push(thread::spawn(move || {
                if pinned {
                    let _ = pin_current_thread_to(&[core]);
                }
                let mut n = 0u64;
                while Instant::now() < deadline {
                    std::hint::black_box(data_work());
                    n += 1;
                }
                done2.fetch_add(n, Ordering::Relaxed);
            }));
        }
        for w in workers {
            w.join().unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        for nth in noise {
            nth.join().unwrap();
        }
        done.load(Ordering::Relaxed)
    };

    let unpinned = run_phase(false) as f64;
    let pinned = run_phase(true) as f64;
    println!(
        "[bench] cpubudget affinity ({ncpu} cores, transport→core0, data→cores {data_cores:?}): \
         data-plane work units in 1.5 s under transport-crypto contention — \
         unpinned {unpinned:.0} vs pinned {pinned:.0} ({:.2}× data throughput when \
         transport is confined off the data cores)",
        pinned / unpinned
    );
}
