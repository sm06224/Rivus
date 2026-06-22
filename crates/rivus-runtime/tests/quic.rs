//! QUIC distributed-execution transport (design §28.12.5-3, feature `quic`) —
//! end-to-end over loopback. Mints ephemeral self-signed identities, ships the
//! IR over a QUIC stream, and asserts the streamed result is byte-identical to a
//! local run (interpret==distribute, §0.5) and that the static-key pin is
//! enforced.
#![cfg(feature = "quic")]

use rivus_runtime::distributed::Handler;
use rivus_runtime::distributed_quic::{quic_run_remote, quic_worker, QuicConfig};
use rivus_runtime::{run, RunOptions};
use std::sync::Arc;
use std::thread;

fn render_flow(src: &str) -> Result<Vec<u8>, String> {
    let graph = rivus_parser::parse(src).map_err(|e| format!("{e:?}"))?;
    let (graph, _r) = rivus_optimizer::optimize(graph);
    let res = run(&graph, RunOptions::default()).map_err(|e| format!("{e:?}"))?;
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
}

fn handler() -> Handler {
    Arc::new(render_flow)
}

fn temp_csv() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("rivus_quic_{}.csv", std::process::id()));
    std::fs::write(&p, "name,age\nalice,30\nbob,17\ncarol,42\n").unwrap();
    p
}

/// One test covering both QUIC scenarios **sequentially** — they are deliberately
/// not two `#[test]`s: each spins up two small multi-threaded runtimes (worker +
/// client), and running both in parallel on a 4-vCPU box oversubscribes the
/// scheduler and can stall a loopback connection past the idle timeout. Run
/// sequentially they are fast and deterministic.
///
/// (a) **Round-trip**: mutual static-key auth, ship the IR over a QUIC bidi
/// stream, stream the rendered result back, assert it is **byte-identical** to a
/// local run (interpret==distribute, §0.5).
/// (b) **Pin rejection**: a client that pins the wrong worker key refuses the
/// connection at the application layer (the static-key boundary, §28.12.4/5).
#[test]
fn quic_protected_channel_round_trip_and_pinning() {
    let path = temp_csv();

    // (a) byte-identical round-trip.
    let src = format!(
        "Adults:\n open {}\n |? age >= 18\n |> name age\n;",
        path.display()
    );
    let worker = quic_worker("127.0.0.1:0", QuicConfig::default()).expect("bind quic worker");
    let addr = worker.addr().to_string();
    let h = handler();
    // Detached: the client-side `got == expected` is the verification; we don't
    // join the worker (its `conn.closed()` may wait out the idle timeout for the
    // graceful close, which is cosmetic once the client has the result).
    thread::spawn(move || {
        let _ = worker.serve_once(h);
    });
    let got = quic_run_remote(&addr, &QuicConfig::default(), &src).expect("quic remote run");
    let expected = render_flow(&src).unwrap();
    assert_eq!(
        got, expected,
        "QUIC distribute == interpret (byte-identical)"
    );
    assert!(String::from_utf8_lossy(&got).contains("alice,30"));
    assert!(!String::from_utf8_lossy(&got).contains("bob"));

    // (b) wrong static-key pin is rejected (worker detached — the assertion is
    // client-side, right after the handshake).
    let worker2 = quic_worker("127.0.0.1:0", QuicConfig::default()).expect("bind");
    let addr2 = worker2.addr().to_string();
    let h2 = handler();
    thread::spawn(move || {
        let _ = worker2.serve_once(h2);
    });
    let bad = QuicConfig {
        allow_peer_keys: Some(vec!["00".repeat(32)]),
        ..QuicConfig::default()
    };
    let err = quic_run_remote(&addr2, &bad, &src).expect_err("wrong pin rejected");
    assert!(err.contains("not in the pinned allowlist"), "{err}");

    std::fs::remove_file(&path).ok();
}
