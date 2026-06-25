//! Networking execution (design §33, feature `net`) — end-to-end over loopback,
//! zero external network. A std-only HTTP server and TCP feed are spun up on an
//! ephemeral `127.0.0.1` port; the flow fetches / subscribes through Rivus's own
//! `net` transport and the result is asserted exactly.
#![cfg(feature = "net")]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

use rivus_runtime::{run, RunOptions, RunResult};

fn run_src(src: &str, chunk_size: usize) -> RunResult {
    let graph = rivus_parser::parse(src).expect("parse");
    let (graph, _report) = rivus_optimizer::optimize(graph);
    run(
        &graph,
        RunOptions {
            chunk_size,
            ..Default::default()
        },
    )
    .expect("run")
}

/// Collect the `(value-per-column)` rows of the labelled scope's output as
/// strings, in order.
fn rows(res: &RunResult, label: &str, cols: &[&str]) -> Vec<Vec<String>> {
    let o = res
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some(label))
        .expect("scope output");
    let mut out = Vec::new();
    for c in &o.chunks {
        let idx: Vec<usize> = cols.iter().map(|n| c.schema.index_of(n).unwrap()).collect();
        for r in 0..c.len {
            out.push(idx.iter().map(|&i| c.value(r, i).to_string()).collect());
        }
    }
    out
}

/// Serve `body` once on an ephemeral loopback port, framed either with
/// `Content-Length` or `Transfer-Encoding: chunked`. Returns the port.
fn serve_once(body: &'static str, chunked: bool) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        if let Some(Ok(mut s)) = listener.incoming().next() {
            // Drain the request line + headers so the client isn't RST.
            let mut buf = [0u8; 2048];
            let _ = s.read(&mut buf);
            let resp = if chunked {
                // Two chunks to exercise the de-chunking across reads.
                let (a, b) = body.split_at(body.len() / 2);
                format!(
                    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n\
                     {:x}\r\n{}\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                    a.len(),
                    a,
                    b.len(),
                    b
                )
            } else {
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
            };
            let _ = s.write_all(resp.as_bytes());
            let _ = s.flush();
        }
    });
    port
}

const CSV: &str = "name,age\nalice,30\nbob,17\ncarol,42\n";

#[test]
fn http_get_content_length() {
    let port = serve_once(CSV, false);
    let src = format!(
        "Adults:\n open \"http://127.0.0.1:{port}/data.csv\"\n |? age >= 18\n |> name age\n;"
    );
    let res = run_src(&src, 4096);
    assert!(
        res.errors.iter().all(|e| !e.is_fatal()),
        "no fatal errors: {:?}",
        res.errors
    );
    let got = rows(&res, "Adults", &["name", "age"]);
    assert_eq!(
        got,
        vec![
            vec!["alice".to_string(), "30".to_string()],
            vec!["carol".to_string(), "42".to_string()],
        ]
    );
}

#[test]
fn http_get_chunked() {
    let port = serve_once(CSV, true);
    let src = format!("All:\n open \"http://127.0.0.1:{port}/data.csv\"\n |> name age\n;");
    let res = run_src(&src, 4096);
    assert!(
        res.errors.iter().all(|e| !e.is_fatal()),
        "no fatal errors: {:?}",
        res.errors
    );
    let got = rows(&res, "All", &["name", "age"]);
    assert_eq!(
        got.len(),
        3,
        "all three rows decode across chunk boundaries"
    );
    assert_eq!(got[2], vec!["carol".to_string(), "42".to_string()]);
}

#[test]
fn http_get_chunk_size_independent() {
    // The decoded result must not depend on the engine chunk size (§0.5).
    let mut last: Option<Vec<Vec<String>>> = None;
    for cs in [1usize, 2, 4096] {
        let port = serve_once(CSV, false);
        let src = format!("R:\n open \"http://127.0.0.1:{port}/data.csv\"\n |> name age\n;");
        let res = run_src(&src, cs);
        let got = rows(&res, "R", &["name", "age"]);
        if let Some(prev) = &last {
            assert_eq!(&got, prev, "chunk-size independence @cs={cs}");
        }
        last = Some(got);
    }
}

#[test]
fn http_remote_host_denied_without_capability() {
    // A non-loopback host with no RIVUS_CAP_NET_HOSTS allowlist is rejected
    // before any connection — the source has no data (fatal), and the message
    // names the target but never an allowlist.
    std::env::remove_var("RIVUS_CAP_NET_HOSTS");
    let graph = rivus_parser::parse("X:\n open \"http://example.com/data.csv\"\n |> name\n;")
        .expect("parse");
    let (graph, _r) = rivus_optimizer::optimize(graph);
    let res = run(&graph, RunOptions::default()).expect("run returns (continue-first)");
    assert!(
        res.errors
            .iter()
            .any(|e| e.message.contains("example.com") && e.message.contains("capability")),
        "capability denial surfaced: {:?}",
        res.errors
    );
}

const JSONL: &str = "{\"name\":\"alice\",\"age\":30}\n{\"name\":\"bob\",\"age\":17}\n{\"name\":\"carol\",\"age\":42}\n";

#[test]
fn http_get_jsonl() {
    let port = serve_once(JSONL, false);
    let src =
        format!("J:\n open \"http://127.0.0.1:{port}/data.jsonl\"\n |? age >= 18\n |> name age\n;");
    let res = run_src(&src, 4096);
    assert!(
        res.errors.iter().all(|e| !e.is_fatal()),
        "no fatal errors: {:?}",
        res.errors
    );
    let got = rows(&res, "J", &["name", "age"]);
    assert_eq!(
        got,
        vec![
            vec!["alice".to_string(), "30".to_string()],
            vec!["carol".to_string(), "42".to_string()],
        ]
    );
}
/// Feed `lines` over a single accepted loopback TCP connection, then close it —
/// `subscribe` ends on peer close (the unbounded source's natural EOF).
fn feed_once(lines: &'static str) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        if let Some(Ok(mut s)) = listener.incoming().next() {
            let _ = s.write_all(lines.as_bytes());
            let _ = s.flush();
            // Dropping `s` closes the connection → EOF on the reader.
        }
    });
    port
}

#[test]
fn subscribe_tcp_stream_until_peer_close() {
    let port = feed_once("name,age\nalice,30\nbob,17\ncarol,42\ndave,55\n");
    // Give the listener a moment to be ready (bind is sync, but the accept loop
    // races the client connect on a cold thread).
    thread::sleep(std::time::Duration::from_millis(50));
    let src = format!("Feed:\n subscribe \"tcp://127.0.0.1:{port}\"\n |? age >= 18\n |> name\n;");
    let res = run_src(&src, 2);
    assert!(
        res.errors.iter().all(|e| !e.is_fatal()),
        "no fatal errors: {:?}",
        res.errors
    );
    let got = rows(&res, "Feed", &["name"]);
    assert_eq!(
        got,
        vec![
            vec!["alice".to_string()],
            vec!["carol".to_string()],
            vec!["dave".to_string()],
        ]
    );
}

#[test]
fn subscribe_jsonl_stream() {
    let port = feed_once(JSONL);
    thread::sleep(std::time::Duration::from_millis(50));
    let src =
        format!("Feed:\n subscribe \"tcp://127.0.0.1:{port}\" as json\n |? age >= 18\n |> name\n;");
    let res = run_src(&src, 2);
    assert!(
        res.errors.iter().all(|e| !e.is_fatal()),
        "no fatal errors: {:?}",
        res.errors
    );
    let got = rows(&res, "Feed", &["name"]);
    assert_eq!(
        got,
        vec![vec!["alice".to_string()], vec!["carol".to_string()]]
    );
}

#[test]
fn subscribe_blocking_op_refused() {
    // An aggregate downstream of the unbounded subscribe source needs the whole
    // stream → refused pre-run (never-silent, §28.12.0). Parse/build is std.
    let graph = rivus_parser::parse("G:\n subscribe \"tcp://127.0.0.1:9\"\n |# name count:name\n;")
        .expect("parse");
    let (graph, _r) = rivus_optimizer::optimize(graph);
    let err = run(&graph, RunOptions::default()).expect_err("blocking-op refusal");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("unbounded"),
        "refusal mentions unbounded: {msg}"
    );
}

#[test]
fn subscribe_connect_failure_is_fatal_not_panic() {
    // Dialing a closed port surfaces a fatal (continue-first), never a panic.
    let src = "D:\n subscribe \"tcp://127.0.0.1:1\"\n |> name\n;";
    let graph = rivus_parser::parse(src).expect("parse");
    let (graph, _r) = rivus_optimizer::optimize(graph);
    let res = run(&graph, RunOptions::default()).expect("run returns");
    assert!(
        res.errors.iter().any(|e| e.is_fatal()),
        "connect failure is fatal: {:?}",
        res.errors
    );
}

// ─── Protected-channel distributed execution (§33 / §17) ──────────────────────
//
// Ship the IR (the deployment artifact) to a worker over the capability-gated
// channel; the worker runs it and streams the rendered result back. We assert
// the bytes equal a *local* run's render — interpret==distribute byte-identity
// (§0.5). Everything is loopback (the §28.12.5-1 exception).
use rivus_runtime::distributed::{self, LinkConfig};
use std::sync::Arc;

/// Render a flow's outputs to deterministic bytes — used both as the worker's
/// handler and as the local expected value, so equality is real byte-identity.
fn render_flow(src: &str) -> Result<Vec<u8>, String> {
    let graph = rivus_parser::parse(src).map_err(|e| format!("{e:?}"))?;
    let (graph, _r) = rivus_optimizer::optimize(graph);
    let res = run(&graph, RunOptions::default()).map_err(|e| format!("{e:?}"))?;
    let mut out = String::new();
    for o in &res.outputs {
        out.push_str(o.label.as_deref().unwrap_or("-"));
        out.push('\n');
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

fn handler() -> distributed::Handler {
    Arc::new(render_flow)
}

#[test]
fn distributed_exec_round_trips_byte_identical() {
    // A self-contained flow over a small CSV file (worker and client share the
    // same filesystem here — they're both loopback for the test).
    let dir = std::env::temp_dir();
    let path = dir.join(format!("rivus_dist_{}.csv", std::process::id()));
    std::fs::write(&path, "name,age\nalice,30\nbob,17\ncarol,42\n").unwrap();
    let src = format!(
        "Adults:\n open {}\n |? age >= 18\n |> name age\n;",
        path.display()
    );

    let (addr, listener) = distributed::bind_ephemeral().unwrap();
    let cfg = LinkConfig::default();
    let h = handler();
    let worker = thread::spawn(move || distributed::serve_on(&listener, &cfg, h));

    let client_cfg = LinkConfig::default();
    let got = distributed::run_remote(&addr, &client_cfg, &src).expect("remote run");
    let _ = worker.join().unwrap();

    let expected = render_flow(&src).unwrap();
    std::fs::remove_file(&path).ok();
    assert_eq!(
        got, expected,
        "distribute == interpret (byte-identical render)"
    );
    assert!(String::from_utf8_lossy(&got).contains("alice,30"));
    assert!(!String::from_utf8_lossy(&got).contains("bob"));
}

#[test]
fn distributed_emits_telemetry_events() {
    // §34 channel separation + event-centric observability: the worker narrates
    // structured events on the telemetry channel while the data channel carries
    // the result. The client demuxes both.
    let dir = std::env::temp_dir();
    let path = dir.join(format!("rivus_evt_{}.csv", std::process::id()));
    std::fs::write(&path, "name,age\nalice,30\nbob,17\n").unwrap();
    let src = format!("R:\n open {}\n |> name\n;", path.display());

    let (addr, listener) = distributed::bind_ephemeral().unwrap();
    let cfg = LinkConfig::default();
    let h = handler();
    let worker = thread::spawn(move || distributed::serve_on(&listener, &cfg, h));

    let mut events = Vec::new();
    let got =
        distributed::run_remote_observed(&addr, &LinkConfig::default(), &src, |e| events.push(e))
            .expect("remote run");
    let _ = worker.join();
    std::fs::remove_file(&path).ok();

    assert!(!got.is_empty(), "result still flows on the data channel");
    let joined = events.join(" | ");
    assert!(joined.contains("flow.started"), "events: {joined}");
    assert!(joined.contains("flow.completed"), "events: {joined}");
    assert!(joined.contains("transfer.done"), "events: {joined}");
}

#[test]
fn distributed_session_reuses_one_connection_for_many_jobs() {
    // §34.4 s2': a Session runs many jobs over ONE connection (amortizing
    // connect/handshake). Each job is byte-identical to a local run; the worker's
    // job loop handles them in sequence on the same socket.
    let dir = std::env::temp_dir();
    let csv = dir.join(format!("rivus_sess_{}.csv", std::process::id()));
    std::fs::write(&csv, "name,age\nalice,30\nbob,17\ncarol,42\ndave,55\n").unwrap();

    let (addr, listener) = distributed::bind_ephemeral().unwrap();
    let h = handler();
    let cfg_w = LinkConfig::default();
    // The worker serves a whole session (many jobs) on one accepted connection.
    let worker = thread::spawn(move || distributed::serve_on(&listener, &cfg_w, h));

    let mut session =
        distributed::Session::connect(&addr, &LinkConfig::default()).expect("open session");
    let queries = [
        format!("A:\n open {}\n |? age >= 18\n |> name\n;", csv.display()),
        format!("A:\n open {}\n |? age >= 50\n |> name\n;", csv.display()),
        format!("A:\n open {}\n |> name age\n;", csv.display()),
    ];
    let mut got = Vec::new();
    for q in &queries {
        got.push(session.run(q).expect("session job"));
    }
    drop(session); // closes the session → the worker's job loop ends
    let _ = worker.join();

    for (q, g) in queries.iter().zip(&got) {
        assert_eq!(
            *g,
            render_flow(q).unwrap(),
            "each session job is byte-identical"
        );
    }
    std::fs::remove_file(&csv).ok();
    assert!(String::from_utf8_lossy(&got[1]).contains("dave")); // age >= 50
    assert!(!String::from_utf8_lossy(&got[1]).contains("alice"));
}

#[test]
fn distributed_worker_error_propagates() {
    let (addr, listener) = distributed::bind_ephemeral().unwrap();
    let cfg = LinkConfig::default();
    let h = handler();
    let worker = thread::spawn(move || distributed::serve_on(&listener, &cfg, h));
    // A flow that fails to parse → the worker returns ERR, surfaced as Err here.
    let err = distributed::run_remote(&addr, &LinkConfig::default(), "this is not rivus !!!")
        .expect_err("worker error propagates");
    let _ = worker.join();
    assert!(!err.is_empty());
}

#[test]
fn distributed_peer_allowlist_denies_remote() {
    // A non-loopback peer with no allowlist is refused before dialing.
    let cfg = LinkConfig::default();
    let err = distributed::run_remote("10.0.0.7:9000", &cfg, "X:\n open x.csv\n;")
        .expect_err("peer not allowlisted");
    assert!(err.contains("10.0.0.7"), "names the peer: {err}");
}

#[test]
fn distributed_no_raw_public_listener() {
    // Binding a non-loopback address without the trusted-interface capability is
    // refused (§28.12.5-1: no raw listeners).
    let cfg = LinkConfig::default();
    let err = distributed::serve_once("0.0.0.0:0", &cfg, handler()).expect_err("no raw listener");
    assert!(
        err.contains("trusted interface") || err.contains("§28.12.5"),
        "{err}"
    );
}
