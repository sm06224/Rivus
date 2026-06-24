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
