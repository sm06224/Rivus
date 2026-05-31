//! Live observability HTTP server (Epic #30, Pillar B — issue #32).
//!
//! `rivus run … --serve [ADDR]` runs the flow on a worker thread and exposes a
//! **std-only** HTTP/1.1 server (no third-party deps): a `TcpListener`, a
//! hand-written request line parser, and Server-Sent Events. Heavy rendering is
//! pushed to the browser (an embedded HTML/JS/SVG dashboard served as a static
//! string); the Rust side only ships JSON snapshots from Pillar A's
//! [`RuntimeSnapshot`]. `cargo deny` stays green — zero new crates.
//!
//! Routes:
//!   `GET /`         → the embedded dashboard (auto-configures from the snapshot)
//!   `GET /snapshot` → the latest snapshot as one JSON object (polling)
//!   `GET /events`   → `text/event-stream` live feed of snapshots (SSE)

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Shared observability state the worker publishes into and the HTTP handlers
/// read from. `seq` increments on every publish so SSE handlers can detect new
/// snapshots by polling cheaply; `done` flips when the run finishes.
#[derive(Default)]
pub struct Hub {
    inner: Mutex<HubState>,
}

#[derive(Default)]
struct HubState {
    /// Latest snapshot rendered as a JSON object (viz::render_snapshot_json).
    latest: Option<String>,
    /// Monotonic publish counter (SSE handlers send only when this advances).
    seq: u64,
    /// True once the run has finished (lets SSE handlers close gracefully).
    done: bool,
}

impl Hub {
    pub fn new() -> Arc<Hub> {
        Arc::new(Hub::default())
    }

    /// Publish a new snapshot JSON (called from the run's progress hook).
    pub fn publish(&self, json: String) {
        let mut s = self.inner.lock().unwrap();
        s.latest = Some(json);
        s.seq += 1;
    }

    /// Mark the run finished (SSE streams then send a final event and close).
    pub fn finish(&self) {
        let mut s = self.inner.lock().unwrap();
        s.done = true;
        s.seq += 1;
    }

    fn read(&self) -> (Option<String>, u64, bool) {
        let s = self.inner.lock().unwrap();
        (s.latest.clone(), s.seq, s.done)
    }
}

/// The embedded dashboard. Pure HTML/JS/SVG (no CDN, no framework): it opens an
/// SSE connection to `/events`, parses each snapshot, and redraws a simple bar
/// chart + table that auto-configures from the node list. Kept deliberately
/// small and dependency-free.
pub const DASHBOARD_HTML: &str = r##"<!doctype html>
<html><head><meta charset="utf-8"><title>Rivus live</title>
<style>
 body{font:14px/1.4 system-ui,monospace;margin:1.5rem;background:#0b0e14;color:#cdd6f4}
 h1{font-size:1.1rem;color:#89b4fa} .meta{color:#a6adc8;margin-bottom:1rem}
 .bar{height:14px;background:#89b4fa;border-radius:2px}
 table{border-collapse:collapse;width:100%} td,th{text-align:left;padding:3px 8px}
 th{color:#a6adc8;border-bottom:1px solid #313244} tr.done td{color:#a6e3a1}
 .track{background:#313244;border-radius:2px;width:200px}
</style></head><body>
<h1>Rivus — live execution</h1>
<div class="meta" id="meta">connecting…</div>
<table><thead><tr><th>node</th><th>kind</th><th>rows</th><th></th><th>state</th></tr></thead>
<tbody id="rows"></tbody></table>
<script>
const fmt=n=>n.toLocaleString();
function draw(s){
 const max=Math.max(1,...s.nodes.map(n=>n.rows_out));
 document.getElementById('meta').textContent=
   `${fmt(s.rows_seen)} rows · ${(s.elapsed_ms/1000).toFixed(1)}s · mode ${s.mode}`;
 const tb=document.getElementById('rows'); tb.innerHTML='';
 for(const n of s.nodes){
   const tr=document.createElement('tr'); if(n.finished)tr.className='done';
   const w=Math.round(n.rows_out/max*200);
   tr.innerHTML=`<td>${n.label}</td><td>${n.kind}</td><td>${fmt(n.rows_out)}</td>`+
     `<td><div class="track"><div class="bar" style="width:${w}px"></div></div></td>`+
     `<td>${n.finished?'done':'live'}${n.errors?' !'+n.errors:''}</td>`;
   tb.appendChild(tr);
 }
}
const es=new EventSource('/events');
es.onmessage=e=>{try{draw(JSON.parse(e.data))}catch(_){}};
es.onerror=()=>{document.getElementById('meta').textContent+=' (stream closed)';es.close()};
</script></body></html>"##;

/// Bind a server on `addr` (e.g. `127.0.0.1:0` for an ephemeral port). Returns
/// the listener and its resolved address. A bind failure is returned to the
/// caller for a graceful fallback (run without `--serve`).
pub fn bind(addr: &str) -> std::io::Result<(TcpListener, String)> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?.to_string();
    Ok((listener, local))
}

/// Serve until the run is `done` and the listener stops. Each connection is
/// handled inline (one short-lived request, or a long-lived SSE stream on its
/// own thread). Runs on the calling thread; `hub` is shared with the worker.
pub fn serve(listener: TcpListener, hub: Arc<Hub>) {
    // Poll accept() so we notice `done` and exit even with no further requests.
    listener.set_nonblocking(true).ok();
    // Keep serving briefly after `done` so the browser receives the terminal
    // snapshot and a viewer can still poll `/snapshot` (a short grace window).
    let mut grace: u32 = 0;
    loop {
        match listener.accept() {
            Ok((s, _)) => {
                let hub = Arc::clone(&hub);
                std::thread::spawn(move || handle(s, hub));
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if hub.read().2 {
                    grace += 1;
                    if grace > 40 {
                        // ~2 s after finish with no traffic → stop.
                        break;
                    }
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => break,
        }
    }
}

fn handle(mut stream: TcpStream, hub: Arc<Hub>) {
    let path = match read_request_target(&mut stream) {
        Some(p) => p,
        None => return,
    };
    match path.as_str() {
        "/" => respond(
            &mut stream,
            "200 OK",
            "text/html; charset=utf-8",
            DASHBOARD_HTML,
        ),
        "/snapshot" => {
            let body = hub.read().0.unwrap_or_else(|| "{}".to_string());
            respond(&mut stream, "200 OK", "application/json", &body);
        }
        "/events" => stream_events(stream, hub),
        _ => respond(&mut stream, "404 Not Found", "text/plain", "not found"),
    }
}

/// Parse just the request target from the first request line: `GET /path HTTP/1.1`.
fn read_request_target(stream: &mut TcpStream) -> Option<String> {
    let mut r = BufReader::new(stream);
    let mut line = String::new();
    r.read_line(&mut line).ok()?;
    let mut parts = line.split_whitespace();
    let _method = parts.next()?;
    let target = parts.next()?.to_string();
    // Drain the rest of the headers (we don't need them).
    let mut hdr = String::new();
    while r.read_line(&mut hdr).ok()? > 0 {
        if hdr == "\r\n" || hdr == "\n" {
            break;
        }
        hdr.clear();
    }
    Some(target)
}

/// Write a complete, closed HTTP/1.1 response.
fn respond(stream: &mut TcpStream, status: &str, content_type: &str, body: &str) {
    let _ = write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\nAccess-Control-Allow-Origin: *\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.flush();
}

/// Stream snapshots as Server-Sent Events until the run finishes. Sends the
/// current snapshot immediately, then a new event whenever `seq` advances.
fn stream_events(mut stream: TcpStream, hub: Arc<Hub>) {
    if write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\nAccess-Control-Allow-Origin: *\r\n\r\n"
    )
    .is_err()
    {
        return;
    }
    let mut last_seq = 0u64;
    loop {
        let (latest, seq, done) = hub.read();
        if seq != last_seq {
            last_seq = seq;
            if let Some(json) = &latest {
                // SSE frame: `data: <json>\n\n`.
                if write!(stream, "data: {json}\n\n").is_err() || stream.flush().is_err() {
                    return;
                }
            }
        }
        if done {
            // One last flush of the terminal snapshot already happened above.
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A blocking one-shot GET used by the CLI test helper (and handy for scripts):
/// connect, request `path`, return the response body.
#[allow(dead_code)]
pub fn get_body(addr: &str, path: &str) -> std::io::Result<String> {
    let mut s = TcpStream::connect(addr)?;
    write!(
        s,
        "GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"
    )?;
    let mut buf = String::new();
    s.read_to_string(&mut buf)?;
    Ok(buf.split("\r\n\r\n").nth(1).unwrap_or("").to_string())
}
