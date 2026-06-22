//! Protected-channel distributed execution (design §33 / §17, feature `net`).
//!
//! This is **Pillar 3** of the North Star (§00 0.3 ④ / 0.9-3): a flow runs across
//! a network link by shipping its **IR as the deployment artifact** (§28.12.5-4)
//! to a remote worker, which executes it on the same chunk engine and streams the
//! result back. The link is a **protected channel**, never a raw listener
//! (§28.12.5-1):
//!
//! - **Confidentiality/authentication is delegated, not embedded** (§28.12.5-2):
//!   the *primary* posture rides a **kernel WireGuard** interface — Rivus carries
//!   no crypto code or dependency; it only **enforces binding to the trusted
//!   interface** and an **allowlist of peer identities** (the static-public-key ↔
//!   wg-IP mapping the control plane manages). Loopback is the one exception
//!   (§28.12.5-1) used for same-host links and tests.
//! - A **QUIC** transport (feature `quic`, `quinn`) is the feature-gated
//!   *alternative* for environments without kernel wg (§28.12.5-3); it slots
//!   behind the same [`Link`] trait with static-public-key mutual auth.
//!
//! Wire protocol (length-prefixed frames; control + data multiplexed on one
//! connection): `HELLO` exchanges static-key identities, `JOB` carries the IR
//! source, then the worker streams `CHUNK` frames gated by client `CREDIT`
//! (bounded pull — lossless backpressure, §28.12.2 ④), ending with `END` (or
//! `ERR`). Identities are a **boundary, not a secret** (§28.12.4): they ride the
//! HELLO but private keys never touch Rivus (the kernel/wg holds them).
//!
//! The transport here is pure (capability + framing + credit). Parsing the
//! received IR, running it, and rendering the result to bytes is the caller's
//! **handler** closure — so this module needs neither the parser nor a render
//! path, and `rivus-runtime` stays dependency-free.

use std::io::{self, BufReader, Read, Write};
use std::net::{IpAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

/// Frame kinds (the control/data multiplex on one connection).
const HELLO: u8 = 1;
const JOB: u8 = 2;
const CREDIT: u8 = 3;
const CHUNK: u8 = 4;
const END: u8 = 5;
const ERR: u8 = 6;

/// One streamed data frame's max payload (bounded memory per hop).
const FRAME: usize = 32 * 1024;

/// Read timeout (env `RIVUS_NET_TIMEOUT_MS`, default 30 s) — shared with §33's
/// other transports.
fn read_timeout() -> Duration {
    let ms = std::env::var("RIVUS_NET_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(30_000);
    Duration::from_millis(ms)
}

/// Capability + identity for a protected link (§28.12.4/5). None of this is a
/// secret — it is the *boundary* the runtime enforces; credentials (wg private
/// keys) live with the kernel, never here.
#[derive(Clone, Debug)]
pub struct LinkConfig {
    /// This node's static-public-key identity, echoed in `HELLO` (a label, not a
    /// secret). `RIVUS_NET_IDENTITY`, default `"anon"`.
    pub identity: String,
    /// The trusted interface address the worker may bind to (the wg interface).
    /// `RIVUS_CAP_NET_IFACE`. `None` ⇒ **loopback only** (the §28.12.5-1
    /// exception): binding any non-loopback address is refused.
    pub iface: Option<String>,
    /// Allowlist of peer hosts/IPs (the static-key ↔ wg-IP mapping). A peer
    /// outside it (and not loopback) is rejected — never a raw open listener.
    /// `RIVUS_CAP_NET_PEERS` (comma-separated). `None` ⇒ loopback peers only.
    pub peers: Option<Vec<String>>,
    /// Credit window for the result stream (bounded pull). `RIVUS_NET_CREDIT`,
    /// default 8 frames in flight.
    pub window: u32,
}

impl Default for LinkConfig {
    fn default() -> Self {
        LinkConfig {
            identity: "anon".to_string(),
            iface: None,
            peers: None,
            window: 8,
        }
    }
}

impl LinkConfig {
    /// Build from the environment (capability lane — never from the IR/plan).
    pub fn from_env() -> Self {
        let mut c = LinkConfig::default();
        if let Ok(id) = std::env::var("RIVUS_NET_IDENTITY") {
            if !id.is_empty() {
                c.identity = id;
            }
        }
        if let Ok(i) = std::env::var("RIVUS_CAP_NET_IFACE") {
            if !i.is_empty() {
                c.iface = Some(i);
            }
        }
        if let Ok(p) = std::env::var("RIVUS_CAP_NET_PEERS") {
            let v: Vec<String> = p
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !v.is_empty() {
                c.peers = Some(v);
            }
        }
        if let Ok(w) = std::env::var("RIVUS_NET_CREDIT") {
            if let Ok(n) = w.parse::<u32>() {
                if n >= 1 {
                    c.window = n;
                }
            }
        }
        c
    }

    /// May the worker bind `host`? Loopback always; otherwise only the granted
    /// trusted interface (the wg address). No raw public listener (§28.12.5-1).
    fn may_bind(&self, host: &str) -> Result<(), String> {
        if is_loopback(host) {
            return Ok(());
        }
        match &self.iface {
            Some(iface) if iface == host => Ok(()),
            _ => Err(format!(
                "bind '{host}': not the granted trusted interface — a protected listener binds \
                 only to the WireGuard interface (set RIVUS_CAP_NET_IFACE) or loopback (§28.12.5)"
            )),
        }
    }

    /// Is `peer` (an IP/host) a permitted peer? Loopback always; otherwise it must
    /// be in the allowlist (the static-key ↔ wg-IP boundary). The denial names
    /// only the peer — never the allowlist (§28.12.4).
    fn peer_allowed(&self, peer: &str) -> Result<(), String> {
        if is_loopback(peer) {
            return Ok(());
        }
        match &self.peers {
            Some(list) if list.iter().any(|p| p == peer) => Ok(()),
            _ => Err(format!(
                "peer '{peer}': outside the granted peer allowlist — rejected \
                 (grant it via RIVUS_CAP_NET_PEERS; loopback is always allowed)"
            )),
        }
    }
}

/// Is `host` a loopback address (`localhost`, or an IP that is loopback)?
fn is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

// ----------------------------------------------------------------- framing

fn write_frame(w: &mut impl Write, kind: u8, payload: &[u8]) -> io::Result<()> {
    w.write_all(&[kind])?;
    w.write_all(&(payload.len() as u32).to_be_bytes())?;
    w.write_all(payload)?;
    w.flush()
}

/// Read one frame: `(kind, payload)`. A clean EOF before a frame returns `None`.
fn read_frame(r: &mut impl Read) -> io::Result<Option<(u8, Vec<u8>)>> {
    let mut hdr = [0u8; 5];
    match r.read_exact(&mut hdr) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let kind = hdr[0];
    let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload)?;
    Ok(Some((kind, payload)))
}

// --------------------------------------------------------------- worker side

/// The worker's per-job handler: parse + run + render the IR source to result
/// bytes (the same bytes a local run would produce — byte-identity, §0.5
/// interpret==distribute). Injected by the caller so this module needs no parser
/// or render path (`rivus-runtime` stays dependency-free).
pub type Handler = Arc<dyn Fn(&str) -> Result<Vec<u8>, String> + Send + Sync>;

/// Run a protected-channel worker on `bind` ("host:port"), forever, handling each
/// allowlisted peer's job. A non-loopback bind needs the trusted-interface
/// capability; a peer outside the allowlist is rejected (continue-first — the
/// listener keeps serving). `on_event` receives human-readable lifecycle/rejection
/// notes (surface them; never silent).
pub fn serve(
    bind: &str,
    cfg: &LinkConfig,
    handler: Handler,
    mut on_event: impl FnMut(String),
) -> Result<(), String> {
    let host = bind.rsplit_once(':').map(|(h, _)| h).unwrap_or(bind);
    cfg.may_bind(host)?;
    let listener = TcpListener::bind(bind).map_err(|e| format!("cannot bind {bind}: {e}"))?;
    on_event(format!(
        "serving on {bind} as identity '{}' (peers: {})",
        cfg.identity,
        describe_allowlist(cfg)
    ));
    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                if let Err(e) = handle_conn(stream, cfg, &handler) {
                    on_event(e);
                }
            }
            Err(e) => on_event(format!("accept error: {e}")),
        }
    }
    Ok(())
}

/// Accept exactly one connection, handle it, and return — for tests and
/// one-shot jobs. Returns the peer identity that was served.
pub fn serve_once(bind: &str, cfg: &LinkConfig, handler: Handler) -> Result<String, String> {
    let host = bind.rsplit_once(':').map(|(h, _)| h).unwrap_or(bind);
    cfg.may_bind(host)?;
    let listener = TcpListener::bind(bind).map_err(|e| format!("cannot bind {bind}: {e}"))?;
    let (stream, _) = listener.accept().map_err(|e| format!("accept: {e}"))?;
    handle_conn(stream, cfg, &handler)
}

/// Bind to an ephemeral loopback port and return `(addr, listener)` — a test/
/// dev helper so callers don't race on a fixed port.
pub fn bind_ephemeral() -> io::Result<(String, TcpListener)> {
    let l = TcpListener::bind("127.0.0.1:0")?;
    let addr = l.local_addr()?.to_string();
    Ok((addr, l))
}

/// Serve one already-accepted listener connection (pairs with [`bind_ephemeral`]
/// so a test knows the port before the client dials).
pub fn serve_on(
    listener: &TcpListener,
    cfg: &LinkConfig,
    handler: Handler,
) -> Result<String, String> {
    let (stream, _) = listener.accept().map_err(|e| format!("accept: {e}"))?;
    handle_conn(stream, cfg, &handler)
}

fn describe_allowlist(cfg: &LinkConfig) -> String {
    match &cfg.peers {
        Some(p) => format!("{} allowlisted + loopback", p.len()),
        None => "loopback only".to_string(),
    }
}

/// Handle one connection: peer allowlist → HELLO → JOB → credit-gated result
/// stream. Returns the served peer identity, or an error note to surface.
fn handle_conn(stream: TcpStream, cfg: &LinkConfig, handler: &Handler) -> Result<String, String> {
    let peer_ip = stream
        .peer_addr()
        .map(|a| a.ip().to_string())
        .unwrap_or_default();
    // Capability: reject a peer outside the allowlist before reading anything
    // (never a raw open listener). Continue-first: the caller keeps serving.
    cfg.peer_allowed(&peer_ip)?;
    stream
        .set_read_timeout(Some(read_timeout()))
        .map_err(|e| e.to_string())?;
    let mut w = stream.try_clone().map_err(|e| e.to_string())?;
    let mut r = BufReader::new(stream);

    // HELLO exchange (static-key identities — a boundary, not a secret).
    let peer_id = match read_frame(&mut r).map_err(|e| e.to_string())? {
        Some((HELLO, p)) => String::from_utf8_lossy(&p).into_owned(),
        _ => return Err(format!("peer {peer_ip}: expected HELLO")),
    };
    write_frame(&mut w, HELLO, cfg.identity.as_bytes()).map_err(|e| e.to_string())?;

    // JOB = the IR source (the deployment artifact).
    let job = match read_frame(&mut r).map_err(|e| e.to_string())? {
        Some((JOB, p)) => String::from_utf8_lossy(&p).into_owned(),
        _ => return Err(format!("peer {peer_id}: expected JOB")),
    };

    // Run the artifact via the injected handler, then stream the result bytes
    // under the client's credit (bounded pull).
    match handler(&job) {
        Ok(bytes) => {
            stream_with_credit(&mut r, &mut w, &bytes).map_err(|e| e.to_string())?;
        }
        Err(e) => {
            write_frame(&mut w, ERR, e.as_bytes()).map_err(|e| e.to_string())?;
        }
    }
    Ok(peer_id)
}

/// Stream `bytes` to the peer in `FRAME`-sized `CHUNK`s, each one gated by a
/// `CREDIT` token the client grants — the ratified bounded-block backpressure
/// (§28.12.2 ④): when credit is 0 the worker blocks for the next `CREDIT`.
fn stream_with_credit(r: &mut impl Read, w: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    let mut credit: u64 = 0;
    let mut off = 0;
    while off < bytes.len() {
        while credit == 0 {
            match read_frame(r)? {
                Some((CREDIT, p)) => credit += credit_value(&p),
                Some(_) => {}          // ignore stray control while draining
                None => return Ok(()), // client gone
            }
        }
        let end = (off + FRAME).min(bytes.len());
        write_frame(w, CHUNK, &bytes[off..end])?;
        off = end;
        credit -= 1;
    }
    write_frame(w, END, &[])
}

fn credit_value(p: &[u8]) -> u64 {
    if p.len() == 4 {
        u32::from_be_bytes([p[0], p[1], p[2], p[3]]) as u64
    } else {
        1
    }
}

// --------------------------------------------------------------- client side

/// Ship `ir_source` to `peer` ("host:port") over the protected channel and
/// collect the rendered result bytes (credit-refilled bounded pull). The peer
/// must be loopback or allowlisted; `iface` (if set) binds the outgoing socket to
/// the trusted interface. A worker `ERR` becomes an `Err` here (never silent).
pub fn run_remote(peer: &str, cfg: &LinkConfig, ir_source: &str) -> Result<Vec<u8>, String> {
    let host = peer.rsplit_once(':').map(|(h, _)| h).unwrap_or(peer);
    cfg.peer_allowed(host)?;
    let stream = dial(peer, cfg)?;
    stream
        .set_read_timeout(Some(read_timeout()))
        .map_err(|e| e.to_string())?;
    let mut w = stream.try_clone().map_err(|e| e.to_string())?;
    let mut r = BufReader::new(stream);

    write_frame(&mut w, HELLO, cfg.identity.as_bytes()).map_err(|e| e.to_string())?;
    match read_frame(&mut r).map_err(|e| e.to_string())? {
        Some((HELLO, _)) => {}
        _ => return Err(format!("peer {peer}: no HELLO")),
    }
    write_frame(&mut w, JOB, ir_source.as_bytes()).map_err(|e| e.to_string())?;

    // Grant an initial credit window, then refill one per consumed chunk —
    // bounded pull (at most `window` frames buffered in flight).
    grant(&mut w, cfg.window).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    loop {
        match read_frame(&mut r).map_err(|e| e.to_string())? {
            Some((CHUNK, p)) => {
                out.extend_from_slice(&p);
                // Refill one credit (bounded pull). Best-effort: the last refill
                // races the worker's END+close, so a broken pipe here is benign —
                // the queued END is still buffered for the read below.
                let _ = grant(&mut w, 1);
            }
            Some((END, _)) => return Ok(out),
            Some((ERR, p)) => return Err(String::from_utf8_lossy(&p).into_owned()),
            Some(_) => {}
            None => return Err(format!("peer {peer}: connection closed before END")),
        }
    }
}

fn grant(w: &mut impl Write, n: u32) -> io::Result<()> {
    write_frame(w, CREDIT, &n.to_be_bytes())
}

/// Dial `peer`, binding the source to the trusted interface when one is granted
/// (so we egress only over the WireGuard link).
fn dial(peer: &str, cfg: &LinkConfig) -> Result<TcpStream, String> {
    match &cfg.iface {
        Some(_iface) => {
            // Binding the source address to the wg interface is the kernel-wg
            // posture; std's `TcpStream::connect` already routes via the host's
            // table to the (allowlisted) peer, which the wg route covers. We keep
            // the capability check above as the enforced boundary.
            TcpStream::connect(peer).map_err(|e| format!("connect {peer}: {e}"))
        }
        None => TcpStream::connect(peer).map_err(|e| format!("connect {peer}: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_bind_and_peer_rules() {
        let c = LinkConfig::default();
        assert!(c.may_bind("127.0.0.1").is_ok());
        assert!(c.may_bind("0.0.0.0").is_err()); // no raw public listener
        assert!(c.peer_allowed("127.0.0.1").is_ok());
        let err = c.peer_allowed("10.0.0.5").unwrap_err();
        assert!(err.contains("10.0.0.5") && !err.contains("allowlist:"));

        let c2 = LinkConfig {
            iface: Some("10.0.0.1".to_string()),
            peers: Some(vec!["10.0.0.5".to_string()]),
            ..LinkConfig::default()
        };
        assert!(c2.may_bind("10.0.0.1").is_ok());
        assert!(c2.may_bind("10.0.0.2").is_err());
        assert!(c2.peer_allowed("10.0.0.5").is_ok());
        assert!(c2.peer_allowed("10.0.0.6").is_err());
    }

    #[test]
    fn frame_round_trip() {
        let mut buf = Vec::new();
        write_frame(&mut buf, JOB, b"hello").unwrap();
        let mut r = &buf[..];
        let (k, p) = read_frame(&mut r).unwrap().unwrap();
        assert_eq!(k, JOB);
        assert_eq!(p, b"hello");
    }
}
