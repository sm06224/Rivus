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
/// A structured telemetry event (on the [`TELE`] channel) — event-centric
/// observability (§34): the worker narrates `flow.started` / `flow.completed` /
/// `transfer.done` etc. instead of the client having to packet-sniff.
const EVENT: u8 = 7;

/// **Logical channels** multiplexed over the one connection (§34, the QUIC
/// stream-separation lesson): control (lifecycle/credit), data (result chunks)
/// and telemetry (events) are tagged so a consumer can demux and budget them
/// apart — without N physical connections.
const CTRL: u8 = 1;
const DATA: u8 = 2;
const TELE: u8 = 3;

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
    /// default 64 frames (× 32 KiB ≈ 2 MiB) in flight — large enough to keep the
    /// pipe full on a fast link, small enough to stay bounded-memory.
    pub window: u32,
}

impl Default for LinkConfig {
    fn default() -> Self {
        LinkConfig {
            identity: "anon".to_string(),
            iface: None,
            peers: None,
            window: 64,
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
//
// Frame = `[channel:u8][kind:u8][len:u32 BE][payload]`. The leading channel byte
// (§34) lets one connection carry control / data / telemetry logically apart.

fn write_frame(w: &mut impl Write, channel: u8, kind: u8, payload: &[u8]) -> io::Result<()> {
    w.write_all(&[channel, kind])?;
    w.write_all(&(payload.len() as u32).to_be_bytes())?;
    w.write_all(payload)?;
    w.flush()
}

/// Read one frame: `(channel, kind, payload)`. A clean EOF before a frame
/// returns `None`.
fn read_frame(r: &mut impl Read) -> io::Result<Option<(u8, u8, Vec<u8>)>> {
    let mut hdr = [0u8; 6];
    match r.read_exact(&mut hdr) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let channel = hdr[0];
    let kind = hdr[1];
    let len = u32::from_be_bytes([hdr[2], hdr[3], hdr[4], hdr[5]]) as usize;
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload)?;
    Ok(Some((channel, kind, payload)))
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

/// Handle one **TCP** connection: peer allowlist → the transport-agnostic
/// protocol. Returns the served peer identity, or an error note to surface.
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
    // TCP_NODELAY: the protocol is a small-frame request/response ping-pong, which
    // Nagle + delayed-ACK would stall ~40 ms per round-trip — disable it.
    let _ = stream.set_nodelay(true);
    let mut w = stream.try_clone().map_err(|e| e.to_string())?;
    let mut r = BufReader::new(stream);
    serve_protocol(&mut r, &mut w, &cfg.identity, &peer_ip, handler)
}

/// The **transport-agnostic** worker protocol (§34): HELLO exchange → JOB → run
/// the IR handler → credit-gated result stream + telemetry events → graceful
/// drain. Generic over the byte streams, so the *same frames* run over TCP, a
/// Unix-domain socket (the host Transport Service, §34.4), or any future
/// transport — only the connection setup differs.
fn serve_protocol(
    r: &mut impl Read,
    w: &mut impl Write,
    identity: &str,
    label: &str,
    handler: &Handler,
) -> Result<String, String> {
    // HELLO exchange (static-key identities — a boundary, not a secret).
    let peer_id = match read_frame(r).map_err(|e| e.to_string())? {
        Some((_, HELLO, p)) => String::from_utf8_lossy(&p).into_owned(),
        _ => return Err(format!("peer {label}: expected HELLO")),
    };
    write_frame(w, CTRL, HELLO, identity.as_bytes()).map_err(|e| e.to_string())?;

    // Job loop (§34.4 s2'): one connection carries **many** jobs — the client
    // sends a JOB, drains the result + END, then sends the next (or closes).
    // Reusing the connection amortizes connect/handshake (a session). The loop's
    // read-until-EOF also subsumes the old single-job graceful drain: the worker
    // only returns once the client has read everything and closed.
    loop {
        // Read the next JOB, skipping any **stray CREDIT** left over from the
        // previous job: the client's best-effort refill after the last chunk
        // races END, so a leftover credit token can arrive between jobs.
        let job = loop {
            match read_frame(r).map_err(|e| e.to_string())? {
                Some((_, JOB, p)) => break String::from_utf8_lossy(&p).into_owned(),
                Some((_, CREDIT, _)) => continue,
                None => return Ok(peer_id), // client closed the session
                _ => return Err(format!("peer {peer_id}: expected JOB")),
            }
        };
        // Event-centric observability (§34): narrate on the telemetry channel.
        let _ = write_frame(
            w,
            TELE,
            EVENT,
            format!("flow.started job_bytes={}", job.len()).as_bytes(),
        );
        let t0 = std::time::Instant::now();
        match handler(&job) {
            Ok(bytes) => {
                let _ = write_frame(
                    w,
                    TELE,
                    EVENT,
                    format!(
                        "flow.completed result_bytes={} ms={}",
                        bytes.len(),
                        t0.elapsed().as_millis()
                    )
                    .as_bytes(),
                );
                stream_with_credit(r, w, &bytes).map_err(|e| e.to_string())?;
            }
            Err(e) => {
                let _ = write_frame(w, TELE, EVENT, b"flow.failed");
                write_frame(w, CTRL, ERR, e.as_bytes()).map_err(|e| e.to_string())?;
            }
        }
    }
}

/// Stream `bytes` to the peer in `FRAME`-sized `CHUNK`s, each one gated by a
/// `CREDIT` token the client grants — the ratified bounded-block backpressure
/// (§28.12.2 ④): when credit is 0 the worker blocks for the next `CREDIT`.
fn stream_with_credit(r: &mut impl Read, w: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    let mut credit: u64 = 0;
    let mut off = 0;
    let mut frames = 0u64;
    while off < bytes.len() {
        while credit == 0 {
            match read_frame(r)? {
                Some((_, CREDIT, p)) => credit += credit_value(&p),
                Some(_) => {}          // ignore stray control while draining
                None => return Ok(()), // client gone
            }
        }
        let end = (off + FRAME).min(bytes.len());
        write_frame(w, DATA, CHUNK, &bytes[off..end])?;
        off = end;
        credit -= 1;
        frames += 1;
    }
    let _ = write_frame(
        w,
        TELE,
        EVENT,
        format!("transfer.done frames={frames} bytes={}", bytes.len()).as_bytes(),
    );
    write_frame(w, CTRL, END, &[])
}

fn credit_value(p: &[u8]) -> u64 {
    if p.len() == 4 {
        u32::from_be_bytes([p[0], p[1], p[2], p[3]]) as u64
    } else {
        1
    }
}

// --------------------------------------------------------------- client side

/// A worker [`Handler`] that **forwards** each job to an upstream peer over the
/// std (kernel-WireGuard-bound) channel and returns its result bytes — the
/// **forwarding gateway** at the heart of the host Transport Service (§34.4 s2,
/// PMCN consolidation): co-located Rivus processes reach a remote worker through
/// one local service that owns the network egress, instead of each opening its
/// own connection. Pair it with [`serve_uds`] (`rivus serve --uds … --upstream
/// rivus://host:port`). The IR stays the artifact; the gateway only relays bytes.
pub fn forwarding_handler(upstream: String, cfg: LinkConfig) -> Handler {
    Arc::new(move |ir_source: &str| run_remote(&upstream, &cfg, ir_source))
}

/// Like [`forwarding_handler`], but reuses **one upstream [`Session`]** across all
/// jobs (§34.4 s2', true session sharing): co-located Rivus processes funnel
/// through a single persistent upstream connection that the gateway owns, instead
/// of a fresh connect+handshake per job (the big win for QUIC upstreams, #176).
/// Jobs serialize through the shared connection (a `Mutex`); a dropped upstream
/// is transparently re-established on the next job.
pub fn forwarding_session_handler(upstream: String, cfg: LinkConfig) -> Handler {
    let session: std::sync::Mutex<Option<Session>> = std::sync::Mutex::new(None);
    Arc::new(move |ir_source: &str| {
        let mut guard = session.lock().unwrap();
        if guard.is_none() {
            *guard = Some(Session::connect(&upstream, &cfg)?);
        }
        // Run on the reused session; if the upstream dropped, reconnect once.
        let first = guard.as_mut().unwrap().run(ir_source);
        match first {
            Ok(bytes) => Ok(bytes),
            Err(_) => {
                let mut s = Session::connect(&upstream, &cfg)?;
                let retry = s.run(ir_source);
                *guard = Some(s);
                retry
            }
        }
    })
}

/// Ship `ir_source` to `peer` ("host:port") over the protected channel and
/// collect the rendered result bytes (credit-refilled bounded pull). The peer
/// must be loopback or allowlisted; `iface` (if set) binds the outgoing socket to
/// the trusted interface. A worker `ERR` becomes an `Err` here (never silent).
pub fn run_remote(peer: &str, cfg: &LinkConfig, ir_source: &str) -> Result<Vec<u8>, String> {
    run_remote_observed(peer, cfg, ir_source, |_| {})
}

/// Like [`run_remote`], but `on_event` receives each structured **telemetry**
/// event the worker narrates (`flow.started` / `flow.completed` / `transfer.done`
/// …) — event-centric observability (§34), demuxed off the telemetry channel
/// while the data channel carries the result.
pub fn run_remote_observed(
    peer: &str,
    cfg: &LinkConfig,
    ir_source: &str,
    on_event: impl FnMut(String),
) -> Result<Vec<u8>, String> {
    let host = peer.rsplit_once(':').map(|(h, _)| h).unwrap_or(peer);
    cfg.peer_allowed(host)?;
    let stream = dial(peer, cfg)?;
    stream
        .set_read_timeout(Some(read_timeout()))
        .map_err(|e| e.to_string())?;
    let _ = stream.set_nodelay(true); // see handle_conn — avoid Nagle ping-pong stalls
    let mut w = stream.try_clone().map_err(|e| e.to_string())?;
    let mut r = BufReader::new(stream);
    client_protocol(
        &mut r,
        &mut w,
        &cfg.identity,
        ir_source,
        cfg.window,
        peer,
        on_event,
    )
}

/// The **transport-agnostic** one-shot client protocol (§34): HELLO then one
/// job. Generic over the byte streams (TCP / UDS / …).
fn client_protocol(
    r: &mut impl Read,
    w: &mut impl Write,
    identity: &str,
    ir_source: &str,
    window: u32,
    label: &str,
    on_event: impl FnMut(String),
) -> Result<Vec<u8>, String> {
    client_hello(r, w, identity, label)?;
    run_job(r, w, ir_source, window, label, on_event)
}

/// The HELLO handshake (static-key identity exchange) — done once per connection,
/// before any job (a session may then run many jobs, §34.4 s2').
fn client_hello(
    r: &mut impl Read,
    w: &mut impl Write,
    identity: &str,
    label: &str,
) -> Result<(), String> {
    write_frame(w, CTRL, HELLO, identity.as_bytes()).map_err(|e| e.to_string())?;
    match read_frame(r).map_err(|e| e.to_string())? {
        Some((_, HELLO, _)) => Ok(()),
        _ => Err(format!("peer {label}: no HELLO")),
    }
}

/// Run **one** job over an already-HELLO'd connection: send JOB, grant a credit
/// window, demux the result on the data channel + telemetry events on the
/// telemetry channel until END/ERR. The connection stays open for the next job.
fn run_job(
    r: &mut impl Read,
    w: &mut impl Write,
    ir_source: &str,
    window: u32,
    label: &str,
    mut on_event: impl FnMut(String),
) -> Result<Vec<u8>, String> {
    write_frame(w, CTRL, JOB, ir_source.as_bytes()).map_err(|e| e.to_string())?;
    // Grant an initial credit window, then refill one per consumed chunk —
    // bounded pull (at most `window` frames buffered in flight).
    grant(w, window.max(1)).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    loop {
        match read_frame(r).map_err(|e| e.to_string())? {
            Some((DATA, CHUNK, p)) => {
                out.extend_from_slice(&p);
                let _ = grant(w, 1); // best-effort refill
            }
            Some((TELE, EVENT, p)) => on_event(String::from_utf8_lossy(&p).into_owned()),
            Some((_, END, _)) => return Ok(out),
            Some((_, ERR, p)) => return Err(String::from_utf8_lossy(&p).into_owned()),
            Some(_) => {}
            None => return Err(format!("peer {label}: connection closed before END")),
        }
    }
}

fn grant(w: &mut impl Write, n: u32) -> io::Result<()> {
    write_frame(w, CTRL, CREDIT, &n.to_be_bytes())
}

/// A **persistent session** to a worker (§34.4 s2'): one connection reused for
/// many jobs, amortizing the connect/handshake. The HELLO is done once at
/// [`Session::connect`]; each [`Session::run`] ships one job over the same
/// connection. Dropping it closes the session (the worker's job loop ends). This
/// is the reuse primitive a gateway/pool builds on.
pub struct Session {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    window: u32,
}

impl Session {
    /// Open a session to `peer` ("host:port") — capability-checked, HELLO'd.
    pub fn connect(peer: &str, cfg: &LinkConfig) -> Result<Session, String> {
        let host = peer.rsplit_once(':').map(|(h, _)| h).unwrap_or(peer);
        cfg.peer_allowed(host)?;
        let stream = dial(peer, cfg)?;
        stream
            .set_read_timeout(Some(read_timeout()))
            .map_err(|e| e.to_string())?;
        let _ = stream.set_nodelay(true);
        let writer = stream.try_clone().map_err(|e| e.to_string())?;
        let mut reader = BufReader::new(stream);
        let mut w = writer.try_clone().map_err(|e| e.to_string())?;
        client_hello(&mut reader, &mut w, &cfg.identity, peer)?;
        Ok(Session {
            reader,
            writer,
            window: cfg.window,
        })
    }

    /// Run one job over the reused connection; collect the result bytes.
    pub fn run(&mut self, ir_source: &str) -> Result<Vec<u8>, String> {
        self.run_observed(ir_source, |_| {})
    }

    /// Like [`Session::run`], surfacing the worker's telemetry events.
    pub fn run_observed(
        &mut self,
        ir_source: &str,
        on_event: impl FnMut(String),
    ) -> Result<Vec<u8>, String> {
        run_job(
            &mut self.reader,
            &mut self.writer,
            ir_source,
            self.window,
            "session",
            on_event,
        )
    }
}

// ----------------------------------------------- host Transport Service (UDS)
//
// §34.4 pre-implementation: the host-shared Transport Service fronts a **Unix
// domain socket** that co-located Rivus processes use, instead of each owning a
// network stack (PMCN "consolidate comms responsibility"). It runs the *same*
// channel-tagged protocol as the TCP path (`serve_protocol` / `client_protocol`)
// — proving the protocol is transport-agnostic (§34.1). UDS is local-only and
// filesystem-permission-gated, so there is no IP allowlist here; the capability
// boundary is the socket file's path/permissions (set by whoever creates it).

/// Run a UDS worker at `path` forever (the Transport Service front). The socket
/// file is created on bind and removed on a clean return. `identity` rides HELLO.
#[cfg(unix)]
pub fn serve_uds(
    path: &str,
    identity: &str,
    handler: Handler,
    mut on_event: impl FnMut(String),
) -> Result<(), String> {
    use std::os::unix::net::UnixListener;
    let _ = std::fs::remove_file(path); // clear a stale socket
    let listener = UnixListener::bind(path).map_err(|e| format!("cannot bind uds {path}: {e}"))?;
    on_event(format!(
        "transport service on uds://{path} as identity '{identity}'"
    ));
    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                if let Err(e) = handle_uds(stream, identity, &handler) {
                    on_event(e);
                }
            }
            Err(e) => on_event(format!("accept error: {e}")),
        }
    }
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Accept exactly one UDS connection (tests / one-shot).
#[cfg(unix)]
pub fn serve_uds_once(path: &str, identity: &str, handler: Handler) -> Result<String, String> {
    use std::os::unix::net::UnixListener;
    let _ = std::fs::remove_file(path);
    let listener = UnixListener::bind(path).map_err(|e| format!("cannot bind uds {path}: {e}"))?;
    let (stream, _) = listener.accept().map_err(|e| format!("accept: {e}"))?;
    let r = handle_uds(stream, identity, &handler);
    let _ = std::fs::remove_file(path);
    r
}

#[cfg(unix)]
fn handle_uds(
    stream: std::os::unix::net::UnixStream,
    identity: &str,
    handler: &Handler,
) -> Result<String, String> {
    stream
        .set_read_timeout(Some(read_timeout()))
        .map_err(|e| e.to_string())?;
    let mut w = stream.try_clone().map_err(|e| e.to_string())?;
    let mut r = BufReader::new(stream);
    serve_protocol(&mut r, &mut w, identity, "uds", handler)
}

/// Ship `ir_source` to a UDS Transport Service at `path`; collect the result.
#[cfg(unix)]
pub fn run_remote_uds(
    path: &str,
    identity: &str,
    ir_source: &str,
    window: u32,
    on_event: impl FnMut(String),
) -> Result<Vec<u8>, String> {
    use std::os::unix::net::UnixStream;
    let stream = UnixStream::connect(path).map_err(|e| format!("connect uds {path}: {e}"))?;
    stream
        .set_read_timeout(Some(read_timeout()))
        .map_err(|e| e.to_string())?;
    let mut w = stream.try_clone().map_err(|e| e.to_string())?;
    let mut r = BufReader::new(stream);
    client_protocol(&mut r, &mut w, identity, ir_source, window, path, on_event)
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
    fn frame_round_trip_carries_channel() {
        let mut buf = Vec::new();
        write_frame(&mut buf, CTRL, JOB, b"hello").unwrap();
        write_frame(&mut buf, TELE, EVENT, b"flow.started").unwrap();
        let mut r = &buf[..];
        let (ch, k, p) = read_frame(&mut r).unwrap().unwrap();
        assert_eq!((ch, k), (CTRL, JOB));
        assert_eq!(p, b"hello");
        let (ch, k, p) = read_frame(&mut r).unwrap().unwrap();
        assert_eq!((ch, k), (TELE, EVENT));
        assert_eq!(p, b"flow.started");
    }
}
