//! QUIC transport for distributed execution (design §28.12.5-3, feature `quic`)
//! — the **feature-gated alternative** to riding a kernel WireGuard interface.
//!
//! Same wire protocol as the std path ([`crate::distributed`]): HELLO / JOB /
//! CHUNK / CREDIT / END / ERR framed over **one bidirectional QUIC stream**
//! (control + data multiplexed; QUIC gives per-stream flow control that matches
//! the bounded-pull credit, §28.12.2 ④). Identity is a **static public key**
//! (§28.12.5-4): each side mints a self-signed certificate; the identity is the
//! SHA-256 fingerprint of its DER, and the **allowlist pins allowed peer
//! fingerprints** (`RIVUS_CAP_NET_PEER_KEYS`). The TLS layer accepts any
//! certificate and the *application* enforces the pin after the handshake — the
//! allowlist is a boundary, not a secret (§28.12.4); private keys never leave
//! this process and never touch the IR/telemetry.
//!
//! Async `quinn`/`tokio` is bridged to Rivus's synchronous engine with a small
//! multi-threaded runtime + `block_on`, so the public API mirrors the std
//! transport (`quic_worker` / `quic_run_remote`). A bounded idle timeout +
//! keep-alive ([`transport_config`]) keeps an aborted peer from lingering.

use std::sync::Arc;

use quinn::{ClientConfig, Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};

use crate::distributed::Handler;

// Frame kinds — identical to the std transport.
const HELLO: u8 = 1;
const JOB: u8 = 2;
const CREDIT: u8 = 3;
const CHUNK: u8 = 4;
const END: u8 = 5;
const ERR: u8 = 6;
// §34.2 event-centric observability over QUIC: the worker narrates structured
// telemetry events (`flow.started` / `flow.completed` / `transfer.done` …) as
// `EVENT` frames, demuxed off the result by the observing client (parity with the
// std path's TELE/EVENT channel). By default they ride the result bidi stream,
// tagged by kind (a non-observing client ignores them — backward-compatible, the
// read loop skips unknown kinds). With `QuicConfig::telemetry_stream` they instead
// ride a **dedicated unidirectional QUIC stream** — the §34.1 channel→real-stream
// mapping spike (independent flow control; opt-in pending ratification).
const EVENT: u8 = 7;
const FRAME: usize = 32 * 1024;

/// QUIC capability/identity (the static-key allowlist lane, §28.12.4).
#[derive(Clone, Debug)]
pub struct QuicConfig {
    /// Allowed peer cert fingerprints (hex SHA-256 of the DER). `None` ⇒ accept
    /// any peer but surface its fingerprint (dev/loopback). `RIVUS_CAP_NET_PEER_KEYS`.
    pub allow_peer_keys: Option<Vec<String>>,
    /// Credit window for the result stream (bounded pull). Default 64.
    pub window: u32,
    /// §34.1 spike (opt-in, default off): carry the Telemetry channel on a
    /// **dedicated unidirectional QUIC stream** instead of multiplexing `EVENT`
    /// frames onto the result's bidi stream by kind. This realizes the design's
    /// channel→real-QUIC-stream 1:1 mapping — independent flow control, so a
    /// backed-up data stream can't head-of-line-block telemetry (or vice versa).
    /// Both peers must agree (no negotiation yet — a spike limitation); the proven
    /// single-stream path stays the default. `RIVUS_NET_QUIC_TELEMETRY_STREAM=1`.
    pub telemetry_stream: bool,
}

impl Default for QuicConfig {
    fn default() -> Self {
        // `window` MUST be >= 1: a 0 window means the client grants no credit and
        // the worker blocks forever awaiting more (the bug that stalled the QUIC
        // round-trip — `#[derive(Default)]` would have made it 0).
        QuicConfig {
            allow_peer_keys: None,
            window: 64,
            telemetry_stream: false,
        }
    }
}

impl QuicConfig {
    pub fn from_env() -> Self {
        let mut c = QuicConfig::default();
        if let Ok(p) = std::env::var("RIVUS_CAP_NET_PEER_KEYS") {
            let v: Vec<String> = p
                .split(',')
                .map(|s| s.trim().to_ascii_lowercase())
                .filter(|s| !s.is_empty())
                .collect();
            if !v.is_empty() {
                c.allow_peer_keys = Some(v);
            }
        }
        if let Ok(w) = std::env::var("RIVUS_NET_CREDIT") {
            if let Ok(n) = w.parse::<u32>() {
                if n >= 1 {
                    c.window = n;
                }
            }
        }
        if let Ok(v) = std::env::var("RIVUS_NET_QUIC_TELEMETRY_STREAM") {
            c.telemetry_stream = matches!(v.trim(), "1" | "true" | "yes" | "on");
        }
        c
    }

    fn peer_pinned(&self, fp: &str) -> Result<(), String> {
        match &self.allow_peer_keys {
            None => Ok(()),
            Some(list) if list.iter().any(|k| k == fp) => Ok(()),
            Some(_) => Err(format!(
                "peer key {fp}: not in the pinned allowlist — rejected \
                 (grant it via RIVUS_CAP_NET_PEER_KEYS)"
            )),
        }
    }
}

/// Hex SHA-256 of a certificate DER — the static-key fingerprint / identity.
fn fingerprint(der: &[u8]) -> String {
    let d = ring::digest::digest(&ring::digest::SHA256, der);
    d.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

/// A minted self-signed identity (key pair + cert), cached for an endpoint.
struct Identity {
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
    fingerprint: String,
}

fn mint_identity() -> Result<Identity, String> {
    let c = rcgen::generate_simple_self_signed(vec!["rivus".to_string()])
        .map_err(|e| format!("cannot mint identity cert: {e}"))?;
    let cert = c.cert.der().clone();
    let fingerprint = fingerprint(&cert);
    let key = PrivateKeyDer::try_from(c.key_pair.serialize_der())
        .map_err(|e| format!("cannot serialize identity key: {e}"))?;
    Ok(Identity {
        cert,
        key,
        fingerprint,
    })
}

// --- rustls "accept any cert" verifiers (pinning is enforced at the app layer) ---

#[derive(Debug)]
struct AcceptAnyServer(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for AcceptAnyServer {
    fn verify_server_cert(
        &self,
        _end: &CertificateDer<'_>,
        _inter: &[CertificateDer<'_>],
        _name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[derive(Debug)]
struct AcceptAnyClient(Arc<rustls::crypto::CryptoProvider>);

impl rustls::server::danger::ClientCertVerifier for AcceptAnyClient {
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }
    fn verify_client_cert(
        &self,
        _end: &CertificateDer<'_>,
        _inter: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        Ok(rustls::server::danger::ClientCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// A bounded idle timeout (10 s) + keep-alive (3 s): an aborted or stalled peer
/// is detected promptly instead of lingering on the protocol default, and a live
/// idle connection is kept up by keep-alives (`RIVUS_NET_TIMEOUT_MS` tunes it).
fn transport_config() -> Arc<quinn::TransportConfig> {
    let mut t = quinn::TransportConfig::default();
    let ms = std::env::var("RIVUS_NET_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(10_000);
    if let Ok(idle) = std::time::Duration::from_millis(ms).try_into() {
        t.max_idle_timeout(Some(idle));
    }
    t.keep_alive_interval(Some(std::time::Duration::from_millis((ms / 3).max(1))));
    Arc::new(t)
}

fn server_config(id: &Identity) -> Result<ServerConfig, String> {
    let p = provider();
    let verifier = Arc::new(AcceptAnyClient(p.clone()));
    let crypto = rustls::ServerConfig::builder_with_provider(p)
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![id.cert.clone()], id.key.clone_key())
        .map_err(|e| format!("server tls config: {e}"))?;
    let qsc = quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
        .map_err(|e| format!("quic server config: {e}"))?;
    let mut sc = ServerConfig::with_crypto(Arc::new(qsc));
    sc.transport_config(transport_config());
    Ok(sc)
}

fn client_config(id: &Identity) -> Result<ClientConfig, String> {
    let p = provider();
    let verifier = Arc::new(AcceptAnyServer(p.clone()));
    let crypto = rustls::ClientConfig::builder_with_provider(p)
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_auth_cert(vec![id.cert.clone()], id.key.clone_key())
        .map_err(|e| format!("client tls config: {e}"))?;
    let qcc = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
        .map_err(|e| format!("quic client config: {e}"))?;
    let mut cc = ClientConfig::new(Arc::new(qcc));
    cc.transport_config(transport_config());
    Ok(cc)
}

/// The fingerprint of the peer's certificate on a live connection (the static
/// public-key identity), for the allowlist pin.
fn peer_fingerprint(conn: &quinn::Connection) -> Option<String> {
    let certs = conn.peer_identity()?;
    let certs = certs.downcast::<Vec<CertificateDer<'static>>>().ok()?;
    certs.first().map(|c| fingerprint(c))
}

fn runtime() -> Result<tokio::runtime::Runtime, String> {
    // Multi-threaded (2 workers): quinn's endpoint driver runs continuously on a
    // background thread, so stream data and the connection-close handshake are
    // transmitted even while our `block_on` future is between awaits or returning
    // — a current-thread runtime only moves bytes *during* an active poll, which
    // stalls loopback transfers and the graceful close.
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))
}

// ------------------------------------------------------------- async framing

async fn write_frame(s: &mut quinn::SendStream, kind: u8, payload: &[u8]) -> std::io::Result<()> {
    s.write_all(&[kind]).await?;
    s.write_all(&(payload.len() as u32).to_be_bytes()).await?;
    s.write_all(payload).await?;
    Ok(())
}

async fn read_frame(s: &mut quinn::RecvStream) -> std::io::Result<Option<(u8, Vec<u8>)>> {
    let mut hdr = [0u8; 5];
    match s.read_exact(&mut hdr).await {
        Ok(()) => {}
        // A clean FIN at a frame boundary is end-of-stream, not an error.
        Err(quinn::ReadExactError::FinishedEarly { .. }) => return Ok(None),
        Err(e) => return Err(std::io::Error::other(e.to_string())),
    }
    let kind = hdr[0];
    let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    let mut payload = vec![0u8; len];
    s.read_exact(&mut payload)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e.to_string()))?;
    Ok(Some((kind, payload)))
}

// --------------------------------------------------------------- worker side

/// A bound QUIC worker endpoint (knows its address before serving, so a caller
/// can dial a deterministic port — for tests and one-shot jobs). The endpoint's
/// driver runs on the multi-threaded runtime's background threads, so it stays
/// live across `block_on` calls and never starves mid-transfer.
pub struct QuicWorker {
    endpoint: Endpoint,
    rt: tokio::runtime::Runtime,
    addr: String,
    identity: Arc<Identity>,
    cfg: QuicConfig,
}

/// Bind a QUIC worker on `bind` ("host:port"; `:0` picks an ephemeral port).
pub fn quic_worker(bind: &str, cfg: QuicConfig) -> Result<QuicWorker, String> {
    let rt = runtime()?;
    let identity = Arc::new(mint_identity()?);
    let sc = server_config(&identity)?;
    let sockaddr: std::net::SocketAddr = bind
        .parse()
        .map_err(|e| format!("bad bind address '{bind}': {e}"))?;
    let endpoint = rt
        .block_on(async { Endpoint::server(sc, sockaddr) })
        .map_err(|e| format!("cannot bind QUIC {bind}: {e}"))?;
    let addr = endpoint
        .local_addr()
        .map_err(|e| e.to_string())?
        .to_string();
    Ok(QuicWorker {
        endpoint,
        rt,
        addr,
        identity,
        cfg,
    })
}

impl QuicWorker {
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// This worker's static-key identity fingerprint (grant it to peers).
    pub fn identity(&self) -> &str {
        &self.identity.fingerprint
    }

    /// Accept one connection, run the shipped IR, stream the result. Returns the
    /// authenticated peer fingerprint.
    pub fn serve_once(&self, handler: Handler) -> Result<String, String> {
        self.rt.block_on(async {
            let incoming = self
                .endpoint
                .accept()
                .await
                .ok_or_else(|| "endpoint closed".to_string())?;
            process_conn(incoming, self.cfg.clone(), self.identity.clone(), handler).await
        })
    }

    /// Serve forever, handling each connection on its **own task** so a slow
    /// peer (or the graceful-close wait) never blocks the next handshake — the
    /// idiomatic quinn server shape, and what lets the worker serve many
    /// coordinators concurrently.
    pub fn serve(&self, handler: Handler, mut on_event: impl FnMut(String)) -> Result<(), String> {
        on_event(format!(
            "QUIC serving on {} as key {} (peers: {})",
            self.addr,
            self.identity.fingerprint,
            match &self.cfg.allow_peer_keys {
                Some(k) => format!("{} pinned", k.len()),
                None => "any (dev)".to_string(),
            }
        ));
        self.rt.block_on(async {
            while let Some(incoming) = self.endpoint.accept().await {
                let cfg = self.cfg.clone();
                let id = self.identity.clone();
                let h = handler.clone();
                tokio::spawn(async move {
                    let _ = process_conn(incoming, cfg, id, h).await;
                });
            }
        });
        Ok(())
    }
}

/// Handle one accepted connection: pin the peer, HELLO/JOB exchange, run the IR
/// handler, stream the result. A free async fn (owned args) so `serve` can run it
/// on a spawned task.
async fn process_conn(
    incoming: quinn::Incoming,
    cfg: QuicConfig,
    identity: Arc<Identity>,
    handler: Handler,
) -> Result<String, String> {
    let conn = incoming.await.map_err(|e| format!("handshake: {e}"))?;
    let fp = peer_fingerprint(&conn).ok_or_else(|| "peer presented no certificate".to_string())?;
    // Capability: pin the peer's static key once per **connection** (boundary,
    // not secret) — the expensive part (handshake + cert) is then reused.
    cfg.peer_pinned(&fp)?;
    // §34.4 s2'/§34.1: each job is a fresh **bidi stream** on the reused
    // connection (streams are cheap; the connection's TLS handshake is the cost).
    // Loop accepting streams until the peer closes the connection.
    // Until the peer closes the connection, each accepted bidi stream is one job.
    while let Ok((send, recv)) = conn.accept_bi().await {
        let _ = serve_stream(&conn, send, recv, &identity, &handler, cfg.telemetry_stream).await;
    }
    Ok(fp)
}

/// Emit a telemetry `EVENT`: to the dedicated uni stream `tel` when present
/// (§34.1 channel→stream mapping), else multiplexed onto the result bidi `send`.
async fn emit_event(send: &mut quinn::SendStream, tel: &mut Option<quinn::SendStream>, msg: &[u8]) {
    match tel {
        Some(t) => {
            let _ = write_frame(t, EVENT, msg).await;
        }
        None => {
            let _ = write_frame(send, EVENT, msg).await;
        }
    }
}

/// Serve one job on one bidi stream: HELLO exchange → JOB → run the handler →
/// credit-gated result. The connection (and its handshake) is reused across many
/// of these (a session).
async fn serve_stream(
    conn: &quinn::Connection,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    identity: &Identity,
    handler: &Handler,
    telemetry_stream: bool,
) -> Result<(), String> {
    match read_frame(&mut recv).await.map_err(|e| e.to_string())? {
        Some((HELLO, _)) => {}
        _ => return Err("expected HELLO".to_string()),
    }
    write_frame(&mut send, HELLO, identity.fingerprint.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    let job = match read_frame(&mut recv).await.map_err(|e| e.to_string())? {
        Some((JOB, p)) => String::from_utf8_lossy(&p).into_owned(),
        _ => return Err("expected JOB".to_string()),
    };
    // §34.1 spike: when enabled, open a dedicated unidirectional stream for the
    // Telemetry channel; otherwise events ride the result bidi stream (§34.2).
    let mut tel: Option<quinn::SendStream> = if telemetry_stream {
        conn.open_uni().await.ok()
    } else {
        None
    };
    // §34.2: narrate the job on the telemetry lane — parity with the std worker.
    emit_event(
        &mut send,
        &mut tel,
        format!("flow.started job_bytes={}", job.len()).as_bytes(),
    )
    .await;
    let t0 = std::time::Instant::now();
    match handler(&job) {
        Ok(bytes) => {
            emit_event(
                &mut send,
                &mut tel,
                format!(
                    "flow.completed result_bytes={} ms={}",
                    bytes.len(),
                    t0.elapsed().as_millis()
                )
                .as_bytes(),
            )
            .await;
            stream_with_credit(&mut send, &mut recv, &mut tel, &bytes)
                .await
                .map_err(|e| e.to_string())?
        }
        Err(e) => {
            emit_event(&mut send, &mut tel, b"flow.failed").await;
            write_frame(&mut send, ERR, e.as_bytes())
                .await
                .map_err(|e| e.to_string())?
        }
    }
    if let Some(mut t) = tel {
        let _ = t.finish(); // FIN the telemetry stream so the client's reader ends
    }
    let _ = send.finish();
    Ok(())
}

async fn stream_with_credit(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    tel: &mut Option<quinn::SendStream>,
    bytes: &[u8],
) -> std::io::Result<()> {
    let mut credit: u64 = 0;
    let mut off = 0;
    let mut frames = 0u64;
    while off < bytes.len() {
        while credit == 0 {
            match read_frame(recv).await? {
                Some((CREDIT, p)) if p.len() == 4 => {
                    credit += u32::from_be_bytes([p[0], p[1], p[2], p[3]]) as u64
                }
                Some((CREDIT, _)) => credit += 1,
                Some(_) => {}
                None => return Ok(()),
            }
        }
        let end = (off + FRAME).min(bytes.len());
        write_frame(send, CHUNK, &bytes[off..end]).await?;
        off = end;
        credit -= 1;
        frames += 1;
    }
    emit_event(
        send,
        tel,
        format!("transfer.done frames={frames} bytes={}", bytes.len()).as_bytes(),
    )
    .await;
    write_frame(send, END, &[]).await
}

// --------------------------------------------------------------- client side

/// Ship `ir_source` to a QUIC worker at `peer` and collect the result bytes
/// (one-shot: connect, run one job, close). For many jobs, hold a [`QuicSession`]
/// and reuse the connection (the expensive TLS handshake) — that is the lever for
/// QUIC's per-call cost (#176 / §34.4 s2').
pub fn quic_run_remote(peer: &str, cfg: &QuicConfig, ir_source: &str) -> Result<Vec<u8>, String> {
    let session = QuicSession::connect(peer, cfg)?;
    session.run(ir_source)
}

/// Like [`quic_run_remote`], but `on_event` receives each structured **telemetry**
/// event the worker narrates (`flow.started` / `flow.completed` / `transfer.done`
/// …) — §34.2 event-centric observability over QUIC, demuxed off the result while
/// the same stream carries the chunks (parity with `run_remote_observed`).
pub fn quic_run_observed(
    peer: &str,
    cfg: &QuicConfig,
    ir_source: &str,
    on_event: impl FnMut(String),
) -> Result<Vec<u8>, String> {
    let session = QuicSession::connect(peer, cfg)?;
    session.run_observed(ir_source, on_event)
}

/// A **persistent QUIC session** (§34.4 s2' / §28.12.5-3): one connection — and
/// its TLS handshake + minted identity — reused across many jobs, each a cheap
/// new **bidi stream** (the QUIC stream-multiplexing of §34.1). The static-key
/// pin is checked once at [`QuicSession::connect`]. Dropping it closes the
/// connection (the worker's stream-accept loop ends).
pub struct QuicSession {
    // The endpoint must outlive the connection (it drives it); keep it alive.
    _endpoint: Endpoint,
    conn: quinn::Connection,
    rt: tokio::runtime::Runtime,
    identity: Identity,
    window: u32,
    telemetry_stream: bool,
}

impl QuicSession {
    /// Open a session to `peer`: handshake, then pin the worker's static key.
    pub fn connect(peer: &str, cfg: &QuicConfig) -> Result<QuicSession, String> {
        let rt = runtime()?;
        let identity = mint_identity()?;
        let window = cfg.window;
        let telemetry_stream = cfg.telemetry_stream;
        let cfg = cfg.clone();
        let (endpoint, conn) = rt.block_on(async {
            let sockaddr: std::net::SocketAddr = peer
                .parse()
                .map_err(|e| format!("bad peer address '{peer}': {e}"))?;
            // Bind the client to loopback (not 0.0.0.0): a 0.0.0.0-bound source
            // dialing 127.0.0.1 can mis-route the return path on some hosts.
            let bind: std::net::SocketAddr = if sockaddr.ip().is_loopback() {
                "127.0.0.1:0".parse().unwrap()
            } else {
                "0.0.0.0:0".parse().unwrap()
            };
            let mut endpoint =
                Endpoint::client(bind).map_err(|e| format!("client endpoint: {e}"))?;
            endpoint.set_default_client_config(client_config(&identity)?);
            let conn = endpoint
                .connect(sockaddr, "rivus")
                .map_err(|e| format!("connect {peer}: {e}"))?
                .await
                .map_err(|e| format!("handshake {peer}: {e}"))?;
            // Pin the worker's static key once. On mismatch, close so the worker
            // notices at once, then surface the denial.
            let fp = peer_fingerprint(&conn)
                .ok_or_else(|| "worker presented no certificate".to_string())?;
            if let Err(e) = cfg.peer_pinned(&fp) {
                conn.close(1u32.into(), b"pin");
                endpoint.wait_idle().await;
                return Err(e);
            }
            Ok::<_, String>((endpoint, conn))
        })?;
        Ok(QuicSession {
            _endpoint: endpoint,
            conn,
            rt,
            identity,
            window,
            telemetry_stream,
        })
    }

    /// Run one job over a fresh bidi stream on the reused connection.
    pub fn run(&self, ir_source: &str) -> Result<Vec<u8>, String> {
        self.run_observed(ir_source, |_| {})
    }

    /// Like [`QuicSession::run`], but surfaces the worker's telemetry events
    /// (§34.2) to `on_event` while collecting the result on the same stream.
    pub fn run_observed(
        &self,
        ir_source: &str,
        on_event: impl FnMut(String),
    ) -> Result<Vec<u8>, String> {
        self.rt.block_on(client_stream(
            &self.conn,
            &self.identity.fingerprint,
            ir_source,
            self.window,
            self.telemetry_stream,
            on_event,
        ))
    }
}

/// One job on one bidi stream (the connection is reused): HELLO → JOB → credit →
/// collect the result. Does **not** close the connection (the session owns it).
async fn client_stream(
    conn: &quinn::Connection,
    id_fp: &str,
    ir_source: &str,
    window: u32,
    telemetry_stream: bool,
    mut on_event: impl FnMut(String),
) -> Result<Vec<u8>, String> {
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| format!("open stream: {e}"))?;
    write_frame(&mut send, HELLO, id_fp.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    match read_frame(&mut recv).await.map_err(|e| e.to_string())? {
        Some((HELLO, _)) => {}
        _ => return Err("no HELLO".to_string()),
    }
    write_frame(&mut send, JOB, ir_source.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    // Grant a non-zero credit window (a 0 window would deadlock the worker).
    write_frame(&mut send, CREDIT, &window.max(1).to_be_bytes())
        .await
        .map_err(|e| e.to_string())?;

    // §34.1 spike: in dedicated-telemetry-stream mode the result (Data/Control) and
    // the events (Telemetry) ride **separate** QUIC streams with independent flow
    // control, so we read them concurrently. The worker opens its uni stream right
    // after JOB; a bounded `accept_uni` guards against a pre-result error path.
    if telemetry_stream {
        // Drive the telemetry-stream reader on its own task (no `tokio::macros`
        // dependency) while the result is collected on this task; join after.
        let conn_for_tel = conn.clone();
        let tel_handle = tokio::spawn(async move { read_telemetry_uni(&conn_for_tel).await });
        let res = read_result(&mut send, &mut recv).await;
        let evs = tel_handle.await.unwrap_or_default();
        for e in evs {
            on_event(e);
        }
        res
    } else {
        // Default: events are multiplexed onto the bidi stream — surface inline.
        read_result_observed(&mut send, &mut recv, on_event).await
    }
}

/// Read the result (CHUNK→END/ERR) off the bidi stream, refilling credit; ignores
/// any inline EVENT (it rode the dedicated telemetry stream in this mode).
async fn read_result(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) -> Result<Vec<u8>, String> {
    read_result_observed(send, recv, |_| {}).await
}

/// Read the result off the bidi stream, surfacing any **inline** EVENT to
/// `on_event` (the default single-stream path, §34.2).
async fn read_result_observed(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    mut on_event: impl FnMut(String),
) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    loop {
        match read_frame(recv).await.map_err(|e| e.to_string())? {
            Some((CHUNK, p)) => {
                out.extend_from_slice(&p);
                let _ = write_frame(send, CREDIT, &1u32.to_be_bytes()).await;
            }
            Some((END, _)) => {
                let _ = send.finish(); // FIN our side of the stream; keep the conn
                return Ok(out);
            }
            Some((ERR, p)) => return Err(String::from_utf8_lossy(&p).into_owned()),
            Some((EVENT, p)) => on_event(String::from_utf8_lossy(&p).into_owned()),
            Some(_) => {}
            None => return Err("closed before END".to_string()),
        }
    }
}

/// Accept the worker's dedicated **telemetry** uni stream and drain its EVENT
/// frames until FIN. Bounded by a timeout so a missing stream (a pre-result error
/// path) can never hang the `join!`. Best-effort: errors yield the events so far.
async fn read_telemetry_uni(conn: &quinn::Connection) -> Vec<String> {
    let mut evs = Vec::new();
    let accept = tokio::time::timeout(std::time::Duration::from_secs(5), conn.accept_uni()).await;
    let mut uni = match accept {
        Ok(Ok(s)) => s,
        _ => return evs, // timed out or connection error — no telemetry this job
    };
    while let Ok(Some((kind, p))) = read_frame(&mut uni).await {
        if kind == EVENT {
            evs.push(String::from_utf8_lossy(&p).into_owned());
        }
    }
    evs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_is_stable_hex_sha256() {
        let fp = fingerprint(b"abc");
        // SHA-256("abc") well-known value.
        assert_eq!(
            fp,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn pin_rules() {
        let mut c = QuicConfig::default();
        assert!(c.peer_pinned("deadbeef").is_ok()); // None ⇒ dev accept-any
        c.allow_peer_keys = Some(vec!["abc123".to_string()]);
        assert!(c.peer_pinned("abc123").is_ok());
        assert!(c.peer_pinned("deadbeef").is_err());
    }

    /// Isolation probe: does a quinn bidi stream echo work in THIS environment
    /// with OUR configs, on a single runtime with the server spawned as a task
    /// (the idiomatic pattern)? If this passes, the dual-runtime harness is the
    /// suspect; if it hangs, it is config/env.
    #[test]
    fn quic_bidi_echo_single_runtime() {
        let rt = runtime().unwrap();
        rt.block_on(async {
            let sid = mint_identity().unwrap();
            let cid = mint_identity().unwrap();
            let sc = server_config(&sid).unwrap();
            let ep = Endpoint::server(sc, "127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = ep.local_addr().unwrap();

            // Server task: accept one conn, echo one bidi message.
            let sep = ep.clone();
            let server = tokio::spawn(async move {
                let conn = sep.accept().await.unwrap().await.unwrap();
                let (mut s, mut r) = conn.accept_bi().await.unwrap();
                let mut buf = [0u8; 5];
                r.read_exact(&mut buf).await.unwrap();
                s.write_all(&buf).await.unwrap();
                s.finish().unwrap();
                conn.closed().await;
            });

            // Client.
            let mut cep = Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
            cep.set_default_client_config(client_config(&cid).unwrap());
            let conn = cep.connect(addr, "rivus").unwrap().await.unwrap();
            let (mut s, mut r) = conn.open_bi().await.unwrap();
            s.write_all(b"hello").await.unwrap();
            let mut got = [0u8; 5];
            r.read_exact(&mut got).await.unwrap();
            assert_eq!(&got, b"hello");
            conn.close(0u32.into(), b"ok");
            let _ = server.await;
        });
    }

    /// Reproduce the real worker/client split: TWO runtimes on TWO threads, each
    /// `block_on` (exactly what `serve_once`/`run_remote` do across processes).
    #[test]
    fn quic_bidi_echo_two_runtimes() {
        let sid = mint_identity().unwrap();
        let sc = server_config(&sid).unwrap();
        let srt = runtime().unwrap();
        let ep = srt
            .block_on(async { Endpoint::server(sc, "127.0.0.1:0".parse().unwrap()) })
            .unwrap();
        let addr = ep.local_addr().unwrap();

        let server = std::thread::spawn(move || {
            srt.block_on(async {
                let conn = ep.accept().await.unwrap().await.unwrap();
                let (mut s, mut r) = conn.accept_bi().await.unwrap();
                let mut buf = [0u8; 5];
                r.read_exact(&mut buf).await.unwrap();
                s.write_all(&buf).await.unwrap();
                s.finish().unwrap();
                conn.closed().await;
            });
        });

        let cid = mint_identity().unwrap();
        let crt = runtime().unwrap();
        let got = crt.block_on(async {
            let mut cep = Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
            cep.set_default_client_config(client_config(&cid).unwrap());
            let conn = cep.connect(addr, "rivus").unwrap().await.unwrap();
            let (mut s, mut r) = conn.open_bi().await.unwrap();
            s.write_all(b"world").await.unwrap();
            let mut g = [0u8; 5];
            r.read_exact(&mut g).await.unwrap();
            conn.close(0u32.into(), b"ok");
            g
        });
        assert_eq!(&got, b"world");
        let _ = server.join();
    }
}
