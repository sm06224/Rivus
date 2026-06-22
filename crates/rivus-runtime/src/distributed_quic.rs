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
//! Async `quinn`/`tokio` is bridged to Rivus's synchronous engine with a
//! current-thread runtime and `block_on`, so the public API mirrors the std
//! transport (`serve_once` / `run_remote`).

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
const FRAME: usize = 32 * 1024;

/// QUIC capability/identity (the static-key allowlist lane, §28.12.4).
#[derive(Clone, Debug, Default)]
pub struct QuicConfig {
    /// Allowed peer cert fingerprints (hex SHA-256 of the DER). `None` ⇒ accept
    /// any peer but surface its fingerprint (dev/loopback). `RIVUS_CAP_NET_PEER_KEYS`.
    pub allow_peer_keys: Option<Vec<String>>,
    /// Credit window for the result stream (bounded pull). Default 8.
    pub window: u32,
}

impl QuicConfig {
    pub fn from_env() -> Self {
        let mut c = QuicConfig {
            window: 8,
            ..Default::default()
        };
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
    Ok(ServerConfig::with_crypto(Arc::new(qsc)))
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
    Ok(ClientConfig::new(Arc::new(qcc)))
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
        self.rt.block_on(self.handle_one(handler))
    }

    /// Serve forever; surface each lifecycle/rejection note via `on_event`.
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
        loop {
            match self.rt.block_on(self.handle_one(handler.clone())) {
                Ok(peer) => on_event(format!("served peer {peer}")),
                Err(e) => on_event(e),
            }
        }
    }

    async fn handle_one(&self, handler: Handler) -> Result<String, String> {
        let incoming = self
            .endpoint
            .accept()
            .await
            .ok_or_else(|| "endpoint closed".to_string())?;
        let conn = incoming.await.map_err(|e| format!("handshake: {e}"))?;
        let fp =
            peer_fingerprint(&conn).ok_or_else(|| "peer presented no certificate".to_string())?;
        // Capability: pin the peer's static key (boundary, not secret).
        self.cfg.peer_pinned(&fp)?;
        let (mut send, mut recv) = conn
            .accept_bi()
            .await
            .map_err(|e| format!("accept stream: {e}"))?;

        match read_frame(&mut recv).await.map_err(|e| e.to_string())? {
            Some((HELLO, _)) => {}
            _ => return Err(format!("peer {fp}: expected HELLO")),
        }
        write_frame(&mut send, HELLO, self.identity.fingerprint.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        let job = match read_frame(&mut recv).await.map_err(|e| e.to_string())? {
            Some((JOB, p)) => String::from_utf8_lossy(&p).into_owned(),
            _ => return Err(format!("peer {fp}: expected JOB")),
        };
        // Run the (synchronous) IR handler off the async runtime so the QUIC
        // endpoint driver keeps running while the flow executes.
        let h = handler.clone();
        let result = tokio::task::spawn_blocking(move || h(&job))
            .await
            .map_err(|e| format!("handler task: {e}"))?;
        match result {
            Ok(bytes) => stream_with_credit(&mut send, &mut recv, &bytes)
                .await
                .map_err(|e| e.to_string())?,
            Err(e) => write_frame(&mut send, ERR, e.as_bytes())
                .await
                .map_err(|e| e.to_string())?,
        }
        let _ = send.finish();
        // Give the peer a moment to drain before the connection drops.
        conn.closed().await;
        Ok(fp)
    }
}

async fn stream_with_credit(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    bytes: &[u8],
) -> std::io::Result<()> {
    let mut credit: u64 = 0;
    let mut off = 0;
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
    }
    write_frame(send, END, &[]).await
}

// --------------------------------------------------------------- client side

/// Ship `ir_source` to a QUIC worker at `peer` and collect the result bytes.
/// The worker's static key must be pinned (or `allow_peer_keys` unset for dev).
pub fn quic_run_remote(peer: &str, cfg: &QuicConfig, ir_source: &str) -> Result<Vec<u8>, String> {
    let rt = runtime()?;
    let id = mint_identity()?;
    rt.block_on(quic_client(peer, cfg, ir_source, &id))
}

async fn quic_client(
    peer: &str,
    cfg: &QuicConfig,
    ir_source: &str,
    id: &Identity,
) -> Result<Vec<u8>, String> {
    let sockaddr: std::net::SocketAddr = peer
        .parse()
        .map_err(|e| format!("bad peer address '{peer}': {e}"))?;
    // Bind the client to loopback (not 0.0.0.0): a 0.0.0.0-bound source dialing
    // 127.0.0.1 can mis-route the return path on some hosts.
    let bind: std::net::SocketAddr = if sockaddr.ip().is_loopback() {
        "127.0.0.1:0".parse().unwrap()
    } else {
        "0.0.0.0:0".parse().unwrap()
    };
    let mut endpoint = Endpoint::client(bind).map_err(|e| format!("client endpoint: {e}"))?;
    endpoint.set_default_client_config(client_config(id)?);
    let conn = endpoint
        .connect(sockaddr, "rivus")
        .map_err(|e| format!("connect {peer}: {e}"))?
        .await
        .map_err(|e| format!("handshake {peer}: {e}"))?;
    // Pin the worker's static key (boundary, not secret).
    let fp =
        peer_fingerprint(&conn).ok_or_else(|| "worker presented no certificate".to_string())?;
    cfg.peer_pinned(&fp)?;

    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| format!("open stream: {e}"))?;
    write_frame(&mut send, HELLO, id.fingerprint.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    match read_frame(&mut recv).await.map_err(|e| e.to_string())? {
        Some((HELLO, _)) => {}
        _ => return Err(format!("peer {peer}: no HELLO")),
    }
    write_frame(&mut send, JOB, ir_source.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    write_frame(&mut send, CREDIT, &cfg.window.to_be_bytes())
        .await
        .map_err(|e| e.to_string())?;

    let mut out = Vec::new();
    loop {
        match read_frame(&mut recv).await.map_err(|e| e.to_string())? {
            Some((CHUNK, p)) => {
                out.extend_from_slice(&p);
                let _ = write_frame(&mut send, CREDIT, &1u32.to_be_bytes()).await;
            }
            Some((END, _)) => {
                // Graceful close so the worker's `conn.closed()` returns promptly.
                conn.close(0u32.into(), b"ok");
                return Ok(out);
            }
            Some((ERR, p)) => {
                conn.close(0u32.into(), b"err");
                return Err(String::from_utf8_lossy(&p).into_owned());
            }
            Some(_) => {}
            None => return Err(format!("peer {peer}: closed before END")),
        }
    }
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
}
