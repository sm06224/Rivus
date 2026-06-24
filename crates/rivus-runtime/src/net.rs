//! Networking transport (design §33, feature `net`) — a **std-only** HTTP/1.1
//! GET client and a TCP subscribe dial, both **client-side only** (no listener
//! is ever bound, per §28.12.5-1: "protected channel or none; loopback is the
//! only exception"). Zero third-party dependencies: the whole thing rides
//! `std::net`.
//!
//! Both produce a `Box<dyn BufRead + Send>` the existing codecs decode unchanged
//! (the orthogonality of §28.2 — only the transport layer changes):
//! - [`http_get`] — `open "http://host[:port]/path.csv"`: one bounded GET,
//!   following up to [`MAX_REDIRECTS`] `3xx` redirects, body framed by
//!   `Content-Length` / `Transfer-Encoding: chunked` / connection-close.
//! - [`tcp_connect`] — `subscribe "tcp://host:port"`: dial a feed and stream
//!   newline-delimited records (an **unbounded** source, §33).
//!
//! ## Capability (§28.12.4 / §28.12.5)
//! A reachable endpoint must be **loopback** (`127.0.0.0/8` / `::1` /
//! `localhost`) unless its host (or `host:port`) is listed in the
//! `RIVUS_CAP_NET_HOSTS` allowlist. The allowlist is a *boundary*, not a secret:
//! a denial names only the rejected target, never the allowlist (information
//! minimization). Credentials never appear in this lane at all.
//!
//! `https://` is rejected with guidance: TLS+CA is out of scope (§28.12.5-5,
//! certificate-lifecycle operational load); the protected-channel lane is
//! WireGuard / QUIC (a later slice, §28.12.5-2/3).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{IpAddr, TcpStream};
use std::time::Duration;

/// Cap on `3xx` redirect hops (defends against a redirect loop).
const MAX_REDIRECTS: usize = 5;

/// Read timeout for a network source (env `RIVUS_NET_TIMEOUT_MS`, default 30 s).
/// A bounded GET / a quiet feed should not hang the run forever.
fn read_timeout() -> Duration {
    let ms = std::env::var("RIVUS_NET_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(30_000);
    Duration::from_millis(ms)
}

/// A parsed network endpoint: host, port, and (HTTP only) the request path.
struct Url {
    host: String,
    port: u16,
    path: String,
}

/// Parse `http://host[:port][/path]`. `https://` is rejected with guidance.
fn parse_http_url(url: &str) -> Result<Url, String> {
    let rest = if let Some(r) = url.strip_prefix("http://").or_else(|| {
        // Case-insensitive scheme: lowercase only the scheme to peek.
        url.get(..7)
            .filter(|s| s.eq_ignore_ascii_case("http://"))
            .map(|_| &url[7..])
    }) {
        r
    } else if url.len() >= 8 && url[..8].eq_ignore_ascii_case("https://") {
        return Err(format!(
            "'{url}': https/TLS is not supported (cert lifecycle is out of scope, §28.12.5); \
             use http on a protected channel (loopback, or WireGuard/QUIC — a later slice)"
        ));
    } else {
        return Err(format!("'{url}': not an http:// URL"));
    };
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = split_host_port(authority, 80)?;
    Ok(Url {
        host,
        port,
        path: path.to_string(),
    })
}

/// Split `host[:port]`, falling back to `default_port`. Bare IPv6 in brackets
/// (`[::1]:port`) is handled; a bare IPv6 without brackets is taken as the host.
fn split_host_port(authority: &str, default_port: u16) -> Result<(String, u16), String> {
    if authority.is_empty() {
        return Err("empty host".to_string());
    }
    // `[::1]:80` — bracketed IPv6 literal.
    if let Some(rest) = authority.strip_prefix('[') {
        let (h, after) = rest
            .split_once(']')
            .ok_or_else(|| format!("malformed IPv6 authority '{authority}'"))?;
        let port = match after.strip_prefix(':') {
            Some(p) => p
                .parse()
                .map_err(|_| format!("bad port in '{authority}'"))?,
            None => default_port,
        };
        return Ok((h.to_string(), port));
    }
    // More than one colon and unbracketed → an unbracketed IPv6 literal (which
    // can't carry a port without brackets) → the whole thing is the host.
    if authority.matches(':').count() > 1 {
        return Ok((authority.to_string(), default_port));
    }
    match authority.rsplit_once(':') {
        Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty() => Ok((
            h.to_string(),
            p.parse()
                .map_err(|_| format!("bad port in '{authority}'"))?,
        )),
        _ => Ok((authority.to_string(), default_port)),
    }
}

/// Capability boundary (§28.12.4/5): a non-loopback host must be allowlisted via
/// `RIVUS_CAP_NET_HOSTS` (comma-separated `host` or `host:port`). The denial
/// names only the target — never the allowlist.
fn check_capability(host: &str, port: u16) -> Result<(), String> {
    if is_loopback(host) {
        return Ok(());
    }
    if let Ok(allow) = std::env::var("RIVUS_CAP_NET_HOSTS") {
        let hp = format!("{host}:{port}");
        let granted = allow
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .any(|e| e == host || e == hp);
        if granted {
            return Ok(());
        }
    }
    Err(format!(
        "'{host}:{port}': outside the granted network capability — rejected \
         (loopback is always allowed; grant a remote host via RIVUS_CAP_NET_HOSTS)"
    ))
}

/// Is `host` a loopback address (`localhost`, or an IP that is loopback)?
fn is_loopback(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

/// Open a TCP connection to `host:port` (capability-checked), with the read
/// timeout applied.
fn dial(host: &str, port: u16) -> Result<TcpStream, String> {
    check_capability(host, port)?;
    let stream = TcpStream::connect((host, port))
        .map_err(|e| format!("cannot connect to {host}:{port}: {e}"))?;
    stream
        .set_read_timeout(Some(read_timeout()))
        .map_err(|e| format!("cannot set read timeout: {e}"))?;
    Ok(stream)
}

/// Parse `tcp://host:port` for `subscribe` (an explicit port is required — a
/// feed has no default port).
fn parse_tcp_url(url: &str) -> Result<Url, String> {
    let rest = url
        .strip_prefix("tcp://")
        .or_else(|| {
            url.get(..6)
                .filter(|s| s.eq_ignore_ascii_case("tcp://"))
                .map(|_| &url[6..])
        })
        .ok_or_else(|| {
            format!("'{url}': subscribe needs a tcp:// URL (e.g. tcp://127.0.0.1:9000)")
        })?;
    let authority = rest.split('/').next().unwrap_or(rest);
    let (host, port) = split_host_port(authority, 0)?;
    if port == 0 {
        return Err(format!(
            "'{url}': subscribe needs an explicit port (tcp://host:port)"
        ));
    }
    Ok(Url {
        host,
        port,
        path: String::new(),
    })
}

/// `subscribe "tcp://host:port"` (§33): dial the feed (loopback-or-allowlist
/// capability, §28.12.4) and return its byte stream as a `BufRead` the codec
/// line-streams. **Unbounded** — the stream ends when the peer closes (or a
/// `take N` downstream saturates).
pub(crate) fn tcp_connect(url: &str) -> Result<Box<dyn BufRead + Send>, String> {
    let u = parse_tcp_url(url)?;
    let stream = dial(&u.host, u.port)?;
    Ok(Box::new(BufReader::new(stream)))
}

/// `open "http://…"` (§33): one bounded HTTP/1.1 GET, following up to
/// [`MAX_REDIRECTS`] redirects, returning the response body as a `BufRead` the
/// codec decodes. A non-2xx final status is an error (the source has no data).
pub(crate) fn http_get(url: &str) -> Result<Box<dyn BufRead + Send>, String> {
    let mut current = url.to_string();
    for _ in 0..=MAX_REDIRECTS {
        let u = parse_http_url(&current)?;
        let stream = dial(&u.host, u.port)?;
        let mut reader = BufReader::new(stream);
        send_request(reader.get_mut(), &u)?;
        let (status, headers) = read_response_head(&mut reader)?;
        match status {
            200..=299 => return Ok(Box::new(BufReader::new(http_body(reader, &headers)))),
            300..=399 => {
                let loc = header_value(&headers, "location")
                    .ok_or_else(|| format!("{current}: {status} redirect without Location"))?;
                current = resolve_redirect(&u, loc);
                continue;
            }
            _ => {
                return Err(format!(
                    "GET {current}: HTTP {status} (only 2xx responses carry a body to read)"
                ))
            }
        }
    }
    Err(format!("GET {url}: too many redirects (> {MAX_REDIRECTS})"))
}

/// Send a minimal HTTP/1.1 GET request (`Connection: close` so the body is
/// close-framed when no length is given).
fn send_request(stream: &mut TcpStream, u: &Url) -> Result<(), String> {
    let host_hdr = if u.port == 80 {
        u.host.clone()
    } else {
        format!("{}:{}", u.host, u.port)
    };
    let req = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: rivus/{}\r\nAccept: */*\r\nConnection: close\r\n\r\n",
        u.path,
        host_hdr,
        env!("CARGO_PKG_VERSION"),
    );
    stream
        .write_all(req.as_bytes())
        .map_err(|e| format!("cannot send request to {}: {e}", u.host))?;
    Ok(())
}

/// Read and parse the status line + headers. Returns the status code and the
/// `(lowercased-name, value)` header pairs.
fn read_response_head<R: BufRead>(reader: &mut R) -> Result<(u16, Vec<(String, String)>), String> {
    let mut line = String::new();
    if reader.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
        return Err("empty HTTP response".to_string());
    }
    // Status line: `HTTP/1.1 200 OK`.
    let status: u16 = line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| format!("malformed HTTP status line: {:?}", line.trim_end()))?;
    let mut headers = Vec::new();
    loop {
        let mut h = String::new();
        if reader.read_line(&mut h).map_err(|e| e.to_string())? == 0 {
            break; // connection closed mid-headers
        }
        let t = h.trim_end_matches(['\r', '\n']);
        if t.is_empty() {
            break; // end of headers
        }
        if let Some((k, v)) = t.split_once(':') {
            headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
        }
    }
    Ok((status, headers))
}

/// Find a header value by (already-lowercased) name.
fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

/// Resolve a redirect `Location` against the current URL: absolute `http(s)://`
/// is taken as-is; an absolute path keeps the current host/port; anything else
/// is appended to the current path's directory (best-effort, std-only).
fn resolve_redirect(u: &Url, loc: &str) -> String {
    if loc.len() >= 7 && (loc[..7].eq_ignore_ascii_case("http://"))
        || loc.len() >= 8 && loc[..8].eq_ignore_ascii_case("https://")
    {
        return loc.to_string();
    }
    let host_hdr = if u.port == 80 {
        u.host.clone()
    } else {
        format!("{}:{}", u.host, u.port)
    };
    if let Some(abs) = loc.strip_prefix('/') {
        format!("http://{host_hdr}/{abs}")
    } else {
        let dir = u.path.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
        format!("http://{host_hdr}{dir}/{loc}")
    }
}

/// Body framing (RFC 7230): `chunked` wins over `Content-Length`; otherwise a
/// length, else read-until-close (our request set `Connection: close`).
fn http_body<R: BufRead + Send + 'static>(reader: R, headers: &[(String, String)]) -> HttpBody<R> {
    let chunked = header_value(headers, "transfer-encoding")
        .map(|v| v.to_ascii_lowercase().contains("chunked"))
        .unwrap_or(false);
    let mode = if chunked {
        BodyMode::Chunked {
            remaining: 0,
            done: false,
        }
    } else if let Some(len) = header_value(headers, "content-length").and_then(|v| v.parse().ok()) {
        BodyMode::Length(len)
    } else {
        BodyMode::Close
    };
    HttpBody {
        inner: reader,
        mode,
    }
}

enum BodyMode {
    Length(u64),
    Chunked { remaining: u64, done: bool },
    Close,
}

/// A `Read` over an HTTP response body that handles the three framings. Wrapped
/// in a `BufReader` by the caller so the codec gets `read_line`.
struct HttpBody<R> {
    inner: R,
    mode: BodyMode,
}

impl<R: BufRead> Read for HttpBody<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match &mut self.mode {
            BodyMode::Close => self.inner.read(buf),
            BodyMode::Length(remaining) => {
                if *remaining == 0 {
                    return Ok(0);
                }
                let cap = (*remaining).min(buf.len() as u64) as usize;
                let n = self.inner.read(&mut buf[..cap])?;
                *remaining -= n as u64;
                Ok(n)
            }
            BodyMode::Chunked { remaining, done } => {
                if *done {
                    return Ok(0);
                }
                if *remaining == 0 {
                    // Read the next chunk-size line (hex, optional `;ext`).
                    let mut line = String::new();
                    if self.inner.read_line(&mut line)? == 0 {
                        *done = true;
                        return Ok(0);
                    }
                    let hex = line.trim().split(';').next().unwrap_or("").trim();
                    let size = u64::from_str_radix(hex, 16).map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, "bad chunk size")
                    })?;
                    if size == 0 {
                        // Trailing headers until a blank line, then done.
                        loop {
                            let mut t = String::new();
                            if self.inner.read_line(&mut t)? == 0 {
                                break;
                            }
                            if t.trim_end_matches(['\r', '\n']).is_empty() {
                                break;
                            }
                        }
                        *done = true;
                        return Ok(0);
                    }
                    *remaining = size;
                }
                let cap = (*remaining).min(buf.len() as u64) as usize;
                let n = self.inner.read(&mut buf[..cap])?;
                *remaining -= n as u64;
                if *remaining == 0 {
                    // Consume the CRLF that terminates the chunk data.
                    let mut crlf = [0u8; 2];
                    let _ = self.inner.read_exact(&mut crlf);
                }
                Ok(n)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_http_urls() {
        let u = parse_http_url("http://127.0.0.1:8080/data.csv").unwrap();
        assert_eq!(u.host, "127.0.0.1");
        assert_eq!(u.port, 8080);
        assert_eq!(u.path, "/data.csv");
        let u = parse_http_url("http://localhost/x").unwrap();
        assert_eq!(u.port, 80);
        let u = parse_http_url("http://example.com").unwrap();
        assert_eq!(u.path, "/");
    }

    #[test]
    fn rejects_https() {
        assert!(parse_http_url("https://x/y").is_err());
    }

    #[test]
    fn parses_tcp_urls() {
        let u = parse_tcp_url("tcp://127.0.0.1:9000").unwrap();
        assert_eq!(u.host, "127.0.0.1");
        assert_eq!(u.port, 9000);
        assert!(parse_tcp_url("tcp://127.0.0.1").is_err()); // no port required
    }

    #[test]
    fn ipv6_authority() {
        let (h, p) = split_host_port("[::1]:80", 0).unwrap();
        assert_eq!(h, "::1");
        assert_eq!(p, 80);
        // bare IPv6, no port → host with default
        let (h, p) = split_host_port("::1", 80).unwrap();
        assert_eq!(h, "::1");
        assert_eq!(p, 80);
    }

    #[test]
    fn loopback_detection() {
        assert!(is_loopback("localhost"));
        assert!(is_loopback("127.0.0.1"));
        assert!(is_loopback("::1"));
        assert!(!is_loopback("example.com"));
        assert!(!is_loopback("8.8.8.8"));
    }

    #[test]
    fn capability_loopback_ok_remote_denied() {
        // Loopback is always allowed.
        assert!(check_capability("127.0.0.1", 80).is_ok());
        // A remote host with no allowlist is denied; the message names the
        // target but never an allowlist.
        let err = check_capability("example.com", 443).unwrap_err();
        assert!(err.contains("example.com:443"));
    }
}
