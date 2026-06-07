//! Transport layer (design §28.4): a source path → a byte stream, with the
//! transport's **seekability** the single source of truth for whether a source
//! can be byte-range split for parallel reads.
//!
//! **§28.10 slice 1a — pure move-only extraction.** This module concentrates the
//! file-open / buffering / scheme-classification mechanics that the codecs
//! (`csv`, `jsonl`, the binary source) and the engine previously inlined and
//! duplicated. Behavior is unchanged: the same 256 KiB read buffer, the same
//! error strings, the same classification — so byte-identity and the
//! streaming / byte-range-parallel readers are preserved exactly.
//!
//! Two deliberate shapes, both forward-looking to later slices (§28.10 1b/5):
//! - `File` returns a **concrete** seekable `BufReader<File>`, *not* a
//!   `Box<dyn …>`. The per-line read loop runs tens of millions of times on a
//!   1 GB scan, so the byte source stays monomorphic (no dynamic dispatch on the
//!   hot path). Callers that need a byte range `seek` the returned reader
//!   themselves (a fresh `BufReader` has an empty buffer, so seeking it is
//!   equivalent to seeking the underlying `File` before wrapping).
//! - [`Scheme`] is the transport selection key. It does not yet carry the
//!   first-class `Resource` value (slice 1b) nor a pluggable `dyn Transport`
//!   (http/socket, slice 5); for now it classifies a path and answers the two
//!   questions the runtime asks today: *seekable?* and *compressed?*.

use std::fs::File;
use std::io::BufReader;

/// Streaming read buffer (256 KiB) — large enough to cut syscalls on the big
/// sequential scans (two-pass inference and build each stream the whole file).
/// Shared by the CSV, JSONL and fixed-width binary file readers, which each
/// previously defined this same size locally.
pub(crate) const READ_BUF: usize = 256 * 1024;

/// How a source path is carried by a transport: a seekable local [`File`], the
/// `Stdin` sentinel (`-`), or a `Compressed` file (`.gz` / `.zst` / `.zstd`).
///
/// This is the transport selection key, and — via [`Scheme::is_seekable`] — the
/// one place the runtime decides whether a source can be byte-range split for
/// parallel reads. The previously scattered, duplicated `path == "-"` and
/// compression-suffix checks all resolve here.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Scheme {
    File,
    Stdin,
    Compressed,
}

impl Scheme {
    /// Classify a source path: `-` is stdin; a `.gz` / `.zst` / `.zstd` suffix
    /// (case-insensitive) is compressed; everything else is a plain local file.
    pub(crate) fn of(path: &str) -> Scheme {
        if path == "-" {
            return Scheme::Stdin;
        }
        let lower = path.to_ascii_lowercase();
        if lower.ends_with(".gz") || lower.ends_with(".zst") || lower.ends_with(".zstd") {
            Scheme::Compressed
        } else {
            Scheme::File
        }
    }

    /// Can this source be seeked — i.e. byte-range split for parallel reads? Only
    /// a plain local file qualifies: stdin can't be re-read, and a compressed
    /// stream can't be seeked (its on-disk size is the *compressed* size). Both
    /// of those stay on the serial reader.
    pub(crate) fn is_seekable(self) -> bool {
        matches!(self, Scheme::File)
    }

    /// Is this a compressed source, needing the single-pass decompressing reader
    /// ([`open_compressed`], features `gzip` / `zstd`)?
    pub(crate) fn is_compressed(self) -> bool {
        matches!(self, Scheme::Compressed)
    }
}

/// The `File` transport: opens a real path as a buffered, **seekable** byte
/// stream. Seekability — a property of the concrete return type, which `impl`s
/// `Seek` — is what makes byte-range parallel reads possible; stdin and
/// compressed streams expose no such opener.
pub(crate) struct FileTransport;

impl FileTransport {
    /// Open `path` as a 256 KiB-buffered byte stream positioned at byte 0. The
    /// raw `io::Error` is returned so the caller can attach its own message,
    /// keeping the exact text the error stream surfaces. Callers that stream a
    /// byte range `seek` the returned reader to their range start.
    pub(crate) fn open(path: &str) -> std::io::Result<BufReader<File>> {
        Ok(BufReader::with_capacity(READ_BUF, File::open(path)?))
    }
}

/// Read a whole text source into a string — the non-streamable transport. The
/// `-` sentinel reads stdin (which can't be re-read for two-pass inference);
/// otherwise the whole file is read (used for a top-level JSON array, whose
/// elements may span lines and so can't be line-streamed). Moved here unchanged
/// from the operators module.
pub(crate) fn read_whole(path: &str) -> std::io::Result<String> {
    if path == "-" {
        use std::io::Read;
        let mut s = String::new();
        std::io::stdin().read_to_string(&mut s)?;
        Ok(s)
    } else {
        std::fs::read_to_string(path)
    }
}

/// Wrap `path`'s file in the right streaming decompressor for its extension:
/// `.gz` needs feature `gzip`, `.zst` / `.zstd` need feature `zstd`. An
/// unsupported (or feature-disabled) extension returns an actionable error. The
/// compressed stream can't be seeked, so this returns a boxed `BufRead` (the
/// serial reader's dynamic dispatch is acceptable; it never runs the
/// byte-range-parallel hot path). Moved here unchanged from `csv::open_decoder`.
#[cfg(any(feature = "gzip", feature = "zstd"))]
pub(crate) fn open_compressed(path: &str) -> Result<Box<dyn std::io::BufRead + Send>, String> {
    let f = File::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".gz") {
        #[cfg(feature = "gzip")]
        {
            return Ok(Box::new(BufReader::with_capacity(
                READ_BUF,
                flate2::read::MultiGzDecoder::new(f),
            )));
        }
        #[cfg(not(feature = "gzip"))]
        return Err(format!(
            "'{path}' is gzip-compressed; rebuild with `--features gzip`"
        ));
    }
    if lower.ends_with(".zst") || lower.ends_with(".zstd") {
        #[cfg(feature = "zstd")]
        {
            let dec = ruzstd::decoding::StreamingDecoder::new(f)
                .map_err(|e| format!("cannot read zstd '{path}': {e}"))?;
            return Ok(Box::new(BufReader::with_capacity(READ_BUF, dec)));
        }
        #[cfg(not(feature = "zstd"))]
        return Err(format!(
            "'{path}' is zstd-compressed; rebuild with `--features zstd`"
        ));
    }
    Err(format!("'{path}' has no supported compression extension"))
}
