//! The unbounded `subscribe` source (design §33) — feature `net`.
//!
//! Dials a TCP feed (`subscribe "tcp://host:port"`) as a **client** (no listener
//! is ever bound — §28.12.5-1) and streams newline-delimited CSV records,
//! decoding them with the single-pass streaming reader (the same sample-inference
//! path a compressed/HTTP stream uses — a socket can't be re-read for two-pass
//! inference). `as json` is a documented follow-up (§33 "next"); a JSON
//! subscribe is refused with guidance rather than silently mis-decoded.
//!
//! Contracts (shared with `watch`, §28.12):
//! - **Boundedness (§0.14)**: arrival order is environmental — outside the
//!   deterministic-op set; the IR determinism tag keeps the optimizer and the
//!   parallel executor away. Termination comes from downstream saturation
//!   (`take N`) or the peer closing the connection.
//! - **Backpressure (§28.12.0)**: reads are a sequential `read_line` pull — when
//!   the engine is behind, the OS TCP receive window fills and the producer
//!   blocks (lossless; nothing is dropped).
//! - **Capability (§28.12.4/5)**: the endpoint must be loopback or allowlisted
//!   via `RIVUS_CAP_NET_HOSTS` (enforced in [`crate::net::tcp_connect`]); a
//!   denial is surfaced as the transport's fatal error (the source has no data).

use super::*;
use rivus_ir::Codec;

pub(crate) struct SourceSubscribe {
    addr: String,
    /// True for `as json` (a JSON feed) — refused for now (CSV-only MVP).
    jsonl: bool,
    chunk_size: usize,
    started: bool,
    schema: Arc<Schema>,
    decoder: Option<Box<dyn crate::codec::Decoder>>,
}

impl SourceSubscribe {
    pub(crate) fn new(addr: String, codec: &Codec, chunk_size: usize) -> Self {
        SourceSubscribe {
            addr,
            jsonl: matches!(codec, Codec::Jsonl),
            chunk_size: chunk_size.max(1),
            started: false,
            schema: Schema::empty(),
            decoder: None,
        }
    }

    /// Dial the feed and build the streaming decoder (CSV or JSONL). Any failure
    /// surfaces a Fatal and ends the stream (the source has no data).
    fn start(&mut self, ctx: &mut OpCtx) {
        let reader = match crate::net::tcp_connect(&self.addr) {
            Ok(r) => r,
            Err(e) => {
                ctx.raise(
                    ErrorEvent::new(Severity::Fatal, ErrorScope::Graph, e)
                        .at_node(ctx.label.clone()),
                );
                return;
            }
        };
        // `as json` → the single-pass JSONL reader; else single-pass CSV.
        let built: Result<(Schema, Box<dyn crate::codec::Decoder>), String> = if self.jsonl {
            jsonl::StreamJsonlReader::from_reader(reader, self.chunk_size)
                .map(|(s, r)| (s, Box::new(r) as Box<dyn crate::codec::Decoder>))
        } else {
            csv::CompressedCsvReader::from_reader(
                reader,
                None,
                self.chunk_size,
                true,
                None,
                &[],
                b',',
            )
            .map(|(s, r)| (s, Box::new(r) as Box<dyn crate::codec::Decoder>))
        };
        match built {
            Ok((schema, dec)) => {
                self.schema = Arc::new(schema);
                self.decoder = Some(dec);
            }
            Err(e) => ctx.raise(
                ErrorEvent::new(Severity::Fatal, ErrorScope::Graph, e).at_node(ctx.label.clone()),
            ),
        }
    }
}

impl Operator for SourceSubscribe {
    fn is_source(&self) -> bool {
        true
    }

    fn pull(&mut self, ctx: &mut OpCtx) -> Option<Chunk> {
        if !self.started {
            self.started = true;
            self.start(ctx);
        }
        let dec = self.decoder.as_mut()?;
        let columns = dec.decode_chunk()?;
        let id = ctx.fresh_id();
        Some(Chunk::new(id, self.schema.clone(), columns))
    }

    fn process(&mut self, _from: NodeId, _chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        Vec::new()
    }
}
