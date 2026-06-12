//! The unbounded `watch` source (§28.12, ratified #149) — feature `unbounded`.
//!
//! Subscribes to the OS change-notification mechanism (inotify / kqueue /
//! FSEvents / ReadDirectoryChangesW, via `notify`; #149 ② amended ruling — no
//! polling) on the glob's literal root directory and emits one handle row per
//! **created/modified** file matching the glob — the same bare-column shape as
//! `ls` (`path: Resource` / `name` / `size` / `mtime`), so `read` consumes it
//! unchanged. Removals/renames produce no handle (there is nothing to read).
//!
//! Contracts:
//! - **Boundedness (§0.14)**: arrival order is environmental — the stream is
//!   outside the deterministic-op set (the IR determinism tag keeps the
//!   optimizer and the parallel executor away); byte-identity is asserted only
//!   on bounded sub-DAGs. Termination comes from downstream saturation
//!   (`take N` filled — the engine stops the source) or process interrupt.
//! - **Backpressure (#149 ④)**: events cross a **bounded** queue
//!   (`RIVUS_WATCH_QUEUE`, default 1024) whose producer **blocks when full** —
//!   lossless; never drop/sample. Duplicates *within one drained batch* are
//!   coalesced (an editor save fires several notifications); a path repeating
//!   across batches is by design (each change is a new handle).
//! - **Capability (#149 ⑥)**: with `RIVUS_CAP_WATCH_PATHS` set (comma-separated
//!   path prefixes), a watch root outside the granted set is **rejected as an
//!   event** (Recoverable, continue-first — not fatal) and the event names only
//!   the rejected target: the allowlist is a *boundary*, not a secret, and is
//!   never echoed. Credentials never appear here at all (separate env lane).
//! - Both env knobs are **environment configuration, not data** — outside the
//!   determinism contract, like the route fd budget.

use super::*;
use notify::{EventKind, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, RecvTimeoutError, TryRecvError};
use std::time::Duration;

/// Default bound for the event queue (#149 ④ bounded-block).
const WATCH_QUEUE: usize = 1024;

/// One queued item: a matched uri, or the backend's error text (errors cross
/// the watcher thread as data so the engine surfaces them — never silent).
type Item = Result<String, String>;

pub(crate) struct SourceWatch {
    pattern: String,
    chunk_size: usize,
    started: bool,
    done: bool,
    rx: Option<Receiver<Item>>,
    /// Owns the OS subscription; dropping it unsubscribes.
    _watcher: Option<notify::RecommendedWatcher>,
    schema: Arc<Schema>,
}

impl SourceWatch {
    pub(crate) fn new(pattern: String, chunk_size: usize) -> Self {
        // The same bare-column shape as `ls` (`SourceDiscover`), so `read` and
        // the name/size/mtime predicates work identically on both.
        let schema = Arc::new(Schema::new(vec![
            Field::new("path".to_string(), DataType::Resource),
            Field::new("name".to_string(), DataType::Str),
            Field::new("size".to_string(), DataType::I64),
            Field::new(
                "mtime".to_string(),
                DataType::DateTime {
                    unit: TimeUnit::Sec,
                },
            ),
        ]));
        SourceWatch {
            pattern,
            chunk_size: chunk_size.max(1),
            started: false,
            done: false,
            rx: None,
            _watcher: None,
            schema,
        }
    }

    /// Subscribe: capability boundary, root canonicalization, bounded queue,
    /// OS watcher. Any failure surfaces a Recoverable and ends the stream
    /// (continue-first — the rest of the run proceeds).
    fn start(&mut self, ctx: &mut OpCtx) {
        let (root, rest) = crate::discovery::split_watch_root(&self.pattern);

        // Capability boundary (#149 ⑥): prefix allowlist from the environment.
        // The reject event names the target only — never the allowlist.
        if let Ok(allow) = std::env::var("RIVUS_CAP_WATCH_PATHS") {
            let root_canon = std::fs::canonicalize(&root).unwrap_or_else(|_| PathBuf::from(&root));
            let granted = allow.split(',').filter(|s| !s.is_empty()).any(|p| {
                let p_canon = std::fs::canonicalize(p).unwrap_or_else(|_| PathBuf::from(p));
                root_canon.starts_with(&p_canon)
            });
            if !granted {
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Recoverable,
                        ErrorScope::Graph,
                        format!(
                            "watch '{root}': outside the granted watch capability — rejected \
                             (grant the path via RIVUS_CAP_WATCH_PATHS); the rest of the run \
                             continues"
                        ),
                    )
                    .at_node(ctx.label.clone()),
                );
                self.done = true;
                return;
            }
        }

        let root_canon = match std::fs::canonicalize(&root) {
            Ok(c) => c,
            Err(e) => {
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Recoverable,
                        ErrorScope::Graph,
                        format!("watch '{root}': root directory is not watchable: {e}"),
                    )
                    .at_node(ctx.label.clone()),
                );
                self.done = true;
                return;
            }
        };

        // Bounded-block queue (#149 ④): the notify thread's `send` waits when
        // the engine is behind — lossless backpressure, memory bounded by the
        // queue cap (an ops knob, not data).
        let cap = std::env::var("RIVUS_WATCH_QUEUE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(WATCH_QUEUE);
        let (tx, rx) = std::sync::mpsc::sync_channel::<Item>(cap);

        let rest_h = rest.clone();
        let root_canon_h = root_canon.clone();
        let root_uri = root.clone();
        let handler = move |res: Result<notify::Event, notify::Error>| match res {
            Ok(ev) => {
                if !matches!(ev.kind, EventKind::Create(_) | EventKind::Modify(_)) {
                    return;
                }
                for p in &ev.paths {
                    let Ok(rel) = p.strip_prefix(&root_canon_h) else {
                        continue;
                    };
                    let rel = rel.to_string_lossy().replace('\\', "/");
                    if !crate::discovery::glob_segs_match(&rest_h, &rel) {
                        continue;
                    }
                    if !p.is_file() {
                        continue;
                    }
                    let uri = if root_uri == "." {
                        rel.clone()
                    } else {
                        format!("{}/{rel}", root_uri.trim_end_matches('/'))
                    };
                    // Blocking send = the ratified bounded-block backpressure.
                    // An Err means the engine side is gone — stop forwarding.
                    if tx.send(Ok(uri)).is_err() {
                        return;
                    }
                }
            }
            Err(e) => {
                let _ = tx.send(Err(e.to_string()));
            }
        };
        let mut watcher = match notify::recommended_watcher(handler) {
            Ok(w) => w,
            Err(e) => {
                ctx.raise(
                    ErrorEvent::new(
                        Severity::Recoverable,
                        ErrorScope::Graph,
                        format!("watch '{root}': cannot create the OS watcher: {e}"),
                    )
                    .at_node(ctx.label.clone()),
                );
                self.done = true;
                return;
            }
        };
        if let Err(e) = watcher.watch(&root_canon, RecursiveMode::Recursive) {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Recoverable,
                    ErrorScope::Graph,
                    format!("watch '{root}': cannot subscribe: {e}"),
                )
                .at_node(ctx.label.clone()),
            );
            self.done = true;
            return;
        }
        self.rx = Some(rx);
        self._watcher = Some(watcher);
    }

    /// Build a handle chunk for a batch of uris — the same rows `SourceDiscover`
    /// emits (stat at emission; a failed stat → 0, exactly like `ls`).
    fn handle_chunk(&self, uris: &[String], id: u64) -> Chunk {
        let n = uris.len();
        let mut path = StrColumn::with_capacity(n, 0);
        let mut name = StrColumn::with_capacity(n, 0);
        let mut size = Vec::with_capacity(n);
        let mut mtime = Vec::with_capacity(n);
        for u in uris {
            path.push(u);
            name.push(u.rsplit(['/', '\\']).next().unwrap_or(u));
            let meta = std::fs::metadata(u).ok();
            size.push(meta.as_ref().map(|m| m.len() as i64).unwrap_or(0));
            mtime.push(
                meta.and_then(|m| m.modified().ok())
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0),
            );
        }
        Chunk::new(
            id,
            self.schema.clone(),
            vec![
                Column::resource(path),
                Column::str(name),
                Column::i64(size),
                Column::datetime(DtColumn {
                    ticks: mtime,
                    unit: TimeUnit::Sec,
                }),
            ],
        )
    }
}

impl Operator for SourceWatch {
    fn is_source(&self) -> bool {
        true
    }

    fn pull(&mut self, ctx: &mut OpCtx) -> Option<Chunk> {
        if !self.started {
            self.started = true;
            self.start(ctx);
        }
        if self.done {
            return None;
        }
        let rx = self.rx.as_ref().expect("subscription is live");
        // Wait for the next event. The periodic wake keeps the wait
        // interruptible-by-design; real termination comes from the engine's
        // saturation check (it stops calling pull) or process interrupt.
        let first = loop {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(item) => break item,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    ctx.raise(
                        ErrorEvent::new(
                            Severity::Recoverable,
                            ErrorScope::Graph,
                            format!("watch '{}': the OS subscription ended", self.pattern),
                        )
                        .at_node(ctx.label.clone()),
                    );
                    self.done = true;
                    return None;
                }
            }
        };
        // Drain whatever else is queued, up to one chunk.
        let mut uris: Vec<String> = Vec::new();
        let mut backend_err: Option<String> = None;
        match first {
            Ok(u) => uris.push(u),
            Err(e) => backend_err = Some(e),
        }
        while backend_err.is_none() && uris.len() < self.chunk_size {
            match rx.try_recv() {
                Ok(Ok(u)) => uris.push(u),
                Ok(Err(e)) => backend_err = Some(e),
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }
        if let Some(e) = backend_err {
            ctx.raise(
                ErrorEvent::new(
                    Severity::Recoverable,
                    ErrorScope::Graph,
                    format!(
                        "watch '{}': backend error: {e} — the stream ends; the rest of the \
                         run continues",
                        self.pattern
                    ),
                )
                .at_node(ctx.label.clone()),
            );
            self.done = true;
            if uris.is_empty() {
                return None;
            }
        }
        // Coalesce duplicates within this batch only (see module doc).
        let mut seen = std::collections::HashSet::new();
        uris.retain(|u| seen.insert(u.clone()));
        let id = ctx.fresh_id();
        Some(self.handle_chunk(&uris, id))
    }

    fn process(&mut self, _from: NodeId, _chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        Vec::new()
    }
}
