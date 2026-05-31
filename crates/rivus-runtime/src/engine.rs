//! The single-threaded, chunk-granular DAG scheduler (MVP).
//!
//! Properties from the Scheduler Requirements (Observability spec §8):
//! - **chunk-aware**: one chunk moves through as far as it can each round.
//! - **branch-aware**: fan-out clones a chunk to every outgoing edge.
//! - **mode-aware**: the runtime mode is stamped on every emitted chunk and is
//!   escalated by `on error ... transition` hooks.
//! - **backpressure-aware** (degenerate here): single thread, bounded by the
//!   chunk size; the design doc 05 describes the real credit-based scheme.
//!
//! Continue-first: only `Severity::Fatal` errors stop the loop; everything else
//! accumulates on the error stream and the flow keeps running.

use crate::operators::{self, OpCtx, Operator};
use crate::telemetry::{NodeSnapshot, NodeTelemetry, RuntimeSnapshot, WorkerTelemetry};
use rivus_core::{Chunk, DataType, ErrorEvent, ErrorScope, Mode, RivusError, Severity};
use rivus_ir::{HookAction, HookEvent, NodeId, Op, PlanGraph};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct RunOptions {
    /// Maximum rows per chunk emitted by sources.
    pub chunk_size: usize,
    /// Show a live progress line on stderr during a (serial) run.
    pub progress: bool,
    /// Cap how many rows each un-sinked leaf captures for display. `None` keeps
    /// everything (library default). When set, a flow that has no sink and no
    /// blocking operator stops reading once the cap is met — so a bare
    /// `open big.csv` previews instantly in bounded memory instead of
    /// materializing the whole file. A file sink (`save`) always drains in full.
    pub max_capture: Option<usize>,
}

impl Default for RunOptions {
    fn default() -> Self {
        RunOptions {
            chunk_size: 4096,
            progress: false,
            max_capture: None,
        }
    }
}

/// One captured leaf output (a sink or an unconsumed scope tail).
#[derive(Debug, Clone)]
pub struct Output {
    pub node_id: NodeId,
    pub label: Option<String>,
    pub chunks: Vec<Chunk>,
}

#[derive(Debug, Clone)]
pub struct RunResult {
    pub telemetry: Vec<NodeTelemetry>,
    pub errors: Vec<ErrorEvent>,
    pub final_mode: Mode,
    pub outputs: Vec<Output>,
    /// Per-worker breakdown for a parallel run (empty on the serial path). Lets
    /// callers see parallel skew that the node-aggregate `telemetry` hides.
    pub workers: Vec<WorkerTelemetry>,
    /// Wall time from run start to the first chunk a source produced — the
    /// "time to first row". `None` if no row was ever produced.
    pub first_row_latency: Option<Duration>,
    /// Per-column type-inference outcomes `(name, type, widened)` from CSV
    /// sources that inferred a schema (A4 telemetry). Empty for declared/sample
    /// schemas and non-CSV sources.
    pub inference: Vec<(String, DataType, bool)>,
}

impl RunResult {
    pub fn total_rows_out(&self) -> u64 {
        self.outputs
            .iter()
            .map(|o| o.chunks.iter().map(|c| c.len as u64).sum::<u64>())
            .sum()
    }
}

/// A live-progress subscriber: called with a [`RuntimeSnapshot`] periodically as
/// a (serial) run streams. The base for live TUI / HTTP dashboards (Pillar B).
/// `None` everywhere is the default and costs nothing (no snapshot is built).
pub type ProgressHook<'a> = &'a mut dyn FnMut(&RuntimeSnapshot);

pub fn run(graph: &PlanGraph, opts: RunOptions) -> Result<RunResult, RivusError> {
    run_with_progress(graph, opts, None)
}

/// Like [`run`], but with an optional live-progress hook (Observability §14.4 /
/// Epic #30 A5). The hook fires on the serial path only — the parallel path runs
/// workers silently and reports per-worker telemetry in the final `RunResult`.
pub fn run_with_progress(
    graph: &PlanGraph,
    opts: RunOptions,
    hook: Option<ProgressHook>,
) -> Result<RunResult, RivusError> {
    if graph.topo_order().is_none() {
        return Err(RivusError::Build("flow graph contains a cycle".into()));
    }
    // Data-parallel fast path for stateless, single-source flows; else serial.
    // (A subscriber forces the serial path so it sees a coherent live stream.)
    if hook.is_none() {
        if let Some(res) = try_parallel(graph, &opts) {
            return Ok(res);
        }
    }
    // Sink-less, non-blocking flows with a capture cap are previews: let the
    // CSV source sample-infer its schema so it starts instantly.
    let preview = opts.max_capture.is_some() && !must_drain(graph);
    let ops = build_ops(graph, &opts, None, preview);
    Ok(drive(graph, ops, 0, opts.progress, opts.max_capture, hook))
}

/// Build a cheap point-in-time [`RuntimeSnapshot`] from the live node telemetry.
/// O(nodes); only called when a progress hook is attached.
fn build_snapshot(
    elapsed: Duration,
    rows_seen: u64,
    mode: Mode,
    telemetry: &[NodeTelemetry],
) -> RuntimeSnapshot {
    let nodes = telemetry
        .iter()
        .map(|t| NodeSnapshot {
            node_id: t.node_id,
            label: t.label.clone(),
            kind: t.kind.clone(),
            rows_in: t.rows_in,
            rows_out: t.rows_out,
            errors: t.errors,
            mode: t.mode,
            finished: t.finished,
        })
        .collect();
    RuntimeSnapshot {
        elapsed,
        rows_seen,
        mode,
        nodes,
    }
}

/// A flow that must read all input to be correct: a file sink (writes every
/// row) or a blocking/replay operator (needs the whole stream). Used to decide
/// whether a row cap may also stop the source early / sample-infer.
fn must_drain(graph: &PlanGraph) -> bool {
    graph.nodes.iter().any(|nd| {
        matches!(
            nd.op,
            Op::SinkCsv { .. }
                | Op::SinkJsonl { .. }
                | Op::SinkJson { .. }
                | Op::GroupBy { .. }
                | Op::Sort { .. }
                | Op::Distinct { .. }
                | Op::Describe
                | Op::Join { .. }
                | Op::StreamRef { .. }
                // bfill/mean/median buffer the whole stream and emit on finish
                // (they need a value from a later chunk, or a whole-column
                // statistic) → must drain.
                | Op::Fill {
                    method:
                        rivus_ir::FillMethod::Bfill
                        | rivus_ir::FillMethod::Mean
                        | rivus_ir::FillMethod::Median,
                    ..
                }
        )
    })
}

/// Build one operator per node. In parallel mode, `src_override` replaces the
/// single source (with a partition's `mem_source` or a byte-range streaming
/// source) and any file sink becomes a collector (rows gathered and written
/// once after the merge).
fn build_ops(
    graph: &PlanGraph,
    opts: &RunOptions,
    src_override: Option<(NodeId, Box<dyn Operator>)>,
    preview: bool,
) -> Vec<Box<dyn Operator>> {
    let (ov_id, mut ov_op) = match src_override {
        Some((id, op)) => (Some(id), Some(op)),
        None => (None, None),
    };
    graph
        .nodes
        .iter()
        .map(|node| {
            if Some(node.id) == ov_id {
                ov_op.take().expect("source override used once")
            } else if ov_id.is_some()
                && matches!(
                    node.op,
                    Op::SinkCsv { .. } | Op::SinkJsonl { .. } | Op::SinkJson { .. }
                )
            {
                operators::collector()
            } else {
                operators::build(
                    &node.op,
                    &graph.inputs_of(node.id),
                    opts.chunk_size,
                    preview,
                )
            }
        })
        .collect()
}

/// Drive the DAG to completion with a pre-built operator set (the chunk-granular
/// scheduler). `chunk_id_base` seeds chunk ids so parallel workers don't collide.
#[allow(clippy::too_many_arguments)]
fn drive(
    graph: &PlanGraph,
    mut ops: Vec<Box<dyn Operator>>,
    chunk_id_base: u64,
    progress: bool,
    max_capture: Option<usize>,
    mut hook: Option<ProgressHook>,
) -> RunResult {
    // (hook is mutated in place via as_mut() to publish snapshots)
    let n = graph.nodes.len();
    let topo = graph.topo_order().expect("acyclic (checked by caller)");
    // Publish a live snapshot every `SNAPSHOT_EVERY` source chunks when a hook
    // is attached (cheap, O(nodes), and only when subscribed).
    const SNAPSHOT_EVERY: u64 = 8;
    let mut chunks_pulled: u64 = 0;

    // Only a sink-less, non-blocking flow may stop the source early on a cap.
    let must_drain = must_drain(graph);

    let mut in_q: Vec<VecDeque<(NodeId, Chunk)>> = (0..n).map(|_| VecDeque::new()).collect();
    let mut done = vec![false; n];
    let mut upstream_remaining: Vec<usize> = (0..n).map(|i| graph.inputs_of(i).len()).collect();
    let mut results: HashMap<NodeId, Vec<Chunk>> = HashMap::new();
    let mut errors: Vec<ErrorEvent> = Vec::new();
    let mut telemetry: Vec<NodeTelemetry> = graph
        .nodes
        .iter()
        .map(|node| {
            NodeTelemetry::new(
                node.id,
                label_of(graph, node.id),
                node.op.kind_str().to_string(),
            )
        })
        .collect();
    let mut mode = Mode::Normal;
    let mut next_chunk_id: u64 = chunk_id_base;
    let mut fatal = false;

    let mut prog = Progress::new(progress);
    let mut rows_seen: u64 = 0;
    let mut total_captured: usize = 0;
    let mut truncated = false;
    // Time-to-first-row: wall from the start of the drive loop to the first
    // chunk any source produces. Pure accounting (does not affect results).
    let run_start = Instant::now();
    let mut first_row_latency: Option<Duration> = None;

    let mut active = true;
    while active && !fatal {
        active = false;
        for &nid in &topo {
            if done[nid] {
                continue;
            }
            let label = telemetry[nid].label.clone();
            let before = errors.len();
            let start = Instant::now();
            let mut produced: Vec<Chunk> = Vec::new();
            let mut finished_now = false;

            // Preview satisfied: a sink-less, non-blocking flow has captured
            // enough rows — stop reading the source instead of streaming the
            // rest of (potentially) a 15 GB file just to show a preview.
            let preview_satisfied =
                matches!(max_capture, Some(cap) if !must_drain && total_captured >= cap);

            if ops[nid].is_source() {
                if preview_satisfied {
                    finished_now = true;
                } else {
                    let mut ctx = OpCtx {
                        label,
                        errors: &mut errors,
                        next_chunk_id: &mut next_chunk_id,
                    };
                    match ops[nid].pull(&mut ctx) {
                        Some(chunk) => {
                            if first_row_latency.is_none() && chunk.len > 0 {
                                first_row_latency = Some(run_start.elapsed());
                            }
                            rows_seen += chunk.len as u64;
                            produced.push(chunk);
                            prog.tick(rows_seen);
                            // A5: publish a periodic live snapshot to a subscriber.
                            chunks_pulled += 1;
                            if let Some(h) = hook.as_mut() {
                                if chunks_pulled.is_multiple_of(SNAPSHOT_EVERY) {
                                    h(&build_snapshot(
                                        run_start.elapsed(),
                                        rows_seen,
                                        mode,
                                        &telemetry,
                                    ));
                                }
                            }
                        }
                        None => finished_now = true,
                    }
                }
            } else if let Some((from, chunk)) = in_q[nid].pop_front() {
                telemetry[nid].chunks_in += 1;
                telemetry[nid].rows_in += chunk.len as u64;
                let mut ctx = OpCtx {
                    label,
                    errors: &mut errors,
                    next_chunk_id: &mut next_chunk_id,
                };
                produced = ops[nid].process(from, chunk, &mut ctx);
            } else if upstream_remaining[nid] == 0 {
                let mut ctx = OpCtx {
                    label,
                    errors: &mut errors,
                    next_chunk_id: &mut next_chunk_id,
                };
                produced = ops[nid].finish(&mut ctx);
                finished_now = true;
            } else {
                // Waiting on upstream; no work this visit.
                continue;
            }

            telemetry[nid].busy += start.elapsed();

            let new_errors = errors.len() - before;
            if new_errors > 0 {
                telemetry[nid].errors += new_errors as u64;
                if errors[before..].iter().any(ErrorEvent::is_fatal) {
                    fatal = true;
                    mode = Mode::Halted;
                }
                // The error stream is graph-level: an `on error` hook declared
                // in any scope can respond to a new event (continue-first).
                apply_error_hooks(graph, &errors[before..], &mut mode);
                telemetry[nid].mode = mode;
            }

            if !produced.is_empty() {
                distribute(
                    graph,
                    nid,
                    produced,
                    mode,
                    &mut telemetry,
                    &mut in_q,
                    &mut results,
                    max_capture,
                    &mut total_captured,
                    &mut truncated,
                );
            }
            active = true;

            if finished_now {
                done[nid] = true;
                telemetry[nid].finished = true;
                for s in graph.outputs_of(nid) {
                    upstream_remaining[s] = upstream_remaining[s].saturating_sub(1);
                }
            }
        }
    }

    prog.finish(rows_seen);

    // A4: gather per-column inference outcomes from any source that inferred a
    // schema (telemetry; empty otherwise). Cheap, runs once at the end.
    let inference: Vec<(String, DataType, bool)> =
        ops.iter().flat_map(|op| op.inference()).collect();

    // A5: a final snapshot so a subscriber always observes the terminal state.
    if let Some(h) = hook.as_mut() {
        h(&build_snapshot(
            run_start.elapsed(),
            rows_seen,
            mode,
            &telemetry,
        ));
    }

    if truncated {
        if let Some(cap) = max_capture {
            errors.push(ErrorEvent::new(
                Severity::Info,
                ErrorScope::Graph,
                format!(
                    "output preview limited to {cap} row(s) — add a sink (e.g. `save out.csv`) to materialize all"
                ),
            ));
        }
    }

    let mut outputs: Vec<Output> = results
        .into_iter()
        .map(|(node_id, chunks)| Output {
            node_id,
            label: graph.nodes[node_id].label.clone(),
            chunks,
        })
        .collect();
    outputs.sort_by_key(|o| o.node_id);

    RunResult {
        telemetry,
        errors,
        final_mode: mode,
        outputs,
        workers: Vec::new(),
        first_row_latency,
        inference,
    }
}

/// A throttled live-progress line on stderr (≈5 Hz). No-op unless enabled, and
/// only used on the serial path (parallel workers stay silent). Writes to
/// stderr so a `save stdout` sink keeps stdout clean.
struct Progress {
    on: bool,
    start: Instant,
    last: Instant,
    drew: bool,
}

impl Progress {
    fn new(on: bool) -> Self {
        let now = Instant::now();
        Progress {
            on,
            start: now,
            last: now,
            drew: false,
        }
    }

    fn tick(&mut self, rows: u64) {
        if !self.on || self.last.elapsed() < Duration::from_millis(200) {
            return;
        }
        self.last = Instant::now();
        self.drew = true;
        let secs = self.start.elapsed().as_secs_f64();
        let rate = if secs > 0.0 { rows as f64 / secs } else { 0.0 };
        use std::io::Write;
        eprint!(
            "\r\x1b[2K  \u{2026} {} rows  {secs:.1}s  {} rows/s",
            group_thousands(rows),
            group_thousands(rate as u64)
        );
        let _ = std::io::stderr().flush();
    }

    fn finish(&mut self, rows: u64) {
        if !self.on {
            return;
        }
        use std::io::Write;
        let secs = self.start.elapsed().as_secs_f64();
        let rate = if secs > 0.0 { rows as f64 / secs } else { 0.0 };
        // Only emit the summary when we actually streamed enough to draw.
        if self.drew {
            eprintln!(
                "\r\x1b[2K  \u{2713} {} rows in {secs:.1}s  ({} rows/s)",
                group_thousands(rows),
                group_thousands(rate as u64)
            );
            let _ = std::io::stderr().flush();
        }
    }
}

/// Format an integer with thousands separators (e.g. 12_345_678 → "12,345,678").
fn group_thousands(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    let first = bytes.len() % 3;
    for (i, &b) in bytes.iter().enumerate() {
        if i >= first && i > 0 && (i - first).is_multiple_of(3) {
            out.push(',');
        }
        out.push(b as char);
    }
    out
}

/// Stream a large CSV in parallel: infer the global schema once (in parallel),
/// then have each worker stream one newline-aligned **byte range** through an
/// identical stateless sub-DAG and merge in source order — all in bounded
/// memory (no whole-file materialization). Returns `None` for non-CSV sources
/// (binary/jsonl large files fall back to the serial streaming reader).
fn try_streaming_parallel(
    graph: &PlanGraph,
    opts: &RunOptions,
    src_id: NodeId,
    path: &str,
    threads: usize,
) -> Option<RunResult> {
    let (projection, prefilter, header, declared, delim) = match &graph.nodes[src_id].op {
        Op::OpenCsv {
            projection,
            prefilter,
            header,
            declared,
            delim,
            ..
        } => (
            projection.clone(),
            prefilter.clone(),
            *header,
            declared.clone(),
            *delim,
        ),
        _ => return None, // only CSV has a streaming-parallel plan for now
    };

    // Each worker streams its byte range to a per-worker *part file* (bounded
    // memory — no output buffering), then the parts are concatenated in source
    // order into the final destination. A `-` sink assembles to stdout (so the
    // Unix-filter form is parallel too). Bail only when there is no file/stdout
    // sink (a preview/print flow has nothing to write in parallel without
    // buffering — that stays on the serial path).
    let mut sinks: Vec<(NodeId, String, bool, u8)> = Vec::new();
    for nd in &graph.nodes {
        match &nd.op {
            Op::SinkCsv { path, delim } => sinks.push((nd.id, path.clone(), false, *delim)),
            Op::SinkJsonl { path } => sinks.push((nd.id, path.clone(), true, b',')),
            _ => {}
        }
    }
    if sinks.is_empty() {
        return None;
    }

    let crate::csv::CsvParallelPlan {
        schema,
        dtypes,
        keep,
        ncols,
        ranges,
        bad_rows,
        prefilter: pre,
    } = crate::csv::plan_parallel(
        path,
        projection.as_deref(),
        threads,
        &prefilter,
        header,
        declared.as_deref(),
        delim,
    )
    .ok()?;
    let nparts = ranges.len();
    if nparts < 2 {
        return None; // not worth threading; let the caller's serial path run
    }
    let schema = std::sync::Arc::new(schema);

    let part_path = |final_path: &str, i: usize| format!("{final_path}.rivpart{i}");

    let results: Vec<RunResult> = std::thread::scope(|scope| {
        let sinks = &sinks;
        let schema = &schema;
        let dtypes = &dtypes;
        let keep = &keep;
        let pre = &pre;
        let handles: Vec<_> = ranges
            .iter()
            .enumerate()
            .map(|(i, &(a, b))| {
                scope.spawn(move || {
                    let mut src = Some(operators::csv_range_source(
                        path,
                        dtypes.clone(),
                        keep.clone(),
                        ncols,
                        schema.clone(),
                        a,
                        b,
                        opts.chunk_size,
                        pre.clone(),
                        delim,
                    ));
                    let ops: Vec<Box<dyn Operator>> = graph
                        .nodes
                        .iter()
                        .map(|node| {
                            if node.id == src_id {
                                src.take().expect("one source")
                            } else if let Some((_, fp, jsonl, sdelim)) =
                                sinks.iter().find(|(id, _, _, _)| *id == node.id)
                            {
                                let pp = part_path(fp, i);
                                if *jsonl {
                                    operators::jsonl_sink(pp)
                                } else {
                                    operators::csv_sink(pp, *sdelim)
                                }
                            } else {
                                operators::build(
                                    &node.op,
                                    &graph.inputs_of(node.id),
                                    opts.chunk_size,
                                    false,
                                )
                            }
                        })
                        .collect();
                    drive(graph, ops, (i as u64) << 40, false, None, None)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let mut src_errors = Vec::new();
    if bad_rows > 0 {
        src_errors.push(
            ErrorEvent::new(
                Severity::Recoverable,
                ErrorScope::Item,
                format!("{bad_rows} malformed row(s) skipped"),
            )
            .at_node(label_of(graph, src_id)),
        );
    }
    let mut res = merge_results(graph, results, src_errors, false);

    // Concatenate each sink's part files in source order into its final path.
    for (_, final_path, jsonl, _) in &sinks {
        let parts: Vec<String> = (0..nparts).map(|i| part_path(final_path, i)).collect();
        if let Err(e) = concat_parts(final_path, &parts, *jsonl) {
            res.errors.push(ErrorEvent::new(
                Severity::Critical,
                ErrorScope::Graph,
                format!("cannot assemble '{final_path}': {e}"),
            ));
        }
    }
    Some(res)
}

/// Concatenate worker part files into `final_path` in order. For CSV, keep the
/// header of the first non-empty part and drop the rest; JSONL has no header.
/// Streams part-by-part (bounded memory) and removes the parts when done.
fn concat_parts(final_path: &str, parts: &[String], jsonl: bool) -> std::io::Result<()> {
    use std::io::{BufRead, Write};
    // `-` writes the assembled stream to stdout (keeps the Unix-filter contract
    // working under the parallel path); any other path is a real output file.
    let sink: Box<dyn Write> = if final_path == "-" {
        Box::new(std::io::stdout())
    } else {
        Box::new(std::fs::File::create(final_path)?)
    };
    let mut out = std::io::BufWriter::new(sink);
    let mut header_done = jsonl;
    for part in parts {
        let f = match std::fs::File::open(part) {
            Ok(f) => f,
            Err(_) => continue,
        };
        let mut r = std::io::BufReader::new(f);
        if !jsonl {
            let mut first = Vec::new();
            if r.read_until(b'\n', &mut first)? == 0 {
                continue; // empty part (worker produced no rows)
            }
            if !header_done {
                out.write_all(&first)?;
                header_done = true;
            }
            // else: drop this part's duplicate header
        }
        std::io::copy(&mut r, &mut out)?;
    }
    out.flush()?;
    for p in parts {
        let _ = std::fs::remove_file(p);
    }
    Ok(())
}

/// The file path of a single-file source op (for the parallel size gate).
fn source_path(op: &Op) -> Option<&str> {
    match op {
        Op::OpenCsv { path, .. } | Op::OpenJsonl { path } | Op::OpenBinary { path, .. } => {
            Some(path)
        }
        _ => None,
    }
}

/// Is this source path compressed (`.gz`/`.zst`/`.zstd`)? Such sources are read
/// serially in a single pass (a compressed stream can't be seeked for
/// byte-range parallelism).
fn is_compressed_source(path: &str) -> bool {
    if path == "-" {
        return false;
    }
    let l = path.to_ascii_lowercase();
    l.ends_with(".gz") || l.ends_with(".zst") || l.ends_with(".zstd")
}

/// Rank for escalating runtime modes when merging parallel partitions.
fn mode_rank(m: Mode) -> u8 {
    match m {
        Mode::Normal => 0,
        Mode::Degraded => 1,
        Mode::Recovery => 2,
        Mode::Isolation => 3,
        Mode::Emergency => 4,
        Mode::Halted => 5,
    }
}

/// Split chunks into `n` contiguous groups (order-preserving on concatenation).
fn partition(all: Vec<Chunk>, n: usize) -> Vec<Vec<Chunk>> {
    let total = all.len();
    let base = total / n;
    let rem = total % n;
    let mut it = all.into_iter();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let take = base + if i < rem { 1 } else { 0 };
        let group: Vec<Chunk> = it.by_ref().take(take).collect();
        if !group.is_empty() {
            out.push(group);
        }
    }
    out
}

/// Minimum CSV file size (bytes) for the byte-range streaming-parallel reader.
/// Below it, the in-memory chunk-partition path handles the file (or the serial
/// reader, for tiny inputs). Override with `RIVUS_PARALLEL_MIN_BYTES` (e.g. `0`
/// to always stream-parallel a file source); default 8 MiB.
fn parallel_min_bytes() -> u64 {
    std::env::var("RIVUS_PARALLEL_MIN_BYTES")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(8 * 1024 * 1024)
}

/// Attempt data-parallel execution. Eligible flows have exactly one file source
/// and no stateful operators (group/join/stream). The source is parsed once
/// (its parse is already internally parallel), then contiguous chunk partitions
/// are run through identical stateless sub-DAGs on worker threads and merged in
/// source order. Returns `None` (→ serial) when ineligible or too small.
fn try_parallel(graph: &PlanGraph, opts: &RunOptions) -> Option<RunResult> {
    let threads = std::thread::available_parallelism()
        .map(|t| t.get())
        .unwrap_or(1);
    if threads < 2 {
        return None;
    }
    // Escape hatch: force the serial streaming path. A true single-thread
    // baseline for benchmarking, and a safety valve on constrained hosts.
    if std::env::var_os("RIVUS_NO_PARALLEL").is_some() {
        return None;
    }

    let mut source: Option<NodeId> = None;
    for node in &graph.nodes {
        match &node.op {
            Op::OpenCsv { .. } | Op::OpenBinary { .. } | Op::OpenJsonl { .. } => {
                if source.is_some() {
                    return None; // multiple sources → serial
                }
                source = Some(node.id);
            }
            // Stateful or replay → not partitionable; run serially. `Take`
            // keeps a global running count, `Sort` orders across all rows, and
            // `Distinct` carries a global seen-set — per-partition execution
            // would be wrong for all three. Force the serial path.
            Op::GroupBy { .. }
            | Op::Join { .. }
            | Op::StreamRef { .. }
            | Op::Take { .. }
            | Op::Sort { .. }
            | Op::Distinct { .. }
            | Op::Describe => return None,
            // Directional / statistical fill carries state across rows/chunks
            // (forward value, backward buffer, or a whole-column statistic) →
            // not partitionable. A constant fill is stateless and stays eligible.
            Op::Fill {
                method:
                    rivus_ir::FillMethod::Ffill
                    | rivus_ir::FillMethod::Bfill
                    | rivus_ir::FillMethod::Mean
                    | rivus_ir::FillMethod::Median,
                ..
            } => return None,
            _ => {}
        }
    }
    let src_id = source?;

    // A sink-less preview (CLI `rivus run open big.csv`) wants the instant,
    // bounded-memory serial path — never materialize for it.
    if opts.max_capture.is_some() && !must_drain(graph) {
        return None;
    }

    // A compressed source can't be seeked (so no byte-range parallel or
    // two-pass) and its on-disk size is the *compressed* size — force the
    // serial, single-pass streaming reader (bounded memory), which `run()`
    // falls through to.
    if source_path(&graph.nodes[src_id].op).is_some_and(is_compressed_source) {
        return None;
    }

    // The chunk-partition path materializes the whole input to split it. For a
    // large file, stream it in parallel instead (byte ranges, no buffering);
    // non-CSV large sources fall back to the serial streaming reader.
    let min_bytes = parallel_min_bytes();
    if let Some(path) = source_path(&graph.nodes[src_id].op) {
        if path != "-" {
            if let Ok(meta) = std::fs::metadata(path) {
                if meta.len() >= min_bytes {
                    return try_streaming_parallel(graph, opts, src_id, path, threads);
                }
            }
        }
    }

    // Parse the source once.
    let mut src_op = operators::build(&graph.nodes[src_id].op, &[], opts.chunk_size, false);
    let mut src_errors: Vec<ErrorEvent> = Vec::new();
    let mut next_id: u64 = 0;
    let mut all: Vec<Chunk> = Vec::new();
    {
        let mut ctx = OpCtx {
            label: label_of(graph, src_id),
            errors: &mut src_errors,
            next_chunk_id: &mut next_id,
        };
        while let Some(c) = src_op.pull(&mut ctx) {
            all.push(c);
        }
    }
    let src_fatal = src_errors.iter().any(ErrorEvent::is_fatal);

    // Too few chunks to be worth threads: run once over the already-parsed data.
    if all.len() < threads * 2 {
        let ops = build_ops(
            graph,
            opts,
            Some((src_id, operators::mem_source(all))),
            false,
        );
        let mut res = drive(graph, ops, 0, false, None, None);
        // Write any collected sink (build_ops made it a collector).
        flush_parallel_sinks(graph, &mut res);
        res.errors.splice(0..0, src_errors);
        if src_fatal {
            res.final_mode = Mode::Halted;
        }
        return Some(res);
    }

    // Run partitions on worker threads.
    let parts = partition(all, threads);
    let results: Vec<RunResult> = std::thread::scope(|scope| {
        let handles: Vec<_> = parts
            .into_iter()
            .enumerate()
            .map(|(i, chunks)| {
                scope.spawn(move || {
                    let src = operators::mem_source(chunks);
                    let ops = build_ops(graph, opts, Some((src_id, src)), false);
                    drive(graph, ops, (i as u64) << 40, false, None, None)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    Some(merge_results(graph, results, src_errors, src_fatal))
}

/// In the single-partition path, file sinks were built as collectors; write
/// their gathered rows once and drop them from `outputs` (a sink consumes).
fn flush_parallel_sinks(graph: &PlanGraph, res: &mut RunResult) {
    let mut kept = Vec::new();
    for out in std::mem::take(&mut res.outputs) {
        if let Some((path, result)) = write_sink(&graph.nodes[out.node_id].op, &out.chunks) {
            if let Err(e) = result {
                res.errors.push(
                    ErrorEvent::new(
                        Severity::Critical,
                        ErrorScope::Graph,
                        format!("cannot write '{path}': {e}"),
                    )
                    .at_node(label_of(graph, out.node_id)),
                );
            }
        } else {
            kept.push(out);
        }
    }
    res.outputs = kept;
}

/// If `op` is a file sink, write `chunks` to it once and return (path, result).
fn write_sink<'a>(op: &'a Op, chunks: &[Chunk]) -> Option<(&'a str, std::io::Result<()>)> {
    match op {
        Op::SinkCsv { path, delim } => {
            Some((path, operators::write_csv_file(path, chunks, *delim)))
        }
        Op::SinkJsonl { path } => Some((path, operators::write_jsonl_file(path, chunks))),
        Op::SinkJson { path } => Some((path, operators::write_json_file(path, chunks))),
        _ => None,
    }
}

/// Merge per-partition results in source order: concatenate outputs, sum
/// telemetry, union the error stream, escalate the mode, and write each file
/// sink exactly once.
fn merge_results(
    graph: &PlanGraph,
    results: Vec<RunResult>,
    src_errors: Vec<ErrorEvent>,
    src_fatal: bool,
) -> RunResult {
    let mut telemetry: Vec<NodeTelemetry> = graph
        .nodes
        .iter()
        .map(|node| {
            NodeTelemetry::new(
                node.id,
                label_of(graph, node.id),
                node.op.kind_str().to_string(),
            )
        })
        .collect();
    let mut errors: Vec<ErrorEvent> = src_errors;
    let mut mode = Mode::Normal;
    let mut by_node: BTreeMap<NodeId, Vec<Chunk>> = BTreeMap::new();
    // Per-worker breakdown (one entry per input RunResult, in source order).
    let mut workers: Vec<WorkerTelemetry> = Vec::with_capacity(results.len());
    // Earliest first-row across workers (they run concurrently).
    let mut first_row_latency: Option<Duration> = None;

    for (worker, res) in results.into_iter().enumerate() {
        if let Some(l) = res.first_row_latency {
            first_row_latency = Some(first_row_latency.map_or(l, |cur| cur.min(l)));
        }
        if mode_rank(res.final_mode) > mode_rank(mode) {
            mode = res.final_mode;
        }
        // This worker's contribution: the rows that reached its sink/leaf nodes
        // (a sink's `rows_in`), plus any rows captured as un-sinked output.
        // Captured before the per-node sums fold it away. In the streaming-
        // parallel path the sink is a collector writing a part file, so its
        // `rows_in` is the right "rows this worker produced".
        let mut w_rows: u64 = 0;
        for (i, t) in res.telemetry.iter().enumerate() {
            if matches!(
                graph.nodes[i].op,
                Op::SinkCsv { .. } | Op::SinkJsonl { .. } | Op::SinkJson { .. } | Op::SinkPrint
            ) {
                w_rows += t.rows_in;
            }
        }
        for o in &res.outputs {
            w_rows += o.chunks.iter().map(|c| c.len as u64).sum::<u64>();
        }
        let w_busy = res.telemetry.iter().map(|t| t.busy).sum();
        workers.push(WorkerTelemetry {
            worker,
            rows_out: w_rows,
            busy: w_busy,
        });
        errors.extend(res.errors);
        for (i, t) in res.telemetry.into_iter().enumerate() {
            telemetry[i].chunks_in += t.chunks_in;
            telemetry[i].chunks_out += t.chunks_out;
            telemetry[i].rows_in += t.rows_in;
            telemetry[i].rows_out += t.rows_out;
            telemetry[i].errors += t.errors;
            telemetry[i].busy += t.busy;
            telemetry[i].finished |= t.finished;
            if t.mode != Mode::Normal {
                telemetry[i].mode = t.mode;
            }
        }
        for o in res.outputs {
            by_node.entry(o.node_id).or_default().extend(o.chunks);
        }
    }
    if src_fatal {
        mode = Mode::Halted;
    }

    let mut outputs = Vec::new();
    for (node_id, chunks) in by_node {
        if let Some((path, result)) = write_sink(&graph.nodes[node_id].op, &chunks) {
            if let Err(e) = result {
                errors.push(
                    ErrorEvent::new(
                        Severity::Critical,
                        ErrorScope::Graph,
                        format!("cannot write '{path}': {e}"),
                    )
                    .at_node(label_of(graph, node_id)),
                );
            }
        } else {
            outputs.push(Output {
                node_id,
                label: graph.nodes[node_id].label.clone(),
                chunks,
            });
        }
    }

    RunResult {
        telemetry,
        errors,
        final_mode: mode,
        outputs,
        workers,
        first_row_latency,
        // Per-worker byte-range readers infer globally; the merged view doesn't
        // surface their inference (empty — telemetry, not a contract).
        inference: Vec::new(),
    }
}

/// Push a node's produced chunks to its successors (fan-out) or capture them as
/// a leaf output. Stamps the current runtime mode on every chunk.
#[allow(clippy::too_many_arguments)]
fn distribute(
    graph: &PlanGraph,
    nid: NodeId,
    chunks: Vec<Chunk>,
    mode: Mode,
    telemetry: &mut [NodeTelemetry],
    in_q: &mut [VecDeque<(NodeId, Chunk)>],
    results: &mut HashMap<NodeId, Vec<Chunk>>,
    max_capture: Option<usize>,
    total_captured: &mut usize,
    truncated: &mut bool,
) {
    let succ = graph.outputs_of(nid);
    for mut chunk in chunks {
        chunk.meta.mode = mode;
        telemetry[nid].chunks_out += 1;
        telemetry[nid].rows_out += chunk.len as u64;
        if succ.is_empty() {
            // Leaf capture for display. With a cap, keep at most `cap` rows per
            // leaf (bounded memory); telemetry still counts the true total above.
            if let Some(cap) = max_capture {
                let have: usize = results
                    .get(&nid)
                    .map(|v| v.iter().map(|c| c.len).sum())
                    .unwrap_or(0);
                if have >= cap {
                    *truncated = true;
                    continue;
                }
                let room = cap - have;
                if chunk.len > room {
                    *truncated = true;
                    let idx: Vec<usize> = (0..room).collect();
                    *total_captured += room;
                    results.entry(nid).or_default().push(chunk.gather(&idx));
                    continue;
                }
            }
            *total_captured += chunk.len;
            results.entry(nid).or_default().push(chunk);
        } else {
            for (k, &s) in succ.iter().enumerate() {
                if k + 1 == succ.len() {
                    in_q[s].push_back((nid, chunk));
                    break;
                }
                in_q[s].push_back((nid, chunk.clone()));
            }
        }
    }
}

fn apply_error_hooks(graph: &PlanGraph, new_errors: &[ErrorEvent], mode: &mut Mode) {
    for node in &graph.nodes {
        for hook in &node.hooks {
            if hook.event != HookEvent::Error {
                continue;
            }
            let triggered = new_errors.iter().any(|e| match hook.min_severity {
                Some(min) => e.severity >= min,
                None => true,
            });
            if triggered {
                if let HookAction::Transition(m) = &hook.action {
                    *mode = *m;
                }
            }
        }
    }
}

fn label_of(graph: &PlanGraph, id: NodeId) -> String {
    graph.nodes[id]
        .label
        .clone()
        .unwrap_or_else(|| format!("{}#{id}", graph.nodes[id].op.kind_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gendata;

    #[test]
    fn thousands_separator() {
        assert_eq!(group_thousands(0), "0");
        assert_eq!(group_thousands(42), "42");
        assert_eq!(group_thousands(999), "999");
        assert_eq!(group_thousands(1_000), "1,000");
        assert_eq!(group_thousands(12_345), "12,345");
        assert_eq!(group_thousands(123_456), "123,456");
        assert_eq!(group_thousands(48_000_000), "48,000,000");
    }

    /// Streaming-parallel (byte-range workers writing ordered part files) must
    /// produce a **byte-identical** output file to the serial streaming reader.
    /// Forced on a small file (4 ranges) so it runs in CI, and the assertion
    /// also guards the header-dedup + source-order concatenation.
    #[test]
    fn streaming_parallel_matches_serial() {
        let rows = 30_000;
        let data = gendata::clean(rows, 7);
        let path = gendata::write_temp("stream_par", &data);
        let psafe = path.to_string_lossy().to_string();
        let out_serial = format!("{psafe}.serial.out");
        let out_par = format!("{psafe}.par.out");
        let opts = RunOptions::default();

        // Serial reference (bypass try_parallel): the real sink writes the file.
        let gs = rivus_parser::parse(&format!(
            "S:\n open {psafe}\n |? age >= 45\n |> name age\n save {out_serial}\n;"
        ))
        .unwrap();
        let ops = build_ops(&gs, &opts, None, false);
        let _ = drive(&gs, ops, 0, false, None, None);

        // Forced streaming-parallel over 4 byte ranges → ordered part-file concat.
        let gp = rivus_parser::parse(&format!(
            "S:\n open {psafe}\n |? age >= 45\n |> name age\n save {out_par}\n;"
        ))
        .unwrap();
        let src_id = gp
            .nodes
            .iter()
            .position(|nd| matches!(nd.op, Op::OpenCsv { .. }))
            .unwrap();
        try_streaming_parallel(&gp, &opts, src_id, &psafe, 4)
            .expect("streaming-parallel should engage");

        let a = std::fs::read_to_string(&out_serial).unwrap();
        let b = std::fs::read_to_string(&out_par).unwrap();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&out_serial);
        let _ = std::fs::remove_file(&out_par);

        assert!(a.lines().count() > 1, "expected real output");
        assert_eq!(
            a, b,
            "streaming-parallel output must equal serial, byte-for-byte"
        );
    }
}
