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
use rivus_ir::{Codec, HookAction, HookEvent, NodeId, Op, PlanGraph};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
    /// Execution-strategy preference (Pillar C). `Auto` (default) autotunes
    /// serial-vs-parallel from CPU count + input size; `Low` forces the
    /// single-thread bounded reader; `Fast` prefers the byte-range parallel
    /// reader more aggressively. All produce byte-identical results.
    pub memory: crate::analytics::MemoryPref,
}

impl Default for RunOptions {
    fn default() -> Self {
        RunOptions {
            chunk_size: 4096,
            progress: false,
            max_capture: None,
            memory: crate::analytics::MemoryPref::Auto,
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
    /// The autotuner's chosen-strategy rationale (Pillar C): serial-vs-parallel
    /// and why. `None` when there is no file source to decide on.
    pub strategy: Option<String>,
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
    hook: Option<&mut (dyn FnMut(&RuntimeSnapshot) + '_)>,
) -> Result<RunResult, RivusError> {
    let mut res = run_dispatch(graph, opts, hook)?;
    // Never-silent: a `$x` value hole that reaches execution with no binding
    // would evaluate to null in silence (e.g. running a template scope on its
    // own without filling its holes). Surface each unbound hole once as a
    // recoverable event so the loss is visible (§25.3, continue-first).
    for name in graph.unbound_holes() {
        res.errors.push(ErrorEvent::new(
            Severity::Recoverable,
            ErrorScope::Graph,
            format!(
                "value hole ${name} is unbound (no binding supplied) — it evaluates to null; \
                 bind it at the call site (e.g. `| flow {name}=…`)"
            ),
        ));
    }
    Ok(res)
}

fn run_dispatch(
    graph: &PlanGraph,
    opts: RunOptions,
    mut hook: Option<&mut (dyn FnMut(&RuntimeSnapshot) + '_)>,
) -> Result<RunResult, RivusError> {
    if graph.topo_order().is_none() {
        return Err(RivusError::Build("flow graph contains a cycle".into()));
    }
    // Pillar C autotuner: decide serial-vs-parallel from the host probe + input
    // size, and surface the rationale (Observability §13). The decision is
    // *measured* — the byte-range parallel reader is both faster and
    // bounded-memory (see `analytics`), so `Auto`/`Fast` attempt it and `Low`
    // forces single-thread. A live hook always forces serial (coherent stream).
    let env = crate::analytics::Analytics::probe();
    let src_size = single_file_source_size(graph);
    let min_parallel = parallel_min_bytes_for(opts.memory);
    let (strat, note) =
        crate::analytics::choose_strategy(opts.memory, &env, src_size, min_parallel);
    // Only a real file source has a strategy worth reporting.
    let has_file_source = single_file_source(graph).is_some();
    // Sink-less, non-blocking flows with a capture cap are previews: let the
    // CSV source sample-infer its schema so it starts instantly (and never
    // materialize for the parallel path).
    let preview = opts.max_capture.is_some() && !must_drain(graph);

    // Observable First: a live hook (TUI / `--serve`) must NOT force the serial
    // path — observing the run must not throttle it. The parallel paths feed the
    // hook an aggregate cross-worker snapshot instead (see `ParProgress`), so the
    // view is coarser but the *processing* stays fully parallel.
    if strat == crate::analytics::Strategy::Parallel {
        // Parallel group-by (#41): a linear `source → … → group` flow whose
        // aggregates are byte-identical under partition→merge. Tried before the
        // stateless path (which bails on a group → serial).
        if let Some((src, grp, sink)) = eligible_group_flow(graph) {
            // Bind first so the `hook` reborrow is released at the `;` (not held
            // across the following attempts / the serial-fallback move).
            let attempt = try_parallel_group(graph, &opts, src, grp, sink, hook.as_deref_mut());
            if let Some(mut res) = attempt {
                res.strategy = has_file_source.then(|| format!("{note}; parallel group-by"));
                return Ok(res);
            }
        }
        // Opt-in unbounded (#50): parallelize a non-splittable source's group-by
        // by materializing it. Only for `MemoryPref::Unbounded` (the user's
        // explicit choice to trade bounded memory for speed).
        if let Some((src, grp, sink)) = eligible_group_flow_any(graph) {
            let attempt = try_unbounded_group(graph, &opts, src, grp, sink, hook.as_deref_mut());
            if let Some(mut res) = attempt {
                res.strategy =
                    has_file_source.then(|| format!("{note}; unbounded group-by (materialized)"));
                return Ok(res);
            }
        }
        let attempt = try_parallel(graph, &opts, min_parallel, hook.as_deref_mut());
        if let Some(mut res) = attempt {
            res.strategy = has_file_source.then(|| note.clone());
            return Ok(res);
        }
        // Parallel was chosen but didn't run: a preview (latency-first, stays
        // serial) or a non-partitionable flow (stateful op, multiple sources).
        let ops = build_ops(graph, &opts, None, preview);
        let mut res = drive(graph, ops, 0, opts.progress, opts.max_capture, hook);
        let why = if preview {
            "preview → serial"
        } else {
            "not partitionable → serial"
        };
        res.strategy = has_file_source.then(|| format!("{note}; {why}"));
        return Ok(res);
    }
    // The autotuner chose serial (small input or `--memory low`); the hook
    // streams the run as usual.
    let ops = build_ops(graph, &opts, None, preview);
    let mut res = drive(graph, ops, 0, opts.progress, opts.max_capture, hook);
    res.strategy = has_file_source.then(|| note.clone());
    Ok(res)
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
    mut hook: Option<&mut (dyn FnMut(&RuntimeSnapshot) + '_)>,
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
        // The autotuner's rationale is set by `run_with_progress` (it owns the
        // serial-vs-parallel decision); drive itself doesn't decide.
        strategy: None,
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

/// Stream a large **splittable** source (CSV / JSONL) in parallel: infer the
/// global schema once, then have each worker stream one newline-aligned **byte
/// range** through an identical stateless sub-DAG to a per-worker part file, all
/// in bounded memory (no whole-file materialization), and concatenate the parts
/// in source order. Returns `None` for a non-splittable source, no sink, or too
/// small (the caller's serial path runs instead).
fn try_streaming_parallel(
    graph: &PlanGraph,
    opts: &RunOptions,
    src_id: NodeId,
    threads: usize,
    mut hook: Option<&mut (dyn FnMut(&RuntimeSnapshot) + '_)>,
) -> Option<RunResult> {
    let plan = plan_parallel_source(&graph.nodes[src_id].op, threads)?;

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

    let nparts = plan.ranges.len();
    let part_path = |final_path: &str, i: usize| format!("{final_path}.rivpart{i}");

    let prog = ParProgress::new(nparts, graph.nodes.len(), src_id);
    let results: Vec<RunResult> = std::thread::scope(|scope| {
        let sinks = &sinks;
        let plan = &plan;
        let prog = &prog;
        let handles: Vec<_> = plan
            .ranges
            .iter()
            .enumerate()
            .map(|(i, &(a, b))| {
                scope.spawn(move || {
                    let mut src = Some(plan.make_source(a, b, opts.chunk_size));
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
                    let mut wh = prog.worker_hook(i);
                    let r = drive(graph, ops, (i as u64) << 40, false, None, Some(&mut wh));
                    prog.finish_worker(i, &r.telemetry);
                    r
                })
            })
            .collect();
        if let Some(h) = hook.as_mut() {
            prog.sample_until_done(h, graph);
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let mut src_errors = Vec::new();
    if plan.bad_rows > 0 {
        src_errors.push(
            ErrorEvent::new(
                Severity::Recoverable,
                ErrorScope::Item,
                format!("{} malformed row(s) skipped", plan.bad_rows),
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
    if let Some(h) = hook.as_mut() {
        h(&prog.snapshot(graph, res.final_mode));
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
        Op::Source { discovery, .. } => Some(discovery.path()),
        _ => None,
    }
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

/// The byte-range threshold the autotuner uses for a given preference. `Low`
/// never parallelizes (`u64::MAX`); `Auto` uses [`parallel_min_bytes`]; `Fast`
/// is more aggressive (1 MiB, still env-overridable to a smaller floor).
fn parallel_min_bytes_for(pref: crate::analytics::MemoryPref) -> u64 {
    use crate::analytics::MemoryPref::*;
    match pref {
        Low => u64::MAX,
        Auto => parallel_min_bytes(),
        Fast => parallel_min_bytes().min(1024 * 1024),
        // Opt-in unbounded: parallelize at any size (#50). The unbounded behavior
        // itself (materializing non-splittable sources) is gated separately so it
        // only fires for this tier.
        Unbounded => 0,
    }
}

/// The single file-source node id, if the flow has exactly one and it's a real
/// file (not stdin `-`). Used to decide whether a strategy is worth reporting.
fn single_file_source(graph: &PlanGraph) -> Option<NodeId> {
    let mut found: Option<NodeId> = None;
    for node in &graph.nodes {
        if matches!(node.op, Op::Source { .. }) {
            if found.is_some() {
                return None; // multiple sources
            }
            found = Some(node.id);
        }
    }
    let id = found?;
    match source_path(&graph.nodes[id].op) {
        Some(p) if p != "-" => Some(id),
        _ => None,
    }
}

/// On-disk size of the single file source, for the autotuner's size threshold.
fn single_file_source_size(graph: &PlanGraph) -> Option<u64> {
    let id = single_file_source(graph)?;
    let path = source_path(&graph.nodes[id].op)?;
    std::fs::metadata(path).ok().map(|m| m.len())
}

/// Attempt data-parallel execution. Eligible flows have exactly one file source
/// and no stateful operators (group/join/stream). The source is parsed once
/// (its parse is already internally parallel), then contiguous chunk partitions
/// are run through identical stateless sub-DAGs on worker threads and merged in
/// source order. Returns `None` (→ serial) when ineligible or too small.
/// A terminal snapshot built from a finished run's per-node telemetry, for the
/// parallel group-by paths (whose workers accumulate a partial group operator
/// rather than streaming per-node `drive` ticks). The run was fully parallel;
/// this just lets the live view land on the completed DAG.
fn final_snapshot(res: &RunResult, src_id: NodeId, elapsed: Duration) -> RuntimeSnapshot {
    let rows_seen = res
        .telemetry
        .iter()
        .find(|t| t.node_id == src_id)
        .map_or(0, |t| t.rows_out);
    build_snapshot(elapsed, rows_seen, res.final_mode, &res.telemetry)
}

/// How often the coordinator samples cross-worker progress for the live hook.
const PAR_SAMPLE: Duration = Duration::from_millis(100);

/// Cross-worker live progress for a parallel run. Observing a run (TUI /
/// `--serve`) must **not** force it onto the serial path — that would let the
/// *view* change the *computation* (the opposite of Observable First). Instead,
/// each worker mirrors its partition's latest per-node row counts into per-worker
/// atomic slots (via a lightweight `drive` hook), and the coordinator thread
/// periodically sums them into one aggregate snapshot for the real hook. The
/// processing stays fully parallel; only the view is coarser (node aggregate
/// across workers, not the per-worker breakdown a serial run can't show either).
struct ParProgress {
    start: Instant,
    nodes: usize,
    workers: usize,
    /// Source node id — its summed `rows_out` is the run's `rows_seen`.
    src_id: NodeId,
    /// `[worker * nodes + node_id]` latest counts, published by workers.
    rows_out: Vec<AtomicU64>,
    rows_in: Vec<AtomicU64>,
    errors: Vec<AtomicU64>,
    /// Workers that have finished (the run is done when this reaches `workers`).
    done: AtomicUsize,
}

impl ParProgress {
    fn new(workers: usize, nodes: usize, src_id: NodeId) -> Self {
        let cells = workers.max(1) * nodes.max(1);
        ParProgress {
            start: Instant::now(),
            nodes,
            workers,
            src_id,
            rows_out: (0..cells).map(|_| AtomicU64::new(0)).collect(),
            rows_in: (0..cells).map(|_| AtomicU64::new(0)).collect(),
            errors: (0..cells).map(|_| AtomicU64::new(0)).collect(),
            done: AtomicUsize::new(0),
        }
    }

    /// A `drive` hook for worker `w`: mirror its current per-node telemetry into
    /// this worker's slots (store, not add — idempotent across ticks).
    fn worker_hook(&self, w: usize) -> impl FnMut(&RuntimeSnapshot) + '_ {
        move |s: &RuntimeSnapshot| {
            for n in &s.nodes {
                if n.node_id < self.nodes {
                    let k = w * self.nodes + n.node_id;
                    self.rows_out[k].store(n.rows_out, Ordering::Relaxed);
                    self.rows_in[k].store(n.rows_in, Ordering::Relaxed);
                    self.errors[k].store(n.errors, Ordering::Relaxed);
                }
            }
        }
    }

    /// Store a worker's *final* per-node telemetry (the last chunks may not have
    /// landed on a hook tick), then mark it done.
    fn finish_worker(&self, w: usize, telemetry: &[NodeTelemetry]) {
        for t in telemetry {
            if t.node_id < self.nodes {
                let k = w * self.nodes + t.node_id;
                self.rows_out[k].store(t.rows_out, Ordering::Relaxed);
                self.rows_in[k].store(t.rows_in, Ordering::Relaxed);
                self.errors[k].store(t.errors, Ordering::Relaxed);
            }
        }
        self.done.fetch_add(1, Ordering::Relaxed);
    }

    fn all_done(&self) -> bool {
        self.done.load(Ordering::Relaxed) >= self.workers
    }

    /// One aggregate snapshot: each node summed across workers.
    fn snapshot(&self, graph: &PlanGraph, mode: Mode) -> RuntimeSnapshot {
        let finished = self.all_done();
        let sum = |id: NodeId, col: &[AtomicU64]| -> u64 {
            (0..self.workers)
                .map(|w| col[w * self.nodes + id].load(Ordering::Relaxed))
                .sum()
        };
        let nodes = graph
            .nodes
            .iter()
            .map(|node| NodeSnapshot {
                node_id: node.id,
                label: label_of(graph, node.id),
                kind: node.op.kind_str().to_string(),
                rows_in: sum(node.id, &self.rows_in),
                rows_out: sum(node.id, &self.rows_out),
                errors: sum(node.id, &self.errors),
                mode,
                finished,
            })
            .collect();
        RuntimeSnapshot {
            elapsed: self.start.elapsed(),
            rows_seen: sum(self.src_id, &self.rows_out),
            mode,
            nodes,
        }
    }

    /// Coordinator loop: push an aggregate snapshot to `hook` every `PAR_SAMPLE`
    /// until every worker has finished. Runs on the coordinator thread (the hook
    /// is not `Send`), concurrently with the workers.
    fn sample_until_done(&self, hook: &mut dyn FnMut(&RuntimeSnapshot), graph: &PlanGraph) {
        while !self.all_done() {
            std::thread::sleep(PAR_SAMPLE);
            hook(&self.snapshot(graph, Mode::Normal));
        }
    }
}

fn try_parallel(
    graph: &PlanGraph,
    opts: &RunOptions,
    min_bytes: u64,
    mut hook: Option<&mut (dyn FnMut(&RuntimeSnapshot) + '_)>,
) -> Option<RunResult> {
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
            Op::Source { .. } => {
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
    if source_path(&graph.nodes[src_id].op)
        .is_some_and(|p| crate::transport::Scheme::of(p).is_compressed())
    {
        return None;
    }

    // The chunk-partition path materializes the whole input to split it. For a
    // large file, stream it in parallel instead (byte ranges, no buffering);
    // non-CSV large sources fall back to the serial streaming reader. The
    // threshold is the autotuner's (`min_bytes`), so the decision and the reader
    // agree exactly.
    if let Some(path) = source_path(&graph.nodes[src_id].op) {
        // Byte-range streaming needs a *seekable* source. A compressed source was
        // already routed serial above, so the only non-seekable case left here is
        // stdin (`-`) — making this equivalent to the prior `path != "-"`.
        if crate::transport::Scheme::of(path).is_seekable() {
            if let Ok(meta) = std::fs::metadata(path) {
                if meta.len() >= min_bytes {
                    return try_streaming_parallel(graph, opts, src_id, threads, hook);
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
        let mut res = drive(graph, ops, 0, false, None, hook);
        // Write any collected sink (build_ops made it a collector).
        flush_parallel_sinks(graph, &mut res);
        res.errors.splice(0..0, src_errors);
        if src_fatal {
            res.final_mode = Mode::Halted;
        }
        return Some(res);
    }

    // Run partitions on worker threads. A live hook observes aggregate progress
    // without forcing serial (Observable First) via the shared `ParProgress`.
    let parts = partition(all, threads);
    let nworkers = parts.len();
    let prog = ParProgress::new(nworkers, graph.nodes.len(), src_id);
    let results: Vec<RunResult> = std::thread::scope(|scope| {
        let prog = &prog;
        let handles: Vec<_> = parts
            .into_iter()
            .enumerate()
            .map(|(i, chunks)| {
                scope.spawn(move || {
                    let src = operators::mem_source(chunks);
                    let ops = build_ops(graph, opts, Some((src_id, src)), false);
                    let mut wh = prog.worker_hook(i);
                    let r = drive(graph, ops, (i as u64) << 40, false, None, Some(&mut wh));
                    prog.finish_worker(i, &r.telemetry);
                    r
                })
            })
            .collect();
        if let Some(h) = hook.as_mut() {
            prog.sample_until_done(h, graph);
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let res = merge_results(graph, results, src_errors, src_fatal);
    if let Some(h) = hook.as_mut() {
        h(&prog.snapshot(graph, res.final_mode));
    }
    Some(res)
}

/// Stateless ops allowed on the pre-group path (an **allowlist**, so an unknown
/// or future stateful op is never silently parallelized; #41). Each runs
/// per-chunk with no cross-row/chunk state, so a byte-range worker can stream its
/// range through them in bounded memory.
fn pre_group_op_allowed(op: &Op) -> bool {
    matches!(
        op,
        Op::Filter { .. }
            | Op::Project { .. }
            | Op::ProjectExpr { .. }
            | Op::FilterProject { .. }
            | Op::Rename { .. }
            | Op::Drop { .. }
            | Op::Cast { .. }
            | Op::Reorder { .. }
            | Op::DropNa { .. }
            | Op::Fill {
                method: rivus_ir::FillMethod::Value(_),
                ..
            }
    )
}

/// If the flow is a single linear pipeline `CSV source → allowlisted stateless
/// ops → GroupBy → (leaf | one sink)`, return `(source_id, group_id,
/// optional_sink_id)` — the shape the *bounded* parallel group-by scheduler
/// (#41) handles. `None` keeps the caller on the serial (bounded) path.
fn eligible_group_flow(graph: &PlanGraph) -> Option<(NodeId, NodeId, Option<NodeId>)> {
    eligible_group_flow_inner(graph, true)
}

/// Splittable sources the bounded byte-range path can stream (CSV / JSONL /
/// fixed-width binary).
fn is_splittable_source(op: &Op) -> bool {
    matches!(op, Op::Source { .. })
}

/// Same shape as [`eligible_group_flow`] but accepting **any** single source
/// (CSV / JSONL / binary, compressed included) — used by the opt-in *unbounded*
/// materialized path (#50), which can parallelize a non-splittable source.
fn eligible_group_flow_any(graph: &PlanGraph) -> Option<(NodeId, NodeId, Option<NodeId>)> {
    eligible_group_flow_inner(graph, false)
}

fn eligible_group_flow_inner(
    graph: &PlanGraph,
    splittable_only: bool,
) -> Option<(NodeId, NodeId, Option<NodeId>)> {
    let mut source = None;
    let mut group = None;
    for node in &graph.nodes {
        match &node.op {
            Op::Source { .. } => {
                if source.is_some() {
                    return None;
                }
                source = Some(node.id);
            }
            Op::GroupBy { .. } => {
                if group.is_some() {
                    return None;
                }
                group = Some(node.id);
            }
            _ => {}
        }
    }
    let (src, grp) = (source?, group?);
    // The bounded byte-range path needs a splittable source (CSV / JSONL).
    if splittable_only && !is_splittable_source(&graph.nodes[src].op) {
        return None;
    }
    // The group's downstream must be empty (leaf) or exactly one (leaf) sink.
    let sink = match graph.outputs_of(grp).as_slice() {
        [] => None,
        [s] => {
            let s = *s;
            if !matches!(
                graph.nodes[s].op,
                Op::SinkCsv { .. } | Op::SinkJsonl { .. } | Op::SinkJson { .. }
            ) || !graph.outputs_of(s).is_empty()
            {
                return None;
            }
            Some(s)
        }
        _ => return None,
    };
    // source → group must be a linear chain of *allowlisted* stateless ops (each
    // node exactly one input and one consumer).
    let mut cur = grp;
    let mut path_len = 0usize;
    while cur != src {
        let ins = graph.inputs_of(cur);
        if ins.len() != 1 || graph.outputs_of(ins[0]).len() != 1 {
            return None;
        }
        let prev = ins[0];
        if prev != src {
            if !pre_group_op_allowed(&graph.nodes[prev].op) {
                return None;
            }
            path_len += 1;
        }
        cur = prev;
    }
    // No stray nodes: exactly source + pre-group ops + group + optional sink.
    if graph.nodes.len() != path_len + 2 + sink.is_some() as usize {
        return None;
    }
    Some((src, grp, sink))
}

/// The stateless pre-group ops strictly between `src` and `group`, in
/// source→group order.
fn pre_group_path(graph: &PlanGraph, src: NodeId, group: NodeId) -> Vec<NodeId> {
    let mut path = Vec::new();
    let mut cur = graph.inputs_of(group)[0];
    while cur != src {
        path.push(cur);
        cur = graph.inputs_of(cur)[0];
    }
    path.reverse();
    path
}

/// Stream a source through the pre-group ops chunk-by-chunk into a partial
/// `GroupBy` — one byte-range worker (#41). Holds only the current chunk and the
/// group state (O(group cardinality)), never the whole range, so peak memory is
/// input-size independent. Returns the partial state, errors, and rows grouped.
fn stream_into_group(
    graph: &PlanGraph,
    opts: &RunOptions,
    mut src: Box<dyn Operator>,
    src_label: &str,
    path: &[NodeId],
    group_id: NodeId,
) -> (operators::GroupBy, Vec<ErrorEvent>, u64) {
    let mut errors = Vec::new();
    let mut next_id = 0u64;
    let mut ops: Vec<(NodeId, Box<dyn Operator>)> = path
        .iter()
        .map(|&nid| {
            (
                nid,
                operators::build(
                    &graph.nodes[nid].op,
                    &graph.inputs_of(nid),
                    opts.chunk_size,
                    false,
                ),
            )
        })
        .collect();
    let mut group = operators::new_group(&graph.nodes[group_id].op).expect("group op");
    let mut rows = 0u64;
    // Push one chunk through ops[from..] then into the group.
    let feed = |ops: &mut [(NodeId, Box<dyn Operator>)],
                from: usize,
                start: Vec<Chunk>,
                group: &mut operators::GroupBy,
                errors: &mut Vec<ErrorEvent>,
                next_id: &mut u64,
                rows: &mut u64| {
        let mut level = start;
        for (nid, op) in ops.iter_mut().skip(from) {
            let mut out = Vec::new();
            let mut ctx = OpCtx {
                label: label_of(graph, *nid),
                errors,
                next_chunk_id: next_id,
            };
            for c in level {
                out.extend(op.process(*nid, c, &mut ctx));
            }
            level = out;
        }
        let mut ctx = OpCtx {
            label: label_of(graph, group_id),
            errors,
            next_chunk_id: next_id,
        };
        for c in level {
            *rows += c.len as u64;
            group.process(group_id, c, &mut ctx);
        }
    };
    loop {
        let chunk = {
            let mut ctx = OpCtx {
                label: src_label.to_string(),
                errors: &mut errors,
                next_chunk_id: &mut next_id,
            };
            match src.pull(&mut ctx) {
                Some(c) => c,
                None => break,
            }
        };
        feed(
            &mut ops,
            0,
            vec![chunk],
            &mut group,
            &mut errors,
            &mut next_id,
            &mut rows,
        );
    }
    // Trailing finish() per op (allowlisted ops are stateless → empty, but flow
    // any output through the downstream ops + group for correctness).
    for i in 0..ops.len() {
        let trailing = {
            let mut ctx = OpCtx {
                label: label_of(graph, ops[i].0),
                errors: &mut errors,
                next_chunk_id: &mut next_id,
            };
            ops[i].1.finish(&mut ctx)
        };
        if !trailing.is_empty() {
            feed(
                &mut ops,
                i + 1,
                trailing,
                &mut group,
                &mut errors,
                &mut next_id,
                &mut rows,
            );
        }
    }
    (group, errors, rows)
}

/// Resolve the group-input schema (after the pre-group ops) by streaming the
/// first byte range until an output chunk appears — bounded (one chunk at a
/// time, no accumulation). `None` if the range yields no rows through the chain.
fn sample_group_input_schema(
    graph: &PlanGraph,
    opts: &RunOptions,
    mut src: Box<dyn Operator>,
    src_label: &str,
    path: &[NodeId],
) -> Option<std::sync::Arc<rivus_core::Schema>> {
    let mut errors = Vec::new();
    let mut next_id = 0u64;
    let mut ops: Vec<(NodeId, Box<dyn Operator>)> = path
        .iter()
        .map(|&nid| {
            (
                nid,
                operators::build(
                    &graph.nodes[nid].op,
                    &graph.inputs_of(nid),
                    opts.chunk_size,
                    false,
                ),
            )
        })
        .collect();
    loop {
        let chunk = {
            let mut ctx = OpCtx {
                label: src_label.to_string(),
                errors: &mut errors,
                next_chunk_id: &mut next_id,
            };
            src.pull(&mut ctx)?
        };
        let mut level = vec![chunk];
        for (nid, op) in &mut ops {
            let mut out = Vec::new();
            let mut ctx = OpCtx {
                label: label_of(graph, *nid),
                errors: &mut errors,
                next_chunk_id: &mut next_id,
            };
            for c in level {
                out.extend(op.process(*nid, c, &mut ctx));
            }
            level = out;
        }
        if let Some(c) = level.first() {
            return Some(c.schema.clone());
        }
    }
}

/// Parallel group-by (#41): each byte-range worker streams its range through the
/// pre-group ops into a partial group state (bounded memory — O(group
/// cardinality), input-size independent, like the streaming-parallel reader),
/// then the partials merge in source order. Only taken when every aggregate is
/// byte-identical under partition→merge (checked against the *group-input* schema
/// so a pre-group `cast` to decimal counts). `None` (→ serial) otherwise.
fn try_parallel_group(
    graph: &PlanGraph,
    opts: &RunOptions,
    src_id: NodeId,
    group_id: NodeId,
    sink_id: Option<NodeId>,
    mut hook: Option<&mut (dyn FnMut(&RuntimeSnapshot) + '_)>,
) -> Option<RunResult> {
    let t0 = Instant::now();
    let threads = std::thread::available_parallelism()
        .map(|t| t.get())
        .unwrap_or(1);
    if threads < 2 || std::env::var_os("RIVUS_NO_PARALLEL").is_some() {
        return None;
    }
    if opts.max_capture.is_some() && !must_drain(graph) {
        return None;
    }
    // Plan a bounded byte-range read of the (splittable) source — CSV or JSONL.
    let plan = plan_parallel_source(&graph.nodes[src_id].op, threads)?;
    let path_nodes = pre_group_path(graph, src_id, group_id);
    let aggs = match &graph.nodes[group_id].op {
        Op::GroupBy { aggs, .. } => aggs.clone(),
        _ => return None,
    };
    let src_label = label_of(graph, src_id);

    // Resolve the group-input column types (post pre-group ops) from a sample of
    // range 0; bail to serial if any aggregate isn't partition→merge safe.
    let (a0, b0) = plan.ranges[0];
    let sample_src = plan.make_source(a0, b0, opts.chunk_size);
    let in_schema = sample_group_input_schema(graph, opts, sample_src, &src_label, &path_nodes)?;
    let safe = operators::group_parallel_safe(&aggs, |name| {
        in_schema.index_of(name).map(|i| in_schema.fields[i].dtype)
    });
    if !safe {
        return None;
    }

    // One streaming worker per byte range; bounded per-worker memory.
    let partials: Vec<(operators::GroupBy, Vec<ErrorEvent>, u64)> = std::thread::scope(|scope| {
        let plan = &plan;
        let path_nodes = &path_nodes;
        let src_label = &src_label;
        let handles: Vec<_> = plan
            .ranges
            .iter()
            .map(|&(a, b)| {
                scope.spawn(move || {
                    let src = plan.make_source(a, b, opts.chunk_size);
                    stream_into_group(graph, opts, src, src_label, path_nodes, group_id)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let res = finalize_group_partials(graph, src_id, group_id, sink_id, partials);
    if let Some(h) = hook.as_mut() {
        h(&final_snapshot(&res, src_id, t0.elapsed()));
    }
    Some(res)
}

/// A planned bounded byte-range read of a splittable source (CSV or JSONL): the
/// global schema, the newline-aligned ranges, and the per-format knobs needed to
/// open a streaming worker for any range (#41 / #49).
struct ParPlan {
    schema: std::sync::Arc<rivus_core::Schema>,
    ranges: Vec<(u64, u64)>,
    path: String,
    bad_rows: usize,
    src: ParSource,
    /// Provenance mode of the source op (design §28.6): each byte-range worker
    /// derives the same origin handle from `path`, so provenance is partition-
    /// independent and byte-identical to the serial reader.
    provenance: rivus_ir::Provenance,
}

enum ParSource {
    Csv {
        dtypes: Vec<DataType>,
        dt_specs: Vec<Option<std::sync::Arc<crate::csv::DtSpec>>>,
        keep: Vec<usize>,
        ncols: usize,
        prefilter: Vec<(usize, rivus_ir::CmpOp, f64)>,
        str_prefilter: Vec<String>,
        delim: u8,
    },
    Jsonl {
        names: Vec<String>,
        dtypes: Vec<DataType>,
    },
    Binary {
        fields: Vec<(String, rivus_ir::BinType)>,
        offsets: Vec<usize>,
        rec_size: usize,
        endian: rivus_ir::Endian,
    },
}

impl ParPlan {
    /// Open a streaming source for byte range `[a, b)`.
    fn make_source(&self, a: u64, b: u64, chunk_size: usize) -> Box<dyn Operator> {
        // Every worker derives the same origin handle from the same path, so the
        // provenance it stamps (and the materialized `filename` column) is
        // partition-independent — byte-identical to the serial reader.
        match &self.src {
            ParSource::Csv {
                dtypes,
                dt_specs,
                keep,
                ncols,
                prefilter,
                str_prefilter,
                delim,
            } => operators::csv_range_source(
                &self.path,
                dtypes.clone(),
                dt_specs.clone(),
                keep.clone(),
                *ncols,
                self.schema.clone(),
                a,
                b,
                chunk_size,
                prefilter.clone(),
                str_prefilter.clone(),
                *delim,
                self.provenance,
            ),
            ParSource::Jsonl { names, dtypes } => operators::jsonl_range_source(
                &self.path,
                names.clone(),
                dtypes.clone(),
                self.schema.clone(),
                a,
                b,
                chunk_size,
                self.provenance,
            ),
            ParSource::Binary {
                fields,
                offsets,
                rec_size,
                endian,
            } => operators::bin_range_source(
                &self.path,
                fields.clone(),
                offsets.clone(),
                *rec_size,
                *endian,
                self.schema.clone(),
                a as usize / rec_size,
                (b - a) as usize / rec_size,
                chunk_size,
                self.provenance,
            ),
        }
    }
}

/// Plan a bounded byte-range parallel read for a splittable source op (a seekable
/// CSV or a line-oriented JSONL file with ≥2 ranges). `None` for stdin,
/// compressed, a JSON array, binary (no streaming reader yet), or too small.
fn plan_parallel_source(op: &Op, threads: usize) -> Option<ParPlan> {
    // Only a source op plans a parallel read; dispatch on its codec.
    let Op::Source {
        discovery,
        codec,
        provenance,
        ..
    } = op
    else {
        return None;
    };
    let provenance = *provenance;
    match codec {
        Codec::Csv {
            projection,
            prefilter,
            str_prefilter,
            header,
            declared,
            dt_formats,
            delim,
        } => {
            let path = source_path(op).filter(|p| crate::transport::Scheme::of(p).is_seekable())?;
            let plan = crate::csv::plan_parallel(
                path,
                projection.as_deref(),
                threads,
                prefilter,
                str_prefilter,
                *header,
                declared.as_deref(),
                dt_formats,
                *delim,
            )
            .ok()?;
            if plan.ranges.len() < 2 {
                return None;
            }
            Some(ParPlan {
                schema: std::sync::Arc::new(plan.schema),
                ranges: plan.ranges,
                path: path.to_string(),
                bad_rows: plan.bad_rows,
                src: ParSource::Csv {
                    dtypes: plan.dtypes,
                    dt_specs: plan.dt_specs,
                    keep: plan.keep,
                    ncols: plan.ncols,
                    prefilter: plan.prefilter,
                    str_prefilter: plan.str_prefilter,
                    delim: *delim,
                },
                provenance,
            })
        }
        Codec::Jsonl => {
            let path = discovery.path();
            if path == "-" {
                return None;
            }
            let (schema, names, dtypes, ranges, bad_rows) =
                crate::jsonl::plan_parallel(path, threads)?;
            Some(ParPlan {
                schema: std::sync::Arc::new(schema),
                ranges,
                path: path.to_string(),
                bad_rows,
                src: ParSource::Jsonl { names, dtypes },
                provenance,
            })
        }
        Codec::Binary {
            fields,
            endian,
            c_align,
        } => {
            let path = discovery.path();
            if path == "-" {
                return None;
            }
            let (offsets, rec_size) = operators::bin_layout(fields, *c_align)?;
            let len = std::fs::metadata(path).ok()?.len() as usize;
            let recs = len / rec_size;
            // Fixed-width → split the record count into ≤ `threads` record ranges;
            // byte bounds are just `rec_index * rec_size` (no boundary scan).
            let nparts = threads.min(recs);
            if nparts < 2 {
                return None;
            }
            let ranges: Vec<(u64, u64)> = (0..nparts)
                .map(|i| {
                    let s = recs * i / nparts;
                    let e = recs * (i + 1) / nparts;
                    ((s * rec_size) as u64, (e * rec_size) as u64)
                })
                .filter(|(a, b)| b > a)
                .collect();
            if ranges.len() < 2 {
                return None;
            }
            Some(ParPlan {
                schema: std::sync::Arc::new(operators::bin_schema(fields)),
                ranges,
                path: path.to_string(),
                bad_rows: len % rec_size,
                src: ParSource::Binary {
                    fields: fields.clone(),
                    offsets,
                    rec_size,
                    endian: *endian,
                },
                provenance,
            })
        }
        // `ls` discovery is not a byte-range read — it runs on the serial path
        // (the enumerator already emits in deterministic, chunk-sized batches).
        Codec::Discover => None,
    }
}

/// Merge per-worker partial group states in source order, finalize once, and
/// build the `RunResult` (full per-node telemetry, per-worker rows, sink write or
/// leaf capture). Shared by the bounded streaming (#41) and opt-in unbounded
/// (#50) group paths.
fn finalize_group_partials(
    graph: &PlanGraph,
    src_id: NodeId,
    group_id: NodeId,
    sink_id: Option<NodeId>,
    partials: Vec<(operators::GroupBy, Vec<ErrorEvent>, u64)>,
) -> RunResult {
    let mut iter = partials.into_iter();
    let (mut merged, mut errors, mut total_rows) = iter.next().expect("≥1 worker");
    let mut workers = vec![WorkerTelemetry {
        worker: 0,
        rows_out: total_rows,
        busy: Duration::ZERO,
    }];
    for (i, (g, errs, rows)) in iter.enumerate() {
        merged.merge_from(g);
        errors.extend(errs);
        total_rows += rows;
        workers.push(WorkerTelemetry {
            worker: i + 1,
            rows_out: rows,
            busy: Duration::ZERO,
        });
    }
    let mut fin_id: u64 = 0;
    let out_chunks = {
        let mut ctx = OpCtx {
            label: label_of(graph, group_id),
            errors: &mut errors,
            next_chunk_id: &mut fin_id,
        };
        merged.finish(&mut ctx)
    };
    let rows_out: u64 = out_chunks.iter().map(|c| c.len as u64).sum();

    // One telemetry entry per node (viz indexes by node id); fill in the counts
    // we know (source/group/sink) and mark all finished.
    let mut telemetry: Vec<NodeTelemetry> = graph
        .nodes
        .iter()
        .map(|node| {
            let mut t = NodeTelemetry::new(
                node.id,
                label_of(graph, node.id),
                node.op.kind_str().to_string(),
            );
            t.finished = true;
            t
        })
        .collect();
    telemetry[src_id].rows_out = total_rows;
    telemetry[group_id].rows_in = total_rows;
    telemetry[group_id].rows_out = rows_out;
    if let Some(sink) = sink_id {
        telemetry[sink].rows_in = rows_out;
    }

    // Halt the run if any worker (or the merge) raised a fatal error, so the
    // parallel group-by's final mode — and the CLI exit code — match the serial
    // path. Data is unaffected (#48; the serial/`merge_results` paths derive the
    // mode the same way).
    let final_mode = if errors.iter().any(ErrorEvent::is_fatal) {
        Mode::Halted
    } else {
        Mode::Normal
    };

    let mut res = RunResult {
        telemetry,
        errors,
        final_mode,
        outputs: Vec::new(),
        workers,
        first_row_latency: None,
        inference: Vec::new(),
        strategy: None,
    };

    if let Some(sink) = sink_id {
        if let Some((path, Err(e))) = write_sink(&graph.nodes[sink].op, &out_chunks) {
            res.errors.push(
                ErrorEvent::new(
                    Severity::Critical,
                    ErrorScope::Graph,
                    format!("cannot write '{path}': {e}"),
                )
                .at_node(label_of(graph, sink)),
            );
        }
    } else {
        res.outputs.push(Output {
            node_id: group_id,
            label: graph.nodes[group_id].label.clone(),
            chunks: out_chunks,
        });
    }
    res
}

/// Run the pre-group ops on already-materialized `chunks` then a partial
/// `GroupBy` — one worker of the opt-in **unbounded** path (#50), which trades
/// the bounded guarantee (the whole input is materialized before partitioning) to
/// parallelize a non-splittable source (compressed / JSONL / binary).
fn worker_partial_group_materialized(
    graph: &PlanGraph,
    opts: &RunOptions,
    path: &[NodeId],
    group_id: NodeId,
    chunks: Vec<Chunk>,
) -> (operators::GroupBy, Vec<ErrorEvent>, u64) {
    let mut errors = Vec::new();
    let mut next_id = 0u64;
    let mut cur = chunks;
    for &nid in path {
        let mut op = operators::build(
            &graph.nodes[nid].op,
            &graph.inputs_of(nid),
            opts.chunk_size,
            false,
        );
        let mut out = Vec::new();
        let mut ctx = OpCtx {
            label: label_of(graph, nid),
            errors: &mut errors,
            next_chunk_id: &mut next_id,
        };
        for c in cur {
            out.extend(op.process(nid, c, &mut ctx));
        }
        out.extend(op.finish(&mut ctx));
        cur = out;
    }
    let mut g = operators::new_group(&graph.nodes[group_id].op).expect("group op");
    let rows: u64 = cur.iter().map(|c| c.len as u64).sum();
    let mut ctx = OpCtx {
        label: label_of(graph, group_id),
        errors: &mut errors,
        next_chunk_id: &mut next_id,
    };
    for c in cur {
        g.process(group_id, c, &mut ctx);
    }
    (g, errors, rows)
}

/// Opt-in **unbounded** parallel group-by (#50): materialize the whole input,
/// partition it, and aggregate each partition in parallel. Trades the bounded
/// guarantee for speed on a **non-splittable** source the streaming path can't
/// parallelize — only ever taken for `MemoryPref::Unbounded` (the user's explicit
/// choice). Still byte-identical to serial (same deterministic merge); the only
/// difference is peak memory (O(input)). `None` if not unbounded, not the group
/// shape, unsafe aggregates, or too small.
fn try_unbounded_group(
    graph: &PlanGraph,
    opts: &RunOptions,
    src_id: NodeId,
    group_id: NodeId,
    sink_id: Option<NodeId>,
    mut hook: Option<&mut (dyn FnMut(&RuntimeSnapshot) + '_)>,
) -> Option<RunResult> {
    let t0 = Instant::now();
    if opts.memory != crate::analytics::MemoryPref::Unbounded {
        return None;
    }
    let threads = std::thread::available_parallelism()
        .map(|t| t.get())
        .unwrap_or(1);
    if threads < 2 || std::env::var_os("RIVUS_NO_PARALLEL").is_some() {
        return None;
    }
    if opts.max_capture.is_some() && !must_drain(graph) {
        return None;
    }

    // Materialize the source (the opt-in unbounded cost).
    let mut src_errors: Vec<ErrorEvent> = Vec::new();
    let mut next_id: u64 = 0;
    let mut all: Vec<Chunk> = Vec::new();
    {
        let mut src_op = operators::build(&graph.nodes[src_id].op, &[], opts.chunk_size, false);
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

    let path = pre_group_path(graph, src_id, group_id);
    let aggs = match &graph.nodes[group_id].op {
        Op::GroupBy { aggs, .. } => aggs.clone(),
        _ => return None,
    };
    // Safety against the group-input schema (post pre-group ops): run the first
    // chunk through the pre-group ops and read the resulting schema.
    let in_schema = {
        let mut errors = Vec::new();
        let mut nid_ctr = 0u64;
        let mut cur = vec![all.first()?.clone()];
        for &nid in &path {
            let mut op = operators::build(
                &graph.nodes[nid].op,
                &graph.inputs_of(nid),
                opts.chunk_size,
                false,
            );
            let mut out = Vec::new();
            let mut ctx = OpCtx {
                label: label_of(graph, nid),
                errors: &mut errors,
                next_chunk_id: &mut nid_ctr,
            };
            for c in cur {
                out.extend(op.process(nid, c, &mut ctx));
            }
            cur = out;
        }
        cur.first().map(|c| c.schema.clone())?
    };
    let safe = operators::group_parallel_safe(&aggs, |name| {
        in_schema.index_of(name).map(|i| in_schema.fields[i].dtype)
    });
    if !safe || all.len() < threads * 2 {
        return None;
    }

    let parts = partition(all, threads);
    let partials: Vec<(operators::GroupBy, Vec<ErrorEvent>, u64)> = std::thread::scope(|scope| {
        let path = &path;
        let handles: Vec<_> = parts
            .into_iter()
            .map(|chunks| {
                scope.spawn(move || {
                    worker_partial_group_materialized(graph, opts, path, group_id, chunks)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let mut res = finalize_group_partials(graph, src_id, group_id, sink_id, partials);
    res.errors.splice(0..0, src_errors);
    if src_fatal {
        res.final_mode = Mode::Halted;
    }
    if let Some(h) = hook.as_mut() {
        h(&final_snapshot(&res, src_id, t0.elapsed()));
    }
    Some(res)
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
        // `run_with_progress` stamps the parallel rationale onto the returned
        // result; merge_results itself doesn't decide.
        strategy: None,
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
            .position(|nd| matches!(nd.op, Op::Source { .. }))
            .unwrap();
        try_streaming_parallel(&gp, &opts, src_id, 4, None)
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
