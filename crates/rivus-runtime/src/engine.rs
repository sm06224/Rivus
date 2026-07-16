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
use rivus_ir::{Codec, Expr, HookAction, HookEvent, NodeId, Op, PathExpr, PlanGraph, SinkCodec};
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

/// Cadence for live-progress snapshots (TUI / `--serve`): publish **at most**
/// this often, time-based, on both the serial loop and the parallel coordinator.
/// Observation overhead is then bounded by wall-clock — not chunk count — so a
/// fast run with many small chunks never pays a snapshot build + JSON + publish
/// on the hot path for every few chunks (PERF-H). The browser interpolates
/// between frames, so ~10 fps is smooth.
const SNAPSHOT_INTERVAL: Duration = Duration::from_millis(100);

/// A live-progress subscriber: called with a [`RuntimeSnapshot`] periodically as
/// a run streams. The base for live TUI / HTTP dashboards (Pillar B).
/// `None` everywhere is the default and costs nothing (no snapshot is built).
pub type ProgressHook<'a> = &'a mut dyn FnMut(&RuntimeSnapshot);

pub fn run(graph: &PlanGraph, opts: RunOptions) -> Result<RunResult, RivusError> {
    run_with_progress(graph, opts, None)
}

/// Like [`run`], but with an optional live-progress hook (Observability §14.4 /
/// Epic #30 A5). Observable First: a hook **never forces the serial path** — the
/// parallel paths feed it an aggregate cross-worker snapshot via `ParProgress`,
/// so observing a run never throttles it (PERF-H). Snapshots are time-based
/// ([`SNAPSHOT_INTERVAL`]) on every path.
pub fn run_with_progress(
    graph: &PlanGraph,
    opts: RunOptions,
    hook: Option<&mut (dyn FnMut(&RuntimeSnapshot) + '_)>,
) -> Result<RunResult, RivusError> {
    // §29.5-6 s4 (never-silent): a build without the `regex` feature cannot
    // evaluate `~` / regexp(). Refuse the plan explicitly before running —
    // evaluating every test to false would be a silent wrong answer.
    if cfg!(not(feature = "regex")) && graph.uses_regexp() {
        return Err(RivusError::Build(
            "this flow uses a regular expression (`~` / regexp()), but this build has the \
             `regex` feature disabled — rebuild with `--features regex` (the default build \
             stays zero-dependency)"
                .into(),
        ));
    }
    // §28.12.0 (ratified #149 ①): a blocking operator (group/sort/describe/
    // join, or a whole-stream fill) downstream of an unbounded source emits
    // only on finish — which never comes — so it would hang silently. Windows
    // are a later slice; refuse with guidance (never-silent). `take N` is NOT
    // offered as the fix (#154 ruling (b)): it bounds the row *count* but not
    // *which* rows arrive (arrival order is environmental, §0.14), so a
    // take-then-aggregate result would be non-deterministic — the refusal
    // stays; only the wording dropped `take N`. Checked before the feature
    // gate: the plan's *shape* is invalid in every build, so the message is the
    // same with or without the feature.
    if graph.uses_unbounded() {
        let tag = graph.unbounded_nodes();
        for node in &graph.nodes {
            if tag[node.id] && node.op.is_blocking() {
                return Err(RivusError::Build(format!(
                    "`{}` needs the whole stream, but it is downstream of an unbounded \
                     source (`watch` / `subscribe`) that never ends — aggregating an \
                     unbounded source needs a window (a later slice); remove the unbounded \
                     source, or wait for the windowing slice",
                    node.op.kind_str()
                )));
            }
        }
    }
    // §28.12 (ratified #149 ⑤, never-silent): a build without the `unbounded`
    // feature cannot evaluate an unbounded source (`watch`). Refuse the plan
    // explicitly before running — the same shape as `regex`/`gzip`. Parsing and
    // `rivus explain` stay always-std.
    // Only the file-`watch` source rides the `unbounded`/`notify` feature; the
    // network `subscribe` is unbounded too but rides `net` (checked below). So
    // this consults `uses_watch`, not `uses_unbounded`.
    if cfg!(not(feature = "unbounded")) && graph.uses_watch() {
        return Err(RivusError::Build(
            "this flow uses an unbounded source (`watch`), but this build has the \
             `unbounded` feature disabled — rebuild with `--features unbounded` (the \
             default build stays zero-dependency)"
                .into(),
        ));
    }
    // §33 (never-silent): a build without the `net` feature cannot evaluate a
    // networked source (`open "http://…"`). Refuse the plan explicitly before
    // running — the same shape as `regex`/`unbounded`. Parsing and `rivus
    // explain` stay always-std (the URL round-trips in any build).
    // SUPPLY-CHAIN adapter gate (never-silent): a build without the `parquet`
    // feature cannot decode a Parquet source. Refuse the plan explicitly before
    // running — the same shape as `regex`/`gzip`. Parsing and `rivus explain`
    // stay always-std (the codec round-trips in any build).
    if cfg!(not(feature = "parquet")) && graph.uses_parquet() {
        return Err(RivusError::Build(
            "this flow reads a Parquet source, but this build has the `parquet` feature \
             disabled — rebuild with `--features parquet` (the default build stays \
             zero-dependency)"
                .into(),
        ));
    }
    if cfg!(not(feature = "net")) && graph.uses_net() {
        return Err(RivusError::Build(
            "this flow uses a network source (`open \"http://…\"`), but this build has the \
             `net` feature disabled — rebuild with `--features net` (the default build \
             stays zero-dependency)"
                .into(),
        ));
    }
    // Plan Validation Gate (#191/#195/#200): refuse-with-guidance for mistakes
    // the runtime would otherwise swallow. Same pass as the CLI's `check`.
    plan_validate(graph)?;
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
    // forces single-thread. A live hook does NOT change the strategy (Observable
    // First): the parallel paths observe via an aggregate snapshot (`ParProgress`).
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
    // Parallel read→group (slice 6, 統括指示: 負けるな): a
    // `ls → read → [stateless/broadcast-join]* → group → [sort]* → [sink]`
    // flow runs one streaming worker per FILE with partial GroupBys merged like
    // #41 (associative lanes only — checked; bails to serial else). The
    // size-based strategy chooser can't see a multi-file input's size (there is
    // no single file source), so the shape is detected here and the size/memory
    // threshold is honored inside the runner (sum of file sizes vs the same
    // autotuner threshold).
    if let Some(shape) = eligible_read_group_flow(graph) {
        let attempt = try_parallel_read_group(graph, &opts, &shape, hook.as_deref_mut());
        if let Some(res) = attempt {
            return Ok(res);
        }
    }
    // Slice 10: the pure-ETL shape (read→…→save, no group) — per-file segment
    // encoding, concatenated in uri order (byte-identical to the serial sink).
    if let Some(shape) = eligible_read_sink_flow(graph) {
        let attempt = try_parallel_read_sink(graph, &opts, &shape, hook.as_deref_mut());
        if let Some(res) = attempt {
            return Ok(res);
        }
    }
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
            Op::Sink { .. }
                | Op::GroupBy { .. }
                | Op::Sort { .. }
                | Op::Distinct { .. }
                | Op::Describe
                | Op::Join { .. }
                | Op::AsofJoin { .. }
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
            } else if ov_id.is_some() && matches!(node.op, Op::Sink { .. }) {
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
/// Could a chunk pulled from `src` still have an observable effect — is there a
/// path from `src` to a sink / leaf capture that does not pass a **saturated**
/// operator (a `take N` that has emitted its N)? When no such path remains, an
/// **unbounded** source stops (§28.12) — its only self-termination. Memoized
/// per node (saturation is a node property, path-independent). Only consulted
/// for unbounded sources; a bounded source never early-stops (pinned by test).
fn unbounded_effect_remains(graph: &PlanGraph, src: NodeId, ops: &[Box<dyn Operator>]) -> bool {
    fn effect(
        graph: &PlanGraph,
        n: NodeId,
        ops: &[Box<dyn Operator>],
        memo: &mut [Option<bool>],
    ) -> bool {
        if let Some(v) = memo[n] {
            return v;
        }
        memo[n] = Some(false); // DAG, but guard anyway
        let v = if ops[n].saturated() {
            false
        } else {
            let outs = graph.outputs_of(n);
            // A leaf (sink, print, or engine capture) is the observable effect.
            outs.is_empty() || outs.into_iter().any(|m| effect(graph, m, ops, memo))
        };
        memo[n] = Some(v);
        v
    }
    let mut memo: Vec<Option<bool>> = vec![None; graph.nodes.len()];
    graph
        .outputs_of(src)
        .into_iter()
        .any(|m| effect(graph, m, ops, &mut memo))
}

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
    // Publish a live snapshot at most every `SNAPSHOT_INTERVAL` (time-based, not
    // per-chunk) when a hook is attached, so the snapshot build + JSON + publish
    // on the hot path is bounded by wall-clock, not by chunk count / throughput
    // (PERF-H). `None` until the first publish, which paints immediately.
    let mut last_pub: Option<Instant> = None;

    // Only a sink-less, non-blocking flow may stop the source early on a cap.
    let must_drain = must_drain(graph);

    // §28.12: per-node flag for unbounded sources (`watch`). Only these consult
    // the downstream-saturation check below — a bounded source keeps the exact
    // drain-to-exhaustion loop (pinned by test).
    let unbounded_src: Vec<bool> = graph
        .nodes
        .iter()
        .map(|nd| matches!(&nd.op, Op::Source { discovery, .. } if discovery.is_unbounded()))
        .collect();

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
                } else if unbounded_src[nid] && !unbounded_effect_remains(graph, nid, &ops) {
                    // §28.12: every downstream path from this unbounded source
                    // passes a saturated operator (a filled `take N`) — nothing
                    // it could ever emit is observable anymore. Stop it: the
                    // only self-termination an endless stream has. Checked
                    // *before* pull, so a satisfied flow never re-enters the
                    // blocking wait.
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
                            // A5: publish a periodic live snapshot to a subscriber,
                            // time-based so a high chunk rate can't flood the hot
                            // path (PERF-H); the first chunk paints immediately.
                            if let Some(h) = hook.as_mut() {
                                let due = match last_pub {
                                    None => true,
                                    Some(t) => t.elapsed() >= SNAPSHOT_INTERVAL,
                                };
                                if due {
                                    last_pub = Some(Instant::now());
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
                let extra = apply_error_hooks(graph, &errors[before..], &mut mode);
                errors.extend(extra);
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
        // JSON-array sinks stay serial (the bracketed form can't concatenate
        // per-worker parts), exactly as before the Sink unification.
        match &nd.op {
            Op::Sink {
                route: rivus_ir::Route::Fixed(path),
                codec: SinkCodec::Csv { delim },
                ..
            } => sinks.push((nd.id, path.clone(), false, *delim)),
            Op::Sink {
                route: rivus_ir::Route::Fixed(path),
                codec: SinkCodec::Jsonl,
                ..
            } => sinks.push((nd.id, path.clone(), true, b',')),
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
    if let Some(w) = &plan.header_warning {
        src_errors.push(
            ErrorEvent::new(Severity::Warn, ErrorScope::Item, w.clone())
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
        let p = crate::transport::adjust_path(final_path);
        Box::new(std::fs::File::create(p)?)
    };
    let mut out = std::io::BufWriter::new(sink);
    let mut header_done = jsonl;
    for part in parts {
        let p = crate::transport::adjust_path(part);
        let f = match std::fs::File::open(p) {
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
        let adjusted = crate::transport::adjust_path(p);
        let _ = std::fs::remove_file(adjusted);
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

/// How often the coordinator samples cross-worker progress for the live hook —
/// the same time-based cadence as the serial loop ([`SNAPSHOT_INTERVAL`]).
const PAR_SAMPLE: Duration = SNAPSHOT_INTERVAL;

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
            Op::Source { discovery, .. } => {
                // An unbounded source (`watch`, §28.12) never partitions or
                // materializes — byte-identity is asserted only on bounded
                // sub-DAGs, so the unbounded flow stays on the serial
                // streaming loop (ratified #149 ③/④).
                if discovery.is_unbounded() {
                    return None;
                }
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
            | Op::AsofJoin { .. }
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
            // Session boundaries depend on the previous row's ts per group
            // (order-dependent state across chunks) → not partitionable; the
            // serial path keeps byte-identity (§36.5, same family as ffill).
            Op::Sessionize { .. } => return None,
            // Shift depends on earlier rows per group (order-dependent state) →
            // not partitionable; serial keeps chunk-size independence (#65).
            Op::Shift { .. } => return None,
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
/// Run a [`ReadGroupShape`]: discovery serially, broadcast right sides
/// serially (they are small bounded sources), then one worker per file (waves
/// of ≤ core count) streaming into partial `GroupBy`s, merged like #41; the
/// post-group tail (sorts, sink) runs serially on the merged output.
/// One worker's partial result: its `GroupBy` state, errors, and rows grouped.
type PartialGroup = (operators::GroupBy, Vec<ErrorEvent>, u64);

/// One read→sink segment worker's result: errors, rows written, header line.
type SegmentResult = (Vec<ErrorEvent>, u64, Option<String>);

/// A broadcast join's prebuilt right side: concatenated chunk, hash table,
/// and right-key column indices — built once, shared by every worker.
type BroadcastRight = operators::BuiltRight;

/// The column names the ops between the read and the group actually consume
/// (probe projection pushdown, design/41 Stage A-1): the broadcast probe then
/// gathers ONLY these output columns instead of every column of every output
/// row. `None` disables pruning — a positional `$_[i]` reference, an op
/// outside the modeled set, or any expression whose column reads can't be
/// enumerated keeps the old keep-everything shape. Over-approximation is
/// always safe: a kept-but-unused column costs exactly what it did before.
fn fused_used_columns(graph: &PlanGraph, shape: &ReadGroupShape) -> Option<Vec<String>> {
    fn add(name: &str, out: &mut Vec<String>) {
        if !out.iter().any(|n| n == name) {
            out.push(name.to_string());
        }
    }
    /// Every column `e` reads, by name; `false` = not enumerable → disable.
    fn expr_cols(e: &rivus_ir::Expr, out: &mut Vec<String>) -> bool {
        use rivus_ir::Expr as E;
        match e {
            E::Field { name, access } => {
                if access.is_column() {
                    add(name, out);
                }
                true
            }
            // Positions shift when columns are pruned — never prune under one.
            E::FieldAt(_) => false,
            E::SubView { base, .. } => {
                add(base, out);
                true
            }
            E::Path(p) => {
                add(&p.root, out);
                true
            }
            E::Literal(_) | E::Hole(_) => true,
            E::Compare { left, right, .. } | E::Arith { left, right, .. } => {
                expr_cols(left, out) && expr_cols(right, out)
            }
            E::And(a, b) | E::Or(a, b) => expr_cols(a, out) && expr_cols(b, out),
            E::Cast { expr, .. } => expr_cols(expr, out),
            E::Func { args, .. } => args.iter().all(|a| expr_cols(a, out)),
            E::Case { branches, default } => {
                branches
                    .iter()
                    .all(|(c, v)| expr_cols(c, out) && expr_cols(v, out))
                    && default.as_deref().is_none_or(|d| expr_cols(d, out))
            }
        }
    }
    let mut used = Vec::new();
    for step in &shape.path {
        let nid = match step {
            ReadPathStep::Stateless(n) => *n,
            ReadPathStep::Broadcast { join_id, .. } => *join_id,
        };
        match &graph.nodes[nid].op {
            // A LATER probe reads its left keys from THIS probe's (possibly
            // pruned) output — keep them. (The probe's own left keys are read
            // from its input chunk, so collecting them over-approximates for
            // the first join; over-approximation is safe.)
            Op::Join { left_keys, .. } => {
                for k in left_keys {
                    add(&k.root, &mut used);
                }
            }
            Op::Cast { casts } => {
                for (n, _) in casts {
                    add(n, &mut used);
                }
            }
            Op::Filter { pred } => {
                if !expr_cols(pred, &mut used) {
                    return None;
                }
            }
            Op::FilterProject { preds, fields } => {
                for p in preds {
                    if !expr_cols(p, &mut used) {
                        return None;
                    }
                }
                if let Some(fs) = fields {
                    for f in fs {
                        add(f, &mut used);
                    }
                }
            }
            Op::Project { fields } => {
                for f in fields {
                    add(f, &mut used);
                }
            }
            Op::ProjectExpr { items, .. } => {
                for (e, _) in items {
                    if !expr_cols(e, &mut used) {
                        return None;
                    }
                }
            }
            // Rename remaps names (the set would go stale); DropNa/Reorder/…
            // read or reorder every column — outside the modeled set.
            _ => return None,
        }
    }
    let Op::GroupBy { keys, aggs } = &graph.nodes[shape.group_id].op else {
        return None;
    };
    for k in keys {
        add(&k.root, &mut used);
    }
    for (_, c) in aggs {
        add(c, &mut used);
    }
    Some(used)
}

fn try_parallel_read_group(
    graph: &PlanGraph,
    opts: &RunOptions,
    shape: &ReadGroupShape,
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
    let Op::Read { fmt, provenance } = &graph.nodes[shape.read_id].op else {
        return None;
    };
    let (fmt, provenance) = (*fmt, *provenance);
    let read_label = label_of(graph, shape.read_id);

    // 1) Discovery, serially (milliseconds): pull every handle chunk.
    let mut pre_errors: Vec<ErrorEvent> = Vec::new();
    let mut pre_id = 0u64;
    let mut uris: Vec<String> = Vec::new();
    {
        let mut op = operators::build(
            &graph.nodes[shape.discovery_id].op,
            &[],
            opts.chunk_size,
            false,
        );
        let mut ctx = OpCtx {
            label: label_of(graph, shape.discovery_id),
            errors: &mut pre_errors,
            next_chunk_id: &mut pre_id,
        };
        while let Some(ch) = op.pull(&mut ctx) {
            uris_of_chunk(&ch, &mut uris);
        }
    }
    if uris.is_empty() || pre_errors.iter().any(ErrorEvent::is_fatal) {
        return None; // let the serial path raise its own errors
    }
    uris.sort();
    // Honor the same size threshold the single-source autotuner uses (a small
    // input isn't worth the fan-out; MemoryPref::Low keeps its serial promise).
    let total_bytes: u64 = uris
        .iter()
        .filter_map(|u| std::fs::metadata(u).ok().map(|m| m.len()))
        .sum();
    if total_bytes < parallel_min_bytes_for(opts.memory) {
        return None;
    }

    // 2) Broadcast right sides, serially (small bounded sources): concat +
    //    hash-index ONCE, shared by every worker's streaming prober.
    let mut rights: Vec<(NodeId, BroadcastRight)> = Vec::new();
    for step in &shape.path {
        if let ReadPathStep::Broadcast { join_id, right_src } = step {
            let mut op = operators::build(&graph.nodes[*right_src].op, &[], opts.chunk_size, false);
            let mut chunks = Vec::new();
            let mut ctx = OpCtx {
                label: label_of(graph, *right_src),
                errors: &mut pre_errors,
                next_chunk_id: &mut pre_id,
            };
            while let Some(ch) = op.pull(&mut ctx) {
                chunks.push(ch);
            }
            let Op::Join { right_keys, .. } = &graph.nodes[*join_id].op else {
                return None;
            };
            let built = operators::BroadcastProbe::build_right(&chunks, right_keys)?;
            rights.push((*right_src, built));
        }
    }
    if pre_errors.iter().any(ErrorEvent::is_fatal) {
        return None;
    }

    // 3) Phase 1 — open every file lazily (schema now, rows later), in waves.
    let planner = operators::Read::new(fmt, provenance, opts.chunk_size);
    let mut opened: Vec<(String, rivus_core::Schema, operators::FileDecoder)> = Vec::new();
    let mut quarantined: Vec<ErrorEvent> = Vec::new();
    {
        let mut slots: Vec<Option<Result<(rivus_core::Schema, operators::FileDecoder), String>>> =
            (0..uris.len()).map(|_| None).collect();
        for (wave_start, wave) in uris
            .chunks(threads)
            .enumerate()
            .map(|(w, c)| (w * threads, c))
        {
            let planner = &planner;
            let wave_results: Vec<_> = std::thread::scope(|s| {
                let handles: Vec<_> = wave
                    .iter()
                    .map(|uri| s.spawn(move || planner.open_file_stream(uri)))
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });
            for (i, r) in wave_results.into_iter().enumerate() {
                slots[wave_start + i] = Some(r);
            }
        }
        for (uri, slot) in uris.iter().zip(slots) {
            match slot.expect("slot filled") {
                Ok((schema, dec)) => opened.push((uri.clone(), schema, dec)),
                Err(e) => quarantined.push(
                    ErrorEvent::new(
                        Severity::Recoverable,
                        ErrorScope::Item,
                        format!("read: skipped '{uri}': {e}"),
                    )
                    .at_node(read_label.clone()),
                ),
            }
        }
    }
    if opened.is_empty() {
        return None;
    }
    let t_open = t0.elapsed();

    // 4) The union schema every worker reconciles to (single source of truth).
    let (union, fname) = operators::union_by_name(opened.iter().map(|(_, s, _)| s), provenance);
    let uschema = std::sync::Arc::new(rivus_core::Schema::new(union.clone()));

    // 5) Partition→merge safety: run ONE chunk of the first file through a
    //    scratch pipeline to learn the group-input schema, then check every
    //    aggregate rides an associative lane (#41). Bail to serial otherwise.
    let aggs = match &graph.nodes[shape.group_id].op {
        Op::GroupBy { aggs, .. } => aggs.clone(),
        _ => return None,
    };
    // Probe projection pushdown (design/41 Stage A-1): the downstream column
    // set, proved from the path's expressions; `None` keeps every column.
    let used = fused_used_columns(graph, shape);
    // Fused row loop (design/41 Stage A): graph-level eligibility; the
    // schema-level half resolves per worker on the first join-input chunk.
    let fused_sp = fused_shape_plan(graph, shape);
    // The sample chunk comes from the ALREADY-OPENED first file's decoder —
    // the old form re-opened the file, which re-ran its whole inference pass
    // (a full serial scan) just to type-check one chunk. The columns are
    // cloned for the scratch run and the originals are handed to worker 0 as
    // a preface, so the worker still processes every row exactly once, in
    // order — byte-identical, one inference pass cheaper.
    let preface0: Vec<rivus_core::Column>;
    {
        let (uri0, schema0, dec0) = &mut opened[0];
        let cols0 = dec0.next_chunk()?;
        let mut serrors = Vec::new();
        let mut sid = 0u64;
        let ch0 = operators::reconcile_chunk(
            &union,
            &uschema,
            fname.as_deref(),
            &provenance.source(uri0),
            uri0,
            schema0,
            cols0.clone(),
            0,
        );
        // Mini-pipeline: process the one chunk, then cascade finishes.
        let mut ops: Vec<(NodeId, NodeId, Box<dyn Operator>)> = Vec::new();
        let mut prev = shape.read_id;
        for step in &shape.path {
            match step {
                ReadPathStep::Stateless(nid) => {
                    ops.push((
                        *nid,
                        prev,
                        operators::build(
                            &graph.nodes[*nid].op,
                            &graph.inputs_of(*nid),
                            opts.chunk_size,
                            false,
                        ),
                    ));
                    prev = *nid;
                }
                ReadPathStep::Broadcast { join_id, right_src } => {
                    let Op::Join {
                        left_keys, kind, ..
                    } = &graph.nodes[*join_id].op
                    else {
                        return None;
                    };
                    let (right, table, rk) = &rights
                        .iter()
                        .find(|(id, _)| id == right_src)
                        .expect("right side")
                        .1;
                    let op: Box<dyn Operator> = Box::new(operators::BroadcastProbe::new(
                        left_keys.clone(),
                        *kind,
                        right.clone(),
                        table.clone(),
                        rk.clone(),
                        used.clone(),
                    ));
                    ops.push((*join_id, prev, op));
                    prev = *join_id;
                }
            }
        }
        let mut level = vec![ch0];
        for (nid, from, op) in ops.iter_mut() {
            let mut ctx = OpCtx {
                label: label_of(graph, *nid),
                errors: &mut serrors,
                next_chunk_id: &mut sid,
            };
            let mut out = Vec::new();
            for c in std::mem::take(&mut level) {
                out.extend(op.process(*from, c, &mut ctx));
            }
            out.extend(op.finish(&mut ctx));
            level = out;
        }
        let sample = level.first()?;
        let in_schema = sample.schema.clone();
        let safe = operators::group_parallel_safe(&aggs, |name| {
            in_schema.index_of(name).map(|i| in_schema.fields[i].dtype)
        });
        if !safe {
            return None;
        }
        preface0 = cols0;
    }
    let t_scratch = t0.elapsed();

    // 6) Workers: one file each, in waves of ≤ core count. Worker 0 receives
    // the safety-check chunk's columns as a preface (decoded once, above).
    let mut partials: Vec<Option<PartialGroup>> = (0..opened.len()).map(|_| None).collect();
    let opened_files: Vec<(String, rivus_core::Schema, operators::FileDecoder)> = opened;
    {
        let mut preface0 = Some(preface0);
        let mut wave: Vec<(usize, (String, rivus_core::Schema, operators::FileDecoder))> =
            Vec::new();
        let mut it = opened_files.into_iter().enumerate();
        loop {
            wave.clear();
            for _ in 0..threads {
                match it.next() {
                    Some(x) => wave.push(x),
                    None => break,
                }
            }
            if wave.is_empty() {
                break;
            }
            let results: Vec<(usize, PartialGroup)> = std::thread::scope(|s| {
                let union = &union;
                let uschema = &uschema;
                let fname = fname.as_deref();
                let rights = &rights;
                let read_label = read_label.as_str();
                let handles: Vec<_> = wave
                    .drain(..)
                    .map(|(idx, (uri, schema, dec))| {
                        let preface = if idx == 0 { preface0.take() } else { None };
                        let keep = &used;
                        let fsp = &fused_sp;
                        s.spawn(move || {
                            (
                                idx,
                                worker_read_partial_group(
                                    graph, opts, shape, &uri, &schema, dec, preface, keep, fsp,
                                    union, uschema, fname, provenance, rights, read_label,
                                ),
                            )
                        })
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });
            for (idx, r) in results {
                partials[idx] = Some(r);
            }
        }
    }

    let t_workers = t0.elapsed();

    // 7) Merge partials in uri order; then the serial tail on the tiny result.
    let mut partials_iter = partials.into_iter().map(|p| p.expect("worker ran"));
    let (mut merged, mut errors, mut total_rows) = partials_iter.next()?;
    let mut workers = vec![WorkerTelemetry {
        worker: 0,
        rows_out: total_rows,
        busy: Duration::ZERO,
    }];
    for (i, (g, errs, rows)) in partials_iter.enumerate() {
        merged.merge_from(g);
        errors.extend(errs);
        total_rows += rows;
        workers.push(WorkerTelemetry {
            worker: i + 1,
            rows_out: rows,
            busy: Duration::ZERO,
        });
    }
    // Prepend discovery/right-side/quarantine events so the stream reads in
    // graph order like the serial run.
    let mut all_errors = pre_errors;
    all_errors.extend(quarantined);
    all_errors.extend(errors);
    let mut errors = all_errors;

    let mut fin_id = 0u64;
    let mut level = {
        let mut ctx = OpCtx {
            label: label_of(graph, shape.group_id),
            errors: &mut errors,
            next_chunk_id: &mut fin_id,
        };
        merged.finish(&mut ctx)
    };
    let group_rows_out: u64 = level.iter().map(|c| c.len as u64).sum();

    let mut outputs: Vec<Output> = Vec::new();
    let mut wrote_sink = false;
    for &nid in &shape.tail {
        if let Op::Sink { .. } = &graph.nodes[nid].op {
            if let Some((path, result, eval_fails)) = write_sink(&graph.nodes[nid].op, &level) {
                if eval_fails > 0 {
                    errors.push(route_eval_event(eval_fails, label_of(graph, nid)));
                }
                if let Err(e) = result {
                    errors.push(
                        ErrorEvent::new(
                            Severity::Critical,
                            ErrorScope::Graph,
                            format!("cannot write '{path}': {e}"),
                        )
                        .at_node(label_of(graph, nid)),
                    );
                }
            }
            wrote_sink = true;
        } else {
            let mut op = operators::build(
                &graph.nodes[nid].op,
                &graph.inputs_of(nid),
                opts.chunk_size,
                false,
            );
            let mut ctx = OpCtx {
                label: label_of(graph, nid),
                errors: &mut errors,
                next_chunk_id: &mut fin_id,
            };
            let mut out = Vec::new();
            for c in std::mem::take(&mut level) {
                out.extend(op.process(nid, c, &mut ctx));
            }
            out.extend(op.finish(&mut ctx));
            level = out;
        }
    }
    if !wrote_sink {
        let last = shape.tail.iter().copied().last().unwrap_or(shape.group_id);
        outputs.push(Output {
            node_id: last,
            label: graph.nodes[last].label.clone(),
            chunks: level,
        });
    }

    // Telemetry: one entry per node, key counts filled.
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
    telemetry[shape.read_id].rows_out = total_rows;
    telemetry[shape.group_id].rows_in = total_rows;
    telemetry[shape.group_id].rows_out = group_rows_out;

    let final_mode = if errors.iter().any(ErrorEvent::is_fatal) {
        Mode::Halted
    } else {
        Mode::Normal
    };
    let mut res = RunResult {
        telemetry,
        errors,
        final_mode,
        outputs,
        workers,
        first_row_latency: None,
        inference: Vec::new(),
        strategy: None,
    };
    res.strategy = Some("parallel read group-by (per-file workers)".to_string());
    if std::env::var_os("RIVUS_WORKER_PROF").is_some() {
        // Wall between phase boundaries: discovery+rights+open / the one
        // scratch safety chunk / worker waves / merge+serial tail.
        eprintln!(
            "[WPROF-PHASE] open={}ms scratch={}ms workers={}ms merge+tail={}ms",
            t_open.as_millis(),
            (t_scratch - t_open).as_millis(),
            (t_workers - t_scratch).as_millis(),
            (t0.elapsed() - t_workers).as_millis()
        );
    }
    if let Some(h) = hook.as_mut() {
        h(&final_snapshot(&res, shape.discovery_id, t0.elapsed()));
    }
    Some(res)
}

/// Slice 10（統括指示: 勝ちやすいパターンだけではダメ）: a parallelizable
/// **read→sink** flow — the pure-ETL shape `ls → read → [stateless]* →
/// (⋈ small-source)* → save file.csv` with NO group. Each worker streams ONE
/// file through its pipeline and encodes rows into a **per-file temp segment**
/// (headerless, the same `write_cell` formatter as the serial sink); the
/// finalize step writes the header and concatenates the segments **in uri
/// order** — the bytes are identical to the serial run (same formatter, same
/// row order), with bounded memory (a chunk + a write buffer per worker).
/// One read→sink worker: stream one file through the per-worker pipeline and
/// encode surviving rows into a headerless CSV segment (same `write_cell`
/// formatter as the serial sink ⇒ identical bytes). Returns its errors, rows
/// written, and the header line of its output schema (captured from the first
/// chunk it saw — every worker's is identical).
#[allow(clippy::too_many_arguments)]
fn worker_read_to_segment(
    graph: &PlanGraph,
    opts: &RunOptions,
    shape: &ReadGroupShape,
    uri: &str,
    file_schema: &rivus_core::Schema,
    mut dec: operators::FileDecoder,
    union: &[rivus_core::Field],
    uschema: &std::sync::Arc<rivus_core::Schema>,
    fname: Option<&str>,
    provenance: rivus_ir::Provenance,
    rights: &[(NodeId, BroadcastRight)],
    read_label: &str,
    seg: &str,
    delim: u8,
) -> (Vec<ErrorEvent>, u64, Option<String>) {
    use std::io::Write as _;
    let mut errors = Vec::new();
    let mut next_id = 0u64;
    let mut ops: Vec<(NodeId, NodeId, Box<dyn Operator>)> = Vec::new();
    let mut prev = shape.read_id;
    for step in &shape.path {
        match step {
            ReadPathStep::Stateless(nid) => {
                let op = operators::build(
                    &graph.nodes[*nid].op,
                    &graph.inputs_of(*nid),
                    opts.chunk_size,
                    false,
                );
                ops.push((*nid, prev, op));
                prev = *nid;
            }
            ReadPathStep::Broadcast { join_id, right_src } => {
                let Op::Join {
                    left_keys, kind, ..
                } = &graph.nodes[*join_id].op
                else {
                    unreachable!("shape detector matched a join");
                };
                let (right, table, rk) = &rights
                    .iter()
                    .find(|(id, _)| id == right_src)
                    .expect("prebuilt right side")
                    .1;
                let op: Box<dyn Operator> = Box::new(operators::BroadcastProbe::new(
                    left_keys.clone(),
                    *kind,
                    right.clone(),
                    table.clone(),
                    rk.clone(),
                    // Sink shape: the saved file needs every column — no pruning.
                    None,
                ));
                ops.push((*join_id, prev, op));
                prev = *join_id;
            }
        }
    }
    let handle = provenance.source(uri);
    let mut rows = 0u64;
    let mut header: Option<String> = None;
    let file = match std::fs::File::create(seg) {
        Ok(f) => f,
        Err(e) => {
            errors.push(
                ErrorEvent::new(
                    Severity::Critical,
                    ErrorScope::Graph,
                    format!("cannot write segment '{seg}': {e}"),
                )
                .at_node(read_label.to_string()),
            );
            return (errors, 0, None);
        }
    };
    let mut w = std::io::BufWriter::with_capacity(256 * 1024, file);
    let sep = delim as char;
    let mut line = String::new();
    let mut emit = |chunks: Vec<Chunk>,
                    header: &mut Option<String>,
                    rows: &mut u64,
                    errors: &mut Vec<ErrorEvent>| {
        for ch in chunks {
            if header.is_none() {
                *header = Some(format!(
                    "{}\n",
                    ch.schema.field_names().join(&sep.to_string())
                ));
            }
            for row in 0..ch.len {
                line.clear();
                for c in 0..ch.columns.len() {
                    if c > 0 {
                        line.push(sep);
                    }
                    operators::write_cell(&mut line, &ch.columns[c], row, delim);
                }
                line.push('\n');
                if let Err(e) = w.write_all(line.as_bytes()) {
                    errors.push(
                        ErrorEvent::new(
                            Severity::Critical,
                            ErrorScope::Graph,
                            format!("segment write failed: {e}"),
                        )
                        .at_node(read_label.to_string()),
                    );
                    return;
                }
                *rows += 1;
            }
        }
    };
    let run_level = |ops: &mut [(NodeId, NodeId, Box<dyn Operator>)],
                     start_idx: usize,
                     start: Vec<Chunk>,
                     errors: &mut Vec<ErrorEvent>,
                     next_id: &mut u64|
     -> Vec<Chunk> {
        let mut level = start;
        for (nid, from, op) in ops.iter_mut().skip(start_idx) {
            let mut out = Vec::new();
            let mut ctx = OpCtx {
                label: label_of(graph, *nid),
                errors,
                next_chunk_id: next_id,
            };
            for c in level {
                out.extend(op.process(*from, c, &mut ctx));
            }
            level = out;
        }
        level
    };
    let mut t_dec = std::time::Duration::ZERO;
    let mut t_rec = std::time::Duration::ZERO;
    let mut t_ops = std::time::Duration::ZERO;
    let mut t_emit = std::time::Duration::ZERO;
    loop {
        let t0 = Instant::now();
        let Some(cols) = dec.next_chunk() else { break };
        t_dec += t0.elapsed();
        let id = next_id;
        next_id += 1;
        let t1 = Instant::now();
        let ch =
            operators::reconcile_chunk(union, uschema, fname, &handle, uri, file_schema, cols, id);
        t_rec += t1.elapsed();
        let t2 = Instant::now();
        let out = run_level(&mut ops, 0, vec![ch], &mut errors, &mut next_id);
        t_ops += t2.elapsed();
        let t3 = Instant::now();
        emit(out, &mut header, &mut rows, &mut errors);
        t_emit += t3.elapsed();
    }
    if std::env::var_os("RIVUS_WORKER_PROF").is_some() {
        eprintln!(
            "[WPROF-SINK] {uri}: decode={}ms reconcile={}ms ops={}ms emit={}ms",
            t_dec.as_millis(),
            t_rec.as_millis(),
            t_ops.as_millis(),
            t_emit.as_millis()
        );
    }
    for i in 0..ops.len() {
        let fin = {
            let (nid, _, op) = &mut ops[i];
            let mut ctx = OpCtx {
                label: label_of(graph, *nid),
                errors: &mut errors,
                next_chunk_id: &mut next_id,
            };
            op.finish(&mut ctx)
        };
        if !fin.is_empty() {
            let out = run_level(&mut ops, i + 1, fin, &mut errors, &mut next_id);
            emit(out, &mut header, &mut rows, &mut errors);
        }
    }
    let _ = w.flush();
    let bad = dec.bad_rows();
    if bad > 0 {
        errors.push(
            ErrorEvent::new(
                Severity::Recoverable,
                ErrorScope::Item,
                format!("read '{uri}': {bad} malformed row(s) skipped"),
            )
            .at_node(read_label.to_string()),
        );
    }
    (errors, rows, header)
}

fn try_parallel_read_sink(
    graph: &PlanGraph,
    opts: &RunOptions,
    shape: &ReadGroupShape,
    mut hook: Option<&mut (dyn FnMut(&RuntimeSnapshot) + '_)>,
) -> Option<RunResult> {
    let t0 = Instant::now();
    let threads = std::thread::available_parallelism()
        .map(|t| t.get())
        .unwrap_or(1);
    if threads < 2 || std::env::var_os("RIVUS_NO_PARALLEL").is_some() {
        return None;
    }
    // Only the plain fixed-path CSV sink in this slice (jsonl/json/partitioned
    // routes fall back to serial).
    let sink_id = shape.group_id; // for this shape, `group_id` slot holds the sink
    let Op::Sink {
        route: rivus_ir::Route::Fixed(out_path),
        codec: SinkCodec::Csv { delim },
        ..
    } = &graph.nodes[sink_id].op
    else {
        return None;
    };
    if out_path == "-" {
        return None;
    }
    let Op::Read { fmt, provenance } = &graph.nodes[shape.read_id].op else {
        return None;
    };
    let (fmt, provenance, delim) = (*fmt, *provenance, *delim);
    let read_label = label_of(graph, shape.read_id);

    // Discovery + broadcast right sides + lazy opens + union: identical to the
    // read→group runner.
    let mut pre_errors: Vec<ErrorEvent> = Vec::new();
    let mut pre_id = 0u64;
    let mut uris: Vec<String> = Vec::new();
    {
        let mut op = operators::build(
            &graph.nodes[shape.discovery_id].op,
            &[],
            opts.chunk_size,
            false,
        );
        let mut ctx = OpCtx {
            label: label_of(graph, shape.discovery_id),
            errors: &mut pre_errors,
            next_chunk_id: &mut pre_id,
        };
        while let Some(ch) = op.pull(&mut ctx) {
            uris_of_chunk(&ch, &mut uris);
        }
    }
    if uris.is_empty() || pre_errors.iter().any(ErrorEvent::is_fatal) {
        return None;
    }
    uris.sort();
    let total_bytes: u64 = uris
        .iter()
        .filter_map(|u| std::fs::metadata(u).ok().map(|m| m.len()))
        .sum();
    if total_bytes < parallel_min_bytes_for(opts.memory) {
        return None;
    }
    let mut rights: Vec<(NodeId, BroadcastRight)> = Vec::new();
    for step in &shape.path {
        if let ReadPathStep::Broadcast { join_id, right_src } = step {
            let mut op = operators::build(&graph.nodes[*right_src].op, &[], opts.chunk_size, false);
            let mut chunks = Vec::new();
            let mut ctx = OpCtx {
                label: label_of(graph, *right_src),
                errors: &mut pre_errors,
                next_chunk_id: &mut pre_id,
            };
            while let Some(ch) = op.pull(&mut ctx) {
                chunks.push(ch);
            }
            let Op::Join { right_keys, .. } = &graph.nodes[*join_id].op else {
                return None;
            };
            let built = operators::BroadcastProbe::build_right(&chunks, right_keys)?;
            rights.push((*right_src, built));
        }
    }
    if pre_errors.iter().any(ErrorEvent::is_fatal) {
        return None;
    }
    let planner = operators::Read::new(fmt, provenance, opts.chunk_size);
    let mut opened: Vec<(String, rivus_core::Schema, operators::FileDecoder)> = Vec::new();
    let mut quarantined: Vec<ErrorEvent> = Vec::new();
    {
        let mut slots: Vec<Option<Result<(rivus_core::Schema, operators::FileDecoder), String>>> =
            (0..uris.len()).map(|_| None).collect();
        for (wave_start, wave) in uris
            .chunks(threads)
            .enumerate()
            .map(|(w, c)| (w * threads, c))
        {
            let planner = &planner;
            let wave_results: Vec<_> = std::thread::scope(|s| {
                let handles: Vec<_> = wave
                    .iter()
                    .map(|uri| s.spawn(move || planner.open_file_stream(uri)))
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });
            for (i, r) in wave_results.into_iter().enumerate() {
                slots[wave_start + i] = Some(r);
            }
        }
        for (uri, slot) in uris.iter().zip(slots) {
            match slot.expect("slot filled") {
                Ok((schema, dec)) => opened.push((uri.clone(), schema, dec)),
                Err(e) => quarantined.push(
                    ErrorEvent::new(
                        Severity::Recoverable,
                        ErrorScope::Item,
                        format!("read: skipped '{uri}': {e}"),
                    )
                    .at_node(read_label.clone()),
                ),
            }
        }
    }
    if opened.is_empty() {
        return None;
    }
    let (union, fname) = operators::union_by_name(opened.iter().map(|(_, s, _)| s), provenance);
    let uschema = std::sync::Arc::new(rivus_core::Schema::new(union.clone()));

    // Workers: stream file → ops → encode rows into a temp segment.
    let seg_path = |i: usize| format!("{out_path}.rivus-part-{i:04}");
    let mut worker_results: Vec<Option<SegmentResult>> = (0..opened.len()).map(|_| None).collect();
    {
        let mut wave: Vec<(usize, (String, rivus_core::Schema, operators::FileDecoder))> =
            Vec::new();
        let mut it = opened.into_iter().enumerate();
        loop {
            wave.clear();
            for _ in 0..threads {
                match it.next() {
                    Some(x) => wave.push(x),
                    None => break,
                }
            }
            if wave.is_empty() {
                break;
            }
            let results: Vec<(usize, SegmentResult)> = std::thread::scope(|s| {
                let union = &union;
                let uschema = &uschema;
                let fname = fname.as_deref();
                let rights = &rights;
                let read_label = read_label.as_str();
                let seg_path = &seg_path;
                let handles: Vec<_> = wave
                    .drain(..)
                    .map(|(idx, (uri, schema, dec))| {
                        s.spawn(move || {
                            (
                                idx,
                                worker_read_to_segment(
                                    graph,
                                    opts,
                                    shape,
                                    &uri,
                                    &schema,
                                    dec,
                                    union,
                                    uschema,
                                    fname,
                                    provenance,
                                    rights,
                                    read_label,
                                    &seg_path(idx),
                                    delim,
                                ),
                            )
                        })
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });
            for (idx, r) in results {
                worker_results[idx] = Some(r);
            }
        }
    }

    // Finalize: header + segments in uri order → the target (byte-identical to
    // the serial writer: same formatter, same row order).
    let mut errors = pre_errors;
    errors.extend(quarantined);
    let mut total_rows = 0u64;
    let mut header: Option<String> = None;
    let nseg = worker_results.len();
    for r in worker_results.into_iter() {
        let (errs, rows, hdr) = r.expect("worker ran");
        errors.extend(errs);
        total_rows += rows;
        if header.is_none() {
            header = hdr; // first worker (uri order) that saw a chunk
        }
    }
    // Header + segments in uri order. No chunk anywhere → an empty file,
    // exactly like the serial `write_csv_file` with no chunks.
    let write_res: std::io::Result<()> = (|| {
        use std::io::Write as _;
        let p = crate::transport::adjust_path(out_path);
        let f = std::fs::File::create(p)?;
        let mut w = std::io::BufWriter::with_capacity(256 * 1024, f);
        if let Some(h) = &header {
            w.write_all(h.as_bytes())?;
        }
        for i in 0..nseg {
            if let Ok(mut rf) = std::fs::File::open(seg_path(i)) {
                std::io::copy(&mut rf, &mut w)?;
            }
        }
        w.flush()
    })();
    for i in 0..nseg {
        let _ = std::fs::remove_file(seg_path(i));
    }
    if let Err(e) = write_res {
        errors.push(
            ErrorEvent::new(
                Severity::Critical,
                ErrorScope::Graph,
                format!("cannot write '{out_path}': {e}"),
            )
            .at_node(label_of(graph, sink_id)),
        );
    }

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
    telemetry[shape.read_id].rows_out = total_rows;
    telemetry[sink_id].rows_in = total_rows;
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
        workers: Vec::new(),
        first_row_latency: None,
        inference: Vec::new(),
        strategy: None,
    };
    res.strategy = Some("parallel read sink (per-file segments)".to_string());
    if let Some(h) = hook.as_mut() {
        h(&final_snapshot(&res, shape.discovery_id, t0.elapsed()));
    }
    Some(res)
}

/// Slice 6（統括指示: 負けるな・全てが流れ）: a parallelizable **read→group**
/// flow — `ls → read → [stateless]* → (⋈ small-source)* → group → [sort]* →
/// [sink]`. Each worker streams ONE file (schema known up front, rows on
/// demand) through its own copy of the pipeline into a partial `GroupBy`;
/// partials merge exactly like the byte-range parallel group-by (#41), so the
/// group output is byte-identical to the serial path (associative lanes only,
/// checked). A broadcast join's right side (a bare bounded source) is
/// materialized once and pre-fed to every worker's join instance.
struct ReadGroupShape {
    discovery_id: NodeId,
    read_id: NodeId,
    path: Vec<ReadPathStep>,
    group_id: NodeId,
    /// group → … → leaf inclusive (sorts, then an optional sink); run serially
    /// on the merged group output (16 rows, not 10M — serial is right here).
    tail: Vec<NodeId>,
}

enum ReadPathStep {
    Stateless(NodeId),
    Broadcast { join_id: NodeId, right_src: NodeId },
}

/// Slice 10 shape: `ls → read → [stateless/broadcast-join]* → sink` (a leaf
/// fixed-path sink, NO group). Reuses [`ReadGroupShape`] with the sink node in
/// the `group_id` slot and an empty tail.
fn eligible_read_sink_flow(graph: &PlanGraph) -> Option<ReadGroupShape> {
    let mut read = None;
    let mut sink = None;
    for n in &graph.nodes {
        match &n.op {
            Op::Read { .. } => {
                if read.is_some() {
                    return None;
                }
                read = Some(n.id);
            }
            Op::Sink { .. } => {
                if sink.is_some() {
                    return None;
                }
                sink = Some(n.id);
            }
            Op::GroupBy { .. } => return None, // the group runner owns that shape
            _ => {}
        }
    }
    let (read_id, sink_id) = (read?, sink?);
    if !graph.outputs_of(sink_id).is_empty() {
        return None;
    }
    let ins = graph.inputs_of(read_id);
    if ins.len() != 1 {
        return None;
    }
    let disc = ins[0];
    let Op::Source { discovery, .. } = &graph.nodes[disc].op else {
        return None;
    };
    if discovery.is_unbounded()
        || graph.outputs_of(disc).len() != 1
        || !graph.inputs_of(disc).is_empty()
    {
        return None;
    }
    let mut path = Vec::new();
    let mut cur = read_id;
    loop {
        let outs = graph.outputs_of(cur);
        if outs.len() != 1 {
            return None;
        }
        let next = outs[0];
        if next == sink_id {
            break;
        }
        match &graph.nodes[next].op {
            Op::Join {
                kind: rivus_ir::JoinKind::Inner | rivus_ir::JoinKind::Left,
                ..
            } => {
                let jins = graph.inputs_of(next);
                if jins.len() != 2 || jins[0] != cur {
                    return None;
                }
                let right = jins[1];
                let Op::Source { discovery: d2, .. } = &graph.nodes[right].op else {
                    return None;
                };
                if d2.is_unbounded()
                    || graph.outputs_of(right).len() != 1
                    || !graph.inputs_of(right).is_empty()
                {
                    return None;
                }
                path.push(ReadPathStep::Broadcast {
                    join_id: next,
                    right_src: right,
                });
            }
            op if pre_group_op_allowed(op) => path.push(ReadPathStep::Stateless(next)),
            _ => return None,
        }
        cur = next;
    }
    let rights = path
        .iter()
        .filter(|s| matches!(s, ReadPathStep::Broadcast { .. }))
        .count();
    if graph.nodes.len() != 2 + path.len() + rights + 1 {
        return None;
    }
    Some(ReadGroupShape {
        discovery_id: disc,
        read_id,
        path,
        group_id: sink_id,
        tail: Vec::new(),
    })
}

fn eligible_read_group_flow(graph: &PlanGraph) -> Option<ReadGroupShape> {
    let (mut read, mut group) = (None, None);
    for n in &graph.nodes {
        match &n.op {
            Op::Read { .. } => {
                if read.is_some() {
                    return None;
                }
                read = Some(n.id);
            }
            Op::GroupBy { .. } => {
                if group.is_some() {
                    return None;
                }
                group = Some(n.id);
            }
            _ => {}
        }
    }
    let (read_id, group_id) = (read?, group?);
    // read's single input: a bounded discovery source feeding only read.
    let ins = graph.inputs_of(read_id);
    if ins.len() != 1 {
        return None;
    }
    let disc = ins[0];
    let Op::Source { discovery, .. } = &graph.nodes[disc].op else {
        return None;
    };
    if discovery.is_unbounded()
        || graph.outputs_of(disc).len() != 1
        || !graph.inputs_of(disc).is_empty()
    {
        return None;
    }
    // read → group: a single-consumer chain of allowlisted stateless ops and
    // broadcast-able joins (read side LEFT, kind inner/left, right = bare
    // bounded source consumed only by this join).
    let mut path = Vec::new();
    let mut cur = read_id;
    loop {
        let outs = graph.outputs_of(cur);
        if outs.len() != 1 {
            return None;
        }
        let next = outs[0];
        if next == group_id {
            break;
        }
        match &graph.nodes[next].op {
            Op::Join {
                kind: rivus_ir::JoinKind::Inner | rivus_ir::JoinKind::Left,
                ..
            } => {
                let jins = graph.inputs_of(next);
                if jins.len() != 2 || jins[0] != cur {
                    return None; // the streamed (read) side must be the LEFT input
                }
                let right = jins[1];
                let Op::Source { discovery: d2, .. } = &graph.nodes[right].op else {
                    return None;
                };
                if d2.is_unbounded()
                    || graph.outputs_of(right).len() != 1
                    || !graph.inputs_of(right).is_empty()
                {
                    return None;
                }
                path.push(ReadPathStep::Broadcast {
                    join_id: next,
                    right_src: right,
                });
            }
            op if pre_group_op_allowed(op) => path.push(ReadPathStep::Stateless(next)),
            _ => return None,
        }
        cur = next;
    }
    if graph.inputs_of(group_id).as_slice() != [cur] {
        return None;
    }
    // group tail: sorts then an optional leaf sink, single-consumer throughout.
    let mut tail = Vec::new();
    let mut t = group_id;
    loop {
        match graph.outputs_of(t).as_slice() {
            [] => break,
            [n] => {
                let n = *n;
                match &graph.nodes[n].op {
                    Op::Sort { .. } => {
                        tail.push(n);
                        t = n;
                    }
                    Op::Sink { .. } => {
                        if !graph.outputs_of(n).is_empty() {
                            return None;
                        }
                        tail.push(n);
                        break;
                    }
                    _ => return None,
                }
            }
            _ => return None,
        }
    }
    // No stray nodes: disc + read + path (+ a right source per join) + group + tail.
    let rights = path
        .iter()
        .filter(|s| matches!(s, ReadPathStep::Broadcast { .. }))
        .count();
    if graph.nodes.len() != 2 + path.len() + rights + 1 + tail.len() {
        return None;
    }
    Some(ReadGroupShape {
        discovery_id: disc,
        read_id,
        path,
        group_id,
        tail,
    })
}

/// Extract resource uris from a discovery chunk (mirrors `Read::process`).
fn uris_of_chunk(chunk: &Chunk, out: &mut Vec<String>) {
    let ci = match chunk.schema.index_of("path") {
        Some(i) if chunk.schema.fields[i].dtype == DataType::Resource => Some(i),
        _ => chunk
            .schema
            .fields
            .iter()
            .position(|f| f.dtype == DataType::Resource),
    };
    if let Some(ci) = ci {
        for r in 0..chunk.len {
            if let rivus_core::Value::Resource(res) = chunk.value(r, ci) {
                out.push(res.uri().to_string());
            }
        }
    }
}

// ------------- design/41 Stage A: fused read->join->group worker loop -------------

/// One projected cell in the fused loop, resolved once per worker. `Coalesce*`
/// is restricted to **Str-lane** columns (checked at resolution), so the
/// coalesced cell is exactly "the borrowed str, or the literal" — the same
/// bytes the columnar `eval_column(coalesce(col, "lit"))` produces.
enum FusedCell {
    Left(usize),
    Right(usize),
    CoalesceLeft(usize, String),
    CoalesceRight(usize, String),
    LitStr(String),
}

/// The graph-level half of fused eligibility (design/41 Stage A): exactly one
/// broadcast join; after it only predicate-only filters and at most one
/// TRAILING `ProjectExpr`. Predicates are `Compare`/`And` over bare column
/// refs and literals only — no `Cast`/`Arith`/`Func` — so the interpreter's
/// cast-fail counter provably stays 0 and evaluating a pred once per LEFT row
/// (instead of once per joined row) is unobservable. Group keys must be bare.
/// Everything before the join stays on the generic per-op path.
struct FusedShapePlan {
    /// Index of the broadcast step in `shape.path` (ops before it run generic).
    split: usize,
    join_id: NodeId,
    right_src: NodeId,
    preds: Vec<rivus_ir::Expr>,
    items: Option<Vec<(rivus_ir::Expr, String)>>,
}

fn fused_shape_plan(graph: &PlanGraph, shape: &ReadGroupShape) -> Option<FusedShapePlan> {
    use rivus_ir::Expr as E;
    let mut split = None;
    for (i, step) in shape.path.iter().enumerate() {
        if matches!(step, ReadPathStep::Broadcast { .. }) {
            if split.is_some() {
                return None; // one join only in this slice
            }
            split = Some(i);
        }
    }
    let split = split?;
    let ReadPathStep::Broadcast { join_id, right_src } = shape.path[split] else {
        unreachable!("split indexes a broadcast step");
    };
    let mut preds: Vec<E> = Vec::new();
    let mut items = None;
    let last = shape.path.len() - 1;
    for (i, step) in shape.path.iter().enumerate().skip(split + 1) {
        let ReadPathStep::Stateless(nid) = step else {
            return None;
        };
        match &graph.nodes[*nid].op {
            Op::Filter { pred } if items.is_none() => preds.push(pred.clone()),
            Op::FilterProject {
                preds: ps,
                fields: None,
            } if items.is_none() => preds.extend(ps.iter().cloned()),
            Op::ProjectExpr { items: it, .. } if items.is_none() && i == last => {
                items = Some(it.clone());
            }
            _ => return None,
        }
    }
    fn leaf_ok(e: &E) -> bool {
        matches!(e, E::Field { .. } | E::Literal(_))
    }
    fn pred_ok(e: &E) -> bool {
        match e {
            E::Compare { left, right, .. } => leaf_ok(left) && leaf_ok(right),
            E::And(a, b) => pred_ok(a) && pred_ok(b),
            _ => false,
        }
    }
    if !preds.iter().all(pred_ok) {
        return None;
    }
    let Op::GroupBy { keys, .. } = &graph.nodes[shape.group_id].op else {
        return None;
    };
    if keys.iter().any(|k| !k.segs.is_empty()) {
        return None;
    }
    Some(FusedShapePlan {
        split,
        join_id,
        right_src,
        preds,
        items,
    })
}

/// The per-worker half, resolved on the first chunk that reaches the join
/// (schema-dependent). `None` -> this worker falls back to the generic ops.
struct FusedPlan {
    lk: Vec<usize>,
    keeps_left: bool,
    preds: Vec<rivus_ir::Expr>,
    key_cells: Vec<FusedCell>,
    agg_cells: Vec<FusedCell>,
}

fn resolve_fused_plan(
    sp: &FusedShapePlan,
    graph: &PlanGraph,
    shape: &ReadGroupShape,
    left: &rivus_core::Schema,
    right: &Chunk,
    rk: &[usize],
) -> Option<FusedPlan> {
    use rivus_core::{DataType, Value};
    use rivus_ir::Expr as E;
    let Op::Join {
        left_keys, kind, ..
    } = &graph.nodes[sp.join_id].op
    else {
        return None;
    };
    if left_keys.iter().any(|k| !k.segs.is_empty()) {
        return None;
    }
    let mut lk = Vec::with_capacity(left_keys.len());
    for k in left_keys {
        lk.push(left.index_of(&k.root)?);
    }
    // Every predicate column must be a LEFT column: the pred is then evaluated
    // on the left chunk with the SHARED interpreter (zero semantics
    // duplication) and its result is identical for every match of that row.
    fn pred_left_only(e: &E, left: &rivus_core::Schema) -> bool {
        match e {
            E::Field { name, access } => !access.is_column() || left.index_of(name).is_some(),
            E::Literal(_) => true,
            E::Compare {
                left: l, right: r, ..
            } => pred_left_only(l, left) && pred_left_only(r, left),
            E::And(a, b) => pred_left_only(a, left) && pred_left_only(b, left),
            _ => false,
        }
    }
    if !sp.preds.iter().all(|p| pred_left_only(p, left)) {
        return None;
    }
    // name -> cell over the joined-output naming: left columns first, then
    // right non-key columns with the `_r` collision suffix judged against the
    // FULL left schema — exactly `BroadcastProbe`'s schema construction.
    let resolve_name = |name: &str| -> Option<FusedCell> {
        if let Some(ci) = left.index_of(name) {
            return Some(FusedCell::Left(ci));
        }
        for (ci, f) in right.schema.fields.iter().enumerate() {
            if rk.contains(&ci) {
                continue;
            }
            let is_r = left.index_of(&f.name).is_some();
            let matches_out =
                (is_r && name == format!("{}_r", f.name)) || (!is_r && name == f.name);
            if matches_out {
                return Some(FusedCell::Right(ci));
            }
        }
        None
    };
    let cell_of = |e: &E| -> Option<FusedCell> {
        match e {
            E::Field { name, access } if access.is_column() => resolve_name(name),
            E::Literal(Value::Str(s)) => Some(FusedCell::LitStr(s.clone())),
            E::Func {
                func: rivus_ir::Func::Coalesce,
                args,
            } if args.len() == 2 => {
                let E::Field { name, access } = &args[0] else {
                    return None;
                };
                if !access.is_column() {
                    return None;
                }
                let E::Literal(Value::Str(lit)) = &args[1] else {
                    return None;
                };
                match resolve_name(name)? {
                    FusedCell::Left(ci) if left.fields[ci].dtype == DataType::Str => {
                        Some(FusedCell::CoalesceLeft(ci, lit.clone()))
                    }
                    FusedCell::Right(ci) if right.schema.fields[ci].dtype == DataType::Str => {
                        Some(FusedCell::CoalesceRight(ci, lit.clone()))
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    };
    // Group keys / aggs resolve through the projection (alias -> item expr)
    // when one exists, else directly against the joined names.
    let lookup = |name: &str| -> Option<FusedCell> {
        match &sp.items {
            Some(items) => {
                let (e, _) = items.iter().find(|(_, a)| a == name)?;
                cell_of(e)
            }
            None => resolve_name(name),
        }
    };
    let Op::GroupBy { keys, aggs } = &graph.nodes[shape.group_id].op else {
        return None;
    };
    let key_cells: Vec<FusedCell> = keys
        .iter()
        .map(|k| lookup(&k.root))
        .collect::<Option<_>>()?;
    let agg_cells: Vec<FusedCell> = aggs.iter().map(|(_, c)| lookup(c)).collect::<Option<_>>()?;
    Some(FusedPlan {
        lk,
        keeps_left: kind.keeps_left(),
        preds: sp.preds.clone(),
        key_cells,
        agg_cells,
    })
}

/// Append one projected key cell to the composite grouping key — the exact
/// encoding of `push_group_key_field` (`\x00` = null, `\x01` + text = present;
/// Str lanes borrow, other lanes render via the same `Value` `Display`).
fn fused_push_key(
    cell: &FusedCell,
    l: &Chunk,
    li: usize,
    r: &Chunk,
    ri: Option<usize>,
    key: &mut String,
) {
    use rivus_core::ColumnData;
    use std::fmt::Write as _;
    let push_col = |c: &Chunk, ci: usize, row: usize, key: &mut String| {
        if c.columns[ci].is_null(row) {
            key.push('\u{0}');
        } else {
            key.push('\u{1}');
            match c.columns[ci].data() {
                ColumnData::Str(s) => key.push_str(s.get(row)),
                _ => {
                    let _ = write!(key, "{}", c.value(row, ci));
                }
            }
        }
    };
    match cell {
        FusedCell::Left(ci) => push_col(l, *ci, li, key),
        FusedCell::Right(ci) => match ri {
            Some(r_) => push_col(r, *ci, r_, key),
            None => key.push('\u{0}'), // left join: null-padded right cell
        },
        FusedCell::CoalesceLeft(ci, lit) => {
            key.push('\u{1}');
            match l.columns[*ci].data() {
                ColumnData::Str(s) if !l.columns[*ci].is_null(li) => key.push_str(s.get(li)),
                _ => key.push_str(lit),
            }
        }
        FusedCell::CoalesceRight(ci, lit) => {
            key.push('\u{1}');
            match ri {
                Some(r_) if !r.columns[*ci].is_null(r_) => match r.columns[*ci].data() {
                    ColumnData::Str(s) => key.push_str(s.get(r_)),
                    _ => key.push_str(lit),
                },
                _ => key.push_str(lit),
            }
        }
        FusedCell::LitStr(sv) => {
            key.push('\u{1}');
            key.push_str(sv);
        }
    }
}

/// The projected cell as a `Value` (null-aware) — feeds `AggAcc::observe` and
/// the first-seen group's key-parts rendering, matching what the generic path
/// reads off the projected chunk.
fn fused_value(
    cell: &FusedCell,
    l: &Chunk,
    li: usize,
    r: &Chunk,
    ri: Option<usize>,
) -> rivus_core::Value {
    use rivus_core::Value;
    match cell {
        FusedCell::Left(ci) => l.value(li, *ci),
        FusedCell::Right(ci) => match ri {
            Some(r_) => r.value(r_, *ci),
            None => Value::Null,
        },
        FusedCell::CoalesceLeft(ci, lit) => {
            if l.columns[*ci].is_null(li) {
                Value::Str(lit.clone())
            } else {
                l.value(li, *ci)
            }
        }
        FusedCell::CoalesceRight(ci, lit) => match ri {
            Some(r_) if !r.columns[*ci].is_null(r_) => r.value(r_, *ci),
            _ => Value::Str(lit.clone()),
        },
        FusedCell::LitStr(sv) => Value::Str(sv.clone()),
    }
}

/// Reused per-row buffers for the fused loop (no per-row heap).
#[derive(Default)]
struct FusedScratch {
    keybuf: String,
    comp: String,
    vals: Vec<Option<rivus_core::Value>>,
}

/// The fused row loop for one post-prefix chunk: join-key probe -> (left-only)
/// filter -> composite group key + agg values straight off the source lanes ->
/// `GroupBy::observe_row`. No intermediate `Chunk` is ever built; the group
/// state machinery is `GroupBy::process`'s own, so a fused stream is
/// byte-identical to the generic op chain (proven by the fused-vs-generic
/// tests and the standard-fixture `cmp`s).
#[allow(clippy::too_many_arguments)]
fn fused_feed_chunk(
    ch: &Chunk,
    plan: &FusedPlan,
    right: &Chunk,
    table: &operators::JoinTable,
    group: &mut operators::GroupBy,
    rows: &mut u64,
    sc: &mut FusedScratch,
) {
    debug_assert_eq!(plan.agg_cells.len(), group.agg_count());
    let mut predf = 0u64;
    for li in 0..ch.len {
        sc.keybuf.clear();
        let matched = if operators::fill_join_key(ch, &plan.lk, li, &mut sc.keybuf) {
            table.get(sc.keybuf.as_str())
        } else {
            None
        };
        // Left-only predicates: one evaluation per left row covers every
        // match (the joined row's left cells are this row's cells). The
        // eligibility gate excludes cast-capable predicate shapes, so the
        // fail counter provably stays 0 (asserted in debug).
        let passes = |predf: &mut u64| {
            plan.preds
                .iter()
                .all(|p| crate::eval::eval_predicate_acc(p, ch, li, predf))
        };
        let emit = |ri: Option<usize>,
                    group: &mut operators::GroupBy,
                    rows: &mut u64,
                    sc: &mut FusedScratch| {
            *rows += 1;
            sc.comp.clear();
            for (j, cell) in plan.key_cells.iter().enumerate() {
                if j > 0 {
                    sc.comp.push('\u{1f}');
                }
                fused_push_key(cell, ch, li, right, ri, &mut sc.comp);
            }
            sc.vals.clear();
            for cell in &plan.agg_cells {
                sc.vals.push(Some(fused_value(cell, ch, li, right, ri)));
            }
            let parts = || {
                plan.key_cells
                    .iter()
                    .map(|c| fused_value(c, ch, li, right, ri).to_string())
                    .collect::<Vec<String>>()
            };
            group.observe_row(&sc.comp, parts, &sc.vals);
        };
        match matched {
            Some(rs) => {
                if passes(&mut predf) {
                    for &ri in rs {
                        emit(Some(ri), group, rows, sc);
                    }
                }
            }
            None if plan.keeps_left && passes(&mut predf) => {
                emit(None, group, rows, sc);
            }
            None => {}
        }
    }
    debug_assert_eq!(predf, 0, "eligibility excludes cast-capable predicates");
}

/// Per-worker fused-mode state (design/41 Stage A). `plan` resolves on the
/// first chunk that reaches the join; `off` latches a failed resolution so
/// every later chunk takes the generic ops unchanged.
struct FusedState<'a> {
    sp: Option<&'a FusedShapePlan>,
    /// (right chunk, hash table, right key indices) of the fused join.
    ctx: Option<(&'a Chunk, &'a operators::JoinTable, &'a [usize])>,
    plan: Option<FusedPlan>,
    off: bool,
    scratch: FusedScratch,
    t_fused: std::time::Duration,
}

/// Push chunks from `start_idx` onward; the group consumes the survivors.
/// In fused mode the ops from the join onward are BYPASSED: rows go straight
/// from the join probe into `GroupBy::observe_row` (see `fused_feed_chunk`).
/// A worker whose first join-input chunk fails plan resolution latches
/// `off` and runs the generic chain — including that same chunk — so the
/// fallback is never lossy.
#[allow(clippy::too_many_arguments)]
fn worker_feed(
    graph: &PlanGraph,
    shape: &ReadGroupShape,
    ops: &mut [(NodeId, NodeId, Box<dyn Operator>)],
    start_idx: usize,
    start: Vec<Chunk>,
    group: &mut operators::GroupBy,
    errors: &mut Vec<ErrorEvent>,
    next_id: &mut u64,
    rows: &mut u64,
    t_ops: &mut [std::time::Duration],
    fu: &mut FusedState<'_>,
) {
    let mut level = start;
    let mut i = start_idx;
    let fused_at = match (&fu.sp, fu.off) {
        (Some(sp), false) => sp.split,
        _ => usize::MAX,
    };
    while i < ops.len() && i < fused_at {
        let (nid, from, op) = &mut ops[i];
        let t = Instant::now();
        let mut out = Vec::new();
        let mut ctx = OpCtx {
            label: label_of(graph, *nid),
            errors,
            next_chunk_id: next_id,
        };
        for c in level {
            out.extend(op.process(*from, c, &mut ctx));
        }
        level = out;
        t_ops[i] += t.elapsed();
        i += 1;
    }
    if i == fused_at && i < ops.len() {
        let (right, table, rk) = fu.ctx.expect("fused ctx present when sp is");
        let sp = fu.sp.expect("checked above");
        let mut fallback: Vec<Chunk> = Vec::new();
        for c in level {
            if fu.plan.is_none() && !fu.off {
                match resolve_fused_plan(sp, graph, shape, &c.schema, right, rk) {
                    Some(p) => fu.plan = Some(p),
                    None => fu.off = true,
                }
            }
            if let (Some(plan), false) = (&fu.plan, fu.off) {
                let t = Instant::now();
                fused_feed_chunk(&c, plan, right, table, group, rows, &mut fu.scratch);
                fu.t_fused += t.elapsed();
            } else {
                fallback.push(c);
            }
        }
        if fallback.is_empty() {
            return;
        }
        level = fallback;
        while i < ops.len() {
            let (nid, from, op) = &mut ops[i];
            let t = Instant::now();
            let mut out = Vec::new();
            let mut ctx = OpCtx {
                label: label_of(graph, *nid),
                errors,
                next_chunk_id: next_id,
            };
            for c in level {
                out.extend(op.process(*from, c, &mut ctx));
            }
            level = out;
            t_ops[i] += t.elapsed();
            i += 1;
        }
    }
    let t = Instant::now();
    let mut ctx = OpCtx {
        label: label_of(graph, shape.group_id),
        errors,
        next_chunk_id: next_id,
    };
    for c in level {
        *rows += c.len as u64;
        group.process(shape.group_id, c, &mut ctx);
    }
    *t_ops.last_mut().expect("group slot") += t.elapsed();
}

/// One worker: stream one file (lazy decoder) through reconcile → the path ops
/// (a broadcast join is pre-fed its materialized right side) into a partial
/// `GroupBy` — or, when the fused plan resolves, straight from the join probe
/// into `GroupBy::observe_row` with no intermediate chunk (design/41 Stage A).
#[allow(clippy::too_many_arguments)]
fn worker_read_partial_group(
    graph: &PlanGraph,
    opts: &RunOptions,
    shape: &ReadGroupShape,
    uri: &str,
    file_schema: &rivus_core::Schema,
    mut dec: operators::FileDecoder,
    // Columns already decoded from this file (the parallel-safety sample
    // chunk): processed FIRST, so the stream is the whole file in order.
    preface: Option<Vec<rivus_core::Column>>,
    // Probe projection pushdown set (design/41 Stage A-1); `None` = keep all.
    keep: &Option<Vec<String>>,
    // Fused-loop shape plan (design/41 Stage A); `None` = generic ops only.
    fused_sp: &Option<FusedShapePlan>,
    union: &[rivus_core::Field],
    uschema: &std::sync::Arc<rivus_core::Schema>,
    fname: Option<&str>,
    provenance: rivus_ir::Provenance,
    rights: &[(NodeId, BroadcastRight)],
    read_label: &str,
) -> (operators::GroupBy, Vec<ErrorEvent>, u64) {
    let mut errors = Vec::new();
    let mut next_id = 0u64;
    // Per-worker op instances, with each broadcast join pre-fed its right side.
    let mut ops: Vec<(NodeId, NodeId, Box<dyn Operator>)> = Vec::new(); // (node, from, op)
    let mut prev = shape.read_id;
    for step in &shape.path {
        match step {
            ReadPathStep::Stateless(nid) => {
                let op = operators::build(
                    &graph.nodes[*nid].op,
                    &graph.inputs_of(*nid),
                    opts.chunk_size,
                    false,
                );
                ops.push((*nid, prev, op));
                prev = *nid;
            }
            ReadPathStep::Broadcast { join_id, right_src } => {
                // Streaming prober: probes each arriving chunk against the
                // shared prebuilt right side and emits immediately — no left
                // buffering, no drain-time concat (which dominated the worker
                // profile). Byte-identical to the blocking join (see
                // `BroadcastProbe`).
                let Op::Join {
                    left_keys, kind, ..
                } = &graph.nodes[*join_id].op
                else {
                    unreachable!("shape detector matched a join");
                };
                let (right, table, rk) = &rights
                    .iter()
                    .find(|(id, _)| id == right_src)
                    .expect("prebuilt right side")
                    .1;
                let op: Box<dyn Operator> = Box::new(operators::BroadcastProbe::new(
                    left_keys.clone(),
                    *kind,
                    right.clone(),
                    table.clone(),
                    rk.clone(),
                    keep.clone(),
                ));
                ops.push((*join_id, prev, op));
                prev = *join_id;
            }
        }
    }
    let mut group = operators::new_group(&graph.nodes[shape.group_id].op).expect("group op");
    let mut rows = 0u64;
    let handle = provenance.source(uri);

    // Push chunks from `start_idx` onward; the group consumes the survivors.
    // `t_ops` accumulates per-op wall (one slot per pipeline op, last = the
    // group) for the WPROF breakdown — a handful of `Instant` reads per chunk,
    // free at chunk granularity.
    let mut t_ops: Vec<std::time::Duration> = vec![std::time::Duration::ZERO; ops.len() + 1];
    // Fused-mode state: present only when the graph-level gate passed AND the
    // right side of the fused join was prebuilt.
    let mut fu = FusedState {
        sp: fused_sp.as_ref(),
        ctx: fused_sp.as_ref().and_then(|sp| {
            rights
                .iter()
                .find(|(id, _)| *id == sp.right_src)
                .map(|(_, (r, t, rk))| (r.as_ref(), t.as_ref(), rk.as_slice()))
        }),
        plan: None,
        off: fused_sp.is_none(),
        scratch: FusedScratch::default(),
        t_fused: std::time::Duration::ZERO,
    };
    if fu.ctx.is_none() {
        fu.off = true;
    }

    let mut t_dec = std::time::Duration::ZERO;
    let mut t_rec = std::time::Duration::ZERO;
    let mut t_feed = std::time::Duration::ZERO;
    // The pre-decoded safety-sample chunk streams first (file order).
    if let Some(cols) = preface {
        let id = next_id;
        next_id += 1;
        let t1 = Instant::now();
        let ch =
            operators::reconcile_chunk(union, uschema, fname, &handle, uri, file_schema, cols, id);
        t_rec += t1.elapsed();
        let t2 = Instant::now();
        worker_feed(
            graph,
            shape,
            &mut ops,
            0,
            vec![ch],
            &mut group,
            &mut errors,
            &mut next_id,
            &mut rows,
            &mut t_ops,
            &mut fu,
        );
        t_feed += t2.elapsed();
    }
    loop {
        let t0 = Instant::now();
        let Some(cols) = dec.next_chunk() else { break };
        t_dec += t0.elapsed();
        let id = next_id;
        next_id += 1;
        let t1 = Instant::now();
        let ch =
            operators::reconcile_chunk(union, uschema, fname, &handle, uri, file_schema, cols, id);
        t_rec += t1.elapsed();
        let t2 = Instant::now();
        worker_feed(
            graph,
            shape,
            &mut ops,
            0,
            vec![ch],
            &mut group,
            &mut errors,
            &mut next_id,
            &mut rows,
            &mut t_ops,
            &mut fu,
        );
        t_feed += t2.elapsed();
    }
    if std::env::var_os("RIVUS_WORKER_PROF").is_some() {
        let per_op: Vec<String> = ops
            .iter()
            .map(|(nid, _, _)| label_of(graph, *nid))
            .chain(std::iter::once("group".to_string()))
            .zip(t_ops.iter())
            .map(|(l, d)| format!("{l}={}ms", d.as_millis()))
            .collect();
        eprintln!(
            "[WPROF] {uri}: decode={}ms reconcile={}ms feed={}ms [{} fused={}ms{}]",
            t_dec.as_millis(),
            t_rec.as_millis(),
            t_feed.as_millis(),
            per_op.join(" "),
            fu.t_fused.as_millis(),
            if fu.plan.is_some() { " (active)" } else { "" }
        );
    }
    // Drain: cascade each op's finish through the rest of the pipeline (a
    // blocking join emits everything here).
    let t3 = Instant::now();
    for i in 0..ops.len() {
        let fin = {
            let (nid, _, op) = &mut ops[i];
            let mut ctx = OpCtx {
                label: label_of(graph, *nid),
                errors: &mut errors,
                next_chunk_id: &mut next_id,
            };
            op.finish(&mut ctx)
        };
        if !fin.is_empty() {
            worker_feed(
                graph,
                shape,
                &mut ops,
                i + 1,
                fin,
                &mut group,
                &mut errors,
                &mut next_id,
                &mut rows,
                &mut t_ops,
                &mut fu,
            );
        }
    }
    if std::env::var_os("RIVUS_WORKER_PROF").is_some() {
        eprintln!("[WPROF] {uri}: drain={}ms", t3.elapsed().as_millis());
    }
    // Per-file malformed rows AFTER draining (compressed streams accrue while
    // decoding); same message the serial read raises, at the read node.
    let bad = dec.bad_rows();
    if bad > 0 {
        errors.push(
            ErrorEvent::new(
                Severity::Recoverable,
                ErrorScope::Item,
                format!("read '{uri}': {bad} malformed row(s) skipped"),
            )
            .at_node(read_label.to_string()),
        );
    }
    (group, errors, rows)
}

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
            if !matches!(graph.nodes[s].op, Op::Sink { .. }) || !graph.outputs_of(s).is_empty() {
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
    /// (BUG-F) Never-silent notice when a column-naming schema without
    /// `noheader` consumed a data-looking first line; surfaced once like the
    /// serial reader so the parallel error stream matches.
    header_warning: Option<String>,
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
        jtypes: Vec<crate::jsonl::JType>,
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
            ParSource::Jsonl { names, jtypes } => operators::jsonl_range_source(
                &self.path,
                names.clone(),
                jtypes.clone(),
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
        // A columnar container has no byte-range line split — the Parquet
        // source streams row groups serially (downstream transforms still
        // parallelize on the chunk-partition path).
        Codec::Parquet => None,
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
                header_warning: plan.header_warning,
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
            let (schema, names, jtypes, ranges, bad_rows) =
                crate::jsonl::plan_parallel(path, threads)?;
            Some(ParPlan {
                schema: std::sync::Arc::new(schema),
                ranges,
                path: path.to_string(),
                bad_rows,
                header_warning: None,
                src: ParSource::Jsonl { names, jtypes },
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
                header_warning: None,
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
        Codec::Discover { .. } => None,
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
        if let Some((path, result, eval_fails)) = write_sink(&graph.nodes[sink].op, &out_chunks) {
            if eval_fails > 0 {
                res.errors
                    .push(route_eval_event(eval_fails, label_of(graph, sink)));
            }
            if let Err(e) = result {
                res.errors.push(
                    ErrorEvent::new(
                        Severity::Critical,
                        ErrorScope::Graph,
                        format!("cannot write '{path}': {e}"),
                    )
                    .at_node(label_of(graph, sink)),
                );
            }
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
        if let Some((path, result, eval_fails)) =
            write_sink(&graph.nodes[out.node_id].op, &out.chunks)
        {
            if eval_fails > 0 {
                res.errors
                    .push(route_eval_event(eval_fails, label_of(graph, out.node_id)));
            }
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

/// If `op` is a file sink, write `chunks` to it once and return
/// (path, result, computed-placeholder eval failures). Every caller surfaces
/// the eval-fail count via [`route_eval_event`], so the parallel paths report
/// it exactly like the serial operator (never-silent across strategies —
/// review #146; same shape as #145 fix 3).
fn write_sink<'a>(op: &'a Op, chunks: &[Chunk]) -> Option<(&'a str, std::io::Result<()>, u64)> {
    match op {
        Op::Sink { route, codec, .. } => match route {
            rivus_ir::Route::Fixed(path) => {
                let res = match codec {
                    SinkCodec::Csv { delim } => operators::write_csv_file(path, chunks, *delim),
                    SinkCodec::Jsonl => operators::write_jsonl_file(path, chunks),
                    SinkCodec::Json => operators::write_json_file(path, chunks),
                };
                Some((path.as_str(), res, 0))
            }
            // Partitioned route: stream the merged chunks through the same
            // bounded LRU writer the serial operator uses (chunk-wise grouping,
            // no whole-stream gather), so the merge path's peak memory no longer
            // holds a second full copy of the output. Bytes and within-partition
            // order are unchanged (shared formatters + stream order). Every
            // partition is attempted (continue-first) and ALL failures are
            // aggregated so the parallel path surfaces them like the serial
            // operator does (never-silent across strategies, not just the first
            // one).
            rivus_ir::Route::Template {
                template,
                by,
                flat,
                exprs,
            } => {
                let mut eval_fails = 0u64;
                let mut writer = crate::route::RouteWriter::new(*codec);
                for chunk in chunks {
                    let groups = crate::route::group_by_path(
                        std::slice::from_ref(chunk),
                        template,
                        by,
                        *flat,
                        *codec,
                        exprs,
                        &mut eval_fails,
                    );
                    writer.write_groups(groups);
                }
                // A partition that keeps failing fails once per chunk in the
                // streaming writer; the aggregate surface stays one entry per
                // partition (first error wins), as the buffered write reported.
                let mut seen = std::collections::HashSet::new();
                let fails: Vec<(String, std::io::Error)> = writer
                    .finish()
                    .into_iter()
                    .filter(|(p, _)| seen.insert(p.clone()))
                    .collect();
                let res = if fails.is_empty() {
                    Ok(())
                } else {
                    let list: Vec<String> =
                        fails.iter().map(|(p, e)| format!("{p}: {e}")).collect();
                    Err(std::io::Error::other(format!(
                        "{} partition(s) failed: {}",
                        fails.len(),
                        list.join("; ")
                    )))
                };
                Some((template.as_str(), res, eval_fails))
            }
        },
        _ => None,
    }
}

/// The Recoverable event for computed-placeholder eval failures — one shared
/// constructor so the parallel callers surface the same wording as the serial
/// `SinkRoute` operator.
fn route_eval_event(n: u64, label: String) -> ErrorEvent {
    ErrorEvent::new(
        Severity::Recoverable,
        ErrorScope::Item,
        format!(
            "save route: {n} value(s) could not be evaluated in a computed placeholder; \
             routed to the {} partition",
            crate::route::NULL_PARTITION
        ),
    )
    .at_node(label)
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
            if matches!(graph.nodes[i].op, Op::Sink { .. } | Op::SinkPrint) {
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
        if let Some((path, result, eval_fails)) = write_sink(&graph.nodes[node_id].op, &chunks) {
            if eval_fails > 0 {
                errors.push(route_eval_event(eval_fails, label_of(graph, node_id)));
            }
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

/// **Plan Validation Gate** (#191/#195/#200 — the "errors that teach" pass).
/// Plan-time, pre-run, read-only over the IR: catch mistakes the engine would
/// otherwise swallow at runtime, and refuse with guidance (never-silent).
///
/// 1. **Empty program** (#195): zero nodes is a mistake, not a success.
/// 2. **Silent-no-op hooks** (#200): `on error: route X` parses but is not
///    wired — refuse up front instead of ignoring it at runtime.
/// 3. **Unknown bare-column references** (#191) when the input schema is
///    **declared** (static, via `node_schemas` §32.1): a typo like
///    `|? aeg >= 60` becomes a plan error with a "did you mean 'age'?" hint
///    and the available columns — instead of silently filtering every row
///    out. Inferred schemas stay `None` → skipped (honesty rule §32.1); the
///    runtime warns handle those. Nested paths (`user.age`) keep the ratified
///    §32.8③ runtime policy (typed null + counted) and are not checked here.
///
/// Called by [`run`]/[`run_with_progress`] before dispatch, and by the CLI's
/// `check` so a validation-only pass gives the same guidance.
pub fn plan_validate(graph: &PlanGraph) -> Result<(), RivusError> {
    if graph.nodes.is_empty() {
        return Err(RivusError::Build(
            "no flow found (the program is empty) — write at least one scope, e.g. \
             `Name:\n    open file.csv\n;`"
                .into(),
        ));
    }
    for node in &graph.nodes {
        for h in &node.hooks {
            if let HookAction::Route(target) = &h.action {
                return Err(RivusError::Build(format!(
                    "`on {}: route {target}` is not yet implemented (only `transition <mode>` \
                     and `log \"…\"` are wired) — routing error items to a flow is a later \
                     slice; remove the hook or use `log`/`transition`",
                    h.event.as_str()
                )));
            }
        }
    }

    let schemas = graph.node_schemas();
    for node in &graph.nodes {
        let inputs = graph.inputs_of(node.id);
        let schema_of = |k: usize| -> Option<&rivus_core::Schema> {
            inputs.get(k).and_then(|&i| schemas[i].as_ref())
        };
        // (col, input-slot) references this op makes against its input schema(s).
        let mut refs: Vec<(String, usize)> = Vec::new();
        match &node.op {
            Op::Filter { pred } | Op::Validate { pred, .. } => {
                collect_bare_fields(pred, 0, &mut refs)
            }
            Op::FilterProject { preds, fields } => {
                for p in preds {
                    collect_bare_fields(p, 0, &mut refs);
                }
                if let Some(fs) = fields {
                    refs.extend(fs.iter().map(|f| (f.clone(), 0)));
                }
            }
            Op::Project { fields } => refs.extend(fields.iter().map(|f| (f.clone(), 0))),
            Op::ProjectExpr { items, views } => {
                for (e, _) in items {
                    collect_bare_fields(e, 0, &mut refs);
                }
                refs.extend(views.iter().map(|v| (v.col.clone(), 0)));
            }
            Op::GroupBy { keys, aggs } => {
                refs.extend(bare_keys(keys));
                refs.extend(aggs.iter().map(|(_, c)| (c.clone(), 0)));
            }
            Op::Sort { keys } => {
                refs.extend(bare_keys(keys.iter().map(|(k, _)| k)));
            }
            Op::Distinct { keys } => refs.extend(bare_keys(keys)),
            Op::Join {
                left_keys,
                right_keys,
                ..
            } => {
                refs.extend(bare_keys(left_keys));
                refs.extend(bare_keys(right_keys).into_iter().map(|(c, _)| (c, 1)));
            }
            Op::DropNa { cols } | Op::Drop { cols } | Op::Reorder { cols } => {
                refs.extend(cols.iter().map(|c| (c.clone(), 0)))
            }
            Op::Fill { col, .. } => refs.push((col.clone(), 0)),
            Op::Sessionize { ts, by, .. } => {
                refs.push((ts.clone(), 0));
                refs.extend(by.iter().map(|c| (c.clone(), 0)));
            }
            Op::Shift { col, by, .. } => {
                refs.push((col.clone(), 0));
                refs.extend(by.iter().map(|c| (c.clone(), 0)));
            }
            Op::AsofJoin { by, ts, .. } => {
                // ts + by must exist on the left (slot 0); the right side is a
                // separate input stream whose schema the Gate checks at its own
                // node, so only the left references are validated here.
                refs.push((ts.clone(), 0));
                refs.extend(by.iter().map(|c| (c.clone(), 0)));
            }
            Op::Cast { casts } => refs.extend(casts.iter().map(|(c, _)| (c.clone(), 0))),
            Op::Rename { pairs } => refs.extend(pairs.iter().map(|(f, _)| (f.clone(), 0))),
            _ => {}
        }
        for (col, slot) in refs {
            let Some(schema) = schema_of(slot) else {
                continue; // inferred/unknown schema: runtime policy applies
            };
            if schema.index_of(&col).is_some() {
                continue;
            }
            let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
            // A bare aggregate name (`|# d count`) parses as a group *key*, so
            // it lands here as an unknown column — teach the `func:col` form
            // instead of suggesting a lookalike (#191 family).
            let bare_agg_key = rivus_ir::AggFunc::parse(&col).is_some()
                && matches!(&node.op, Op::GroupBy { keys, .. }
                    if keys.iter().any(|k| k.is_bare() && k.root == col));
            let hint = if bare_agg_key {
                format!(
                    " — a bare word in `|#` is a group key; aggregates take the `func:col` \
                     form (e.g. `{col}:price`), and `count` is always emitted"
                )
            } else {
                rivus_core::suggest::suggest_similar(&col, names.iter().copied())
                    .map(|sug| format!(" — did you mean '{sug}'?"))
                    .unwrap_or_default()
            };
            let mut line = node.op.to_src_line();
            if line.chars().count() > 48 {
                line = format!("{}…", line.chars().take(47).collect::<String>());
            }
            return Err(RivusError::Build(format!(
                "unknown column '{col}' in `{line}`{hint} (available: {})",
                names.join(", ")
            )));
        }
    }
    Ok(())
}

/// Bare-field references of an expression (columns only): `Field` with a
/// column access. Positional refs, holes, provenance accessors, sub-views and
/// nested paths are skipped — each has its own resolution policy.
fn collect_bare_fields(e: &Expr, slot: usize, out: &mut Vec<(String, usize)>) {
    match e {
        Expr::Field { name, access } if access.is_column() => out.push((name.clone(), slot)),
        Expr::Field { .. }
        | Expr::FieldAt(_)
        | Expr::SubView { .. }
        | Expr::Path(_)
        | Expr::Literal(_)
        | Expr::Hole(_) => {}
        Expr::Compare { left, right, .. } | Expr::Arith { left, right, .. } => {
            collect_bare_fields(left, slot, out);
            collect_bare_fields(right, slot, out);
        }
        Expr::And(a, b) | Expr::Or(a, b) => {
            collect_bare_fields(a, slot, out);
            collect_bare_fields(b, slot, out);
        }
        Expr::Cast { expr, .. } => collect_bare_fields(expr, slot, out),
        Expr::Func { args, .. } => args.iter().for_each(|a| collect_bare_fields(a, slot, out)),
        Expr::Case { branches, default } => {
            for (c, v) in branches {
                collect_bare_fields(c, slot, out);
                collect_bare_fields(v, slot, out);
            }
            if let Some(d) = default {
                collect_bare_fields(d, slot, out);
            }
        }
    }
}

/// The bare (single-segment) keys of a key list, as slot-0 references. A
/// nested key (`user.age`) keeps the §32.8③ runtime policy and is skipped.
fn bare_keys<'a>(keys: impl IntoIterator<Item = &'a PathExpr>) -> Vec<(String, usize)> {
    keys.into_iter()
        .filter(|k| k.is_bare())
        .map(|k| (k.root.clone(), 0))
        .collect()
}

fn apply_error_hooks(
    graph: &PlanGraph,
    new_errors: &[ErrorEvent],
    mode: &mut Mode,
) -> Vec<ErrorEvent> {
    let mut extra = Vec::new();
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
                match &hook.action {
                    HookAction::Transition(m) => *mode = *m,
                    // `on error: log "…"` (#200): narrate the user's message on
                    // the error stream, once per triggering batch. Info-level —
                    // the log is commentary, not a new failure.
                    HookAction::Log(msg) => extra.push(
                        ErrorEvent::new(Severity::Info, ErrorScope::Chunk, format!("log: {msg}"))
                            .at_node(label_of(graph, node.id)),
                    ),
                    // Route is rejected up front by `plan_validate` (#200);
                    // unreachable here, and deliberately NOT a silent no-op.
                    HookAction::Route(_) => {}
                }
            }
        }
    }
    extra
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
