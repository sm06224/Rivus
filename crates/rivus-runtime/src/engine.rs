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
use crate::telemetry::NodeTelemetry;
use rivus_core::{Chunk, ErrorEvent, ErrorScope, Mode, RivusError, Severity};
use rivus_ir::{HookAction, HookEvent, NodeId, Op, PlanGraph};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct RunOptions {
    /// Maximum rows per chunk emitted by sources.
    pub chunk_size: usize,
}

impl Default for RunOptions {
    fn default() -> Self {
        RunOptions { chunk_size: 4096 }
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
}

impl RunResult {
    pub fn total_rows_out(&self) -> u64 {
        self.outputs
            .iter()
            .map(|o| o.chunks.iter().map(|c| c.len as u64).sum::<u64>())
            .sum()
    }
}

pub fn run(graph: &PlanGraph, opts: RunOptions) -> Result<RunResult, RivusError> {
    if graph.topo_order().is_none() {
        return Err(RivusError::Build("flow graph contains a cycle".into()));
    }
    // Data-parallel fast path for stateless, single-source flows; else serial.
    if let Some(res) = try_parallel(graph, &opts) {
        return Ok(res);
    }
    let ops = build_ops(graph, &opts, None);
    Ok(drive(graph, ops, 0))
}

/// Build one operator per node. In parallel mode, `mem` replaces the single
/// source with a pre-parsed [`operators::mem_source`] and any file sink with a
/// collector (rows gathered and written once after the merge).
fn build_ops(
    graph: &PlanGraph,
    opts: &RunOptions,
    mem: Option<(NodeId, Vec<Chunk>)>,
) -> Vec<Box<dyn Operator>> {
    let (mem_id, mut mem_chunks) = match mem {
        Some((id, c)) => (Some(id), Some(c)),
        None => (None, None),
    };
    graph
        .nodes
        .iter()
        .map(|node| {
            if Some(node.id) == mem_id {
                operators::mem_source(mem_chunks.take().unwrap_or_default())
            } else if mem_id.is_some()
                && matches!(node.op, Op::SinkCsv { .. } | Op::SinkJsonl { .. })
            {
                operators::collector()
            } else {
                operators::build(&node.op, &graph.inputs_of(node.id), opts.chunk_size)
            }
        })
        .collect()
}

/// Drive the DAG to completion with a pre-built operator set (the chunk-granular
/// scheduler). `chunk_id_base` seeds chunk ids so parallel workers don't collide.
fn drive(graph: &PlanGraph, mut ops: Vec<Box<dyn Operator>>, chunk_id_base: u64) -> RunResult {
    let n = graph.nodes.len();
    let topo = graph.topo_order().expect("acyclic (checked by caller)");

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

            if ops[nid].is_source() {
                let mut ctx = OpCtx {
                    label,
                    errors: &mut errors,
                    next_chunk_id: &mut next_chunk_id,
                };
                match ops[nid].pull(&mut ctx) {
                    Some(chunk) => produced.push(chunk),
                    None => finished_now = true,
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
            | Op::Distinct { .. } => return None,
            _ => {}
        }
    }
    let src_id = source?;

    // Parse the source once.
    let mut src_op = operators::build(&graph.nodes[src_id].op, &[], opts.chunk_size);
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
        let ops = build_ops(graph, opts, Some((src_id, all)));
        let mut res = drive(graph, ops, 0);
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
                    let ops = build_ops(graph, opts, Some((src_id, chunks)));
                    drive(graph, ops, (i as u64) << 40)
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
        Op::SinkCsv { path } => Some((path, operators::write_csv_file(path, chunks))),
        Op::SinkJsonl { path } => Some((path, operators::write_jsonl_file(path, chunks))),
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

    for res in results {
        if mode_rank(res.final_mode) > mode_rank(mode) {
            mode = res.final_mode;
        }
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
    }
}

/// Push a node's produced chunks to its successors (fan-out) or capture them as
/// a leaf output. Stamps the current runtime mode on every chunk.
fn distribute(
    graph: &PlanGraph,
    nid: NodeId,
    chunks: Vec<Chunk>,
    mode: Mode,
    telemetry: &mut [NodeTelemetry],
    in_q: &mut [VecDeque<(NodeId, Chunk)>],
    results: &mut HashMap<NodeId, Vec<Chunk>>,
) {
    let succ = graph.outputs_of(nid);
    for mut chunk in chunks {
        chunk.meta.mode = mode;
        telemetry[nid].chunks_out += 1;
        telemetry[nid].rows_out += chunk.len as u64;
        if succ.is_empty() {
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
