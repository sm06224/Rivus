//! Per-node telemetry (Observability spec §14).
//!
//! Telemetry is core, not bolt-on (Master principle #4). Every flow node
//! accumulates counters as chunks pass through; the CLI's ASCII visualizer and
//! any future TUI/SVG renderer read straight from these structs.

use rivus_core::Mode;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct NodeTelemetry {
    pub node_id: usize,
    pub label: String,
    pub kind: String,
    pub chunks_in: u64,
    pub chunks_out: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub errors: u64,
    pub busy: Duration,
    pub mode: Mode,
    pub finished: bool,
}

impl NodeTelemetry {
    pub fn new(node_id: usize, label: String, kind: String) -> Self {
        NodeTelemetry {
            node_id,
            label,
            kind,
            chunks_in: 0,
            chunks_out: 0,
            rows_in: 0,
            rows_out: 0,
            errors: 0,
            busy: Duration::ZERO,
            mode: Mode::Normal,
            finished: false,
        }
    }

    /// Rows emitted per second of busy time (0 if no time recorded yet).
    pub fn throughput_rows_per_sec(&self) -> f64 {
        let secs = self.busy.as_secs_f64();
        if secs > 0.0 {
            self.rows_out as f64 / secs
        } else {
            0.0
        }
    }

    /// Selectivity: rows out / rows in (1.0 if no input rows).
    pub fn selectivity(&self) -> f64 {
        if self.rows_in > 0 {
            self.rows_out as f64 / self.rows_in as f64
        } else {
            1.0
        }
    }
}

/// Per-worker telemetry for a parallel (byte-range) run — one entry per worker,
/// so parallel skew (uneven rows / busy time across workers) is observable
/// instead of being collapsed into the node aggregate. Empty on the serial path,
/// so it's purely additive and never changes existing fields.
#[derive(Debug, Clone)]
pub struct WorkerTelemetry {
    /// Worker index (0-based), in source order over the byte ranges.
    pub worker: usize,
    /// Rows this worker emitted from its byte range (sum over its output leaves).
    pub rows_out: u64,
    /// Total busy time across this worker's sub-DAG.
    pub busy: Duration,
}

/// A cheap, cloneable point-in-time view of one node, for a live snapshot.
#[derive(Debug, Clone)]
pub struct NodeSnapshot {
    pub node_id: usize,
    pub label: String,
    pub kind: String,
    pub rows_in: u64,
    pub rows_out: u64,
    pub errors: u64,
    pub mode: Mode,
    pub finished: bool,
}

/// A consistent snapshot of a run in progress (Observability spec §14.4): the
/// elapsed wall time, rows seen so far, and a per-node view. Published by the
/// engine's optional progress hook so a live TUI / HTTP dashboard (Pillar B) can
/// render the run as it streams, instead of only seeing the final `RunResult`.
/// Building one is O(nodes) and only happens when a subscriber is attached.
#[derive(Debug, Clone)]
pub struct RuntimeSnapshot {
    /// Wall time since the run started.
    pub elapsed: Duration,
    /// Total rows pulled from sources so far.
    pub rows_seen: u64,
    /// Current runtime mode (escalated by error hooks).
    pub mode: Mode,
    /// Per-node view, in node-id order.
    pub nodes: Vec<NodeSnapshot>,
}
