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
