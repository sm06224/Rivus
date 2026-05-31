//! ASCII rendering of the flow graph, telemetry, outputs and error stream.
//!
//! This is the MVP face of "observable-first" (Master principle #4): the same
//! [`NodeTelemetry`] that drives this view is what a future TUI / SVG / live
//! `rivus live` Markdown renderer will read (Observability spec §13).

use rivus_core::Chunk;
use rivus_ir::PlanGraph;
use rivus_optimizer::OptReport;
use rivus_runtime::{Output, RunResult, RuntimeSnapshot};

const BAR_WIDTH: usize = 14;

/// Render a live [`RuntimeSnapshot`] as a single ANSI TUI frame (Pillar B, B1).
/// Deterministic and pure (no I/O), so it's unit-testable: the caller clears the
/// screen / repositions the cursor and prints this each tick. Bars are
/// normalized to the busiest node's `rows_out`; a huge DAG is capped to the top
/// `MAX_ROWS` nodes by rows_out (with a "+N more" line) so a terminal can't be
/// flooded. `elapsed`/`rows_seen`/`mode` head the frame.
pub fn render_snapshot_frame(snap: &RuntimeSnapshot) -> String {
    const MAX_ROWS: usize = 24;
    const BAR: usize = 20;
    let mut s = String::new();
    let secs = snap.elapsed.as_secs_f64();
    let rate = if secs > 0.0 {
        snap.rows_seen as f64 / secs
    } else {
        0.0
    };
    s.push_str(&format!(
        "\u{2550}\u{2550} Rivus live \u{2550}\u{2550}  {} rows  {:.1}s  {} rows/s  [{}]\n",
        group_thousands(snap.rows_seen),
        secs,
        group_thousands(rate as u64),
        snap.mode
    ));

    // Order by rows_out desc for the "hot nodes" view; cap to MAX_ROWS.
    let mut idx: Vec<usize> = (0..snap.nodes.len()).collect();
    idx.sort_by(|&a, &b| snap.nodes[b].rows_out.cmp(&snap.nodes[a].rows_out));
    let max_rows = snap.nodes.iter().map(|n| n.rows_out).max().unwrap_or(0);
    let shown = idx.len().min(MAX_ROWS);
    for &i in idx.iter().take(shown) {
        let n = &snap.nodes[i];
        let filled = if max_rows > 0 {
            ((n.rows_out as f64 / max_rows as f64) * BAR as f64).round() as usize
        } else {
            0
        };
        let bar: String = "\u{2588}".repeat(filled) + &"\u{2591}".repeat(BAR - filled);
        let flag = if n.finished { "done" } else { "live" };
        let errs = if n.errors > 0 {
            format!(" !{}", n.errors)
        } else {
            String::new()
        };
        s.push_str(&format!(
            "  {:<14} {:<8} {bar} {:>10} rows {flag}{errs}\n",
            truncate(&n.label, 14),
            truncate(&n.kind, 8),
            group_thousands(n.rows_out),
        ));
    }
    if idx.len() > shown {
        s.push_str(&format!("  … +{} more node(s)\n", idx.len() - shown));
    }
    s
}

/// Truncate a label to `n` chars with an ellipsis, for fixed-width TUI columns.
fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let keep: String = s.chars().take(n.saturating_sub(1)).collect();
        format!("{keep}\u{2026}")
    }
}

/// Encode a [`RuntimeSnapshot`] as one JSON object (for the `/snapshot` route
/// and SSE `/events` stream). std-only — hand-rolled, with a nested `nodes`
/// array. Numbers are integers/floats; strings are escaped.
pub fn render_snapshot_json(snap: &RuntimeSnapshot) -> String {
    let mut s = String::from("{");
    s.push_str(&format!(
        "\"elapsed_ms\":{:.3},\"rows_seen\":{},\"mode\":",
        snap.elapsed.as_secs_f64() * 1000.0,
        snap.rows_seen
    ));
    json_escape_into(&mut s, &snap.mode.to_string());
    s.push_str(",\"nodes\":[");
    for (i, n) in snap.nodes.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("{{\"node_id\":{},\"label\":", n.node_id));
        json_escape_into(&mut s, &n.label);
        s.push_str(",\"kind\":");
        json_escape_into(&mut s, &n.kind);
        s.push_str(&format!(
            ",\"rows_in\":{},\"rows_out\":{},\"errors\":{},\"mode\":",
            n.rows_in, n.rows_out, n.errors
        ));
        json_escape_into(&mut s, &n.mode.to_string());
        s.push_str(&format!(",\"finished\":{}}}", n.finished));
    }
    s.push_str("]}");
    s
}

/// Group an integer with thousands separators (1234567 → "1,234,567").
fn group_thousands(n: u64) -> String {
    let s = n.to_string();
    let b = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    let first = b.len() % 3;
    for (i, &c) in b.iter().enumerate() {
        if i > 0 && i >= first && (i - first).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c as char);
    }
    out
}

pub fn render_run(graph: &PlanGraph, res: &RunResult) -> String {
    let mut s = String::new();
    s.push_str(&render_graph(graph, res));
    s.push('\n');
    s.push_str(&render_errors(res));
    s.push('\n');
    s.push_str(&render_outputs(&res.outputs));
    s
}

pub fn render_graph(graph: &PlanGraph, res: &RunResult) -> String {
    let mut s = String::new();
    s.push_str(&format!(
        "\u{2592} execution graph   final mode: {}\n",
        res.final_mode
    ));

    let max_rows = res
        .telemetry
        .iter()
        .map(|t| t.rows_out)
        .max()
        .unwrap_or(0)
        .max(1);

    // Print in topological order so data-flow reads top-to-bottom.
    let order = graph
        .topo_order()
        .unwrap_or_else(|| (0..graph.nodes.len()).collect());
    for nid in order {
        let t = &res.telemetry[nid];
        let depth = upstream_depth(graph, nid);
        let indent = "  ".repeat(depth);
        let connector = if depth == 0 { "" } else { "└─ " };
        let name = format!("{indent}{connector}{}", t.label);
        let bar = bar(t.rows_out, max_rows);
        let status = if t.finished { "done" } else { "active" };
        let errs = if t.errors > 0 {
            format!("  !{}", t.errors)
        } else {
            String::new()
        };
        s.push_str(&format!(
            "  {name:<24} {kind:<7} {ri:>5}\u{2192}{ro:<5} {bar} {status}{errs}\n",
            kind = t.kind,
            ri = t.rows_in,
            ro = t.rows_out,
        ));
    }
    s
}

fn bar(value: u64, max: u64) -> String {
    let filled = ((value as f64 / max as f64) * BAR_WIDTH as f64).round() as usize;
    let filled = filled.min(BAR_WIDTH);
    let mut b = String::new();
    for _ in 0..filled {
        b.push('\u{2588}'); // █
    }
    for _ in filled..BAR_WIDTH {
        b.push('\u{2591}'); // ░
    }
    b
}

/// Longest path length from any source to this node (for indentation only).
fn upstream_depth(graph: &PlanGraph, nid: usize) -> usize {
    let inputs = graph.inputs_of(nid);
    if inputs.is_empty() {
        0
    } else {
        1 + inputs
            .iter()
            .map(|&i| upstream_depth(graph, i))
            .max()
            .unwrap_or(0)
    }
}

pub fn render_errors(res: &RunResult) -> String {
    if res.errors.is_empty() {
        return "\u{2592} error stream      (empty)\n".to_string();
    }
    let mut s = format!("\u{2592} error stream      ({})\n", res.errors.len());
    for e in &res.errors {
        s.push_str(&format!("  {e}\n"));
    }
    s
}

/// Render the run as **JSON Lines** (one object per line) for machine consumers
/// — editors, a GUI, or `jq`. Emitted to stderr so stdout stays clean data.
/// Each line is one of: `{"event":"node",…}` per flow node (telemetry counters),
/// `{"event":"error",…}` per error-stream event, and a final
/// `{"event":"summary",…}`. std-only: a tiny hand-rolled JSON writer (no serde).
pub fn render_telemetry_jsonl(graph: &PlanGraph, res: &RunResult) -> String {
    let mut s = String::new();
    for t in &res.telemetry {
        let mut o = JsonObj::new();
        o.str("event", "node");
        o.num("node_id", t.node_id as f64);
        o.str("label", &t.label);
        o.str("kind", &t.kind);
        o.num("chunks_in", t.chunks_in as f64);
        o.num("chunks_out", t.chunks_out as f64);
        o.num("rows_in", t.rows_in as f64);
        o.num("rows_out", t.rows_out as f64);
        o.num("errors", t.errors as f64);
        o.num("busy_ms", t.busy.as_secs_f64() * 1000.0);
        o.num("rows_per_sec", t.throughput_rows_per_sec());
        o.num("selectivity", t.selectivity());
        o.str("mode", &t.mode.to_string());
        o.boolean("finished", t.finished);
        s.push_str(&o.finish());
        s.push('\n');
    }
    for e in &res.errors {
        let mut o = JsonObj::new();
        o.str("event", "error");
        o.str("severity", &e.severity.to_string());
        o.str("scope", error_scope_str(&e.scope));
        o.str("message", &e.message);
        match &e.node {
            Some(n) => o.str("node", n),
            None => o.null("node"),
        }
        match e.chunk_id {
            Some(id) => o.num("chunk_id", id as f64),
            None => o.null("chunk_id"),
        }
        s.push_str(&o.finish());
        s.push('\n');
    }
    let mut o = JsonObj::new();
    o.str("event", "summary");
    o.num("nodes", graph.nodes.len() as f64);
    o.num("rows_out", res.total_rows_out() as f64);
    o.num("errors", res.errors.len() as f64);
    o.str("final_mode", &res.final_mode.to_string());
    // A3: time-to-first-row and the parse phase (source busy), summary-only so
    // the per-node / per-error line contract stays byte-stable.
    if let Some(l) = res.first_row_latency {
        o.num("first_row_latency_ms", l.as_secs_f64() * 1000.0);
    }
    let parse_busy_ms: f64 = res
        .telemetry
        .iter()
        .filter(|t| t.kind == "open")
        .map(|t| t.busy.as_secs_f64() * 1000.0)
        .sum();
    o.num("parse_busy_ms", parse_busy_ms);
    if !res.workers.is_empty() {
        o.num("workers", res.workers.len() as f64);
    }
    // A4: columns whose inferred type widened (int→float). Summary-only; the
    // node/error line contract stays byte-stable.
    let widened: Vec<&str> = res
        .inference
        .iter()
        .filter(|(_, _, w)| *w)
        .map(|(n, _, _)| n.as_str())
        .collect();
    if !widened.is_empty() {
        o.str("widened_columns", &widened.join(","));
    }
    s.push_str(&o.finish());
    s.push('\n');
    s
}

fn error_scope_str(scope: &rivus_core::ErrorScope) -> &'static str {
    use rivus_core::ErrorScope::*;
    match scope {
        Item => "item",
        Chunk => "chunk",
        Branch => "branch",
        Graph => "graph",
    }
}

/// A minimal JSON object writer (std-only). Keys are known-safe identifiers;
/// string *values* are escaped. Integral numbers print without a `.0`.
struct JsonObj {
    buf: String,
    first: bool,
}

impl JsonObj {
    fn new() -> Self {
        JsonObj {
            buf: String::from("{"),
            first: true,
        }
    }
    fn sep(&mut self) {
        if self.first {
            self.first = false;
        } else {
            self.buf.push(',');
        }
    }
    fn key(&mut self, k: &str) {
        self.sep();
        self.buf.push('"');
        self.buf.push_str(k);
        self.buf.push_str("\":");
    }
    fn str(&mut self, k: &str, v: &str) {
        self.key(k);
        json_escape_into(&mut self.buf, v);
    }
    fn num(&mut self, k: &str, v: f64) {
        self.key(k);
        if v.is_finite() && v.fract() == 0.0 && v.abs() < 1e15 {
            self.buf.push_str(&format!("{}", v as i64));
        } else if v.is_finite() {
            self.buf.push_str(&format!("{v}"));
        } else {
            self.buf.push_str("null"); // JSON has no NaN/Inf
        }
    }
    fn boolean(&mut self, k: &str, v: bool) {
        self.key(k);
        self.buf.push_str(if v { "true" } else { "false" });
    }
    fn null(&mut self, k: &str) {
        self.key(k);
        self.buf.push_str("null");
    }
    fn finish(mut self) -> String {
        self.buf.push('}');
        self.buf
    }
}

/// Append `v` as a quoted, escaped JSON string.
fn json_escape_into(out: &mut String, v: &str) {
    out.push('"');
    for c in v.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

pub fn render_outputs(outputs: &[Output]) -> String {
    if outputs.is_empty() {
        return "\u{2592} outputs           (none captured)\n".to_string();
    }
    let mut s = String::new();
    for out in outputs {
        let rows: usize = out.chunks.iter().map(|c| c.len).sum();
        let label = out
            .label
            .clone()
            .unwrap_or_else(|| format!("#{}", out.node_id));
        s.push_str(&format!("\u{2592} {label}  ({rows} rows)\n"));
        s.push_str(&render_table(&out.chunks, 20));
        s.push('\n');
    }
    s
}

/// Render up to `limit` rows of a (homogeneously-typed) chunk list as a table.
fn render_table(chunks: &[Chunk], limit: usize) -> String {
    let Some(first) = chunks.first() else {
        return String::new();
    };
    let headers: Vec<String> = first
        .schema
        .field_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    let ncols = headers.len();

    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut shown = 0usize;
    let total: usize = chunks.iter().map(|c| c.len).sum();
    'outer: for chunk in chunks {
        for r in 0..chunk.len {
            if shown >= limit {
                break 'outer;
            }
            let row: Vec<String> = (0..ncols).map(|c| chunk.value(r, c).to_string()).collect();
            rows.push(row);
            shown += 1;
        }
    }

    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.len());
        }
    }

    let mut s = String::new();
    s.push_str("  ");
    for (i, h) in headers.iter().enumerate() {
        s.push_str(&format!("{:<width$}  ", h, width = widths[i]));
    }
    s.push('\n');
    s.push_str("  ");
    for w in &widths {
        s.push_str(&"-".repeat(*w));
        s.push_str("  ");
    }
    s.push('\n');
    for row in &rows {
        s.push_str("  ");
        for (i, cell) in row.iter().enumerate() {
            s.push_str(&format!("{:<width$}  ", cell, width = widths[i]));
        }
        s.push('\n');
    }
    if total > shown {
        s.push_str(&format!("  ... {} more row(s)\n", total - shown));
    }
    s
}

/// Just the applied-rules report (shown before a `run`).
pub fn render_opt_report(report: &OptReport) -> String {
    let mut s = String::from("\u{2592} optimizer\n");
    for line in report.to_string().lines() {
        s.push_str(&format!("  {line}\n"));
    }
    s
}

/// `explain` optimizer section: applied rules + the regenerated *optimized*
/// source, demonstrating that optimization is a visible graph transformation.
pub fn render_optimization(report: &OptReport, optimized: &PlanGraph) -> String {
    let mut s = String::from("\u{2592} optimizer\n");
    for line in report.to_string().lines() {
        s.push_str(&format!("  {line}\n"));
    }
    if !report.is_empty() {
        s.push_str("\u{2592} optimized source (after transformation)\n");
        for line in optimized.to_source().lines() {
            s.push_str(&format!("  {line}\n"));
        }
    }
    s
}

/// `explain`: dump the DAG structure and regenerate source (reversibility).
pub fn render_explain(graph: &PlanGraph) -> String {
    let mut s = String::new();
    s.push_str("\u{2592} nodes\n");
    for n in &graph.nodes {
        let label = n.label.clone().unwrap_or_else(|| "-".into());
        s.push_str(&format!(
            "  #{:<2} {:<8} label={label:<10} hooks={}\n",
            n.id,
            n.op.kind_str(),
            n.hooks.len()
        ));
    }
    s.push_str("\u{2592} edges\n");
    for e in &graph.edges {
        s.push_str(&format!("  #{} -> #{}  ({:?})\n", e.from, e.to, e.kind));
    }
    if let Some(order) = graph.topo_order() {
        let ids: Vec<String> = order.iter().map(|i| format!("#{i}")).collect();
        s.push_str(&format!("\u{2592} topo order\n  {}\n", ids.join(" -> ")));
    } else {
        s.push_str("\u{2592} topo order\n  (cycle detected)\n");
    }
    s.push_str("\u{2592} regenerated source (IR -> source, reversibility)\n");
    for line in graph.to_source().lines() {
        s.push_str(&format!("  {line}\n"));
    }
    s
}
