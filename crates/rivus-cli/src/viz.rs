//! ASCII rendering of the flow graph, telemetry, outputs and error stream.
//!
//! This is the MVP face of "observable-first" (Master principle #4): the same
//! [`NodeTelemetry`] that drives this view is what a future TUI / SVG / live
//! `rivus live` Markdown renderer will read (Observability spec §13).

use rivus_core::Chunk;
use rivus_ir::{EdgeKind, PlanGraph};
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

/// The **static** DAG topology as JSON for the live dashboard's SVG:
/// `{"nodes":[{"node_id","label","kind"}],"edges":[{"from","to","kind"}]}`.
/// Unlike the per-tick snapshot this never changes during a run, so the
/// dashboard fetches it once to lay out the graph and then animates the flow
/// from the snapshot row counts. `kind` is `"stream"` (data) or `"error"` (the
/// continue-first error side-channel), so the two are drawn differently.
pub fn render_graph_json(graph: &PlanGraph) -> String {
    let mut s = String::from("{\"nodes\":[");
    for (i, n) in graph.nodes.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("{{\"node_id\":{},\"label\":", n.id));
        let label = n
            .label
            .clone()
            .unwrap_or_else(|| n.op.kind_str().to_string());
        json_escape_into(&mut s, &label);
        s.push_str(",\"kind\":");
        json_escape_into(&mut s, n.op.kind_str());
        // The op's IR source line (UX-J): shows *what* the node does — the sort
        // key, filter predicate, cast type, etc. Cheap because the IR is
        // reversible (the single source of truth, surfaced in the viz).
        s.push_str(",\"src\":");
        json_escape_into(&mut s, &n.op.to_src_line());
        // Blocking ops (sort/group/…) get a "buffering" working state (UX-J).
        s.push_str(&format!(",\"blocking\":{}}}", n.op.is_blocking()));
    }
    s.push_str("],\"edges\":[");
    for (i, e) in graph.edges.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        let kind = match e.kind {
            EdgeKind::Stream => "stream",
            EdgeKind::Error => "error",
        };
        s.push_str(&format!(
            "{{\"from\":{},\"to\":{},\"kind\":\"{kind}\"}}",
            e.from, e.to
        ));
    }
    // The full reversible script (UX-J): the dashboard shows it verbatim so the
    // viz is grounded in the exact flow the user wrote.
    s.push_str("],\"script\":");
    json_escape_into(&mut s, &graph.to_source());
    s.push('}');
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
        // A2 exposure (#36): per-worker rows_out / busy so parallel skew is
        // visible, not just the worker count. Hand-rolled JSON array.
        let mut arr = String::from("[");
        for (i, w) in res.workers.iter().enumerate() {
            if i > 0 {
                arr.push(',');
            }
            arr.push_str(&format!(
                "{{\"worker\":{},\"rows_out\":{},\"busy_ms\":{:.3}}}",
                w.worker,
                w.rows_out,
                w.busy.as_secs_f64() * 1000.0
            ));
        }
        arr.push(']');
        o.raw("worker_breakdown", &arr);
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
    // C3: the autotuner's strategy decision (Pillar C). Summary-only.
    if let Some(st) = &res.strategy {
        o.str("strategy", st);
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
    /// Emit `key:json` with the value inserted verbatim (already-valid JSON,
    /// e.g. a nested array). Used for the per-worker breakdown (A2 exposure).
    fn raw(&mut self, k: &str, json: &str) {
        self.key(k);
        self.buf.push_str(json);
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

/// Render the IR as an embeddable Mermaid `flowchart` (§31.4): a generated,
/// **output-only** view — it is never parsed back, so it carries no round-trip
/// burden and is regenerated from the IR each time. Nodes are shaped by role
/// (source = cylinder, sink = parallelogram, join = hexagon, else rectangle)
/// and tinted via `classDef`; labels are surface form (the flow operator and
/// `$_.` sigils dropped, inert `# …` annotations stripped, detail truncated at a
/// token boundary). Stream edges are solid; the continue-first error
/// side-channel is dotted. Pure and deterministic (node order = IR node order),
/// hence unit-testable and idempotent.
pub fn render_mermaid(graph: &PlanGraph) -> String {
    let mut s = String::from("flowchart TD\n");
    let mut used_src = false;
    let mut used_sink = false;
    for n in &graph.nodes {
        let kind = n.op.kind_str();
        let shape = NodeShape::of(kind);
        let detail = mermaid_detail(kind, &n.op.to_src_line(), shape);
        let text = if detail.is_empty() {
            mermaid_escape(kind)
        } else {
            // Head (kind) and detail are escaped independently, then joined with
            // a literal `<br/>` so the line break survives escaping.
            format!("{}<br/>{}", mermaid_escape(kind), mermaid_escape(&detail))
        };
        let (open, close) = shape.delims();
        match shape {
            NodeShape::Source => used_src = true,
            NodeShape::Sink => used_sink = true,
            _ => {}
        }
        s.push_str(&format!(
            "  n{}{open}\"{text}\"{close}{}\n",
            n.id,
            shape.class()
        ));
    }
    for e in &graph.edges {
        match e.kind {
            EdgeKind::Stream => s.push_str(&format!("  n{} --> n{}\n", e.from, e.to)),
            EdgeKind::Error => s.push_str(&format!("  n{} -. error .-> n{}\n", e.from, e.to)),
        }
    }
    if used_src {
        s.push_str("  classDef src fill:#e3f2fd,stroke:#1976d2\n");
    }
    if used_sink {
        s.push_str("  classDef sink fill:#e8f5e9,stroke:#388e3c\n");
    }
    s
}

/// The Mermaid node shape for an op, by `kind_str` (§31.4 review): sources,
/// sinks and joins are visually distinct; everything else is a plain rectangle.
#[derive(Clone, Copy, PartialEq)]
enum NodeShape {
    Source,
    Sink,
    Join,
    Plain,
}

impl NodeShape {
    fn of(kind: &str) -> Self {
        match kind {
            "open" | "ls" | "watch" | "readbin" | "read" | "stream" => NodeShape::Source,
            "save" | "print" => NodeShape::Sink,
            "join" => NodeShape::Join,
            _ => NodeShape::Plain,
        }
    }
    /// The opening / closing delimiters for this shape.
    fn delims(self) -> (&'static str, &'static str) {
        match self {
            NodeShape::Source => ("[(", ")]"), // cylinder
            NodeShape::Sink => ("[/", "/]"),   // parallelogram
            NodeShape::Join => ("{{", "}}"),   // hexagon
            NodeShape::Plain => ("[", "]"),    // rectangle
        }
    }
    /// The `classDef` tint suffix (`:::src` / `:::sink`), or empty.
    fn class(self) -> &'static str {
        match self {
            NodeShape::Source => ":::src",
            NodeShape::Sink => ":::sink",
            _ => "",
        }
    }
}

/// The surface-form detail line for a node (§31.4 review): the IR source line
/// with inert `# …` annotations dropped, `$_.` sigils removed, the leading flow
/// operator / verb stripped (so it doesn't repeat the kind head), and the result
/// truncated at a token boundary.
fn mermaid_detail(kind: &str, src_line: &str, shape: NodeShape) -> String {
    // Drop inert "  # …" annotations (read-only / pre-filter hints — noise here).
    let line = match src_line.split_once("  #") {
        Some((before, _)) => before.trim_end(),
        None => src_line,
    };
    let line = line.replace("$_.", "");
    let detail = match shape {
        // "open PATH" / "ls \"glob\"" / "read as csv" / "stream NAME" → the target.
        NodeShape::Source | NodeShape::Sink => after_first_word(&line),
        _ => {
            // Drop a leading flow operator (`|?`/`|>`/`|#`/`|!`/`->`/`|`); for
            // verb-led ops (sort/take/distinct/…) the verb equals the kind head,
            // so drop it too to avoid repeating it.
            let d = strip_flow_op(&line);
            d.strip_prefix(kind).unwrap_or(&d).trim().to_string()
        }
    };
    truncate_tokens(detail.trim(), 40)
}

/// Everything after the first whitespace-delimited word (the verb), trimmed;
/// empty when there is no second token (e.g. `print`).
fn after_first_word(s: &str) -> String {
    match s.trim().split_once(char::is_whitespace) {
        Some((_, rest)) => rest.trim().to_string(),
        None => String::new(),
    }
}

/// Strip a leading flow-operator token (`|?`/`|>`/`|#`/`|!`/`->`/`|`).
fn strip_flow_op(s: &str) -> String {
    let t = s.trim_start();
    for p in ["|?", "|>", "|#", "|!", "->"] {
        if let Some(rest) = t.strip_prefix(p) {
            return rest.trim_start().to_string();
        }
    }
    t.strip_prefix('|').unwrap_or(t).trim_start().to_string()
}

/// Truncate at a token (word) boundary to at most `max` chars, appending `…`
/// when cut. A single over-long first word is hard-cut.
fn truncate_tokens(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out = String::new();
    for word in s.split(' ') {
        if out.is_empty() {
            if word.chars().count() > max {
                let mut w: String = word.chars().take(max.saturating_sub(1)).collect();
                w.push('…');
                return w;
            }
            out.push_str(word);
        } else if out.chars().count() + 1 + word.chars().count() < max {
            out.push(' ');
            out.push_str(word);
        } else {
            out.push('…');
            return out;
        }
    }
    out
}

/// Sanitize text for a Mermaid quoted node label `["…"]`: replace characters
/// that would break the syntax. Quotes/backticks → `'`, `<`/`>` → HTML entities
/// (so a predicate like `age < 18` doesn't start a tag), brackets/braces/pipes →
/// space. The `<br/>` line break is added by the caller *after* escaping, so it
/// survives.
fn mermaid_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '"' | '`' => out.push('\''),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\n' => out.push(' '),
            '[' | ']' | '{' | '}' | '|' => out.push(' '),
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod ux_j_tests {
    use super::*;

    // UX-J: the static graph JSON must carry each node's IR `to_source` line, a
    // `blocking` flag (sort/group/…), and the full reversible script, so the
    // dashboard can show *what* each node does and render the source verbatim.
    #[test]
    fn graph_json_carries_src_blocking_and_script() {
        let g = rivus_parser::parse("F:\n open data.csv\n |? age >= 20\n sort age desc\n;")
            .expect("parse");
        let json = render_graph_json(&g);
        // Per-node IR source line (the predicate / sort key are visible).
        assert!(json.contains("\"src\":"), "missing per-node src: {json}");
        assert!(
            json.contains("age >= 20"),
            "filter predicate not surfaced: {json}"
        );
        assert!(
            json.contains("sort age desc"),
            "sort key not surfaced: {json}"
        );
        // The blocking flag is present and true for `sort`, false for `filter`.
        assert!(
            json.contains("\"blocking\":true"),
            "sort must be blocking: {json}"
        );
        assert!(
            json.contains("\"blocking\":false"),
            "filter must not block: {json}"
        );
        // The full reversible script is embedded.
        assert!(json.contains("\"script\":"), "missing script: {json}");
        assert!(
            json.contains("open data.csv"),
            "script text missing: {json}"
        );
    }

    // UX-J review fix: only ops that buffer their whole input and emit on finish
    // are "blocking". A streaming op (distinct, ffill, constant fill) must NOT
    // false-show a "buffering" state.
    #[test]
    fn blocking_flag_excludes_streaming_ops() {
        let blocks = |src: &str| -> bool {
            let g = rivus_parser::parse(src).expect("parse");
            render_graph_json(&g).contains("\"blocking\":true")
        };
        // Streaming (stateful but emits as it goes) → not blocking.
        assert!(
            !blocks("S:\n open d.csv\n distinct id\n;"),
            "distinct streams"
        );
        assert!(
            !blocks("S:\n open d.csv\n fill name ffill\n;"),
            "ffill streams"
        );
        assert!(
            !blocks("S:\n open d.csv\n |? age >= 1\n;"),
            "filter streams"
        );
        // Buffer-the-whole-input → blocking.
        assert!(blocks("S:\n open d.csv\n sort id\n;"), "sort blocks");
        assert!(blocks("S:\n open d.csv\n |# id sum:age\n;"), "group blocks");
        assert!(
            blocks("S:\n open d.csv\n fill name bfill\n;"),
            "bfill blocks"
        );
        assert!(
            blocks("S:\n open d.csv\n fill age mean\n;"),
            "mean fill blocks"
        );
    }

    // §31.4: the Mermaid emitter is a pure, output-only view of the IR — a
    // `flowchart` with one node per IR node and solid stream / dotted error
    // edges. Deterministic (node order = IR order), so it's idempotent.
    #[test]
    fn mermaid_renders_nodes_and_edges() {
        let g = rivus_parser::parse("F:\n open data.csv\n |? age >= 20\n sort age desc\n;")
            .expect("parse");
        let m = render_mermaid(&g);
        assert!(m.starts_with("flowchart TD\n"), "header missing:\n{m}");
        // One node-definition line per IR node (each holds a quoted label), and a
        // solid stream edge between them.
        assert_eq!(
            m.lines()
                .filter(|l| l.trim_start().starts_with('n') && l.contains('"'))
                .count(),
            g.nodes.len(),
            "one node per IR node:\n{m}"
        );
        assert!(m.contains(" --> n"), "missing a stream edge:\n{m}");
        // Pure / deterministic → byte-identical on a second call.
        assert_eq!(m, render_mermaid(&g), "mermaid must be deterministic");
    }

    // §31.4 review: nodes are shaped by role and tinted; labels are surface form
    // (no leading flow operator, no `$_.` sigil), with `<`/`>` HTML-escaped.
    #[test]
    fn mermaid_shapes_and_surface_labels() {
        let g = rivus_parser::parse(
            "U:\n open users.csv\n |? age < 20\n |> name age\n save out.csv as csv\n;",
        )
        .expect("parse");
        let m = render_mermaid(&g);
        // Source = cylinder + src class; sink = parallelogram + sink class.
        assert!(
            m.contains("[(\"open<br/>users.csv\")]:::src"),
            "source shape:\n{m}"
        );
        assert!(m.contains("[/\"save<br/>out.csv"), "sink shape:\n{m}");
        assert!(m.contains("classDef src "), "missing src classDef:\n{m}");
        assert!(m.contains("classDef sink "), "missing sink classDef:\n{m}");
        // Surface form: the `$_.` sigil and leading `|?` are gone, `<` escaped.
        assert!(
            m.contains("filter<br/>age &lt; 20"),
            "filter not surfaced:\n{m}"
        );
        assert!(!m.contains("$_."), "raw $_. sigil leaked:\n{m}");
    }

    // A join renders as a hexagon.
    #[test]
    fn mermaid_join_is_hexagon() {
        let g =
            rivus_parser::parse("A: open a.csv ; B: open b.csv ; J: A & B on id ;").expect("parse");
        let m = render_mermaid(&g);
        assert!(m.contains("{{\"join"), "join not a hexagon:\n{m}");
    }

    // Each node label is wrapped in exactly two double quotes (the openers/closers)
    // with no inner quote, and no flow-operator pipe leaks into a label.
    #[test]
    fn mermaid_labels_are_well_formed() {
        let g =
            rivus_parser::parse("U:\n open u.csv\n |? age >= 20\n |> name age\n;").expect("parse");
        let m = render_mermaid(&g);
        for line in m
            .lines()
            .filter(|l| l.trim_start().starts_with('n') && l.contains('"'))
        {
            assert_eq!(
                line.matches('"').count(),
                2,
                "label must have exactly the wrapping quotes: {line}"
            );
            assert!(
                !line.contains('|'),
                "pipe leaked into a Mermaid label: {line}"
            );
        }
    }
}
