//! ASCII rendering of the flow graph, telemetry, outputs and error stream.
//!
//! This is the MVP face of "observable-first" (Master principle #4): the same
//! [`NodeTelemetry`] that drives this view is what a future TUI / SVG / live
//! `rivus live` Markdown renderer will read (Observability spec §13).

use rivus_core::Chunk;
use rivus_ir::{EdgeKind, Op, PlanGraph};
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
        // One-line op summary (#194): the node list used to read as an
        // uninformative `label=- hooks=0` ladder while the same predicate /
        // key / field detail sat only in the Mermaid edge labels — surface it
        // here too, truncated so one node stays one line.
        let mut src = n.op.to_src_line();
        if src.chars().count() > 60 {
            src = format!("{}…", src.chars().take(59).collect::<String>());
        }
        s.push_str(&format!(
            "  #{:<2} {:<8} label={label:<10} hooks={}  {src}\n",
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
    // Decode-column pruning surface (#240 キュー3): the execution-time decode
    // allow-list, computed by the same analysis the engine applies on BOTH the
    // serial and parallel paths — explain shows what `read` will actually
    // decode (CSV; parse failures in unlisted columns are not counted).
    if let Some(cols) = rivus_runtime::read_prune_allow(graph) {
        s.push_str(&format!(
            "\u{2592} decode prune (read decodes only these columns)\n  {}\n",
            cols.join(", ")
        ));
    }
    s.push_str("\u{2592} regenerated source (IR -> source, reversibility)\n");
    for line in graph.to_source().lines() {
        s.push_str(&format!("  {line}\n"));
    }
    s
}

/// Render the IR as an embeddable Mermaid **dataset-centric lineage** (§32.5):
/// a generated, **output-only** view (never parsed back, regenerated from the
/// IR each time). Nodes are the *named typed datasets* — sources, named scopes,
/// joins/merges and the final result — each shown with its columns and a role
/// emoji (🗄️ source, 📦 intermediate, 📄 sink), with the columns supplied by the
/// static schema-propagation pass (§32.1). Edges are the *operations* that
/// transform one dataset into the next (🔍 filter, 🔗 join, 📋 project,
/// 📊 group, 🔀 sort, 🏆 take), folding a chain of intermediate ops onto one
/// edge. Pure and deterministic (node order = IR node order), hence
/// unit-testable and idempotent.
pub fn render_mermaid(graph: &PlanGraph) -> String {
    let n = graph.nodes.len();
    let schemas = graph.node_schemas();
    // A schema-changing op (group/join/project/…) must be its own dataset node
    // so the reshape — especially an aggregate — reads as a distinct step
    // (#166 review B); a schema-invariant row op (filter/sort/take/distinct)
    // folds onto an edge. A foldable op that *feeds* a schema-changing op also
    // becomes a node, so the schema-changing op keeps its own clean edge rather
    // than folding the filter and the aggregate onto one.
    let feeds_hard: Vec<bool> = (0..n)
        .map(|i| {
            graph
                .outputs_of(i)
                .iter()
                .any(|&j| is_schema_changing(&graph.nodes[j].op))
        })
        .collect();
    let ds: Vec<bool> = (0..n)
        .map(|i| {
            let node = &graph.nodes[i];
            is_source(&node.op)
                || is_sink(&node.op)
                || node.label.is_some()
                || graph.outputs_of(i).is_empty()
                || is_schema_changing(&node.op)
                || feeds_hard[i]
        })
        .collect();

    let mut s = String::from("flowchart TD\n");
    let (mut used_src, mut used_sink, mut used_mid) = (false, false, false);
    for i in 0..n {
        if !ds[i] {
            continue;
        }
        // All nodes are plain rectangles (target #161); role is conveyed by the
        // emoji + `classDef` tint, not the box shape (#166 review A).
        let class = role_class(&graph.nodes[i].op);
        match class {
            ":::src" => used_src = true,
            ":::sink" => used_sink = true,
            _ => used_mid = true,
        }
        let mut label = format!(
            "{} {}",
            role_emoji(&graph.nodes[i].op),
            dataset_name(graph, i)
        );
        if let Some(sc) = &schemas[i] {
            if !sc.fields.is_empty() {
                label.push_str("\n\n");
                let cols: Vec<String> = sc
                    .fields
                    .iter()
                    .map(|f| format!("{}:{}", f.name, f.dtype))
                    .collect();
                label.push_str(&cols.join("\n"));
            }
        }
        s.push_str(&format!("  n{i}[\"{}\"]{class}\n", mermaid_escape(&label)));
    }

    // Edges: for each dataset node (other than a source), contract the chain of
    // intermediate ops back to its dataset ancestor(s) and label the edge.
    for i in 0..n {
        if !ds[i] || is_source(&graph.nodes[i].op) {
            continue;
        }
        for &x in &graph.inputs_of(i) {
            let mut labels: Vec<String> = Vec::new();
            let mut cur = x;
            // Walk back through non-dataset ops, collecting their edge labels.
            while !ds[cur] {
                if let Some(l) = edge_op_label(&graph.nodes[cur].op) {
                    labels.push(l);
                }
                let ins = graph.inputs_of(cur);
                if ins.len() != 1 {
                    break;
                }
                cur = ins[0];
            }
            labels.reverse(); // ancestor → … order
                              // This dataset's own producing op is the final operation on the edge
                              // (a source has none; a sink's "save" is the node, not an op label).
            if let Some(l) = edge_op_label(&graph.nodes[i].op) {
                labels.push(l);
            }
            let elabel = labels.join(" + ");
            if elabel.is_empty() {
                s.push_str(&format!("  n{cur} --> n{i}\n"));
            } else {
                s.push_str(&format!(
                    "  n{cur} -->|\"{}\"| n{i}\n",
                    mermaid_escape(&elabel)
                ));
            }
        }
    }

    if used_src {
        s.push_str("  classDef src fill:#e3f2fd,stroke:#1976d2\n");
    }
    if used_mid {
        s.push_str("  classDef mid fill:#fff3e0,stroke:#f57c00\n");
    }
    if used_sink {
        s.push_str("  classDef sink fill:#e8f5e9,stroke:#388e3c\n");
    }
    s
}

/// Is this op a data *source* (the head of a flow)?
fn is_source(op: &Op) -> bool {
    matches!(
        op,
        Op::Source { .. } | Op::Read { .. } | Op::StreamRef { .. }
    )
}

/// Is this op a *sink* (the tail of a flow)?
fn is_sink(op: &Op) -> bool {
    matches!(op, Op::Sink { .. } | Op::SinkPrint)
}

/// The `classDef` tint suffix for a dataset node by role. All nodes are plain
/// rectangles (#166 review A); only the colour + emoji differ.
fn role_class(op: &Op) -> &'static str {
    if is_source(op) {
        ":::src"
    } else if is_sink(op) {
        ":::sink"
    } else {
        ":::mid"
    }
}

/// Does this op change the schema (column set / names / types), so it must be
/// its own dataset node (#166 review B)? Reshapers (project / group / join /
/// merge) and column edits (cast / rename / drop / reorder) do; schema-invariant
/// row ops (filter / sort / take / distinct / dropna / fill) do not.
fn is_schema_changing(op: &Op) -> bool {
    matches!(
        op,
        Op::Project { .. }
            | Op::ProjectExpr { .. }
            | Op::GroupBy { .. }
            | Op::Join { .. }
            | Op::Merge
            | Op::Cast { .. }
            | Op::Rename { .. }
            | Op::Drop { .. }
            | Op::Reorder { .. }
    )
}

/// The role emoji for a dataset node (🗄️ source / 📄 sink / 📦 intermediate).
fn role_emoji(op: &Op) -> &'static str {
    if is_source(op) {
        "🗄️"
    } else if is_sink(op) {
        "📄"
    } else {
        "📦"
    }
}

/// A display name for a dataset node: its scope label when named, else the
/// source/sink file basename, else the op kind.
fn dataset_name(graph: &PlanGraph, i: usize) -> String {
    match &graph.nodes[i].op {
        // I/O endpoints (source / sink) are identified by their **file path**,
        // not a scope/leaf label — so `🗄️ users.csv` and `📄 top.csv` stay
        // symmetric (#166 review). The path wins over a label that happens to
        // sit on the endpoint node.
        Op::Source { discovery, .. } => basename(discovery.path()),
        Op::Sink { route, .. } => route
            .path()
            .map(basename)
            .or_else(|| graph.nodes[i].label.clone())
            .unwrap_or_else(|| "output".into()),
        // Intermediate nodes use their scope label, else the op kind.
        _ => graph.nodes[i]
            .label
            .clone()
            .unwrap_or_else(|| graph.nodes[i].op.kind_str().to_string()),
    }
}

/// The last path component (`a/b/c.csv` → `c.csv`), for a compact node name.
fn basename(path: &str) -> String {
    path.rsplit(['/', '\\']).next().unwrap_or(path).to_string()
}

/// The edge label for an operation that transforms one dataset into the next —
/// an emoji plus the surface form. `None` for nodes that are not operations on
/// an edge (sources, sinks, branch tees).
fn edge_op_label(op: &Op) -> Option<String> {
    let emoji = match op.kind_str() {
        "filter" | "validate" => "🔍",
        "join" => "🔗",
        "project" | "fused" => "📋",
        "group" => "📊",
        "sort" => "🔀",
        "take" => "🏆",
        "distinct" | "dropna" | "fill" | "cast" | "rename" | "drop" | "reorder" => "🔧",
        "merge" => "➕",
        _ => return None, // branch, source, sink, stream, describe, print
    };
    let text = surface(&op.to_src_line());
    if text.is_empty() {
        Some(emoji.to_string())
    } else {
        Some(format!("{emoji} {text}"))
    }
}

/// Surface form of an op's source line for an edge label: drop a leading flow
/// operator (`|?`/`|>`/`|#`/`|!`), the `$_.` field sigil and any inert `# …`
/// annotation, and truncate at a token boundary.
fn surface(src_line: &str) -> String {
    let line = match src_line.split_once("  #") {
        Some((before, _)) => before.trim_end(),
        None => src_line,
    };
    let line = line.replace("$_.", "");
    let line = line.trim_start();
    let line = ["|?", "|>", "|#", "|!"]
        .iter()
        .find_map(|p| line.strip_prefix(p))
        .unwrap_or(line)
        .trim();
    truncate(line, 40)
}

/// Sanitize text for a Mermaid quoted node label `["…"]`: quotes/backticks →
/// `'`, **`<`/`>` → HTML entities** (so a predicate like `age < 18` can't start
/// a tag — they also collide with the `-->` edge arrow and break the render),
/// newline → `<br/>`, brackets/braces/pipes → space. The `<br/>` we emit here is
/// pushed whole, so its own `<`/`>` are not re-escaped.
fn mermaid_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '"' | '`' => out.push('\''),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\n' => out.push_str("<br/>"),
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

    // A line that *defines* a node (`nN[…"…"…]`), as opposed to an edge
    // (`nN -->| … | nM`). Used to scope label assertions to node boxes.
    fn is_node_def(line: &str) -> bool {
        let t = line.trim_start();
        t.starts_with('n') && t.contains('[') && !t.contains("-->")
    }

    // §32.5: the Mermaid emitter is a pure, output-only **dataset-centric**
    // lineage — boxes are datasets (source/named-scope/sink) carrying their
    // columns + a role emoji; edges are the operations between them. Pure and
    // deterministic, so it's idempotent.
    #[test]
    fn mermaid_renders_dataset_lineage() {
        let g = rivus_parser::parse(
            "Adults:\n open u.csv (uid:str age:i64 city:str)\n |? age >= 18\n;\n\
             Summary:\n Adults\n |# city sum:age\n;",
        )
        .expect("parse");
        let m = render_mermaid(&g);
        assert!(m.starts_with("flowchart TD\n"), "header missing:\n{m}");
        // All boxes are plain rectangles (#166 review A): source = `["🗄️ …"]`,
        // never a cylinder. The declared columns ride the box (static schema).
        assert!(
            m.contains("[\"🗄️ u.csv"),
            "source box missing/not a rectangle:\n{m}"
        );
        assert!(
            !m.contains("[("),
            "no cylinder shapes (#166 review A):\n{m}"
        );
        assert!(m.contains("age:i64"), "declared column/type missing:\n{m}");
        // Named scopes are 📦 dataset boxes; the group output is statically typed.
        assert!(m.contains("📦 Adults"), "Adults dataset missing:\n{m}");
        assert!(m.contains("📦 Summary"), "Summary dataset missing:\n{m}");
        assert!(
            m.contains("sum_age:i64"),
            "group output schema missing:\n{m}"
        );
        // Edges carry the operation (emoji + surface form), not the data.
        assert!(
            m.contains("-->|\"🔍 age &gt;= 18\"|"),
            "filter edge missing:\n{m}"
        );
        assert!(m.contains("📊 city sum:age"), "group edge missing:\n{m}");
        assert!(m.contains("classDef src "), "missing classDef:\n{m}");
        // Pure / deterministic → byte-identical on a second call.
        assert_eq!(m, render_mermaid(&g), "mermaid must be deterministic");
    }

    // Only schema-invariant row ops fold: a `sort` + `take` chain into a sink
    // collapses onto one edge (#166 review B — these don't change the schema).
    #[test]
    fn mermaid_folds_schema_invariant_chain_onto_one_edge() {
        let g = rivus_parser::parse(
            "Top:\n open u.csv (city:str amount:i64)\n sort amount desc\n take 5\n save out.csv\n;",
        )
        .expect("parse");
        let m = render_mermaid(&g);
        assert!(
            m.contains("🔀 sort amount desc + 🏆 take 5"),
            "schema-invariant chain not folded onto one edge:\n{m}"
        );
        // The sink is a plain rectangle (#166 review A), not a parallelogram,
        // and is named by its **file path** (symmetric with the source), not the
        // leaf scope label (#166 review).
        assert!(
            m.contains("[\"📄 out.csv"),
            "sink not a rectangle / not named by its file path:\n{m}"
        );
        assert!(
            !m.contains("[/"),
            "no parallelogram shapes (#166 review A):\n{m}"
        );
    }

    // #166 review B: a schema-changing op (group) is its **own** step — the
    // aggregate is never folded onto the same edge as a preceding filter.
    #[test]
    fn mermaid_aggregate_is_its_own_step() {
        let g = rivus_parser::parse(
            "ByCity:\n open u.csv (city:str age:i64)\n |? age >= 18\n |# city sum:age\n;",
        )
        .expect("parse");
        let m = render_mermaid(&g);
        // The filter and the group are on *separate* edges (not "🔍 … + 📊 …").
        assert!(
            m.contains("|\"🔍 age &gt;= 18\"|"),
            "filter not its own edge:\n{m}"
        );
        assert!(
            m.contains("|\"📊 city sum:age\"|"),
            "group not its own edge:\n{m}"
        );
        for line in m.lines() {
            assert!(
                !(line.contains("🔍") && line.contains("📊")),
                "filter and aggregate folded onto one edge:\n{line}"
            );
        }
    }

    // `<`/`>` in an edge label (a predicate like `age < 18`) are HTML-escaped so
    // they can't collide with the `-->` arrow and break the render.
    #[test]
    fn mermaid_escapes_angle_brackets() {
        let g = rivus_parser::parse("U:\n open u.csv (age:i64)\n |? age < 20\n;").expect("parse");
        let m = render_mermaid(&g);
        assert!(m.contains("&lt;"), "`<` not HTML-escaped:\n{m}");
        // No bare `<` survives anywhere except the `<br/>` line breaks we emit.
        assert!(
            !m.replace("<br/>", "").contains('<'),
            "bare `<` leaked (would break the render):\n{m}"
        );
    }

    // Each node box's label is wrapped in exactly two double quotes (no inner
    // quote that would break Mermaid).
    #[test]
    fn mermaid_node_labels_well_formed() {
        let g = rivus_parser::parse("U:\n open u.csv (name:str age:i64)\n |? age >= 20\n;")
            .expect("parse");
        let m = render_mermaid(&g);
        for line in m.lines().filter(|l| is_node_def(l)) {
            assert_eq!(
                line.matches('"').count(),
                2,
                "node label must have exactly the wrapping quotes: {line}"
            );
        }
    }
}
