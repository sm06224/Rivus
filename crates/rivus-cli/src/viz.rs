//! ASCII rendering of the flow graph, telemetry, outputs and error stream.
//!
//! This is the MVP face of "observable-first" (Master principle #4): the same
//! [`NodeTelemetry`] that drives this view is what a future TUI / SVG / live
//! `rivus live` Markdown renderer will read (Observability spec §13).

use rivus_core::Chunk;
use rivus_ir::PlanGraph;
use rivus_optimizer::OptReport;
use rivus_runtime::{Output, RunResult};

const BAR_WIDTH: usize = 14;

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
