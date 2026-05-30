//! `rivus-optimizer` — semantics-preserving DAG transformations.
//!
//! The optimizer is **IR-in / IR-out**: it takes a [`PlanGraph`], rewrites it,
//! and returns a new `PlanGraph` plus an [`OptReport`] describing exactly what
//! changed and why. This satisfies two non-negotiables:
//!
//! - **semantic preservation** (Master §15): a rewrite must not change results.
//! - **optimizer is not opaque** (anti-pattern): every applied rule is recorded
//!   and surfaced (`rivus explain`), and because the IR is reversible the
//!   before/after can always be shown as regenerated source.
//!
//! Rules are added incrementally (design doc 08). The first is `dedup_sources`.

use rivus_ir::{Edge, EdgeKind, Expr, Node, NodeId, Op, PlanGraph};
use std::collections::{HashMap, HashSet};
use std::fmt;

/// A record of the transformations applied during one optimize pass.
#[derive(Debug, Clone, Default)]
pub struct OptReport {
    pub applied: Vec<String>,
}

impl OptReport {
    pub fn is_empty(&self) -> bool {
        self.applied.is_empty()
    }
}

impl fmt::Display for OptReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.applied.is_empty() {
            write!(f, "(no transformations applied)")
        } else {
            for (i, line) in self.applied.iter().enumerate() {
                if i > 0 {
                    writeln!(f)?;
                }
                write!(f, "{line}")?;
            }
            Ok(())
        }
    }
}

/// Run the optimization pipeline over `graph`, returning the rewritten graph
/// and a report of what changed.
pub fn optimize(graph: PlanGraph) -> (PlanGraph, OptReport) {
    let mut report = OptReport::default();
    let graph = dedup_sources(graph, &mut report);
    let graph = fuse_linear(graph, &mut report);
    let graph = project_pushdown(graph, &mut report);
    (graph, report)
}

/// **Projection pushdown.** When every consumer of a CSV source is a
/// `FilterProject` that projects to a known column set (`fields = Some`), the
/// source only needs to *build* the columns those consumers read: the union of
/// each consumer's predicate columns and projected columns. Unused columns are
/// then never parsed or allocated by the reader.
///
/// Safety: a `FilterProject{fields:Some(Y)}` emits only `Y`, so nothing
/// downstream of it can reference a source column outside `preds ∪ Y`. If any
/// consumer is something else (group/join/sink/merge, or a fused node with no
/// projection), the source's output columns are live for unknown downstream
/// use, so we conservatively skip pushdown for that source.
fn project_pushdown(mut graph: PlanGraph, report: &mut OptReport) -> PlanGraph {
    let mut pushed = 0usize;
    for sid in 0..graph.nodes.len() {
        if !matches!(
            graph.nodes[sid].op,
            Op::OpenCsv {
                projection: None,
                ..
            }
        ) {
            continue;
        }
        let consumers = graph.outputs_of(sid);
        if consumers.is_empty() {
            continue;
        }
        let mut needed: Vec<String> = Vec::new();
        let mut safe = true;
        for c in &consumers {
            match &graph.nodes[*c].op {
                Op::FilterProject {
                    preds,
                    fields: Some(proj),
                } => {
                    for p in preds {
                        collect_fields(p, &mut needed);
                    }
                    for f in proj {
                        push_unique(&mut needed, f);
                    }
                }
                _ => {
                    safe = false;
                    break;
                }
            }
        }
        if !safe || needed.is_empty() {
            continue;
        }
        if let Op::OpenCsv { projection, .. } = &mut graph.nodes[sid].op {
            *projection = Some(needed);
            pushed += 1;
        }
    }
    if pushed > 0 {
        report.applied.push(format!(
            "project_pushdown: restricted {pushed} source read(s) to live columns"
        ));
    }
    graph
}

/// Collect referenced field names from a predicate expression.
fn collect_fields(e: &Expr, out: &mut Vec<String>) {
    match e {
        Expr::Field { name, .. } => push_unique(out, name),
        Expr::Literal(_) => {}
        Expr::Compare { left, right, .. } => {
            collect_fields(left, out);
            collect_fields(right, out);
        }
        Expr::And(a, b) | Expr::Or(a, b) => {
            collect_fields(a, out);
            collect_fields(b, out);
        }
        Expr::Arith { left, right, .. } => {
            collect_fields(left, out);
            collect_fields(right, out);
        }
    }
}

fn push_unique(out: &mut Vec<String>, name: &str) {
    if !out.iter().any(|n| n == name) {
        out.push(name.to_string());
    }
}

/// **Operator fusion.** Collapse a linear chain of consecutive `Filter` nodes
/// (and an optional trailing `Project`) into a single `FilterProject` node, so
/// predicates are evaluated in one row scan and only the projected columns are
/// gathered once — eliminating intermediate chunks (design doc 08).
///
/// Conservative: only fuses across edges that are 1-in/1-out, where the
/// absorbed (upstream) node is unlabeled and neither node carries hooks. This
/// preserves every observable output and lifecycle hook.
fn fuse_linear(mut graph: PlanGraph, report: &mut OptReport) -> PlanGraph {
    let mut fused = 0usize;
    while let Some((a, b)) = find_fusable_edge(&graph) {
        graph = merge_fused(graph, a, b);
        fused += 1;
    }
    if fused > 0 {
        report.applied.push(format!(
            "fuse_linear: fused {fused} adjacent filter/project node(s)"
        ));
    }
    graph
}

fn find_fusable_edge(g: &PlanGraph) -> Option<(NodeId, NodeId)> {
    g.edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Stream)
        .map(|e| (e.from, e.to))
        .find(|&(a, b)| can_fuse(g, a, b))
}

fn can_fuse(g: &PlanGraph, a: NodeId, b: NodeId) -> bool {
    let out_a = g.outputs_of(a);
    let in_b = g.inputs_of(b);
    if out_a.len() != 1 || out_a[0] != b || in_b.len() != 1 || in_b[0] != a {
        return false;
    }
    let na = &g.nodes[a];
    let nb = &g.nodes[b];
    if !na.hooks.is_empty() || !nb.hooks.is_empty() {
        return false;
    }
    // The upstream node is absorbed; never drop a named (observable) output.
    if na.label.is_some() {
        return false;
    }
    matches!(
        (&na.op, &nb.op),
        (
            Op::Filter { .. } | Op::FilterProject { fields: None, .. },
            Op::Filter { .. } | Op::Project { .. }
        )
    )
}

/// Merge `b` into `a` (a fused-into-b edge), returning a compacted graph.
fn merge_fused(g: PlanGraph, a: NodeId, b: NodeId) -> PlanGraph {
    let fused_op = match (g.nodes[a].op.clone(), g.nodes[b].op.clone()) {
        (Op::Filter { pred }, Op::Filter { pred: p2 }) => Op::FilterProject {
            preds: vec![pred, p2],
            fields: None,
        },
        (Op::Filter { pred }, Op::Project { fields }) => Op::FilterProject {
            preds: vec![pred],
            fields: Some(fields),
        },
        (
            Op::FilterProject {
                mut preds,
                fields: None,
            },
            Op::Filter { pred },
        ) => {
            preds.push(pred);
            Op::FilterProject {
                preds,
                fields: None,
            }
        }
        (
            Op::FilterProject {
                preds,
                fields: None,
            },
            Op::Project { fields },
        ) => Op::FilterProject {
            preds,
            fields: Some(fields),
        },
        _ => unreachable!("can_fuse restricts op shapes"),
    };
    // `a` inherits `b`'s label (a was unlabeled); `b` is dropped.
    let new_label = g.nodes[b].label.clone();

    let mut new_id: HashMap<NodeId, NodeId> = HashMap::new();
    let mut nodes: Vec<Node> = Vec::with_capacity(g.nodes.len() - 1);
    for n in &g.nodes {
        if n.id == b {
            continue;
        }
        let nid = nodes.len();
        new_id.insert(n.id, nid);
        let (op, label) = if n.id == a {
            (fused_op.clone(), new_label.clone())
        } else {
            (n.op.clone(), n.label.clone())
        };
        nodes.push(Node {
            id: nid,
            label,
            op,
            hooks: n.hooks.clone(),
        });
    }

    let mut out = PlanGraph {
        nodes,
        edges: Vec::new(),
        labels: HashMap::new(),
    };
    let mut seen: HashSet<(NodeId, NodeId, bool)> = HashSet::new();
    for e in &g.edges {
        // Drop the now-internal fused edge a -> b.
        if e.from == a && e.to == b {
            continue;
        }
        // b's outgoing edges become a's; b has no other incoming (1-in == a).
        let from = new_id[&if e.from == b { a } else { e.from }];
        let to = new_id[&if e.to == b { a } else { e.to }];
        if from == to {
            continue;
        }
        let key = (from, to, e.kind == EdgeKind::Stream);
        if seen.insert(key) {
            out.edges.push(Edge {
                from,
                to,
                kind: e.kind,
            });
        }
    }
    // Rebuild the label index from the (relabeled) nodes.
    for n in &out.nodes {
        if let Some(l) = &n.label {
            out.labels.insert(l.clone(), n.id);
        }
    }
    out
}

/// **Common-subexpression elimination for sources.** Two `open <same path>`
/// nodes read the same static file and produce the same stream; reading once
/// and fanning out is equivalent (the engine already supports multi-consumer
/// fan-out). This merges duplicate, *unlabeled* source reads into one.
///
/// Labeled sources are left untouched (conservative: a label is an observable
/// output the user named explicitly).
fn dedup_sources(graph: PlanGraph, report: &mut OptReport) -> PlanGraph {
    // path -> canonical (kept) old node id; later duplicates -> redirect.
    let mut canon: HashMap<&str, NodeId> = HashMap::new();
    let mut redirect: HashMap<NodeId, NodeId> = HashMap::new();
    for n in &graph.nodes {
        if let Op::OpenCsv { path, .. } = &n.op {
            if n.label.is_some() {
                continue;
            }
            match canon.get(path.as_str()) {
                None => {
                    canon.insert(path.as_str(), n.id);
                }
                Some(&c) => {
                    redirect.insert(n.id, c);
                }
            }
        }
    }
    if redirect.is_empty() {
        return graph;
    }

    // Keep every node except the redirected duplicates; assign compact new ids.
    let mut new_id: HashMap<NodeId, NodeId> = HashMap::new();
    let mut nodes: Vec<Node> = Vec::with_capacity(graph.nodes.len() - redirect.len());
    for n in &graph.nodes {
        if redirect.contains_key(&n.id) {
            continue;
        }
        let nid = nodes.len();
        new_id.insert(n.id, nid);
        nodes.push(Node {
            id: nid,
            label: n.label.clone(),
            op: n.op.clone(),
            hooks: n.hooks.clone(),
        });
    }

    let resolve = |old: NodeId| -> NodeId {
        let target = *redirect.get(&old).unwrap_or(&old);
        new_id[&target]
    };

    let mut out = PlanGraph {
        nodes,
        edges: Vec::new(),
        labels: HashMap::new(),
    };
    for (label, &old) in &graph.labels {
        out.labels.insert(label.clone(), resolve(old));
    }

    // Rewire edges; collapse duplicates and drop any self-loops introduced by
    // the merge (a duplicate source pointing at the same consumer as canonical).
    let mut seen: HashSet<(NodeId, NodeId, bool)> = HashSet::new();
    for e in &graph.edges {
        let from = resolve(e.from);
        let to = resolve(e.to);
        if from == to {
            continue;
        }
        let key = (from, to, e.kind == EdgeKind::Stream);
        if seen.insert(key) {
            out.edges.push(Edge {
                from,
                to,
                kind: e.kind,
            });
        }
    }

    report.applied.push(format!(
        "dedup_sources: merged {} duplicate source read(s)",
        redirect.len()
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rivus_ir::Op;

    fn count_opens(g: &PlanGraph) -> usize {
        g.nodes
            .iter()
            .filter(|n| matches!(n.op, Op::OpenCsv { .. }))
            .count()
    }

    #[test]
    fn merges_identical_unlabeled_reads() {
        // Two scopes reading the same file; sources are unlabeled (tails carry
        // the labels), so they should merge into one source feeding both.
        let src = "\
A:\n open data.csv\n |? age >= 20\n;\n\
B:\n open data.csv\n |# country\n;";
        let g = rivus_parser::parse(src).unwrap();
        assert_eq!(count_opens(&g), 2);

        let (opt, report) = optimize(g);
        assert_eq!(count_opens(&opt), 1, "duplicate reads should be merged");
        assert!(!report.is_empty());
        // The single source must now fan out to both downstream consumers.
        let src_id = opt
            .nodes
            .iter()
            .find(|n| matches!(n.op, Op::OpenCsv { .. }))
            .unwrap()
            .id;
        assert_eq!(opt.outputs_of(src_id).len(), 2);
        // Labels are preserved.
        assert!(opt.labels.contains_key("A"));
        assert!(opt.labels.contains_key("B"));
    }

    #[test]
    fn distinct_paths_untouched() {
        let src = "A:\n open a.csv\n;\nB:\n open b.csv\n;";
        let g = rivus_parser::parse(src).unwrap();
        let (opt, report) = optimize(g);
        assert_eq!(count_opens(&opt), 2);
        assert!(report.is_empty());
    }

    fn count_fused(g: &PlanGraph) -> usize {
        g.nodes
            .iter()
            .filter(|n| matches!(n.op, Op::FilterProject { .. }))
            .count()
    }

    #[test]
    fn fuses_filter_then_project() {
        let src = "F:\n open d.csv\n |? age >= 20\n |> name age\n;";
        let g = rivus_parser::parse(src).unwrap();
        let (opt, report) = optimize(g);
        assert_eq!(count_fused(&opt), 1);
        // The fused node carries the tail label and the projection.
        let n = &opt.nodes[opt.labels["F"]];
        match &n.op {
            Op::FilterProject { preds, fields } => {
                assert_eq!(preds.len(), 1);
                assert_eq!(
                    fields.as_deref(),
                    Some(&["name".to_string(), "age".to_string()][..])
                );
            }
            other => panic!("expected FilterProject, got {other:?}"),
        }
        assert!(report.applied.iter().any(|l| l.contains("fuse_linear")));
    }

    #[test]
    fn fuses_chain_of_filters_and_projection() {
        let src = "F:\n open d.csv\n |? age >= 20\n |? age < 60\n |> name\n;";
        let g = rivus_parser::parse(src).unwrap();
        let (opt, _) = optimize(g);
        assert_eq!(count_fused(&opt), 1);
        // open -> fused: exactly two nodes remain.
        assert_eq!(opt.nodes.len(), 2);
        match &opt.nodes[opt.labels["F"]].op {
            Op::FilterProject { preds, fields } => {
                assert_eq!(preds.len(), 2);
                assert!(fields.is_some());
            }
            _ => panic!("expected fused node"),
        }
    }

    #[test]
    fn pushes_projection_into_source() {
        // open | filter(age) | project(name, age)  =>  source builds only {age, name}.
        let src = "F:\n open d.csv\n |? age >= 20\n |> name age\n;";
        let g = rivus_parser::parse(src).unwrap();
        let (opt, report) = optimize(g);
        let s = opt
            .nodes
            .iter()
            .find(|n| matches!(n.op, Op::OpenCsv { .. }))
            .unwrap();
        match &s.op {
            Op::OpenCsv {
                projection: Some(cols),
                ..
            } => {
                // Predicate column `age` and projected `name`, `age`.
                assert!(cols.contains(&"age".to_string()));
                assert!(cols.contains(&"name".to_string()));
            }
            other => panic!("expected source projection, got {other:?}"),
        }
        assert!(report
            .applied
            .iter()
            .any(|l| l.contains("project_pushdown")));
    }

    #[test]
    fn no_pushdown_when_a_consumer_needs_all_columns() {
        // The group consumer can reference any column, so the shared source must
        // keep building everything.
        let src = "\
A:\n open d.csv\n |? age >= 20\n |> name\n;\n\
B:\n open d.csv\n |# country\n;";
        let g = rivus_parser::parse(src).unwrap();
        let (opt, _) = optimize(g); // dedup merges the source; B blocks pushdown
        let s = opt
            .nodes
            .iter()
            .find(|n| matches!(n.op, Op::OpenCsv { .. }))
            .unwrap();
        assert!(
            matches!(
                s.op,
                Op::OpenCsv {
                    projection: None,
                    ..
                }
            ),
            "pushdown must be skipped when a consumer needs all columns"
        );
    }

    #[test]
    fn hooks_block_fusion() {
        // An `on error` hook on the filter must prevent it being absorbed.
        let src = "F:\n open d.csv\n |? age >= 20\n on error: transition degraded ;\n |> name\n;";
        let g = rivus_parser::parse(src).unwrap();
        let (opt, _) = optimize(g);
        assert_eq!(count_fused(&opt), 0, "hooked nodes must not fuse");
    }

    #[test]
    fn optimized_graph_stays_acyclic() {
        let src = "\
A:\n open data.csv\n |? age >= 20\n;\n\
B:\n open data.csv\n |? age < 20\n;\n\
C:\n open data.csv\n |# country\n;";
        let g = rivus_parser::parse(src).unwrap();
        let (opt, _) = optimize(g);
        assert_eq!(count_opens(&opt), 1);
        assert!(opt.topo_order().is_some(), "must remain a DAG");
    }
}
