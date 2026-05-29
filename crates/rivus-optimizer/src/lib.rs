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

use rivus_ir::{Edge, EdgeKind, Node, NodeId, Op, PlanGraph};
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
    (graph, report)
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
        if let Op::OpenCsv { path } = &n.op {
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
