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

use rivus_core::Value;
use rivus_ir::{CmpOp, Codec, Edge, EdgeKind, Expr, Func, Node, NodeId, Op, PlanGraph};
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
    let graph = filter_pushdown(graph, &mut report);
    let graph = string_prefilter(graph, &mut report);
    let graph = discovery_prefilter(graph, &mut report);
    (graph, report)
}

/// **Discovery name-prefilter pushdown** (slice 3b). Annotate an `ls`
/// (`Codec::Discover`) source with required filename substrings drawn from its
/// single `FilterProject` consumer's predicates on the `name` column
/// (`name == "S"`, `contains(name,"S")`, `starts_with`/`ends_with`, the leading
/// literal run of `like`). The enumeration walk then skips a directory entry
/// whose name lacks the substring *before* statting it (a syscall saved per
/// pruned file in a large directory).
///
/// Safety: a **superset** prune (a name with the substring may still fail the
/// real predicate; one lacking it can never pass), and the `FilterProject`
/// re-checks every surviving row — so the result is unchanged (fixed by
/// `optimizer_equiv`). Only top-level `and` conjuncts on `name` contribute.
fn discovery_prefilter(mut graph: PlanGraph, report: &mut OptReport) -> PlanGraph {
    let mut pushed = 0usize;
    for sid in 0..graph.nodes.len() {
        if !matches!(
            &graph.nodes[sid].op,
            Op::Source { codec: Codec::Discover { name_prefilter }, .. } if name_prefilter.is_empty()
        ) {
            continue;
        }
        let consumers = graph.outputs_of(sid);
        if consumers.len() != 1 {
            continue;
        }
        let preds = match &graph.nodes[consumers[0]].op {
            Op::FilterProject { preds, .. } => preds.clone(),
            _ => continue,
        };
        let mut conjuncts: Vec<&Expr> = Vec::new();
        for p in &preds {
            collect_conjuncts(p, &mut conjuncts);
        }
        let needles: Vec<String> = conjuncts
            .into_iter()
            .filter_map(name_substring_atom)
            .filter(|s| !s.is_empty())
            .collect();
        if needles.is_empty() {
            continue;
        }
        if let Op::Source {
            codec: Codec::Discover { name_prefilter },
            ..
        } = &mut graph.nodes[sid].op
        {
            *name_prefilter = needles;
            pushed += 1;
        }
    }
    if pushed > 0 {
        report.applied.push(format!(
            "discovery_prefilter: filename pre-scan on {pushed} `ls` source(s)"
        ));
    }
    graph
}

/// A required substring of the **`name`** column implied by a predicate
/// (`name == "S"`, `contains/starts_with/ends_with(name,"S")`, the leading run of
/// `like(name,"S%…")`). Like [`literal_substring_atom`] but restricted to the
/// `name` field, for discovery pushdown. `None` if none is guaranteed.
fn name_substring_atom(e: &Expr) -> Option<String> {
    let is_name = |x: &Expr| matches!(x, Expr::Field { name, access } if access.is_column() && name == "name");
    if let Expr::Compare {
        left,
        op: CmpOp::Eq,
        right,
    } = e
    {
        if is_name(left) {
            if let Expr::Literal(Value::Str(s)) = right.as_ref() {
                return Some(s.clone());
            }
        }
        if is_name(right) {
            if let Expr::Literal(Value::Str(s)) = left.as_ref() {
                return Some(s.clone());
            }
        }
    }
    if let Expr::Func { func, args } = e {
        if !args.first().is_some_and(is_name) {
            return None;
        }
        let lit = match args.get(1) {
            Some(Expr::Literal(Value::Str(s))) => s.as_str(),
            _ => return None,
        };
        return match func {
            Func::Contains | Func::StartsWith | Func::EndsWith => Some(lit.to_string()),
            Func::Like => {
                let run: String = lit.chars().take_while(|&c| c != '%' && c != '_').collect();
                (!run.is_empty()).then_some(run)
            }
            _ => None,
        };
    }
    None
}

/// **String prefilter pushdown.** Annotate a CSV source with required literal
/// substrings drawn from its single `FilterProject` consumer's predicates
/// (`contains(field,"S")`, `field == "S"`, `starts_with`/`ends_with`, and the
/// literal run of a `like` pattern). The reader then skips any raw line that
/// lacks the substring *before* splitting it — a ripgrep-style byte pre-scan.
///
/// Safety: this is a **superset** filter (a line containing the substring may
/// still fail the real predicate; a line lacking it can never pass), and the
/// `FilterProject` downstream re-checks every surviving row — so the result is
/// unchanged. Only top-level `and` conjuncts contribute (never under `or`).
fn string_prefilter(mut graph: PlanGraph, report: &mut OptReport) -> PlanGraph {
    let mut pushed = 0usize;
    for sid in 0..graph.nodes.len() {
        if !matches!(
            &graph.nodes[sid].op,
            Op::Source { codec: Codec::Csv { str_prefilter, .. }, .. } if str_prefilter.is_empty()
        ) {
            continue;
        }
        let consumers = graph.outputs_of(sid);
        if consumers.len() != 1 {
            continue;
        }
        let preds = match &graph.nodes[consumers[0]].op {
            Op::FilterProject { preds, .. } => preds.clone(),
            _ => continue,
        };
        let mut conjuncts: Vec<&Expr> = Vec::new();
        for p in &preds {
            collect_conjuncts(p, &mut conjuncts);
        }
        let needles: Vec<String> = conjuncts
            .into_iter()
            .filter_map(literal_substring_atom)
            .filter(|s| prescan_safe(s))
            .collect();
        if needles.is_empty() {
            continue;
        }
        if let Op::Source {
            codec: Codec::Csv { str_prefilter, .. },
            ..
        } = &mut graph.nodes[sid].op
        {
            *str_prefilter = needles;
            pushed += 1;
        }
    }
    if pushed > 0 {
        report.applied.push(format!(
            "string_prefilter: literal-substring pre-scan on {pushed} source read(s)"
        ));
    }
    graph
}

/// Extract a required literal substring from a predicate, if one is implied:
/// `contains(f,"S")`, `f == "S"`, `starts_with(f,"S")`, `ends_with(f,"S")`, or
/// the leading literal run of `like(f,"S%…")`. `None` when no substring is
/// guaranteed to appear in a matching line (e.g. `!=`, `or`, numeric, regex).
fn literal_substring_atom(e: &Expr) -> Option<String> {
    // `field == "literal"` — the whole literal must appear in the line.
    if let Expr::Compare {
        left,
        op: CmpOp::Eq,
        right,
    } = e
    {
        // Only a real **column** field can imply a raw-line substring: a
        // `source.<field>` accessor (Access::Source) reads provenance, not the
        // row bytes, so its literal must NOT be pushed to the byte pre-scan.
        if let (Expr::Field { access, .. }, Expr::Literal(Value::Str(s))) =
            (left.as_ref(), right.as_ref())
        {
            if access.is_column() {
                return Some(s.clone());
            }
        }
        if let (Expr::Literal(Value::Str(s)), Expr::Field { access, .. }) =
            (left.as_ref(), right.as_ref())
        {
            if access.is_column() {
                return Some(s.clone());
            }
        }
    }
    // String functions whose match requires a literal substring to be present.
    if let Expr::Func { func, args } = e {
        let lit = match args.get(1) {
            Some(Expr::Literal(Value::Str(s))) => s.as_str(),
            _ => return None,
        };
        return match func {
            Func::Contains | Func::StartsWith | Func::EndsWith => Some(lit.to_string()),
            // `like` with a leading literal run (`"abc%…"` / `"abc_…"`): the run
            // up to the first wildcard must appear. No leading run → no needle.
            Func::Like => {
                let run: String = lit.chars().take_while(|&c| c != '%' && c != '_').collect();
                (!run.is_empty()).then_some(run)
            }
            _ => None,
        };
    }
    None
}

/// Whether a literal needle is safe for the reader's **raw-line** byte pre-scan
/// (issue #37). The pre-scan runs `line.contains(needle)` on the un-decoded CSV
/// bytes, but a logical field value is stored quote-escaped: an embedded `"` is
/// written `""`, and `\n`/`\r` inside a quoted field span multiple raw lines.
/// So a needle containing any of those could fail to match a row that the real
/// predicate accepts — a **false negative** that would break the superset
/// guarantee. We simply decline to push such needles; `FilterProject` still
/// checks every row, so the result stays correct (we only forgo the speedup).
/// The delimiter itself is safe: a value containing it is quoted, so the needle
/// still appears verbatim in the raw bytes.
fn prescan_safe(needle: &str) -> bool {
    !needle.is_empty() && !needle.contains(['"', '\n', '\r'])
}
/// `field <cmp> number` predicates of its single `FilterProject` consumer, so
/// the reader can skip *building* rows that are definitely out. Conservative
/// and additive: the `FilterProject` is left untouched (it re-checks every
/// surviving row), so the result is unchanged — this only avoids work.
///
/// Restricted to a source with exactly one consumer (fan-out would need a
/// predicate common to every branch). Only `Field CMP NumericLiteral` atoms are
/// lifted; `and`/`or`/string/computed predicates stay solely in the consumer.
fn filter_pushdown(mut graph: PlanGraph, report: &mut OptReport) -> PlanGraph {
    let mut pushed = 0usize;
    for sid in 0..graph.nodes.len() {
        if !matches!(&graph.nodes[sid].op, Op::Source { codec: Codec::Csv { prefilter, .. }, .. } if prefilter.is_empty())
        {
            continue;
        }
        let consumers = graph.outputs_of(sid);
        if consumers.len() != 1 {
            continue;
        }
        let preds = match &graph.nodes[consumers[0]].op {
            Op::FilterProject { preds, .. } => preds.clone(),
            _ => continue,
        };
        // Flatten top-level `and` chains; an atom under `or` is *not* a
        // conjunct and must not be lifted.
        let mut conjuncts: Vec<&Expr> = Vec::new();
        for p in &preds {
            collect_conjuncts(p, &mut conjuncts);
        }
        let pf: Vec<(String, CmpOp, f64)> = conjuncts
            .into_iter()
            .filter_map(simple_numeric_atom)
            .collect();
        if pf.is_empty() {
            continue;
        }
        if let Op::Source {
            codec: Codec::Csv { prefilter, .. },
            ..
        } = &mut graph.nodes[sid].op
        {
            *prefilter = pf;
            pushed += 1;
        }
    }
    if pushed > 0 {
        report.applied.push(format!(
            "filter_pushdown: pre-filtered {pushed} source read(s) at the reader"
        ));
    }
    graph
}

/// Flatten a top-level `and` chain into its conjuncts (an `or`, comparison,
/// etc. is itself one conjunct — we never descend into `or`).
fn collect_conjuncts<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
    match e {
        Expr::And(a, b) => {
            collect_conjuncts(a, out);
            collect_conjuncts(b, out);
        }
        other => out.push(other),
    }
}

/// Extract `field <cmp> number` (in either operand order) as `(col, op, rhs)`.
fn simple_numeric_atom(e: &Expr) -> Option<(String, CmpOp, f64)> {
    let Expr::Compare { left, op, right } = e else {
        return None;
    };
    // A `source.<field>` accessor (Access::Source) is not a readable column, so
    // it can't be lifted into the reader's numeric prefilter.
    match (left.as_ref(), right.as_ref()) {
        (Expr::Field { name, access }, Expr::Literal(v)) if access.is_column() => {
            v.as_f64().map(|r| (name.clone(), *op, r))
        }
        (Expr::Literal(v), Expr::Field { name, access }) if access.is_column() => {
            v.as_f64().map(|r| (name.clone(), flip_cmp(*op), r))
        }
        _ => None,
    }
}

/// `lit op field` ⇒ `field flip(op) lit`.
fn flip_cmp(op: CmpOp) -> CmpOp {
    match op {
        CmpOp::Lt => CmpOp::Gt,
        CmpOp::Le => CmpOp::Ge,
        CmpOp::Gt => CmpOp::Lt,
        CmpOp::Ge => CmpOp::Le,
        CmpOp::Eq => CmpOp::Eq,
        CmpOp::Ne => CmpOp::Ne,
    }
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
            Op::Source {
                codec: Codec::Csv {
                    projection: None,
                    ..
                },
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
        if let Op::Source {
            codec: Codec::Csv { projection, .. },
            ..
        } = &mut graph.nodes[sid].op
        {
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
        // A `source.<field>` accessor (Access::Source) reads provenance, not a
        // column, so it is not a "live column" for projection pushdown.
        Expr::Field { name, access } => {
            if access.is_column() {
                push_unique(out, name)
            }
        }
        // A union sub-view `base.name` (§29.3, s2) reads the physical column
        // `base`, so keep `base` live for projection pushdown.
        Expr::SubView { base, .. } => push_unique(out, base),
        // A value hole references no column.
        Expr::Literal(_) | Expr::Hole(_) => {}
        Expr::Compare { left, right, .. } => {
            collect_fields(left, out);
            collect_fields(right, out);
        }
        Expr::And(a, b) | Expr::Or(a, b) => {
            collect_fields(a, out);
            collect_fields(b, out);
        }
        Expr::Cast { expr, .. } => collect_fields(expr, out),
        Expr::Func { args, .. } => {
            for a in args {
                collect_fields(a, out);
            }
        }
        Expr::Arith { left, right, .. } => {
            collect_fields(left, out);
            collect_fields(right, out);
        }
        Expr::Case { branches, default } => {
            for (cond, val) in branches {
                collect_fields(cond, out);
                collect_fields(val, out);
            }
            if let Some(d) = default {
                collect_fields(d, out);
            }
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
            leading_comments: n.leading_comments.clone(),
            applied_from: n.applied_from.clone(),
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
        // CSV-only, by path — unchanged from the format-specific era (other codecs
        // were never deduped; generalizing to all sources is a follow-up).
        if let Op::Source {
            codec: Codec::Csv { .. },
            discovery,
            ..
        } = &n.op
        {
            if n.label.is_some() {
                continue;
            }
            let path = discovery.path();
            match canon.get(path) {
                None => {
                    canon.insert(path, n.id);
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
            leading_comments: n.leading_comments.clone(),
            applied_from: n.applied_from.clone(),
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
            .filter(|n| matches!(n.op, Op::Source { .. }))
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
            .find(|n| matches!(n.op, Op::Source { .. }))
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
    fn filter_pushdown_sets_numeric_prefilter() {
        // `age >= 20` (numeric atom) is lifted onto the reader; the
        // FilterProject still carries it (re-checks), so results are unchanged
        // (gated separately by tests/optimizer_equiv.rs).
        let src = "F:\n open d.csv\n |? age >= 20 and country == \"JP\"\n |> name age\n;";
        let g = rivus_parser::parse(src).unwrap();
        let (opt, report) = optimize(g);
        let s = opt
            .nodes
            .iter()
            .find(|n| matches!(n.op, Op::Source { .. }))
            .unwrap();
        match &s.op {
            Op::Source {
                codec: Codec::Csv { prefilter, .. },
                ..
            } => {
                // Only the numeric atom is lifted; the string compare stays put.
                assert_eq!(prefilter.len(), 1);
                assert_eq!(prefilter[0].0, "age");
                assert_eq!(prefilter[0].2, 20.0);
            }
            other => panic!("expected a CSV source, got {other:?}"),
        }
        assert!(report.applied.iter().any(|l| l.contains("filter_pushdown")));
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
            .find(|n| matches!(n.op, Op::Source { .. }))
            .unwrap();
        match &s.op {
            Op::Source {
                codec:
                    Codec::Csv {
                        projection: Some(cols),
                        ..
                    },
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
            .find(|n| matches!(n.op, Op::Source { .. }))
            .unwrap();
        assert!(
            matches!(
                s.op,
                Op::Source {
                    codec: Codec::Csv {
                        projection: None,
                        ..
                    },
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
