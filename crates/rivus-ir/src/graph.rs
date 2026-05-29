//! The DAG IR.
//!
//! Rivus is DAG-native (Master principle #3): even a "linear" pipeline is a
//! degenerate DAG. Nodes are flow scopes / transforms / events; edges are
//! streams (or error side-channels). The graph is the single source of truth
//! that the optimizer rewrites and that [`PlanGraph::to_source`] regenerates
//! back into readable Rivus source (Master principle #5: IR reversibility).

use crate::expr::Expr;
use rivus_core::{Mode, Severity};
use std::collections::HashMap;
use std::fmt::Write as _;

pub type NodeId = usize;

/// A flow operator. One enum spanning sources, transforms, fan-out/in and
/// sinks — because in Rivus they are all just nodes in the same graph.
#[derive(Debug, Clone)]
pub enum Op {
    /// `open path.csv`. `projection`, when set by the optimizer
    /// (`project_pushdown`), restricts which columns the reader builds — unused
    /// columns are never parsed or allocated.
    OpenCsv {
        path: String,
        projection: Option<Vec<String>>,
    },
    /// `stream X` — replay of a named flow (and, internally, a reference edge).
    StreamRef { name: String },
    /// `|? <pred>`
    Filter { pred: Expr },
    /// `|> field [field ...]`
    Project { fields: Vec<String> },
    /// `|# key` — group / partition by key (MVP: group + count).
    GroupBy { key: String },
    /// Fused linear chain of filters and an optional trailing projection,
    /// produced by the optimizer (`fuse_linear`). All `preds` must pass (AND);
    /// when `fields` is `Some`, only those columns are materialized — gathering
    /// the projected columns once instead of filter-then-project's two passes.
    FilterProject {
        preds: Vec<Expr>,
        fields: Option<Vec<String>>,
    },
    /// `->` fan-out (tee): forwards each chunk to every outgoing edge.
    Branch,
    /// `+` merge: union of all incoming streams.
    Merge,
    /// `&` synchronized join on keys.
    Join { left_key: String, right_key: String },
    /// `print` / default leaf sink.
    SinkPrint,
    /// `save path.csv`
    SinkCsv { path: String },
}

impl Op {
    pub fn kind_str(&self) -> &'static str {
        match self {
            Op::OpenCsv { .. } => "open",
            Op::StreamRef { .. } => "stream",
            Op::Filter { .. } => "filter",
            Op::Project { .. } => "project",
            Op::FilterProject { .. } => "fused",
            Op::GroupBy { .. } => "group",
            Op::Branch => "branch",
            Op::Merge => "merge",
            Op::Join { .. } => "join",
            Op::SinkPrint => "print",
            Op::SinkCsv { .. } => "save",
        }
    }

    /// Render this op as the pipeline fragment that produced it.
    fn to_src_line(&self) -> String {
        match self {
            Op::OpenCsv { path, projection } => match projection {
                Some(cols) => format!("open {path}  # read-only: {}", cols.join(",")),
                None => format!("open {path}"),
            },
            Op::StreamRef { name } => format!("stream {name}"),
            Op::Filter { pred } => format!("|? {pred}"),
            Op::Project { fields } => format!("|> {}", fields.join(" ")),
            Op::FilterProject { preds, fields } => {
                let mut s: String = preds.iter().map(|p| format!("|? {p} ")).collect();
                if let Some(f) = fields {
                    s.push_str(&format!("|> {}", f.join(" ")));
                }
                s.trim_end().to_string()
            }
            Op::GroupBy { key } => format!("|# {key}"),
            Op::Branch => "-> branch".to_string(),
            Op::Merge => "+ merge".to_string(),
            Op::Join {
                left_key,
                right_key,
            } => format!("& on {left_key} = {right_key}"),
            Op::SinkPrint => "print".to_string(),
            Op::SinkCsv { path } => format!("save {path}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeKind {
    /// Normal data flow.
    Stream,
    /// Error side-channel (continue-first error stream).
    Error,
}

#[derive(Debug, Clone)]
pub struct Edge {
    pub from: NodeId,
    pub to: NodeId,
    pub kind: EdgeKind,
}

/// Lifecycle events (Observability spec §10). Hooks are themselves scopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookEvent {
    Begin,
    Process,
    End,
    First,
    Last,
    ChunkBegin,
    ChunkEnd,
    Error,
    Recovery,
    ModeChange,
    Retry,
    Timeout,
}

impl HookEvent {
    pub fn parse(s: &str) -> Option<HookEvent> {
        Some(match s {
            "begin" => HookEvent::Begin,
            "process" => HookEvent::Process,
            "end" => HookEvent::End,
            "first" => HookEvent::First,
            "last" => HookEvent::Last,
            "chunk_begin" => HookEvent::ChunkBegin,
            "chunk_end" => HookEvent::ChunkEnd,
            "error" => HookEvent::Error,
            "recovery" => HookEvent::Recovery,
            "mode_change" => HookEvent::ModeChange,
            "retry" => HookEvent::Retry,
            "timeout" => HookEvent::Timeout,
            _ => return None,
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            HookEvent::Begin => "begin",
            HookEvent::Process => "process",
            HookEvent::End => "end",
            HookEvent::First => "first",
            HookEvent::Last => "last",
            HookEvent::ChunkBegin => "chunk_begin",
            HookEvent::ChunkEnd => "chunk_end",
            HookEvent::Error => "error",
            HookEvent::Recovery => "recovery",
            HookEvent::ModeChange => "mode_change",
            HookEvent::Retry => "retry",
            HookEvent::Timeout => "timeout",
        }
    }
}

/// What a hook does when it fires (MVP subset).
#[derive(Debug, Clone)]
pub enum HookAction {
    /// Route matching items/chunks to a named flow (e.g. `on error: Errors`).
    Route(String),
    /// Escalate the runtime mode (`transition degraded`).
    Transition(Mode),
    /// Emit a log line.
    Log(String),
}

#[derive(Debug, Clone)]
pub struct Hook {
    pub event: HookEvent,
    /// Optional guard: `on error severity >= warning:`
    pub min_severity: Option<Severity>,
    pub action: HookAction,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    /// Scope label, if this node is the visible output of a named scope.
    pub label: Option<String>,
    pub op: Op,
    pub hooks: Vec<Hook>,
}

#[derive(Debug, Clone, Default)]
pub struct PlanGraph {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    /// Scope label → producing node.
    pub labels: HashMap<String, NodeId>,
}

impl PlanGraph {
    pub fn new() -> Self {
        PlanGraph::default()
    }

    pub fn add_node(&mut self, op: Op) -> NodeId {
        let id = self.nodes.len();
        self.nodes.push(Node {
            id,
            label: None,
            op,
            hooks: Vec::new(),
        });
        id
    }

    pub fn label_node(&mut self, id: NodeId, label: impl Into<String>) {
        let label = label.into();
        self.nodes[id].label = Some(label.clone());
        self.labels.insert(label, id);
    }

    pub fn add_edge(&mut self, from: NodeId, to: NodeId, kind: EdgeKind) {
        self.edges.push(Edge { from, to, kind });
    }

    pub fn add_hook(&mut self, id: NodeId, hook: Hook) {
        self.nodes[id].hooks.push(hook);
    }

    pub fn inputs_of(&self, id: NodeId) -> Vec<NodeId> {
        self.edges
            .iter()
            .filter(|e| e.to == id && e.kind == EdgeKind::Stream)
            .map(|e| e.from)
            .collect()
    }

    pub fn outputs_of(&self, id: NodeId) -> Vec<NodeId> {
        self.edges
            .iter()
            .filter(|e| e.from == id && e.kind == EdgeKind::Stream)
            .map(|e| e.to)
            .collect()
    }

    /// Sinks / leaves: nodes with no downstream stream edge.
    pub fn leaves(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|n| self.outputs_of(n.id).is_empty())
            .map(|n| n.id)
            .collect()
    }

    /// Kahn topological order over stream edges. Returns `None` on a cycle
    /// (Rivus forbids cycles in the MVP; feedback edges are future work).
    pub fn topo_order(&self) -> Option<Vec<NodeId>> {
        let n = self.nodes.len();
        let mut indeg = vec![0usize; n];
        for e in &self.edges {
            if e.kind == EdgeKind::Stream {
                indeg[e.to] += 1;
            }
        }
        let mut queue: Vec<NodeId> = (0..n).filter(|&i| indeg[i] == 0).collect();
        let mut order = Vec::with_capacity(n);
        while let Some(id) = queue.pop() {
            order.push(id);
            for succ in self.outputs_of(id) {
                indeg[succ] -= 1;
                if indeg[succ] == 0 {
                    queue.push(succ);
                }
            }
        }
        if order.len() == n {
            Some(order)
        } else {
            None
        }
    }

    /// Regenerate readable Rivus source from the graph (Master principle #5).
    /// This is intentionally best-effort/canonical: the optimizer can rewrite
    /// the graph and we can always show the user the resulting source.
    pub fn to_source(&self) -> String {
        let mut out = String::new();
        // Emit one block per labeled scope, in stable id order.
        let mut labeled: Vec<&Node> = self.nodes.iter().filter(|n| n.label.is_some()).collect();
        labeled.sort_by_key(|n| n.id);

        for node in labeled {
            let label = node.label.as_ref().unwrap();
            let inputs = self.inputs_of(node.id);

            // Merge / join scopes render as `Label: A + B ;`.
            match &node.op {
                Op::Merge => {
                    let names = self.input_labels(&inputs).join(" + ");
                    let _ = writeln!(out, "{label}:\n    {names}\n;");
                    continue;
                }
                Op::Join {
                    left_key,
                    right_key,
                } => {
                    let names = self.input_labels(&inputs).join(" & ");
                    let _ = writeln!(
                        out,
                        "{label}:\n    {names}    # on {left_key} = {right_key}\n;"
                    );
                    continue;
                }
                _ => {}
            }

            // Otherwise walk the linear chain ending at this node.
            let chain = self.linear_chain_to(node.id);
            let _ = writeln!(out, "{label}:");
            for &nid in &chain {
                let _ = writeln!(out, "    {}", self.nodes[nid].op.to_src_line());
                for h in &self.nodes[nid].hooks {
                    self.write_hook(&mut out, h);
                }
            }
            // Render branch children, if any.
            for succ in self.outputs_of(node.id) {
                if let Some(child_label) = &self.nodes[succ].label {
                    if matches!(self.nodes[node.id].op, Op::Branch)
                        || self.is_branch_child(node.id, succ)
                    {
                        let _ = writeln!(out, "    -> {child_label}: ... ;");
                    }
                }
            }
            let _ = writeln!(out, ";");
        }
        out
    }

    fn input_labels(&self, inputs: &[NodeId]) -> Vec<String> {
        inputs
            .iter()
            .map(|&i| {
                self.nodes[i]
                    .label
                    .clone()
                    .unwrap_or_else(|| format!("<{}>", self.nodes[i].op.kind_str()))
            })
            .collect()
    }

    fn is_branch_child(&self, parent: NodeId, _child: NodeId) -> bool {
        self.outputs_of(parent).len() > 1
    }

    /// Collect the linear chain of single-input nodes leading up to `id`,
    /// stopping at fan-in (merge/join) or labeled upstream scopes.
    fn linear_chain_to(&self, id: NodeId) -> Vec<NodeId> {
        let mut chain = vec![id];
        let mut cur = id;
        loop {
            let inputs = self.inputs_of(cur);
            if inputs.len() != 1 {
                break;
            }
            let prev = inputs[0];
            // Stop if the predecessor is itself a labeled scope reused elsewhere.
            if self.nodes[prev].label.is_some() && self.outputs_of(prev).len() > 1 {
                break;
            }
            chain.push(prev);
            cur = prev;
        }
        chain.reverse();
        chain
    }

    fn write_hook(&self, out: &mut String, h: &Hook) {
        let guard = match h.min_severity {
            Some(s) => format!(" severity >= {s}"),
            None => String::new(),
        };
        let _ = writeln!(out, "    on {}{}:", h.event.as_str(), guard);
        match &h.action {
            HookAction::Route(name) => {
                let _ = writeln!(out, "        {name}");
            }
            HookAction::Transition(mode) => {
                let _ = writeln!(out, "        transition {mode}");
            }
            HookAction::Log(msg) => {
                let _ = writeln!(out, "        log \"{msg}\"");
            }
        }
        let _ = writeln!(out, "    ;");
    }
}
