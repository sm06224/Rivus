//! `rivus-ir` — the DAG intermediate representation.
//!
//! The IR is the heart of Rivus: source parses *into* it, the optimizer
//! rewrites it, the runtime executes it, and [`PlanGraph::to_source`] turns it
//! back into readable source. See `docs/design/04-pipeline-ir.md`.

pub mod expr;
pub mod graph;

pub use expr::{Access, CmpOp, Expr};
pub use graph::{
    AggFunc, BinType, Edge, EdgeKind, Endian, Hook, HookAction, HookEvent, Node, NodeId, Op,
    PlanGraph,
};

#[cfg(test)]
mod tests {
    use super::*;
    use rivus_core::Value;

    #[test]
    fn topo_order_of_linear_chain() {
        let mut g = PlanGraph::new();
        let a = g.add_node(Op::OpenCsv {
            path: "users.csv".into(),
            projection: None,
        });
        let b = g.add_node(Op::Filter {
            pred: Expr::Compare {
                left: Box::new(Expr::field("age")),
                op: CmpOp::Ge,
                right: Box::new(Expr::Literal(Value::I64(20))),
            },
        });
        g.add_edge(a, b, EdgeKind::Stream);
        g.label_node(b, "Users");
        let order = g.topo_order().unwrap();
        assert_eq!(order, vec![a, b]);
    }

    #[test]
    fn reversible_source_roundtrips_shape() {
        let mut g = PlanGraph::new();
        let a = g.add_node(Op::OpenCsv {
            path: "users.csv".into(),
            projection: None,
        });
        let b = g.add_node(Op::Filter {
            pred: Expr::Compare {
                left: Box::new(Expr::field("age")),
                op: CmpOp::Ge,
                right: Box::new(Expr::Literal(Value::I64(20))),
            },
        });
        let c = g.add_node(Op::Project {
            fields: vec!["name".into()],
        });
        g.add_edge(a, b, EdgeKind::Stream);
        g.add_edge(b, c, EdgeKind::Stream);
        g.label_node(c, "Users");
        let src = g.to_source();
        assert!(src.contains("Users:"));
        assert!(src.contains("open users.csv"));
        assert!(src.contains("|? $_.age >= 20"));
        assert!(src.contains("|> name"));
    }
}
