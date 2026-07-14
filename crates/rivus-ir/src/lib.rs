//! `rivus-ir` — the DAG intermediate representation.
//!
//! The IR is the heart of Rivus: source parses *into* it, the optimizer
//! rewrites it, the runtime executes it, and [`PlanGraph::to_source`] turns it
//! back into readable source. See `docs/design/04-pipeline-ir.md`.

pub mod expr;
pub mod graph;
pub mod schema_prop;

pub use expr::{is_type_word, Access, ArithOp, CmpOp, Expr, Func, PathExpr, PathSeg};
pub use graph::{
    delim_for_path, delim_modifier_for, is_http_url, parse_route_template, AggFunc, BinType, Codec,
    Discovery, Disposition, Edge, EdgeKind, Endian, FillMethod, Hook, HookAction, HookEvent,
    JoinKind, Node, NodeId, Op, PlanGraph, Provenance, ReadFmt, Route, RouteSeg, ShiftKind,
    SinkCodec, SubView, Transport, ViewDef, COMMA,
};

#[cfg(test)]
mod tests {
    use super::*;
    use rivus_core::Value;

    #[test]
    fn is_http_url_recognizes_scheme_and_is_byte_safe() {
        assert!(is_http_url("http://x/a.csv"));
        assert!(is_http_url("HTTPS://x/a.csv"));
        assert!(is_http_url("  http://x")); // leading space trimmed
        assert!(!is_http_url("ftp://x"));
        assert!(!is_http_url("/local/a.csv"));
        // A non-ASCII path must never panic on the fixed-length prefix slice
        // (a byte index could fall mid-multibyte-char; #178 regression).
        assert!(!is_http_url("テスト/データ.csv"));
        assert!(!is_http_url("日"));
    }

    #[test]
    fn topo_order_of_linear_chain() {
        let mut g = PlanGraph::new();
        let a = g.add_node(Op::source("users.csv", Codec::csv(b',')));
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
        let a = g.add_node(Op::source("users.csv", Codec::csv(b',')));
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
