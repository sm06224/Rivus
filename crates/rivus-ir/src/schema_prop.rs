//! Static schema propagation (§32.1) — the foundation for §32.
//!
//! Computes, for each node of a [`PlanGraph`], the **output schema** (column
//! names + nominal types) it would produce — *statically*, without running it.
//! This is a **read-only analysis over the IR**: it never changes a result and
//! never touches byte-identity. It is the foundation the dataset-centric
//! `explain` (§32.5) and path-key / nested-type resolution (§32.3-4) build on.
//!
//! Two honesty rules (§32.1, ratified #161 ①):
//!
//! * **`None` where unknowable.** A source whose columns aren't declared (a bare
//!   `open f.csv`, JSON, `read`, discovery) has no static schema, and anything
//!   downstream of an unknown schema is unknown too. We surface that as `None`
//!   rather than fabricating columns.
//! * **Nominal types.** Where a type depends on runtime observation (e.g. a
//!   `sum` that stays decimal or overflows to f64, §06.5), we report the
//!   *nominal* / default-lane type. It is a best-effort label for display and
//!   type-checking, deliberately separate from the observed lane; it never
//!   drives execution.

use crate::expr::{Access, ArithOp, Func, PathExpr, PathSeg};
use crate::graph::{AggFunc, Codec, Op, PlanGraph};
use crate::Expr;
use rivus_core::{DataType, Field, Nested, Schema};

impl PlanGraph {
    /// The static output [`Schema`] of every node, indexed by `NodeId`
    /// (§32.1). `None` for a node whose schema can't be known statically (an
    /// undeclared source, or anything downstream of one). Execution-invariant:
    /// pure analysis over the IR.
    pub fn node_schemas(&self) -> Vec<Option<Schema>> {
        let n = self.nodes.len();
        let mut out: Vec<Option<Schema>> = vec![None; n];
        let order = self.topo_order().unwrap_or_else(|| (0..n).collect());
        for nid in order {
            let inputs: Vec<Option<Schema>> = self
                .inputs_of(nid)
                .iter()
                .map(|&i| out[i].clone())
                .collect();
            out[nid] = op_out_schema(&self.nodes[nid].op, &inputs);
        }
        out
    }
}

/// The output schema of one op given its (ordered) input schemas. `None`
/// propagates: a transform whose input is unknown is itself unknown.
fn op_out_schema(op: &Op, inputs: &[Option<Schema>]) -> Option<Schema> {
    // The single upstream schema for the common linear case.
    let input = inputs.first().and_then(|s| s.clone());
    match op {
        // ── Sources: known only when the codec carries explicit columns. ──
        // `with filename` (§28.6, slice 2-②b) materializes a trailing text
        // column — `filename`, or `filename_r` when the data already has one
        // (§27.1 collision rule) — so the static schema must carry it too.
        Op::Source {
            codec, provenance, ..
        } => source_schema(codec).map(|mut s| {
            if provenance.materializes_filename() {
                let name = if s.index_of("filename").is_some() {
                    "filename_r"
                } else {
                    "filename"
                };
                s.fields.push(Field::new(name, DataType::Str));
            }
            s
        }),
        // Reader union / named replay / discovery decode aren't statically known.
        Op::Read { .. } | Op::StreamRef { .. } => None,

        // ── Row-only transforms: schema unchanged. ──
        Op::Filter { .. }
        | Op::Validate { .. }
        | Op::Take { .. }
        | Op::Sort { .. }
        | Op::Distinct { .. }
        | Op::DropNa { .. }
        | Op::Fill { .. }
        | Op::Branch
        | Op::Sink { .. }
        | Op::SinkPrint => input,

        // ── Column-shape transforms (deterministic from the input names). ──
        Op::Project { fields } => input.and_then(|s| s.project(fields)),
        Op::FilterProject { fields, .. } => match fields {
            Some(f) => input.and_then(|s| s.project(f)),
            None => input,
        },
        Op::Drop { cols } => input.map(|s| {
            Schema::new(
                s.fields
                    .into_iter()
                    .filter(|f| !cols.contains(&f.name))
                    .collect(),
            )
        }),
        // `explode COL` keeps every column but replaces the `List` lane with its
        // element field (§32 s4c); other columns are unchanged. A non-list (or
        // unknown) `COL` leaves the schema untouched (runtime warns).
        Op::Explode { col } => input.map(|s| {
            Schema::new(
                s.fields
                    .into_iter()
                    .map(|f| match &f.nested {
                        Some(Nested::List(elem)) if &f.name == col => {
                            // The element field, renamed to the exploded column.
                            let mut e = (**elem).clone();
                            e.name = f.name.clone();
                            e
                        }
                        _ => f,
                    })
                    .collect(),
            )
        }),
        Op::Reorder { cols } => input.map(|s| {
            let mut front: Vec<Field> = Vec::new();
            for c in cols {
                if let Some(i) = s.index_of(c) {
                    front.push(s.fields[i].clone());
                }
            }
            let rest = s
                .fields
                .into_iter()
                .filter(|f| !cols.contains(&f.name))
                .collect::<Vec<_>>();
            front.extend(rest);
            Schema::new(front)
        }),
        Op::Rename { pairs } => input.map(|s| {
            Schema::new(
                s.fields
                    .into_iter()
                    .map(|mut f| {
                        if let Some((_, new)) = pairs.iter().find(|(old, _)| *old == f.name) {
                            f.name = new.clone();
                        }
                        f
                    })
                    .collect(),
            )
        }),
        Op::Cast { casts } => input.map(|s| {
            Schema::new(
                s.fields
                    .into_iter()
                    .map(|mut f| {
                        if let Some((_, ty)) = casts.iter().find(|(c, _)| *c == f.name) {
                            f.dtype = *ty;
                        }
                        f
                    })
                    .collect(),
            )
        }),
        Op::ProjectExpr { items, .. } => input.map(|s| {
            Schema::new(
                items
                    .iter()
                    .map(|(e, alias)| Field::new(alias.clone(), expr_type(e, &s)))
                    .collect(),
            )
        }),

        // ── Reshapers. ──
        Op::GroupBy { keys, aggs } => input.map(|s| group_schema(keys, aggs, &s)),
        Op::Join { right_keys, .. } => join_schema(inputs, right_keys),
        Op::Merge => merge_schema(inputs),

        // `describe` replaces the stream with a fixed summary table whose exact
        // columns we don't pin here — report unknown rather than guess.
        Op::Describe => None,
    }
}

/// The schema a source emits, when statically known (declared CSV columns or a
/// typed binary record); `None` for inferred/undeclared/discovery codecs.
fn source_schema(codec: &Codec) -> Option<Schema> {
    match codec {
        Codec::Csv {
            declared: Some(cols),
            projection,
            ..
        } => {
            let mut s = Schema::new(
                cols.iter()
                    .map(|(n, t)| Field::new(n.clone(), t.unwrap_or(DataType::Str)))
                    .collect(),
            );
            // An optimizer projection pushdown narrows the emitted columns.
            if let Some(p) = projection {
                s = s.project(p).unwrap_or(s);
            }
            Some(s)
        }
        Codec::Binary { fields, .. } => Some(Schema::new(
            fields
                .iter()
                .map(|(n, bt)| Field::new(n.clone(), bt.lane()))
                .collect(),
        )),
        // Undeclared CSV, JSON Lines, and the discovery codec aren't statically
        // typed (columns come from the data / a runtime walk).
        Codec::Csv { declared: None, .. } | Codec::Jsonl | Codec::Discover { .. } => None,
    }
}

/// GroupBy output: each key column (runtime materializes keys on the `Str` lane,
/// design 26 §26.2), then the always-emitted `count:i64`, then one column per
/// aggregate named `{label}_{col}` with its nominal type.
fn group_schema(keys: &[PathExpr], aggs: &[(AggFunc, String)], input: &Schema) -> Schema {
    let mut fields: Vec<Field> = keys
        .iter()
        .map(|k| Field::new(k.column_name(), DataType::Str))
        .collect();
    fields.push(Field::new("count", DataType::I64));
    for (func, col) in aggs {
        let name = format!("{}_{}", func.label(), col);
        let col_ty = input
            .index_of(col)
            .map(|i| input.fields[i].dtype)
            .unwrap_or(DataType::Str);
        // `array_agg` outputs a `List` lane whose element is the aggregated
        // column's type (§32 / #172); other aggregates are flat scalar lanes.
        if matches!(func, AggFunc::ArrayAgg) {
            fields.push(Field::list(name, Field::new("item", col_ty)));
        } else {
            fields.push(Field::new(name, agg_type(func, col_ty)));
        }
    }
    Schema::new(fields)
}

/// The nominal output type of an aggregate over a column of `col_ty` (§32.1).
fn agg_type(func: &AggFunc, col_ty: DataType) -> DataType {
    match func {
        // Counts are integer.
        AggFunc::Count | AggFunc::CountDistinct => DataType::I64,
        // Order/selection aggregates keep the column's type.
        AggFunc::Min | AggFunc::Max | AggFunc::First | AggFunc::Last => col_ty,
        // `sum` keeps an exact numeric lane (int/decimal); otherwise nominal f64.
        AggFunc::Sum => match col_ty {
            DataType::I64 | DataType::Decimal { .. } => col_ty,
            _ => DataType::F64,
        },
        // Mean / spread / percentile are nominally float.
        AggFunc::Avg | AggFunc::Std | AggFunc::Pct(_) => DataType::F64,
        // `array_agg` is a List lane (the nested detail is set by `group_schema`
        // via `Field::list`; this is the bare lane marker).
        AggFunc::ArrayAgg => DataType::List,
    }
}

/// Join output (matches the runtime, join.rs): the left schema followed by the
/// right schema with the right join-keys dropped and name collisions suffixed
/// `_r`. `None` if either side's schema is unknown.
fn join_schema(inputs: &[Option<Schema>], right_keys: &[PathExpr]) -> Option<Schema> {
    let left = inputs.first().and_then(|s| s.clone())?;
    let right = inputs.get(1).and_then(|s| s.clone())?;
    let mut fields = left.fields.clone();
    for f in &right.fields {
        if right_keys.iter().any(|k| k.column_name() == f.name) {
            continue;
        }
        let name = if left.index_of(&f.name).is_some() {
            format!("{}_r", f.name)
        } else {
            f.name.clone()
        };
        fields.push(Field::new(name, f.dtype));
    }
    Some(Schema::new(fields))
}

/// Merge output: union-by-name of the inputs (first-seen column order). `None`
/// if any input schema is unknown.
fn merge_schema(inputs: &[Option<Schema>]) -> Option<Schema> {
    let mut fields: Vec<Field> = Vec::new();
    for input in inputs {
        let s = input.as_ref()?;
        for f in &s.fields {
            if !fields.iter().any(|g| g.name == f.name) {
                fields.push(f.clone());
            }
        }
    }
    Some(Schema::new(fields))
}

/// The nominal type of an expression evaluated against `schema` (§32.1). Always
/// returns a type (text is the universal fallback for an unknown column or an
/// ambiguous function), so a `ProjectExpr` schema lists every output column.
fn expr_type(e: &Expr, schema: &Schema) -> DataType {
    match e {
        Expr::Field {
            name,
            access: Access::Fast,
        } => schema
            .index_of(name)
            .map(|i| schema.fields[i].dtype)
            .unwrap_or(DataType::Str),
        // Provenance (`source.uri`) is text; deep/dynamic access isn't statically
        // resolvable, so it falls back to text.
        Expr::Field { .. } => DataType::Str,
        Expr::FieldAt(i) => schema
            .fields
            .get(*i as usize)
            .map(|f| f.dtype)
            .unwrap_or(DataType::Str),
        // A union sub-view is a zero-copy char slice of a string column.
        Expr::SubView { .. } => DataType::Str,
        // A nested path (§32 s4) resolves its leaf lane through the static nested
        // schema detail; an unresolvable path is nominal text (and runtime null).
        Expr::Path(p) => path_type(p, schema),
        Expr::Literal(v) => v.dtype(),
        // A value hole's type isn't known until bound; nominal text.
        Expr::Hole(_) => DataType::Str,
        Expr::Compare { .. } | Expr::And(_, _) | Expr::Or(_, _) => DataType::Bool,
        Expr::Arith { left, op, right } => {
            // Division yields a float; other arithmetic widens its operands.
            if matches!(op, ArithOp::Div) {
                DataType::F64
            } else {
                widen(expr_type(left, schema), expr_type(right, schema))
            }
        }
        Expr::Cast { ty, .. } => *ty,
        Expr::Func { func, args } => func_type(*func, args, schema),
        Expr::Case { branches, default } => {
            let mut ty = default
                .as_ref()
                .map(|d| expr_type(d, schema))
                .unwrap_or(DataType::Str);
            for (_, v) in branches {
                ty = widen(ty, expr_type(v, schema));
            }
            ty
        }
    }
}

/// The nominal leaf type of a nested path (§32 s4): walk the static nested
/// schema detail (`Field.nested`) one segment at a time. An unresolvable step
/// (missing field, indexing a non-list, a flat leaf with steps remaining) is
/// nominal text — matching the runtime, which yields a typed null there.
fn path_type(p: &PathExpr, schema: &Schema) -> DataType {
    let Some(idx) = schema.index_of(&p.root) else {
        return DataType::Str;
    };
    let mut field = &schema.fields[idx];
    for seg in &p.segs {
        match (seg, field.nested.as_ref()) {
            (PathSeg::Field(name), Some(Nested::Struct(children))) => {
                match children.iter().find(|c| &c.name == name) {
                    Some(child) => field = child,
                    None => return DataType::Str,
                }
            }
            (PathSeg::Index(_), Some(Nested::List(elem))) => field = elem,
            // Type mismatch (indexing a struct, fielding a list, or any step past
            // a flat leaf) → unresolvable → nominal text.
            _ => return DataType::Str,
        }
    }
    field.dtype
}

/// Nominal widening of two numeric lanes (int ⊆ float ⊆ decimal; §06). A
/// non-numeric mix falls back to text.
fn widen(a: DataType, b: DataType) -> DataType {
    use DataType::*;
    match (a, b) {
        (x, y) if x == y => x,
        (Decimal { scale }, I64 | F64) | (I64 | F64, Decimal { scale }) => Decimal { scale },
        (F64, I64) | (I64, F64) => F64,
        // Same-lane numerics handled by the equality arm; anything else nominal.
        (I64, I64) => I64,
        _ => Str,
    }
}

/// The nominal return type of a scalar function (§32.1). Predicates → bool,
/// length/extractors/rounding → i64, datetime ops keep the temporal lane, the
/// rest are text.
fn func_type(func: Func, args: &[Expr], schema: &Schema) -> DataType {
    use Func::*;
    match func {
        // Boolean predicates.
        Contains | StartsWith | EndsWith | Like | Glob | Regexp | IsWeekend => DataType::Bool,
        // Integer results.
        Len | Year | Month | Day | Hour | Minute | Second | Weekday | Round | Floor | Ceil => {
            DataType::I64
        }
        Abs | Trunc | Bucket => args
            .first()
            .map(|a| expr_type(a, schema))
            .unwrap_or(DataType::F64),
        Date => DataType::Date,
        // Text-producing / null-coalescing / everything else is nominal text.
        _ => DataType::Str,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Codec, EdgeKind};
    use crate::{CmpOp, Op};
    use rivus_core::Value;

    fn declared_csv() -> Op {
        Op::Source {
            discovery: crate::Discovery::Fixed("u.csv".into()),
            transport: crate::Transport::Local,
            codec: Codec::Csv {
                header: true,
                declared: Some(vec![
                    ("uid".into(), Some(DataType::Str)),
                    ("age".into(), Some(DataType::I64)),
                    ("city".into(), Some(DataType::Str)),
                ]),
                dt_formats: vec![],
                delim: b',',
                projection: None,
                prefilter: vec![],
                str_prefilter: vec![],
            },
            provenance: crate::Provenance::Off,
        }
    }

    #[test]
    fn declared_source_schema_known() {
        let mut g = PlanGraph::new();
        let s = g.add_node(declared_csv());
        let schemas = g.node_schemas();
        let sc = schemas[s].as_ref().expect("declared source is known");
        assert_eq!(sc.field_names(), vec!["uid", "age", "city"]);
        assert_eq!(sc.fields[1].dtype, DataType::I64);
    }

    #[test]
    fn undeclared_source_is_unknown() {
        let mut g = PlanGraph::new();
        let s = g.add_node(Op::source("u.csv", Codec::csv(b',')));
        assert!(
            g.node_schemas()[s].is_none(),
            "bare open has no static schema"
        );
    }

    #[test]
    fn filter_preserves_and_project_narrows() {
        let mut g = PlanGraph::new();
        let s = g.add_node(declared_csv());
        let f = g.add_node(Op::Filter {
            pred: Expr::Compare {
                left: Box::new(Expr::field("age")),
                op: CmpOp::Ge,
                right: Box::new(Expr::Literal(Value::I64(18))),
            },
        });
        let p = g.add_node(Op::Project {
            fields: vec!["uid".into(), "city".into()],
        });
        g.add_edge(s, f, EdgeKind::Stream);
        g.add_edge(f, p, EdgeKind::Stream);
        let schemas = g.node_schemas();
        // filter keeps the full schema; project narrows to the two names.
        assert_eq!(
            schemas[f].as_ref().unwrap().field_names(),
            vec!["uid", "age", "city"]
        );
        assert_eq!(
            schemas[p].as_ref().unwrap().field_names(),
            vec!["uid", "city"]
        );
    }

    #[test]
    fn group_by_schema_keys_count_aggs() {
        let mut g = PlanGraph::new();
        let s = g.add_node(declared_csv());
        let gb = g.add_node(Op::GroupBy {
            keys: vec![PathExpr::bare("city")],
            aggs: vec![(AggFunc::Sum, "age".into())],
        });
        g.add_edge(s, gb, EdgeKind::Stream);
        let sc = g.node_schemas()[gb].clone().unwrap();
        assert_eq!(sc.field_names(), vec!["city", "count", "sum_age"]);
        // key → Str (runtime materializes keys as text), count → i64,
        // sum over i64 → i64 (exact integer lane).
        assert_eq!(sc.fields[0].dtype, DataType::Str);
        assert_eq!(sc.fields[1].dtype, DataType::I64);
        assert_eq!(sc.fields[2].dtype, DataType::I64);
    }

    #[test]
    fn unknown_propagates_downstream() {
        let mut g = PlanGraph::new();
        let s = g.add_node(Op::source("u.csv", Codec::csv(b','))); // undeclared
        let f = g.add_node(Op::Filter {
            pred: Expr::Literal(Value::Bool(true)),
        });
        g.add_edge(s, f, EdgeKind::Stream);
        assert!(
            g.node_schemas()[f].is_none(),
            "unknown input → unknown output"
        );
    }

    #[test]
    fn project_expr_types_computed_columns() {
        let mut g = PlanGraph::new();
        let s = g.add_node(declared_csv());
        let pe = g.add_node(Op::ProjectExpr {
            items: vec![
                (Expr::field("uid"), "uid".into()),
                (
                    Expr::Compare {
                        left: Box::new(Expr::field("age")),
                        op: CmpOp::Ge,
                        right: Box::new(Expr::Literal(Value::I64(18))),
                    },
                    "adult".into(),
                ),
            ],
            views: vec![],
        });
        g.add_edge(s, pe, EdgeKind::Stream);
        let sc = g.node_schemas()[pe].clone().unwrap();
        assert_eq!(sc.field_names(), vec!["uid", "adult"]);
        assert_eq!(sc.fields[0].dtype, DataType::Str); // uid column
        assert_eq!(sc.fields[1].dtype, DataType::Bool); // comparison → bool
    }
}
