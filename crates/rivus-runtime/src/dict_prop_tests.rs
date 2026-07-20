//! design/42 stage (a) property tests（批准条件①）: a dict-encoded Str lane
//! must be **observationally identical** to the plain lane — same dtype, same
//! `value()`s, same bytes out of every consumer on the operator surface. Each
//! test drives the SAME rows through both representations and asserts
//! equality, so any behavior-deciding `ColumnData::Str` match that forgets
//! the dict lane fails here instead of silently diverging in a flow.

use crate::operators::{self, OpCtx};
use rivus_core::{
    Chunk, Column, ColumnData, DataType, DictColumn, Field, Schema, StrColumn, Validity, Value,
};
use rivus_ir::{AggFunc, Op, PathExpr};
use std::sync::Arc;

/// The adversarial cell set: empties, delimiters, quotes, newlines, multibyte,
/// repeats (so the dictionary actually dedups) and a long outlier.
const CELLS: &[&str] = &[
    "alpha",
    "",
    "a,b",
    "q\"t",
    "line\nbreak",
    "多バイトé",
    "alpha",
    "beta",
    "alpha",
    "多バイトé",
    "the-long-outlier-cell-padding-past-any-inline-buffer",
    "beta",
];
/// Rows 3 and 7 are null (validity, not cell bytes).
const NULLS: &[usize] = &[3, 7];

fn plain_col() -> Column {
    let mut s = StrColumn::with_capacity(CELLS.len(), 0);
    for c in CELLS {
        s.push(c);
    }
    let mut bits = vec![true; CELLS.len()];
    for &n in NULLS {
        bits[n] = false;
    }
    Column::new(ColumnData::Str(s), Validity::from_bits(&bits))
}

fn dict_col() -> Column {
    let mut dict = StrColumn::with_capacity(0, 0);
    let mut seen: Vec<&str> = Vec::new();
    let mut codes = Vec::with_capacity(CELLS.len());
    for c in CELLS {
        let code = match seen.iter().position(|s| s == c) {
            Some(i) => i,
            None => {
                seen.push(c);
                dict.push(c);
                seen.len() - 1
            }
        };
        codes.push(code as u32);
    }
    let mut bits = vec![true; CELLS.len()];
    for &n in NULLS {
        bits[n] = false;
    }
    Column::new(
        ColumnData::StrDict(DictColumn { dict, codes }),
        Validity::from_bits(&bits),
    )
}

fn chunk_with(cat: Column) -> Chunk {
    let n = cat.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("cat".to_string(), DataType::Str),
        Field::new("val".to_string(), DataType::I64),
    ]));
    let val = Column::new(
        ColumnData::I64((0..n as i64).collect()),
        Validity::all_valid(),
    );
    Chunk::new(0, schema, vec![cat, val])
}

/// Null-aware row values — the observational surface.
fn rows(c: &Chunk) -> Vec<Vec<Value>> {
    (0..c.len)
        .map(|r| (0..c.columns.len()).map(|i| c.value(r, i)).collect())
        .collect()
}

fn drive(op: &Op, chunk: Chunk) -> Vec<Chunk> {
    let mut o = operators::build(op, &[], 4096, false);
    let mut errors = Vec::new();
    let mut id = 1u64;
    let mut ctx = OpCtx {
        label: "dict-prop".to_string(),
        errors: &mut errors,
        next_chunk_id: &mut id,
    };
    let mut out = o.process(0, chunk, &mut ctx);
    out.extend(o.finish(&mut ctx));
    out
}

fn assert_same_out(op: &Op, what: &str) {
    let p: Vec<_> = drive(op, chunk_with(plain_col()))
        .iter()
        .map(rows)
        .collect();
    let d: Vec<_> = drive(op, chunk_with(dict_col())).iter().map(rows).collect();
    assert_eq!(p, d, "dict vs plain diverged through {what}");
}

/// The lane IS Str: dtype, len, and every (null-aware) cell value agree.
#[test]
fn dict_lane_is_observationally_str() {
    let (p, d) = (plain_col(), dict_col());
    assert_eq!(p.dtype(), DataType::Str);
    assert_eq!(d.dtype(), DataType::Str, "representation, never a type");
    assert_eq!(p.len(), d.len());
    let (cp, cd) = (chunk_with(p), chunk_with(d));
    assert_eq!(rows(&cp), rows(&cd));
}

/// Sink writers emit identical bytes (CSV quoting rules included).
#[test]
fn dict_lane_write_cell_bytes_identical() {
    let (p, d) = (plain_col(), dict_col());
    for row in 0..p.len() {
        let (mut a, mut b) = (String::new(), String::new());
        operators::write_cell(&mut a, &p, row, b',');
        operators::write_cell(&mut b, &d, row, b',');
        assert_eq!(a, b, "csv cell @row {row}");
        let (mut a, mut b) = (String::new(), String::new());
        operators::sink::write_json_cell(&mut a, &p, row);
        operators::sink::write_json_cell(&mut b, &d, row);
        assert_eq!(a, b, "json cell @row {row}");
    }
}

/// gather keeps the representation, gather_opt materializes — both must read
/// back the plain lane's exact values; append across representations must
/// equal plain+plain (the silent-`_` truncation guard).
#[test]
fn dict_lane_gather_and_append_identical() {
    let (p, d) = (plain_col(), dict_col());
    let idx = [11usize, 0, 6, 6, 2, 9];
    let (gp, gd) = (p.data().gather(&idx), d.data().gather(&idx));
    assert_eq!(gd.dtype(), DataType::Str);
    for (r, _) in idx.iter().enumerate() {
        assert_eq!(gp.value_at(r), gd.value_at(r), "gather @{r}");
    }
    let opt = [Some(4usize), None, Some(10), None, Some(0)];
    let (op_, od) = (p.data().gather_opt(&opt), d.data().gather_opt(&opt));
    for r in 0..opt.len() {
        assert_eq!(op_.value_at(r), od.value_at(r), "gather_opt @{r}");
    }
    for (a, b, what) in [
        (p.data().clone(), d.data().clone(), "plain<-dict"),
        (d.data().clone(), p.data().clone(), "dict<-plain"),
        (d.data().clone(), d.data().clone(), "dict<-dict"),
    ] {
        let mut merged = a;
        merged.append(&b);
        let mut oracle = p.data().clone();
        oracle.append(p.data());
        assert_eq!(merged.len(), oracle.len(), "append len {what}");
        for r in 0..oracle.len() {
            assert_eq!(merged.value_at(r), oracle.value_at(r), "append {what} @{r}");
        }
    }
}

/// Sort / distinct / group / fill drive the same rows to the same outputs.
#[test]
fn dict_lane_operator_surface_identical() {
    assert_same_out(
        &Op::Sort {
            keys: vec![(PathExpr::bare("cat"), false)],
        },
        "sort by cat",
    );
    assert_same_out(&Op::Distinct { keys: vec![] }, "distinct");
    assert_same_out(
        &Op::GroupBy {
            keys: vec![PathExpr::bare("cat")],
            aggs: vec![
                (AggFunc::Count, "val".to_string()),
                (AggFunc::Sum, "val".to_string()),
            ],
        },
        "group by cat",
    );
    assert_same_out(
        &Op::Fill {
            col: "cat".to_string(),
            method: rivus_ir::FillMethod::Ffill,
        },
        "fill cat ffill",
    );
}
