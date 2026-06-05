//! Bug specs (BUG-A/B/C): executable regression specs tracked in docs/TEST-AUDIT.md.
//!
//! Moved verbatim from the former monolithic `stress.rs` (design 26 §26.8.1,
//! mechanical move-only split; logic unchanged).

use super::*;

#[test]
fn dropna_drops_blank_numeric_rows_bug_a() {
    // BUG-A (now fixed by the null model, design 26 STEP 2-②): age infers i64;
    // a blank numeric cell is `null` (not 0), so `dropna age` drops rows 2 and 4.
    let text = "id,age\n1,25\n2,\n3,40\n4,\n";
    let f = TempCsv(gendata::write_temp_bytes("bug_dropna", text.as_bytes()));
    let p = f.0.display();
    let res = run_src(&format!("N:\n open {p}\n dropna age\n |> id\n;"), 4096);
    assert_eq!(
        collect_i64(&res, "N", "id"),
        vec![1, 3],
        "dropna must drop rows whose (numeric) age is blank"
    );
}

#[test]
fn datetime_auto_inferred_without_declaration_bug_b() {
    let text = "ts,v\n2024-06-03T14:30:00,1\n2024-06-04T09:00:00,2\n";
    let f = TempCsv(gendata::write_temp_bytes("bug_dtinfer", text.as_bytes()));
    let p = f.0.display();
    let res = run_src(&format!("D:\n open {p}\n |> ts v\n;"), 4096); // no (ts:datetime)
    let o = res
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some("D"))
        .unwrap();
    let ci = o.chunks[0].schema.index_of("ts").unwrap();
    assert!(
        matches!(
            o.chunks[0].schema.fields[ci].dtype,
            rivus_core::DataType::DateTime { .. }
        ),
        "an undeclared ISO datetime column should infer the datetime lane"
    );
}

#[test]
fn datetime_parses_fractional_and_timezone_bug_c() {
    let text = "ts\n2024-06-03T14:30:00.5\n2024-06-03T14:30:00Z\n2024-06-03T14:30:00+09:00\n";
    let f = TempCsv(gendata::write_temp_bytes("bug_dtfmt", text.as_bytes()));
    let p = f.0.display();
    let res = run_src(&format!("D:\n open {p} (ts:datetime)\n |> ts\n;"), 4096);
    assert!(
        !res.errors
            .iter()
            .any(|e| e.message.contains("could not be parsed")),
        "fractional-second / timezone ISO datetimes should parse, not fail: {:?}",
        res.errors
    );
}
