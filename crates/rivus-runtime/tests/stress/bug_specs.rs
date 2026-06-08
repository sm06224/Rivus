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
fn headerless_schema_surfaces_consumed_data_row_bug_f() {
    // BUG-F, fix (a) surface (maintainer-ratified 2026-06-08): a column-naming
    // schema *without* `noheader` over a data-first file consumes the first line
    // as a header. That consumption is no longer silent — a never-silent Warn
    // surfaces it, and `noheader` is the documented remedy.
    let text = "1,alice,30\n2,bob,40\n"; // no header line; the first line is data
    let f = TempCsv(gendata::write_temp_bytes("bug_headerless", text.as_bytes()));
    let p = f.0.display();

    // No `noheader`: the first data row is consumed as a header, but surfaced.
    let res = run_src(&format!("F:\n open {p} (id:int name:str age:int)\n;"), 4096);
    assert!(
        res.errors
            .iter()
            .any(|e| e.message.contains("looks like data") && e.message.contains("noheader")),
        "the consumed first data row must be surfaced (never-silent): {:?}",
        res.errors
    );

    // The remedy: `noheader` keeps both data rows and surfaces nothing.
    let ok = run_src(
        &format!("F:\n open {p} noheader (id:int name:str age:int)\n;"),
        4096,
    );
    assert_eq!(ok.total_rows_out(), 2, "noheader keeps both data rows");
    assert!(
        ok.errors.is_empty(),
        "noheader has nothing to surface: {:?}",
        ok.errors
    );

    // No false positive: a *real* header (name cells don't parse in the typed
    // lanes) is correctly consumed without a warning.
    let hdr = "a,b,c\n1,alice,30\n2,bob,40\n";
    let fh = TempCsv(gendata::write_temp_bytes("bug_realheader", hdr.as_bytes()));
    let ph = fh.0.display();
    let res2 = run_src(
        &format!("F:\n open {ph} (id:int name:str age:int)\n;"),
        4096,
    );
    assert_eq!(res2.total_rows_out(), 2, "real header keeps both data rows");
    assert!(
        res2.errors.is_empty(),
        "a real header must not be flagged as data: {:?}",
        res2.errors
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
