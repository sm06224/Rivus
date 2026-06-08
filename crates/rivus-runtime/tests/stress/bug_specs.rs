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
fn datetime_cast_in_expression_is_source_aware_bug_d() {
    // BUG-D (fix): an expression `cast` to a temporal lane PARSES a string source
    // (auto formats) — the same *meaning* as the reader's exact path; only the
    // path (speed) differs. A non-null cell that won't parse → null
    // (continue-first) and is surfaced (never-silent). Chunk-size independent.
    let text = "ts\n2026-06-01T14:30:00\n2026-06-02T09:00:00\nBADVALUE\n2026-06-03\n";
    let f = TempCsv(gendata::write_temp_bytes("bug_dcast", text.as_bytes()));
    let p = f.0.display();
    for cs in [1usize, 2, 4096] {
        // The "two casts, same meaning" contract: reading `(ts:datetime)` (exact
        // path) and `(ts:str)` + `cast ts:datetime` (expression path) must yield
        // byte-identical datetime values.
        let reader = run_src(&format!("R:\n open {p} (ts:datetime)\n;"), cs);
        let cast = run_src(&format!("C:\n open {p} (ts:str)\n cast ts:datetime\n;"), cs);
        assert_eq!(
            collect_strings(&reader, "R", "ts"),
            collect_strings(&cast, "C", "ts"),
            "reader exact path and expression cast must be byte-identical @cs={cs}"
        );
        // Continue-first: all 4 rows survive (the bad one → null).
        assert_eq!(cast.total_rows_out(), 4, "rows preserved @cs={cs}");
        // Never-silent: the unparseable cell is surfaced.
        assert!(
            cast.errors
                .iter()
                .any(|e| e.message.contains("could not be cast to datetime")),
            "cast failure must surface (never-silent) @cs={cs}: {:?}",
            cast.errors
        );
    }
    // The same source-aware parse via a computed column `(ts:datetime) as t`.
    let proj = run_src(
        &format!("P:\n open {p} (ts:str)\n |> (ts:datetime) as t\n;"),
        4096,
    );
    assert!(
        proj.errors
            .iter()
            .any(|e| e.message.contains("could not be cast to datetime")),
        "computed-column cast failure must surface: {:?}",
        proj.errors
    );
}

#[test]
fn scalar_cast_failures_surface_bug_d_a2() {
    // BUG-D Slice A-2: cast failures on the SCALAR path — a `|?` predicate and a
    // function argument — are surfaced (never-silent), the value is null
    // (continue-first → the row is filtered as the comparison is false), and the
    // result is chunk-size independent.
    let text = "id,ts\n1,2026-06-01T00:00:00\n2,BAD\n3,2026-06-03T00:00:00\n";
    let f = TempCsv(gendata::write_temp_bytes("bug_d_a2", text.as_bytes()));
    let p = f.0.display();
    for cs in [1usize, 2, 4096] {
        // Predicate cast: BAD → null → (null > x) false → row excluded; June-01 <
        // June-02 excluded; June-03 kept. The failure is surfaced.
        let pred = run_src(
            &format!(
                "F:\n open {p} (id:int ts:str)\n |? ts:datetime > \"2026-06-02T00:00:00\"\n |> id\n;"
            ),
            cs,
        );
        assert_eq!(
            collect_i64(&pred, "F", "id"),
            vec![3],
            "only id 3 passes the predicate @cs={cs}"
        );
        assert!(
            pred.errors
                .iter()
                .any(|e| e.message.contains("could not be cast")),
            "predicate cast failure must surface @cs={cs}: {:?}",
            pred.errors
        );
        // Function-argument cast: year(ts:datetime) — BAD → null inside the arg,
        // surfaced via the projection.
        let func = run_src(
            &format!("G:\n open {p} (id:int ts:str)\n |> id (year(ts:datetime)) as y\n;"),
            cs,
        );
        assert!(
            func.errors
                .iter()
                .any(|e| e.message.contains("could not be cast")),
            "func-arg cast failure must surface @cs={cs}: {:?}",
            func.errors
        );
    }
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
