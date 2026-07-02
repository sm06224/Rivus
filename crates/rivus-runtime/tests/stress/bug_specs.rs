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
                .any(|e| e.message.contains("failed to evaluate")),
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

#[test]
fn numeric_expr_cast_failure_is_null_and_surfaced_not_silent_zero() {
    // #190: `(v:int)` on an unparseable string yielded a SILENT 0 (the
    // classic silent-0), and `(v:float)` a silent NaN. Now: null + counted +
    // surfaced, per lane. An empty cell is "missing" (null, NOT counted —
    // reader parity), and a parseable cell is unchanged.
    for cs in [1usize, 3, 4096] {
        let f = TempCsv(gendata::write_temp_bytes(
            "b190_cast",
            b"id,v\n1,100\n2,notanumber\n3,42\n4,\n5,12.5\n",
        ));
        let p = f.0.display();
        let res = run_src(
            &format!("C:\n open {p} (id:int v:str)\n |> id (v:int) as n\n;"),
            cs,
        );
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("C"))
            .unwrap();
        // Collect (id, n-as-string) across chunks; null renders distinctly.
        let mut vals: Vec<(i64, bool, i64)> = Vec::new(); // (id, n_is_null, n_backing)
        for c in &o.chunks {
            let (ii, ni) = (
                c.schema.index_of("id").unwrap(),
                c.schema.index_of("n").unwrap(),
            );
            for r in 0..c.len {
                let id = match c.value(r, ii) {
                    rivus_core::Value::I64(x) => x,
                    other => panic!("id lane: {other:?}"),
                };
                let is_null = c.columns[ni].is_null(r);
                let backing = match c.value(r, ni) {
                    rivus_core::Value::I64(x) => x,
                    rivus_core::Value::Null => 0,
                    other => panic!("n lane: {other:?}"),
                };
                vals.push((id, is_null, backing));
            }
        }
        vals.sort();
        assert_eq!(vals[0], (1, false, 100), "@cs={cs}");
        assert!(vals[1].1, "'notanumber' must be NULL, not 0 @cs={cs}");
        assert_eq!(vals[2], (3, false, 42), "@cs={cs}");
        assert!(vals[3].1, "empty cell is null @cs={cs}");
        assert_eq!(
            vals[4],
            (5, false, 12),
            "float-looking text truncates @cs={cs}"
        );
        // Exactly ONE failure surfaced ('notanumber'); the empty cell is not
        // a failure (reader parity — no false positives on missing data).
        let msg = res
            .errors
            .iter()
            .find(|e| e.message.contains("could not be cast to i64"))
            .unwrap_or_else(|| panic!("cast failure must surface @cs={cs}: {:?}", res.errors));
        assert!(
            msg.message.starts_with("1 value(s)"),
            "count must be exactly 1 (empty is missing, not a failure) @cs={cs}: {}",
            msg.message
        );
    }
}

#[test]
fn division_by_zero_is_null_and_surfaced_not_silent_inf() {
    // #190: `a / 0` yielded a SILENT IEEE `inf` (and `a % 0` a silent 0 on the
    // int lane) with an empty error stream. Now: null + counted + surfaced.
    for cs in [1usize, 2, 4096] {
        let f = TempCsv(gendata::write_temp_bytes(
            "b190_div0",
            b"id,a,b\n1,10,2\n2,5,0\n3,8,4\n",
        ));
        let p = f.0.display();
        let res = run_src(
            &format!("D:\n open {p} (id:int a:int b:int)\n |> id (a / b) as q (a % b) as m\n;"),
            cs,
        );
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("D"))
            .unwrap();
        let mut rows: Vec<(i64, bool, bool)> = Vec::new(); // (id, q_null, m_null)
        for c in &o.chunks {
            let (ii, qi, mi) = (
                c.schema.index_of("id").unwrap(),
                c.schema.index_of("q").unwrap(),
                c.schema.index_of("m").unwrap(),
            );
            for r in 0..c.len {
                let id = match c.value(r, ii) {
                    rivus_core::Value::I64(x) => x,
                    other => panic!("id lane: {other:?}"),
                };
                rows.push((id, c.columns[qi].is_null(r), c.columns[mi].is_null(r)));
            }
        }
        rows.sort();
        assert_eq!(rows[0], (1, false, false), "@cs={cs}");
        assert_eq!(
            rows[1],
            (2, true, true),
            "5/0 and 5%0 must be NULL @cs={cs}"
        );
        assert_eq!(rows[2], (3, false, false), "@cs={cs}");
        // No inf anywhere in the rendered output.
        for c in &o.chunks {
            for r in 0..c.len {
                for col in 0..c.schema.fields.len() {
                    let s = c.value(r, col).to_string();
                    assert!(!s.contains("inf"), "no silent inf @cs={cs}: {s}");
                }
            }
        }
        // Surfaced (division by zero counts ride the evaluate-failure surface).
        assert!(
            res.errors
                .iter()
                .any(|e| e.message.contains("division by zero")),
            "division by zero must surface @cs={cs}: {:?}",
            res.errors
        );
    }
}

#[test]
fn dropna_reports_dropped_row_count() {
    // #204: dropna silently vanished rows (validate…reject reports its count;
    // dropna is the same never-silent debt). Now one Recoverable on finish.
    for cs in [1usize, 2, 4096] {
        let f = TempCsv(gendata::write_temp_bytes(
            "b204_dropna",
            b"id,name,age\n1,alice,30\n2,,25\n3,carol,\n4,dave,40\n",
        ));
        let p = f.0.display();
        let res = run_src(
            &format!("C:\n open {p} (id:int name:str age:int)\n dropna\n |> id\n;"),
            cs,
        );
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("C"))
            .unwrap();
        let total: usize = o.chunks.iter().map(|c| c.len).sum();
        assert_eq!(total, 2, "rows 2 and 3 dropped @cs={cs}");
        assert!(
            res.errors
                .iter()
                .any(|e| e.message.contains("dropna: dropped 2 row(s)")),
            "dropna must report its dropped count @cs={cs}: {:?}",
            res.errors
        );
    }
}
