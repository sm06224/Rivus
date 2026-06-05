//! Null model (design 26 §26.3 / §26.4): the reader converts blank & parse-fail
//! cells to a first-class `null` (distinct from `0` and from empty `""`), nulls
//! ride through gather/append/value_at, and a null-bearing dataset stays
//! serial == parallel == chunk-size byte-identical.
//!
//! New tests for the null model land here (not in the moved files), per the
//! staged plan in design 26 §26.8.1.

use super::*;

/// Render the `label`/`col` column to one string per row (a null renders `""`).
fn col_strings(src: &str, chunk_size: usize, label: &str, col: &str) -> Vec<String> {
    let res = run_src(src, chunk_size);
    collect_strings(&res, label, col)
}

#[test]
fn blank_numeric_cell_is_null_not_zero() {
    // The BUG-A root cause: a blank numeric cell used to collapse to `0`. It is
    // now `null` — distinct from a real `0` (`id 4` carries a genuine 0). A null
    // renders as empty; a real 0 renders as "0".
    let text = "id,age\n1,25\n2,\n3,40\n4,0\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_null_blank",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let flow = format!("N:\n open {p} (id:int age:int)\n |> id age\n;");
    for cz in [1usize, 2, 3, 4096] {
        assert_eq!(
            col_strings(&flow, cz, "N", "age"),
            vec!["25", "", "40", "0"],
            "blank → null (\"\"), real 0 → \"0\" @cz={cz}",
        );
    }
}

#[test]
fn blank_numeric_is_skipped_by_aggregation() {
    // null is skipped by sum/avg/count(col-implicit): COUNT(*) still counts the
    // row, but the blank does not pull the average toward 0 (the old bug).
    let text = "g,age\na,10\na,\na,30\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_null_agg",
        text.as_bytes(),
    ));
    let p = f.0.display();
    // count = COUNT(*) = 3 rows; avg skips the null → (10+30)/2 = 20 (not 40/3).
    let flow = format!("N:\n open {p} (g:str age:int)\n |# g avg:age\n;");
    for cz in [1usize, 2, 4096] {
        assert_eq!(col_strings(&flow, cz, "N", "count"), vec!["3"], "@cz={cz}");
        assert_eq!(
            col_strings(&flow, cz, "N", "avg_age"),
            vec!["20"],
            "avg skips null @cz={cz}",
        );
    }
}

#[test]
fn parse_failure_is_nullified_and_surfaced() {
    // A non-empty cell that won't parse → null AND surfaced (#80 reworded:
    // "set to null", not "kept as default 0"). A blank is null but NOT surfaced.
    let text = "id,age\n1,25\n2,oops\n3,\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_null_fail",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let flow = format!("N:\n open {p} (id:int age:int)\n |> id age\n;");
    let res = run_src(&flow, 4096);
    assert_eq!(
        collect_strings(&res, "N", "age"),
        vec!["25", "", ""],
        "both the bad cell and the blank render as null",
    );
    let fails = res
        .errors
        .iter()
        .filter(|e| e.message.contains("could not be parsed; set to null"))
        .count();
    assert_eq!(
        fails, 1,
        "exactly the non-empty bad cell surfaces: {:?}",
        res.errors
    );
    assert!(
        !res.errors.iter().any(rivus_core::ErrorEvent::is_fatal),
        "null-ification is continue-first (never fatal)",
    );
}

#[test]
fn quoted_empty_is_empty_string_unquoted_is_null() {
    // The three-way distinction on a Str column (design 26 §26.3): an unquoted
    // empty is null, a quoted "" is a real empty string. Both render as "" in
    // CSV, so we distinguish them through `coalesce`, which fills only nulls.
    let text = "id,name\n1,aki\n2,\n3,\"\"\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_null_quoted",
        text.as_bytes(),
    ));
    let p = f.0.display();
    // coalesce(name, "MISSING") keeps a real value (incl. the real "") and
    // replaces only the null (unquoted empty).
    let flow =
        format!("N:\n open {p} (id:int name:str)\n |> id (coalesce(name, \"MISSING\")) as n\n;");
    for cz in [1usize, 2, 4096] {
        assert_eq!(
            col_strings(&flow, cz, "N", "n"),
            vec!["aki", "MISSING", ""],
            "row2 unquoted-empty → null → filled; row3 quoted \"\" → real empty kept @cz={cz}",
        );
    }
}

#[test]
fn jsonl_null_and_missing_key_are_null() {
    // JSON `null` and a missing key both become null (design 26 §26.3), not 0.
    // `a` is present/null/missing across the three rows; coalesce proves the
    // null-ness (a real 0 would survive coalesce).
    let text = "{\"id\":1,\"a\":5}\n{\"id\":2,\"a\":null}\n{\"id\":3}\n";
    // write_temp names files `.csv`; rename to `.jsonl` so `open` selects the
    // JSON reader by extension.
    let raw = gendata::write_temp("stress_null_json", text);
    let mut jpath = raw.clone();
    jpath.set_extension("jsonl");
    std::fs::rename(&raw, &jpath).unwrap();
    let f = TempCsv(jpath.clone());
    let p = f.0.display();
    let flow = format!("N:\n open {p}\n |> id (coalesce(a, 999)) as a\n;");
    for cz in [1usize, 2, 4096] {
        assert_eq!(
            col_strings(&flow, cz, "N", "a"),
            vec!["5", "999", "999"],
            "explicit null and missing key both coalesce @cz={cz}",
        );
    }
}

#[test]
fn null_bearing_data_is_serial_parallel_chunk_size_byte_identical() {
    // §26.4: validity is positional, so a null-bearing column is byte-identical
    // across serial vs the byte-range parallel reader and across chunk sizes.
    // Build a >1 MiB file (crosses the Fast parallel floor) with ~1/7 blank ages
    // and ~1/13 blank names, then compare saved outputs byte-for-byte.
    let rows = 150_000usize;
    let mut text = String::from("id,age,name\n");
    for i in 0..rows {
        let age = if i % 7 == 0 {
            String::new() // blank → null
        } else {
            (i % 90).to_string()
        };
        let name = if i % 13 == 0 {
            String::new() // unquoted blank → null
        } else {
            format!("n{}", i % 100)
        };
        text.push_str(&format!("{i},{age},{name}\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_null_bi", text.as_bytes()));
    let p = f.0.display();

    let run_to_file = |cs: usize, pref: rivus_runtime::MemoryPref, out: &std::path::Path| {
        let src = format!(
            "D:\n open {p} (id:int age:int name:str)\n |? id >= 0\n |> id age name\n save {}\n;",
            out.display()
        );
        let g = rivus_parser::parse(&src).expect("parse");
        run(
            &g,
            RunOptions {
                chunk_size: cs,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run")
    };

    // Serial oracle (single-threaded reader).
    let ser_out = TempCsv(gendata::write_temp_bytes("null_bi_serial", b""));
    run_to_file(1024, rivus_runtime::MemoryPref::Low, &ser_out.0);
    let oracle = std::fs::read_to_string(&ser_out.0).expect("read serial out");
    assert_eq!(oracle.lines().count(), rows + 1, "oracle row count");
    // The blanks must actually be present as empty fields in the output (null →
    // empty), proving we are exercising the null path, not all-valid data.
    assert!(
        oracle.contains(",,"),
        "expected null cells to render as empty fields",
    );

    for cs in [1usize, 1000, rows] {
        let par_out = TempCsv(gendata::write_temp_bytes("null_bi_parallel", b""));
        let res = run_to_file(cs, rivus_runtime::MemoryPref::Fast, &par_out.0);
        assert!(
            !res.errors.iter().any(rivus_core::ErrorEvent::is_fatal),
            "null-bearing parallel run must not fatal @cs={cs}",
        );
        let got = std::fs::read_to_string(&par_out.0).expect("read parallel out");
        assert_eq!(
            got, oracle,
            "null-bearing output must be byte-identical serial vs parallel @cs={cs}",
        );
    }
}
