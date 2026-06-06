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

// --- STEP 2-② operators: null-aware filter / group-by / distinct / fill /
// cast / sort (design 26 §26.2). ---

#[test]
fn filter_treats_null_as_predicate_false() {
    // §26.2(a): a comparison with a null operand is false, so a null row is
    // never kept — `age == 0` excludes the blank (only the real 0 survives),
    // and `age >= 0` excludes it too.
    let text = "id,age\n1,25\n2,\n3,0\n4,40\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_null_filter",
        text.as_bytes(),
    ));
    let p = f.0.display();
    for cz in [1usize, 2, 4096] {
        assert_eq!(
            collect_i64(
                &run_src(
                    &format!("N:\n open {p} (id:int age:int)\n |? age == 0\n |> id\n;"),
                    cz
                ),
                "N",
                "id"
            ),
            vec![3],
            "age == 0 must match the real 0 only, not the null @cz={cz}",
        );
        assert_eq!(
            collect_i64(
                &run_src(
                    &format!("N:\n open {p} (id:int age:int)\n |? age >= 0\n |> id\n;"),
                    cz
                ),
                "N",
                "id"
            ),
            vec![1, 3, 4],
            "age >= 0 must exclude the null @cz={cz}",
        );
    }
}

#[test]
fn filter_null_kernel_and_interpreter_agree() {
    // The kernel (compilable `field op literal`) and the interpreter (forced by
    // an OR) must treat null identically — byte-identical row sets.
    let text = "id,age\n1,25\n2,\n3,0\n4,40\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_null_filt_parity",
        text.as_bytes(),
    ));
    let p = f.0.display();
    // Kernel path: a pure numeric conjunction.
    let kernel = collect_i64(
        &run_src(
            &format!("N:\n open {p} (id:int age:int)\n |? age >= 0\n |> id\n;"),
            4096,
        ),
        "N",
        "id",
    );
    // Interpreter path: the OR makes `kernel::compile` bail; same logical result.
    let interp = collect_i64(
        &run_src(
            &format!("N:\n open {p} (id:int age:int)\n |? age >= 0 or age >= 999999\n |> id\n;"),
            4096,
        ),
        "N",
        "id",
    );
    assert_eq!(
        kernel, interp,
        "kernel and interpreter must agree on null=false"
    );
    assert_eq!(kernel, vec![1, 3, 4]);
}

#[test]
fn groupby_folds_null_key_into_one_group() {
    // §26.2(b): null group-by keys fold into a single "null group" (kept, not
    // dropped, not split). g = a, null, null, b → groups {a:1, null:2, b:1}.
    let text = "g,v\na,1\n,2\n,3\nb,4\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_null_group",
        text.as_bytes(),
    ));
    let p = f.0.display();
    for cz in [1usize, 2, 4096] {
        let mut counts = collect_i64(
            &run_src(&format!("N:\n open {p} (g:str v:int)\n |# g sum:v\n;"), cz),
            "N",
            "count",
        );
        counts.sort_unstable();
        assert_eq!(
            counts,
            vec![1, 1, 2],
            "the two null keys must fold into one group of 2 @cz={cz}",
        );
    }
}

#[test]
fn distinct_folds_duplicate_nulls() {
    // §26.2(b): distinct folds duplicate nulls to one row (first occurrence).
    // x = a, null, null, b → 3 distinct rows.
    let text = "id,x\n1,a\n2,\n3,\n4,b\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_null_distinct",
        text.as_bytes(),
    ));
    let p = f.0.display();
    for cz in [1usize, 2, 4096] {
        let xs = col_strings(
            &format!("N:\n open {p} (id:int x:str)\n distinct x\n |> x\n;"),
            cz,
            "N",
            "x",
        );
        assert_eq!(
            xs,
            vec!["a", "", "b"],
            "duplicate nulls fold to one @cz={cz}"
        );
    }
}

#[test]
fn fill_replaces_null_on_numeric_lane() {
    // `fill age 0` replaces the null (former blank) with a real 0; existing
    // values are untouched. (Before the null model this was a no-op on numeric.)
    let text = "id,age\n1,25\n2,\n3,40\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_null_fill",
        text.as_bytes(),
    ));
    let p = f.0.display();
    for cz in [1usize, 2, 4096] {
        assert_eq!(
            col_strings(
                &format!("N:\n open {p} (id:int age:int)\n fill age 0\n |> age\n;"),
                cz,
                "N",
                "age"
            ),
            vec!["25", "0", "40"],
            "fill replaces the null with a real 0 @cz={cz}",
        );
    }
}

#[test]
fn cast_propagates_null() {
    // §26.2(c): casting a null yields null (not a coerced 0). age:int (with a
    // null) cast to f64 keeps the null hole.
    let text = "id,age\n1,25\n2,\n3,40\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_null_cast",
        text.as_bytes(),
    ));
    let p = f.0.display();
    for cz in [1usize, 2, 4096] {
        assert_eq!(
            col_strings(
                &format!("N:\n open {p} (id:int age:int)\n |> (age:f64) as af\n;"),
                cz,
                "N",
                "af"
            ),
            vec!["25", "", "40"],
            "cast int→f64 must keep the null @cz={cz}",
        );
    }
}

#[test]
fn sort_orders_nulls_last_ascending_first_descending() {
    // §26.2(b): nulls sort as the largest value — last on ascending, first on
    // descending. age = 25, null, 5, 40.
    let text = "id,age\n1,25\n2,\n3,5\n4,40\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_null_sort",
        text.as_bytes(),
    ));
    let p = f.0.display();
    assert_eq!(
        collect_i64(
            &run_src(
                &format!("N:\n open {p} (id:int age:int)\n sort age\n |> id\n;"),
                4096
            ),
            "N",
            "id"
        ),
        vec![3, 1, 4, 2],
        "ascending: 5,25,40 then null",
    );
    assert_eq!(
        collect_i64(
            &run_src(
                &format!("N:\n open {p} (id:int age:int)\n sort age desc\n |> id\n;"),
                4096
            ),
            "N",
            "id"
        ),
        vec![2, 4, 1, 3],
        "descending: null then 40,25,5",
    );
}

// --- STEP 2-③ aggregation rectification: COUNT(*) vs COUNT(col), first/last
// and count_distinct over non-null (design 26 §26.2d). ---

#[test]
fn count_star_vs_count_col_distinguishes_null() {
    // The implicit `count` is COUNT(*) = the group's row count (null included);
    // `count:v` is COUNT(v) = the non-null tally. g=a has a null v, so a's
    // count=2 but count_v=1.
    let text = "g,v\na,1\na,\nb,3\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_null_count",
        text.as_bytes(),
    ));
    let p = f.0.display();
    for cz in [1usize, 2, 4096] {
        let res = run_src(
            &format!("N:\n open {p} (g:str v:int)\n |# g count:v\n;"),
            cz,
        );
        // Rows come out keyed by g; collect (count, count_v) by sorting on g via
        // the count column order is group-insertion → sort both for stability.
        let mut star = collect_i64(&res, "N", "count");
        let mut col = collect_i64(&res, "N", "count_v");
        star.sort_unstable();
        col.sort_unstable();
        assert_eq!(
            star,
            vec![1, 2],
            "COUNT(*) counts all rows incl. null @cz={cz}"
        );
        assert_eq!(col, vec![1, 1], "COUNT(v) skips the null @cz={cz}");
    }
}

#[test]
fn first_last_are_first_last_non_null() {
    // first/last skip leading/trailing nulls (§26.2d): v = null, x, y → first=x,
    // last=y. (Rectified from "non-empty": a real "" would now count, a null
    // never does.)
    let text = "g,v\na,\na,x\na,y\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_null_firstlast",
        text.as_bytes(),
    ));
    let p = f.0.display();
    for cz in [1usize, 2, 4096] {
        assert_eq!(
            col_strings(
                &format!("N:\n open {p} (g:str v:str)\n |# g first:v last:v\n;"),
                cz,
                "N",
                "first_v"
            ),
            vec!["x"],
            "first non-null is x @cz={cz}",
        );
        assert_eq!(
            col_strings(
                &format!("N:\n open {p} (g:str v:str)\n |# g first:v last:v\n;"),
                cz,
                "N",
                "last_v"
            ),
            vec!["y"],
            "last non-null is y @cz={cz}",
        );
    }
}

#[test]
fn count_distinct_skips_null() {
    // count_distinct counts distinct non-null values (§26.2d): v = x, null, x →
    // 1 distinct (the null is not a value).
    let text = "g,v\na,x\na,\na,x\n";
    let f = TempCsv(gendata::write_temp_bytes("stress_null_cd", text.as_bytes()));
    let p = f.0.display();
    for cz in [1usize, 2, 4096] {
        assert_eq!(
            collect_i64(
                &run_src(
                    &format!("N:\n open {p} (g:str v:str)\n |# g count_distinct:v\n;"),
                    cz
                ),
                "N",
                "count_distinct_v"
            ),
            vec![1],
            "one distinct non-null value @cz={cz}",
        );
    }
}

#[test]
fn groupby_count_col_serial_parallel_byte_identical() {
    // §26.4 with the null-aware aggregates: a null-bearing group-by with
    // count:v (and the implicit COUNT(*)) is byte-identical serial vs the
    // parallel byte-range reader. >1 MiB file forces the Fast parallel path.
    let rows = 300_000usize;
    let mut text = String::from("g,v\n");
    for i in 0..rows {
        let g = format!("grp{:04}", i % 50);
        let v = if i % 7 == 0 {
            String::new() // ~1/7 null
        } else {
            (i % 1000).to_string()
        };
        text.push_str(&format!("{g},{v}\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_null_pcount",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let flow = format!("G:\n open {p} (g:str v:int)\n |# g count:v min:v max:v\n;");
    let collect = |pref: rivus_runtime::MemoryPref| -> (Vec<String>, bool) {
        let g = rivus_parser::parse(&flow).expect("parse");
        let res = run(
            &g,
            RunOptions {
                chunk_size: 4096,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run");
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("G"))
            .unwrap();
        let mut lines = Vec::new();
        for c in &o.chunks {
            for r in 0..c.len {
                let cells: Vec<String> = (0..c.columns.len())
                    .map(|ci| c.value(r, ci).to_string())
                    .collect();
                lines.push(cells.join(","));
            }
        }
        lines.sort();
        (lines, !res.workers.is_empty())
    };
    let (serial, _) = collect(rivus_runtime::MemoryPref::Low);
    let (parallel, engaged) = collect(rivus_runtime::MemoryPref::Fast);
    assert!(engaged, "parallel group-by did not engage");
    assert_eq!(
        parallel, serial,
        "null-aware count:v group-by must be serial==parallel"
    );
}

// --- STEP 2-④ sink null round-trip (design 26 §26.5): null / "" / 0 survive
// read → write → read, symmetrically on CSV and JSON. ---

/// Run `src` to completion (a `save` sink writes the output file).
fn run_to(src: &str) {
    let g = rivus_parser::parse(src).expect("parse");
    run(
        &g,
        RunOptions {
            chunk_size: 4096,
            ..Default::default()
        },
    )
    .expect("run");
}

#[test]
fn csv_null_empty_zero_round_trip() {
    // Three distinct cells: null (unquoted blank), "" (quoted empty), real 0.
    // After read → write → read they must stay distinct (§26.5).
    let text = "id,s,n\n1,a,5\n2,,\n3,\"\",0\n";
    let inf = TempCsv(gendata::write_temp_bytes("rt_csv_in", text.as_bytes()));
    let ip = inf.0.display();
    let outf = TempCsv(gendata::write_temp_bytes("rt_csv_out", b""));
    let op = outf.0.display();

    run_to(&format!(
        "W:\n open {ip} (id:int s:str n:int)\n |> id s n\n save {op}\n;"
    ));

    // The written CSV must distinguish null (bare empty) from "" (quoted).
    let raw = std::fs::read_to_string(&outf.0).expect("read out");
    assert!(
        raw.contains("2,,\n"),
        "null s and null n → bare empty fields: {raw:?}"
    );
    assert!(
        raw.contains("3,\"\",0\n"),
        "real empty string → quoted \"\": {raw:?}"
    );

    // Read the written file back; coalesce proves null vs "" survived.
    let res = run_src(
        &format!("R:\n open {op} (id:int s:str n:int)\n |> id (coalesce(s, \"NULL\")) as s (coalesce(n, 999)) as n\n;"),
        4096,
    );
    assert_eq!(
        collect_strings(&res, "R", "s"),
        vec!["a", "NULL", ""],
        "row2 s round-tripped null (→NULL); row3 s round-tripped empty string",
    );
    assert_eq!(
        collect_strings(&res, "R", "n"),
        vec!["5", "999", "0"],
        "row2 n round-tripped null (→999); row3 n round-tripped real 0",
    );
}

#[test]
fn json_null_empty_round_trip() {
    // JSON: an explicit null and an empty string "" stay distinct across
    // read → write (JSONL) → read (§26.5).
    let text = "{\"id\":1,\"s\":\"a\"}\n{\"id\":2,\"s\":null}\n{\"id\":3,\"s\":\"\"}\n";
    let raw_in = gendata::write_temp("rt_json_in", text);
    let mut ip = raw_in.clone();
    ip.set_extension("jsonl");
    std::fs::rename(&raw_in, &ip).unwrap();
    let _in = TempCsv(ip.clone());
    let raw_out = gendata::write_temp("rt_json_out", "");
    let mut op = raw_out.clone();
    op.set_extension("jsonl");
    std::fs::rename(&raw_out, &op).unwrap();
    let _out = TempCsv(op.clone());

    run_to(&format!(
        "W:\n open {}\n |> id s\n save {}\n;",
        ip.display(),
        op.display()
    ));

    // The written JSONL must emit a bare `null` and a quoted `""` — distinct.
    let raw = std::fs::read_to_string(&op).expect("read json out");
    assert!(
        raw.contains("\"s\":null"),
        "explicit null → bare JSON null: {raw:?}"
    );
    assert!(
        raw.contains("\"s\":\"\""),
        "empty string → quoted \"\": {raw:?}"
    );

    let res = run_src(
        &format!(
            "R:\n open {}\n |> id (coalesce(s, \"NULL\")) as s\n;",
            op.display()
        ),
        4096,
    );
    assert_eq!(
        collect_strings(&res, "R", "s"),
        vec!["a", "NULL", ""],
        "JSON null vs empty string survived the round-trip",
    );
}

#[test]
fn csv_round_trip_is_idempotent_chunk_size_independent() {
    // read → write → read → write must reach a fixed point (byte-identical
    // second write), independent of chunk size — null/empty rendering is stable.
    let text = "id,s,n\n1,a,5\n2,,\n3,\"\",0\n4,z,\n";
    let inf = TempCsv(gendata::write_temp_bytes("rt_idem_in", text.as_bytes()));
    let ip = inf.0.display();
    for cz in [1usize, 2, 4096] {
        let o1 = TempCsv(gendata::write_temp_bytes("rt_idem_o1", b""));
        let o2 = TempCsv(gendata::write_temp_bytes("rt_idem_o2", b""));
        let g1 = rivus_parser::parse(&format!(
            "W:\n open {ip} (id:int s:str n:int)\n |> id s n\n save {}\n;",
            o1.0.display()
        ))
        .expect("parse");
        run(
            &g1,
            RunOptions {
                chunk_size: cz,
                ..Default::default()
            },
        )
        .expect("run");
        let g2 = rivus_parser::parse(&format!(
            "W:\n open {} (id:int s:str n:int)\n |> id s n\n save {}\n;",
            o1.0.display(),
            o2.0.display()
        ))
        .expect("parse");
        run(
            &g2,
            RunOptions {
                chunk_size: cz,
                ..Default::default()
            },
        )
        .expect("run");
        let a = std::fs::read_to_string(&o1.0).unwrap();
        let b = std::fs::read_to_string(&o2.0).unwrap();
        assert_eq!(a, b, "round-trip must be a fixed point @cz={cz}");
    }
}

// --- STEP 2-⑤ parallel-merge null byte-identity (design 26 §26.4): the merge
// path (byte-range reader concat + parallel group-by fold) is serial ==
// parallel == chunk-size on null-bearing data. ---

/// Run `flow` serial (MemoryPref::Low) and parallel (Fast), returning the
/// sorted output lines of label `L` and whether parallel workers engaged.
fn serial_vs_parallel(flow: &str, label: &str) -> (Vec<String>, Vec<String>, bool) {
    let collect = |pref: rivus_runtime::MemoryPref| -> (Vec<String>, bool) {
        let g = rivus_parser::parse(flow).expect("parse");
        let res = run(
            &g,
            RunOptions {
                chunk_size: 4096,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run");
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some(label))
            .unwrap();
        let mut lines = Vec::new();
        for c in &o.chunks {
            for r in 0..c.len {
                let cells: Vec<String> = (0..c.columns.len())
                    .map(|ci| c.value(r, ci).to_string())
                    .collect();
                lines.push(cells.join("\u{1f}"));
            }
        }
        lines.sort();
        (lines, !res.workers.is_empty())
    };
    let (serial, _) = collect(rivus_runtime::MemoryPref::Low);
    let (parallel, engaged) = collect(rivus_runtime::MemoryPref::Fast);
    (serial, parallel, engaged)
}

#[test]
fn parallel_group_null_keys_fold_byte_identical() {
    // A null group KEY must fold into one group identically on the serial and
    // parallel paths (the null-tagged composite key is worker-independent). ~1/9
    // of the key column is null. >1 MiB file forces the parallel path.
    let rows = 300_000usize;
    let mut text = String::from("g,v\n");
    for i in 0..rows {
        let g = if i % 9 == 0 {
            String::new() // null key
        } else {
            format!("grp{:04}", i % 40)
        };
        text.push_str(&format!("{g},{}\n", i % 1000));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_null_pkey",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let flow = format!("G:\n open {p} (g:str v:int)\n |# g count:v min:v max:v\n;");
    let (serial, parallel, engaged) = serial_vs_parallel(&flow, "G");
    assert!(engaged, "parallel group-by did not engage");
    assert_eq!(
        parallel, serial,
        "null-key group-by must be serial == parallel"
    );
    // The null group is present (its key renders empty) — sanity that we tested it.
    assert!(
        serial.iter().any(|l| l.starts_with('\u{1f}')),
        "expected a null-key group"
    );
}

#[test]
fn parallel_group_null_aggs_byte_identical() {
    // Every parallel-safe, null-aware aggregate over a null-bearing column must
    // merge byte-identically: count:v (non-null tally), min/max (skip null),
    // count_distinct (non-null distinct), first/last (non-null, source order).
    let rows = 300_000usize;
    let mut text = String::from("g,v\n");
    for i in 0..rows {
        let g = format!("grp{:03}", i % 40);
        let v = if i % 7 == 0 {
            String::new() // ~1/7 null
        } else {
            format!("val{:03}", i % 200)
        };
        text.push_str(&format!("{g},{v}\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_null_paggs",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let flow =
        format!("G:\n open {p} (g:str v:str)\n |# g count:v count_distinct:v first:v last:v\n;");
    let (serial, parallel, engaged) = serial_vs_parallel(&flow, "G");
    assert!(engaged, "parallel group-by did not engage");
    assert_eq!(
        parallel, serial,
        "null-aware aggregates must be serial == parallel"
    );
}

#[test]
fn parallel_group_all_null_column_byte_identical() {
    // A column that is entirely null in some groups: count:v = 0, first/last =
    // null, min/max = null. Serial and parallel must agree (all-null merge).
    let rows = 200_000usize;
    let mut text = String::from("g,v\n");
    for i in 0..rows {
        let g = format!("grp{:03}", i % 30);
        // group 0 is entirely null; others have values.
        let v = if i % 30 == 0 {
            String::new()
        } else {
            (i % 500).to_string()
        };
        text.push_str(&format!("{g},{v}\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_null_pallnull",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let flow = format!("G:\n open {p} (g:str v:int)\n |# g count count:v min:v max:v\n;");
    let (serial, parallel, engaged) = serial_vs_parallel(&flow, "G");
    assert!(engaged, "parallel group-by did not engage");
    assert_eq!(
        parallel, serial,
        "all-null-group aggregates must be serial == parallel"
    );
}

// --- DuckDB parity (theme 1): join null-key semantics (§26.2a). A null join
// key matches nothing — so output row counts agree with DuckDB (a null key must
// not fold rows together). Expected values below are the DuckDB-correct result. ---

#[test]
fn inner_join_null_key_does_not_match() {
    // L.k = a, null, b ; R.k = a, null, b. DuckDB inner join on k → only the
    // non-null keys match (a, b) = 2 rows; the two null keys do NOT join.
    let src = "\
L: open LP (id:int k:str) ;
R: open RP (k:str v:int) ;
J: L & R on k |> id v\n;";
    let lf = TempCsv(gendata::write_temp_bytes(
        "join_null_l",
        b"id,k\n1,a\n2,\n3,b\n",
    ));
    let rf = TempCsv(gendata::write_temp_bytes(
        "join_null_r",
        b"k,v\na,10\n,99\nb,20\n",
    ));
    let src = src
        .replace("LP", &lf.0.display().to_string())
        .replace("RP", &rf.0.display().to_string());
    let res = run_src(&src, 4096);
    let ids = collect_i64(&res, "J", "id");
    assert_eq!(
        ids,
        vec![1, 3],
        "null keys must not match (DuckDB parity): {ids:?}"
    );
    let vs = collect_i64(&res, "J", "v");
    assert_eq!(
        vs,
        vec![10, 20],
        "matched values only (99 from null-key R row excluded)"
    );
}

#[test]
fn left_join_null_key_row_is_kept_and_padded() {
    // DuckDB left join keeps every left row; the null-key left row (id 2) has no
    // match → its right columns are null. Output = 3 rows (count parity).
    let src = "\
L: open LP (id:int k:str) ;
R: open RP (k:str v:int) ;
J: L &left R on k |> id (coalesce(v, -1)) as v\n;";
    let lf = TempCsv(gendata::write_temp_bytes(
        "join_lnull_l",
        b"id,k\n1,a\n2,\n3,b\n",
    ));
    let rf = TempCsv(gendata::write_temp_bytes(
        "join_lnull_r",
        b"k,v\na,10\n,99\nb,20\n",
    ));
    let src = src
        .replace("LP", &lf.0.display().to_string())
        .replace("RP", &rf.0.display().to_string())
        .replace("-1", "999");
    let res = run_src(&src, 4096);
    assert_eq!(
        collect_i64(&res, "J", "id"),
        vec![1, 2, 3],
        "every left row kept"
    );
    // id 2 (null key) is unmatched → v is null → coalesced to 999.
    assert_eq!(
        collect_i64(&res, "J", "v"),
        vec![10, 999, 20],
        "null-key left row padded with null (→999), DuckDB-parity",
    );
}
