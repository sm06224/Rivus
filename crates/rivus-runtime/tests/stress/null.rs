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
