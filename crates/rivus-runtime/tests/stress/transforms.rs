//! Transforms: schema, casts, string/numeric funcs, fill/dropna, sort, distinct, filter.
//!
//! Moved verbatim from the former monolithic `stress.rs` (design 26 §26.8.1,
//! mechanical move-only split; logic unchanged).

use super::*;

#[test]
fn headerless_csv_positional_columns_chunk_size_independent() {
    // No header row: columns are named c0, c1, c2 and the FIRST line is data.
    let rows = 20_000;
    let mut rng = Rng::new(3);
    let mut text = String::new();
    let mut expect = 0u64;
    for _ in 0..rows {
        let age = rng.below(90);
        text.push_str(&format!("user,x,{age}\n"));
        if age >= 45 {
            expect += 1;
        }
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_nh", text.as_bytes()));
    let p = f.0.display();
    for cs in [1, 7, 1024, 8192, rows] {
        let res = run_src(
            &format!("H:\n open {p} noheader\n |? c2 >= 45\n |> c0 c2\n;"),
            cs,
        );
        assert_eq!(res.total_rows_out(), expect, "noheader filter @cs={cs}");
        assert!(res.errors.is_empty());
    }
}

#[test]
fn declared_schema_renames_and_types_chunk_size_independent() {
    // A header file with columns a,b,c. Declare names (id, code, age) and force
    // `code` to str so leading zeros survive (it would otherwise infer i64).
    let rows = 5_000;
    let mut text = String::from("a,b,c\n");
    let mut kept = 0u64;
    for i in 0..rows {
        let age = (i % 90) as u64;
        text.push_str(&format!("{i},0{i:05},{age}\n")); // code has a leading zero
        if age >= 45 {
            kept += 1;
        }
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_decl", text.as_bytes()));
    let p = f.0.display();
    for cs in [1, 7, 1024, rows] {
        // Declared names are used by the predicate/projection; `code:str` keeps
        // the leading zero intact.
        let res = run_src(
            &format!("D:\n open {p} (id code:str age)\n |? age >= 45\n |> code\n;"),
            cs,
        );
        assert_eq!(res.total_rows_out(), kept, "declared filter @cs={cs}");
        // Every emitted `code` must still start with '0' (kept as a string).
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("D"))
            .unwrap();
        for c in &o.chunks {
            let ci = c.schema.index_of("code").unwrap();
            assert_eq!(c.schema.fields[ci].dtype, rivus_core::DataType::Str);
            for r in 0..c.len {
                assert!(
                    c.value(r, ci).to_string().starts_with('0'),
                    "leading zero lost"
                );
            }
        }
    }
}

#[test]
fn inline_cast_numeric_compare_on_string_column() {
    // `age` is declared str (so a bare compare would be lexical: "100" < "20").
    // `age:int >= N` casts to numeric, so the result matches a numeric oracle and
    // is chunk-size independent.
    let rows = 8_000;
    let mut rng = Rng::new(2);
    let mut text = String::from("name,age\n");
    let mut ge = 0u64;
    for _ in 0..rows {
        let age = rng.below(1000);
        text.push_str(&format!("u,{age}\n"));
        if age >= 500 {
            ge += 1;
        }
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_cast", text.as_bytes()));
    let p = f.0.display();
    for cs in [1, 7, 1024, rows] {
        let res = run_src(
            &format!("C:\n open {p} (name age:str)\n |? age:int >= 500\n;"),
            cs,
        );
        assert_eq!(res.total_rows_out(), ge, "cast compare @cs={cs}");
    }
}

#[test]
fn string_functions_chunk_size_independent() {
    // contains(city, "y") filter + upper(name) projection must match an oracle.
    let rows = 6_000usize;
    let mut text = String::from("name,city\n");
    let cities = ["york", "la", "yyz", "sfo"];
    let mut kept = 0u64;
    for i in 0..rows {
        let city = cities[i % cities.len()];
        text.push_str(&format!("u{i},{city}\n"));
        if city.contains('y') {
            kept += 1;
        }
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_strfn", text.as_bytes()));
    let p = f.0.display();
    for cs in [1, 7, 1024, rows] {
        let res = run_src(
            &format!("S:\n open {p}\n |? contains(city, \"y\")\n |> (upper(name)) as up\n;"),
            cs,
        );
        assert_eq!(res.total_rows_out(), kept, "contains filter @cs={cs}");
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("S"))
            .unwrap();
        for c in &o.chunks {
            let ci = c.schema.index_of("up").unwrap();
            for r in 0..c.len {
                let v = c.value(r, ci).to_string();
                assert_eq!(v, v.to_uppercase(), "upper() not applied");
            }
        }
    }
}

#[test]
fn replace_split_concat_chunk_size_independent() {
    // replace / split_part / concat over a path-like column. Each output row is
    // checked against an independent oracle, and the result must be chunk-size
    // independent (these lower to row-wise eval inside a computed projection).
    let rows = 4_000usize;
    let mut text = String::from("id,path\n");
    for i in 0..rows {
        // paths like "/a/b<i>/c" so split_part(path,"/",3) = "b<i>".
        text.push_str(&format!("{i},/a/b{i}/c\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_strfn2", text.as_bytes()));
    let p = f.0.display();
    for cs in [1usize, 7, 1024, rows] {
        let res = run_src(
            &format!(
                "S:\n open {p}\n |> id (replace(path, \"/\", \"-\")) as r (split_part(path, \"/\", 3)) as seg (concat(id, \"@\", path)) as tag\n;"
            ),
            cs,
        );
        assert_eq!(res.total_rows_out(), rows as u64, "rows @cs={cs}");
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("S"))
            .unwrap();
        for c in &o.chunks {
            let ii = c.schema.index_of("id").unwrap();
            let ri = c.schema.index_of("r").unwrap();
            let si = c.schema.index_of("seg").unwrap();
            let ti = c.schema.index_of("tag").unwrap();
            for r in 0..c.len {
                let id = c.value(r, ii).to_string();
                assert_eq!(
                    c.value(r, ri).to_string(),
                    format!("-a-b{id}-c"),
                    "replace @cs={cs}"
                );
                assert_eq!(
                    c.value(r, si).to_string(),
                    format!("b{id}"),
                    "split @cs={cs}"
                );
                assert_eq!(
                    c.value(r, ti).to_string(),
                    format!("{id}@/a/b{id}/c"),
                    "concat @cs={cs}"
                );
            }
        }
        assert!(res.errors.is_empty(), "errors @cs={cs}");
    }
}

#[test]
fn numeric_and_coalesce_funcs_chunk_size_independent() {
    // abs/round/floor/ceil over a signed-decimal column, and coalesce over a
    // sometimes-blank text column. Each output is checked against an independent
    // oracle and must be chunk-size independent.
    let rows = 4_000usize;
    let mut text = String::from("id,v,name\n");
    let mut vs: Vec<f64> = Vec::with_capacity(rows);
    for i in 0..rows {
        // deterministic signed decimals in [-50.0, 49.5] stepping by 0.5
        let v = (i as f64 % 200.0) * 0.5 - 50.0;
        vs.push(v);
        let name = if i % 3 == 0 {
            String::new()
        } else {
            format!("n{i}")
        };
        text.push_str(&format!("{i},{v},{name}\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_numfn", text.as_bytes()));
    let p = f.0.display();
    for cs in [1usize, 7, 1024, rows] {
        let res = run_src(
            &format!(
                "N:\n open {p}\n |> id (abs(v)) as a (round(v)) as r (floor(v)) as fl (ceil(v)) as ce (coalesce(name, \"NA\")) as nm\n;"
            ),
            cs,
        );
        let out = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("N"))
            .unwrap();
        for c in &out.chunks {
            let ii = c.schema.index_of("id").unwrap();
            let (ai, ri, fi, ci, ni) = (
                c.schema.index_of("a").unwrap(),
                c.schema.index_of("r").unwrap(),
                c.schema.index_of("fl").unwrap(),
                c.schema.index_of("ce").unwrap(),
                c.schema.index_of("nm").unwrap(),
            );
            for row in 0..c.len {
                let id = c.value(row, ii).to_string().parse::<usize>().unwrap();
                let v = vs[id];
                assert_eq!(c.value(row, ai).as_f64().unwrap(), v.abs(), "abs @cs={cs}");
                assert_eq!(
                    c.value(row, ri).as_f64().unwrap(),
                    v.round(),
                    "round @cs={cs}"
                );
                assert_eq!(
                    c.value(row, fi).as_f64().unwrap(),
                    v.floor(),
                    "floor @cs={cs}"
                );
                assert_eq!(
                    c.value(row, ci).as_f64().unwrap(),
                    v.ceil(),
                    "ceil @cs={cs}"
                );
                let want_nm = if id % 3 == 0 {
                    "NA".to_string()
                } else {
                    format!("n{id}")
                };
                assert_eq!(c.value(row, ni).to_string(), want_nm, "coalesce @cs={cs}");
            }
        }
        assert!(res.errors.is_empty(), "errors @cs={cs}");
    }
}

#[test]
fn dropna_and_fill_chunk_size_independent() {
    // city is blank on every 3rd row. dropna city drops those; fill city
    // replaces them. Both must be exact and chunk-size independent.
    let rows = 9_000usize;
    let mut text = String::from("id,city\n");
    let mut nonblank = 0u64;
    for i in 0..rows {
        if i % 3 == 0 {
            text.push_str(&format!("{i},\n")); // blank city
        } else {
            text.push_str(&format!("{i},town\n"));
            nonblank += 1;
        }
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_na", text.as_bytes()));
    let p = f.0.display();
    for cs in [1, 7, 1024, rows] {
        let dn = run_src(&format!("D:\n open {p} (id city:str)\n dropna city\n;"), cs);
        assert_eq!(dn.total_rows_out(), nonblank, "dropna @cs={cs}");

        // fill keeps all rows; none should be blank afterwards.
        let fl = run_src(
            &format!("D:\n open {p} (id city:str)\n fill city \"NA\"\n;"),
            cs,
        );
        assert_eq!(fl.total_rows_out(), rows as u64, "fill keeps rows @cs={cs}");
        let o = fl
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("D"))
            .unwrap();
        for c in &o.chunks {
            let ci = c.schema.index_of("city").unwrap();
            for r in 0..c.len {
                assert!(
                    !c.value(r, ci).to_string().is_empty(),
                    "blank survived fill"
                );
            }
        }
    }
}

#[test]
fn fill_ffill_bfill_chunk_size_independent() {
    // A column of runs of blanks between a few anchors, plus a leading and a
    // trailing blank (which ffill/bfill respectively cannot resolve). ffill
    // carries the previous value forward across chunk boundaries; bfill carries
    // the next value back across them. Both results must be exact and identical
    // regardless of chunk_size — the regression guard for the cross-chunk carry.
    let raw = ["", "", "a", "", "", "b", "", "c", "", "", "", "d", ""];
    let rows = raw.len();
    let mut text = String::from("id,tag\n");
    for (i, v) in raw.iter().enumerate() {
        text.push_str(&format!("{i},{v}\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_fill_dir",
        text.as_bytes(),
    ));
    let p = f.0.display();

    // Independent oracles.
    let mut ff = vec![String::new(); rows];
    let mut last = String::new();
    for i in 0..rows {
        if raw[i].is_empty() {
            ff[i] = last.clone();
        } else {
            ff[i] = raw[i].to_string();
            last = raw[i].to_string();
        }
    }
    let mut bf = vec![String::new(); rows];
    let mut next = String::new();
    for i in (0..rows).rev() {
        if raw[i].is_empty() {
            bf[i] = next.clone();
        } else {
            bf[i] = raw[i].to_string();
            next = raw[i].to_string();
        }
    }

    let collect = |res: &rivus_runtime::RunResult| -> Vec<String> {
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("F"))
            .unwrap();
        let mut out = Vec::new();
        for c in &o.chunks {
            let ci = c.schema.index_of("tag").unwrap();
            for r in 0..c.len {
                out.push(c.value(r, ci).to_string());
            }
        }
        out
    };

    for cs in [1usize, 2, 3, 5, rows] {
        let fwd = run_src(
            &format!("F:\n open {p} (id tag:str)\n fill tag ffill\n;"),
            cs,
        );
        assert_eq!(collect(&fwd), ff, "ffill @cs={cs}");
        assert!(fwd.errors.is_empty(), "ffill errors @cs={cs}");

        let back = run_src(
            &format!("F:\n open {p} (id tag:str)\n fill tag bfill\n;"),
            cs,
        );
        assert_eq!(collect(&back), bf, "bfill @cs={cs}");
        assert!(back.errors.is_empty(), "bfill errors @cs={cs}");
    }
}

#[test]
fn fill_mean_median_chunk_size_independent() {
    // score is blank on every 4th row; the rest are a known numeric sequence.
    // `fill score mean|median` must replace blanks with the column statistic of
    // the non-empty cells, keep the non-empty cells unchanged, and be identical
    // across chunk_size (the statistic is computed over the whole buffered
    // column, a pipeline-breaker like sort).
    let rows = 4_000usize;
    let mut text = String::from("id,score\n");
    let mut present: Vec<f64> = Vec::new();
    for i in 0..rows {
        if i % 4 == 0 {
            text.push_str(&format!("{i},\n")); // blank score
        } else {
            let s = (i % 100) as f64; // deterministic spread 0..99
            text.push_str(&format!("{i},{s}\n"));
            present.push(s);
        }
    }
    // Oracle statistics over the present (non-blank) values.
    let mean = present.iter().sum::<f64>() / present.len() as f64;
    let mut sorted = present.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let rank = 0.5 * (sorted.len() - 1) as f64;
    let (lo, hi, frac) = (
        rank.floor() as usize,
        rank.ceil() as usize,
        rank - rank.floor(),
    );
    let median = sorted[lo] + (sorted[hi] - sorted[lo]) * frac;

    let f = TempCsv(gendata::write_temp_bytes(
        "stress_fillstat",
        text.as_bytes(),
    ));
    let p = f.0.display();

    // Sum of the filled column = sum(present) + (#blanks * statistic). Checking
    // the sum (not exact strings) keeps the oracle robust to float formatting.
    let nblank = (rows / 4) as f64;
    let present_sum: f64 = present.iter().sum();

    let col_sum = |res: &rivus_runtime::RunResult| -> f64 {
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("F"))
            .unwrap();
        let mut sum = 0f64;
        let mut blanks = 0u64;
        for c in &o.chunks {
            let ci = c.schema.index_of("score").unwrap();
            for r in 0..c.len {
                let v = c.value(r, ci).to_string();
                assert!(!v.trim().is_empty(), "blank survived fill");
                sum += v.parse::<f64>().unwrap();
                blanks += 0; // (kept for clarity; blanks already replaced)
            }
        }
        let _ = blanks;
        sum
    };

    for cs in [1usize, 7, 1024, rows] {
        let m = run_src(
            &format!("F:\n open {p} (id score:str)\n fill score mean\n;"),
            cs,
        );
        assert!(
            (col_sum(&m) - (present_sum + nblank * mean)).abs() < 1e-6,
            "fill mean sum @cs={cs}"
        );
        assert!(m.errors.is_empty(), "mean errors @cs={cs}");

        let md = run_src(
            &format!("F:\n open {p} (id score:str)\n fill score median\n;"),
            cs,
        );
        assert!(
            (col_sum(&md) - (present_sum + nblank * median)).abs() < 1e-6,
            "fill median sum @cs={cs}"
        );
    }
}

#[test]
fn large_clean_filter_is_exact() {
    let rows = 50_000;
    let seed = 42;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_clean", &data));
    let p = f.0.display();

    // Run across several chunk sizes: the result must be identical regardless
    // of chunk granularity (chunk-size independence).
    let expected = expected_clean_ge(rows, seed, 45);
    for cs in [1, 7, 1024, 8192, rows] {
        let src = format!("F:\n open {p}\n |? age >= 45\n;");
        let res = run_src(&src, cs);
        assert_eq!(res.total_rows_out(), expected, "chunk_size={cs}");
        assert!(res.errors.is_empty(), "clean data should not error");
    }
}

#[test]
fn take_caps_rows_chunk_size_independent() {
    let rows = 50_000;
    let seed = 42;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_take", &data));
    let p = f.0.display();

    let matched = expected_clean_ge(rows, seed, 45);
    // Limit below and above the number of matches; result is min(N, matched),
    // and must not depend on chunk granularity (a chunk may straddle the cut).
    for n in [
        0u64,
        1,
        123,
        matched.saturating_sub(1),
        matched,
        matched + 1000,
    ] {
        let want = n.min(matched);
        for cs in [1, 7, 1024, 8192, rows] {
            let src = format!("F:\n open {p}\n |? age >= 45\n take {n}\n;");
            let res = run_src(&src, cs);
            assert_eq!(res.total_rows_out(), want, "take {n} @ chunk_size={cs}");
            assert!(res.errors.is_empty(), "clean data should not error");
        }
    }
}

#[test]
fn sort_orders_rows_chunk_size_independent() {
    let rows = 20_000;
    let seed = 7;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_sort", &data));
    let p = f.0.display();

    // Oracle: regenerate the age multiset and sort it independently.
    let mut rng = Rng::new(seed);
    let mut want_asc = Vec::with_capacity(rows);
    for _ in 0..rows {
        let age = rng.below(90) as i64;
        let _score = rng.below(10_000);
        let _country = rng.below(5);
        let _active = rng.below(2);
        want_asc.push(age);
    }
    want_asc.sort_unstable();
    let mut want_desc = want_asc.clone();
    want_desc.reverse();

    // The sorted output must equal the oracle exactly, for every chunk size.
    for cs in [1, 7, 1024, 8192, rows] {
        let asc = run_src(&format!("S:\n open {p}\n sort age\n;"), cs);
        assert_eq!(collect_i64(&asc, "S", "age"), want_asc, "asc @cs={cs}");

        let desc = run_src(&format!("S:\n open {p}\n sort age desc\n;"), cs);
        assert_eq!(collect_i64(&desc, "S", "age"), want_desc, "desc @cs={cs}");
    }
}

#[test]
fn sort_nulls_last_asc_first_desc_byte_identical() {
    // PERF-G pins the comparator hoist's null rule (§26.2b): nulls sort **last**
    // on ascending; descending reverses the whole order so nulls sort **first**;
    // ties keep source order (stable). Must hold for every chunk size.
    let data = "id,v\n1,30\n2,\n3,10\n4,\n5,20\n".as_bytes(); // v is null on rows 2,4
    let f = TempCsv(gendata::write_temp_bytes("stress_sort_null", data));
    let p = f.0.display();
    for cs in [1usize, 2, 64] {
        // asc: 10,20,30 (ids 3,5,1) then nulls (ids 2,4 in source order).
        let asc = run_src(
            &format!("S:\n open {p} (id:int v:int)\n sort v\n |> id\n;"),
            cs,
        );
        assert_eq!(
            collect_i64(&asc, "S", "id"),
            vec![3, 5, 1, 2, 4],
            "nulls last on asc, stable @cs={cs}"
        );
        // desc: nulls first (ids 2,4 stable), then 30,20,10 (ids 1,5,3).
        let desc = run_src(
            &format!("S:\n open {p} (id:int v:int)\n sort v desc\n |> id\n;"),
            cs,
        );
        assert_eq!(
            collect_i64(&desc, "S", "id"),
            vec![2, 4, 1, 5, 3],
            "nulls first on desc, stable @cs={cs}"
        );
    }
}

#[test]
fn multi_key_sort_orders_by_each_key_chunk_size_independent() {
    // `sort team score desc` orders by team ascending, then by score descending
    // within a team. Build rows with deliberate team ties so the secondary key
    // is exercised; compare against an independent Rust sort, every chunk size.
    let rows = 12_000usize;
    let mut rng = Rng::new(23);
    let mut text = String::from("team,score\n");
    let mut tuples: Vec<(i64, i64)> = Vec::with_capacity(rows); // (team, score)
    for _ in 0..rows {
        let team = rng.below(5) as i64; // few teams → many ties
        let score = rng.below(1000) as i64;
        text.push_str(&format!("{team},{score}\n"));
        tuples.push((team, score));
    }
    // Oracle: team asc, then score desc.
    let mut want = tuples.clone();
    want.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));

    let f = TempCsv(gendata::write_temp_bytes("stress_msort", text.as_bytes()));
    let p = f.0.display();
    for cs in [1usize, 7, 1024, rows] {
        let res = run_src(&format!("S:\n open {p}\n sort team score desc\n;"), cs);
        let out = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("S"))
            .unwrap();
        let mut got: Vec<(i64, i64)> = Vec::with_capacity(rows);
        for c in &out.chunks {
            let (ti, si) = (
                c.schema.index_of("team").unwrap(),
                c.schema.index_of("score").unwrap(),
            );
            for r in 0..c.len {
                got.push((
                    c.value(r, ti).as_f64().unwrap() as i64,
                    c.value(r, si).as_f64().unwrap() as i64,
                ));
            }
        }
        assert_eq!(got, want, "multi-key sort @cs={cs}");
    }
}

// Collect (id, v) row order of `sort v` over an `(id:int v:float)` CSV, where v
// is collected as the finite value (None for NaN or the null blank).
fn sort_f64_by_v(p: &std::path::Path, asc: bool, cs: usize) -> Vec<(i64, Option<f64>)> {
    let dir = if asc { "" } else { " desc" };
    let res = run_src(
        &format!(
            "S:\n open {} (id:int v:float)\n sort v{dir}\n;",
            p.display()
        ),
        cs,
    );
    let out = res
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some("S"))
        .unwrap();
    let mut rows = Vec::new();
    for c in &out.chunks {
        let (ii, vi) = (
            c.schema.index_of("id").unwrap(),
            c.schema.index_of("v").unwrap(),
        );
        for r in 0..c.len {
            let id = c.value(r, ii).as_f64().unwrap() as i64;
            let v = if c.columns[vi].is_null(r) {
                None
            } else {
                c.value(r, vi).as_f64().filter(|x| x.is_finite())
            };
            rows.push((id, v));
        }
    }
    rows
}

#[test]
fn sort_f64_lane_with_nulls_orders_ascending_nulls_last() {
    // The single-key decorate-sort owns the f64 lane (PERF-G follow-up). With real
    // values + nulls (no NaN), it must order exactly like the int lane: ascending
    // values, then nulls last (§26.2b), stable, chunk-size independent.
    let data = "id,v\n1,3.5\n2,\n3,1.5\n4,2.5\n5,\n6,0.5\n7,4.5\n8,2.0\n".as_bytes();
    let f = TempCsv(gendata::write_temp_bytes("stress_sort_f64", data));
    for cs in [1usize, 3, 5, 64] {
        let asc = sort_f64_by_v(&f.0, true, cs);
        // 0.5,1.5,2.0,2.5,3.5,4.5 (ids 6,3,8,4,1,7) then nulls (ids 2,5 stable).
        let want: Vec<(i64, Option<f64>)> = vec![
            (6, Some(0.5)),
            (3, Some(1.5)),
            (8, Some(2.0)),
            (4, Some(2.5)),
            (1, Some(3.5)),
            (7, Some(4.5)),
            (2, None),
            (5, None),
        ];
        assert_eq!(asc, want, "f64 asc nulls-last @cs={cs}");
        // desc reverses the whole order → nulls first, values descending.
        let desc = sort_f64_by_v(&f.0, false, cs);
        let want_desc: Vec<(i64, Option<f64>)> = vec![
            (2, None),
            (5, None),
            (7, Some(4.5)),
            (1, Some(3.5)),
            (4, Some(2.5)),
            (8, Some(2.0)),
            (3, Some(1.5)),
            (6, Some(0.5)),
        ];
        assert_eq!(desc, want_desc, "f64 desc nulls-first @cs={cs}");
    }
}

#[test]
fn sort_f64_with_nan_is_chunk_size_independent() {
    // NaN→Equal is an intentionally inconsistent order (it matches the old
    // comparator), so the finite values are NOT guaranteed sorted when NaN is
    // interspersed — but the invariant that matters still holds: the *whole* order
    // is identical for every chunk size (serial == parallel), no row is dropped,
    // and it never panics. (Byte-identity vs the pre-follow-up comparator path is
    // verified out-of-band by diffing 1M-row outputs.)
    let data =
        "id,v\n1,3.5\n2,NaN\n3,\n4,1.5\n5,NaN\n6,2.5\n7,\n8,0.5\n9,4.5\n10,NaN\n11,\n12,2.0\n"
            .as_bytes();
    let f = TempCsv(gendata::write_temp_bytes("stress_sort_nan", data));
    let mut reference: Option<Vec<i64>> = None;
    for cs in [1usize, 3, 5, 64] {
        let ids: Vec<i64> = sort_f64_by_v(&f.0, true, cs)
            .into_iter()
            .map(|r| r.0)
            .collect();
        assert_eq!(ids.len(), 12, "no rows lost @cs={cs}");
        match &reference {
            None => reference = Some(ids),
            Some(want) => assert_eq!(&ids, want, "NaN/null order chunk-size independent @cs={cs}"),
        }
    }
}

#[test]
fn distinct_dedups_chunk_size_independent() {
    let rows = 20_000;
    let seed = 11;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_distinct", &data));
    let p = f.0.display();

    // `country` is one of five fixed values; with 20k rows all five appear, so
    // `distinct country` yields exactly 5 rows regardless of chunk size.
    for cs in [1, 7, 1024, 8192, rows] {
        let res = run_src(&format!("D:\n open {p}\n distinct country\n;"), cs);
        assert_eq!(res.total_rows_out(), 5, "distinct country @cs={cs}");
        assert!(res.errors.is_empty());
    }

    // Whole-row distinct: the surviving count must be identical across chunk
    // sizes (first-occurrence dedup is order-deterministic, not chunk-bound).
    let baseline = run_src(&format!("D:\n open {p}\n distinct\n;"), 4096).total_rows_out();
    assert!(baseline > 0 && baseline <= rows as u64);
    for cs in [1, 7, 8192, rows] {
        let res = run_src(&format!("D:\n open {p}\n distinct\n;"), cs);
        assert_eq!(
            res.total_rows_out(),
            baseline,
            "whole-row distinct @cs={cs}"
        );
    }
}

#[test]
fn computed_columns_are_exact_chunk_size_independent() {
    let rows = 20_000;
    let seed = 5;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_calc", &data));
    let p = f.0.display();

    // `(age * 2 + 1)` must equal the arithmetic on the source `age`, exactly and
    // for every chunk size. Carry `age` through so we can check element-wise.
    for cs in [1, 7, 1024, 8192, rows] {
        let res = run_src(&format!("C:\n open {p}\n |> age (age * 2 + 1) as v\n;"), cs);
        let age = collect_i64(&res, "C", "age");
        let v = collect_i64(&res, "C", "v");
        assert_eq!(age.len(), rows, "row count @cs={cs}");
        assert_eq!(v.len(), rows, "computed row count @cs={cs}");
        for (a, got) in age.iter().zip(&v) {
            assert_eq!(*got, a * 2 + 1, "computed value @cs={cs}");
        }
    }
}

#[test]
fn error_heavy_skips_and_continues() {
    let rows = 40_000;
    let data = gendata::error_heavy(rows, 0.5, 7);
    let f = TempCsv(gendata::write_temp("stress_err", &data));
    let p = f.0.display();

    // Roughly half the rows are malformed; the run must still succeed, surface a
    // recoverable error about skipped rows, and never go fatal.
    let src = format!("F:\n open {p}\n |? age >= 0\n;");
    let res = run_src(&src, 4096);

    assert!(
        res.errors.iter().any(|e| e.message.contains("malformed")),
        "expected a recoverable malformed-row error"
    );
    assert!(
        !res.errors.iter().any(rivus_core::ErrorEvent::is_fatal),
        "error-heavy input must not be fatal (continue-first)"
    );
    let out = res.total_rows_out();
    assert!(out > 0 && out < rows as u64, "kept {out} of {rows}");
}

#[test]
fn mixed_types_degrades_to_string_lane() {
    let rows = 30_000;
    // Pure-int column: inference picks i64, predicate is numeric.
    let pure = gendata::mixed_types(rows, 0.0, 1);
    let fp = TempCsv(gendata::write_temp("stress_pure", &pure));
    let res_pure = run_src(
        &format!("F:\n open {}\n |? value >= 50\n;", fp.0.display()),
        4096,
    );
    assert!(res_pure.errors.is_empty());

    // Mixed column: some cells are non-numeric, so inference falls back to Str
    // and the comparison runs on the string lane — it must still run, not crash.
    let mixed = gendata::mixed_types(rows, 0.3, 1);
    let fm = TempCsv(gendata::write_temp("stress_mixed", &mixed));
    let res_mixed = run_src(
        &format!("F:\n open {}\n |? value >= 50\n;", fm.0.display()),
        4096,
    );
    // Both runs complete; the mixed run produces a (string-comparison) result
    // without going fatal.
    assert!(!res_mixed
        .errors
        .iter()
        .any(rivus_core::ErrorEvent::is_fatal));
}

#[test]
fn string_filter_matches_oracle() {
    // Filter on a string column (country == "JP") must match an independent
    // count, exercising the borrowed-&str predicate fast path across chunk
    // sizes. Also checks `!=` for the complementary count.
    let rows = 40_000;
    let seed = 123;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_strfilter", &data));
    let p = f.0.display();

    // Oracle: replay the generator's PRNG to count JP rows.
    let mut rng = Rng::new(seed);
    let countries = ["JP", "US", "DE", "FR", "BR"];
    let mut jp = 0u64;
    for _ in 0..rows {
        let _age = rng.below(90);
        let _score = rng.below(10_000);
        let c = countries[rng.below(5) as usize];
        let _active = rng.below(2);
        if c == "JP" {
            jp += 1;
        }
    }

    for cs in [1, 1000, 8192] {
        let eq = run_src(&format!("F:\n open {p}\n |? country == \"JP\"\n;"), cs);
        assert_eq!(eq.total_rows_out(), jp, "== chunk_size={cs}");
        let ne = run_src(&format!("F:\n open {p}\n |? country != \"JP\"\n;"), cs);
        assert_eq!(ne.total_rows_out(), rows as u64 - jp, "!= chunk_size={cs}");
    }
}

#[test]
fn rename_and_drop_are_chunk_size_independent() {
    // `rename` changes only column names; `drop` removes columns. Both are
    // stateless, so the result must not depend on chunk size. Verify the output
    // schema and that the kept values survive across chunk sizes.
    let rows = 20_000;
    let mut rng = Rng::new(11);
    let mut text = String::from("name,age,city\n");
    let mut ages: Vec<u64> = Vec::with_capacity(rows);
    for _ in 0..rows {
        let age = rng.below(90);
        ages.push(age);
        text.push_str(&format!("user,{age},NYC\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_rendrop", text.as_bytes()));
    let p = f.0.display();
    // rename age -> years, then drop city: output columns must be [name, years].
    let src = format!("R:\n open {p}\n rename age years\n drop city\n;");
    for cs in [1usize, 7, 1024, 8192, rows] {
        let res = run_src(&src, cs);
        assert!(res.errors.is_empty(), "errors @cs={cs}: {:?}", res.errors);
        let out = &res.outputs[0];
        let total: usize = out.chunks.iter().map(|c| c.len).sum();
        assert_eq!(total, rows, "row count @cs={cs}");
        let first = &out.chunks[0];
        assert_eq!(
            first.schema.field_names(),
            vec!["name", "years"],
            "schema @cs={cs}"
        );
    }
    // Spot-check values: the `years` column equals the original ages, in order.
    let res = run_src(&src, 4096);
    let out = &res.outputs[0];
    let mut got = Vec::with_capacity(rows);
    for c in &out.chunks {
        let yi = c.schema.index_of("years").unwrap();
        for r in 0..c.len {
            got.push(c.value(r, yi).as_f64().unwrap() as u64);
        }
    }
    assert_eq!(got, ages, "renamed column values preserved in order");
}

#[test]
fn reorder_is_chunk_size_independent() {
    // `reorder city age` moves those columns to the front; the rest follow in
    // original order. A permutation — types/values preserved, row count and
    // schema independent of chunk size.
    let rows = 12_000;
    let mut rng = Rng::new(17);
    let mut text = String::from("id,name,age,city\n");
    let mut ages: Vec<u64> = Vec::with_capacity(rows);
    for i in 0..rows {
        let age = rng.below(90);
        ages.push(age);
        text.push_str(&format!("{i},user,{age},NYC\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_reorder", text.as_bytes()));
    let p = f.0.display();
    let src = format!("R:\n open {p}\n reorder city age\n;");
    for cs in [1usize, 7, 1024, rows] {
        let res = run_src(&src, cs);
        assert!(res.errors.is_empty(), "errors @cs={cs}");
        let out = &res.outputs[0];
        let total: usize = out.chunks.iter().map(|c| c.len).sum();
        assert_eq!(total, rows, "row count @cs={cs}");
        assert_eq!(
            out.chunks[0].schema.field_names(),
            vec!["city", "age", "id", "name"],
            "reordered schema @cs={cs}"
        );
    }
    // `age` values survive the permutation, in order.
    let res = run_src(&src, 4096);
    let out = &res.outputs[0];
    let mut got = Vec::with_capacity(rows);
    for c in &out.chunks {
        let ai = c.schema.index_of("age").unwrap();
        for r in 0..c.len {
            got.push(c.value(r, ai).as_f64().unwrap() as u64);
        }
    }
    assert_eq!(got, ages, "reordered column values preserved in order");
}

#[test]
fn cast_verb_retypes_columns_chunk_size_independent() {
    // `code` is declared str (keeps leading zeros); `cast code:int` re-types it,
    // dropping the zeros. The cast result and the column dtype must be exact and
    // chunk-size independent.
    let rows = 5_000usize;
    let mut text = String::from("id,code\n");
    for i in 0..rows {
        text.push_str(&format!("{i},0{i:04}\n")); // leading-zero code
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_cast_verb",
        text.as_bytes(),
    ));
    let p = f.0.display();
    for cs in [1usize, 7, 1024, rows] {
        let res = run_src(
            &format!("C:\n open {p} (id code:str)\n cast code:int\n;"),
            cs,
        );
        let out = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("C"))
            .unwrap();
        // The `code` column is now i64, value == id (leading zeros stripped).
        assert_eq!(
            out.chunks[0].schema.fields[out.chunks[0].schema.index_of("code").unwrap()].dtype,
            rivus_core::DataType::I64,
            "code dtype @cs={cs}"
        );
        let mut got = Vec::with_capacity(rows);
        for c in &out.chunks {
            let ci = c.schema.index_of("code").unwrap();
            for r in 0..c.len {
                got.push(c.value(r, ci).as_f64().unwrap() as i64);
            }
        }
        let want: Vec<i64> = (0..rows as i64).collect();
        assert_eq!(got, want, "cast values @cs={cs}");
        assert!(res.errors.is_empty(), "errors @cs={cs}");
    }
}

#[test]
fn case_when_is_chunk_size_independent() {
    // `case when … then … else … end` computed column buckets each row by its
    // age band, identically across chunk sizes.
    let rows = 20_000;
    let mut rng = Rng::new(13);
    let mut text = String::from("name,age\n");
    let mut expect: Vec<&str> = Vec::with_capacity(rows);
    for _ in 0..rows {
        let age = rng.below(90);
        text.push_str(&format!("user,{age}\n"));
        expect.push(if age >= 60 {
            "senior"
        } else if age >= 18 {
            "adult"
        } else {
            "minor"
        });
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_case", text.as_bytes()));
    let p = f.0.display();
    let src = format!(
        "C:\n open {p}\n |> name (case when age >= 60 then \"senior\" when age >= 18 then \"adult\" else \"minor\" end) as bucket\n;"
    );
    for cs in [1usize, 7, 1024, 8192, rows] {
        let res = run_src(&src, cs);
        assert!(res.errors.is_empty(), "errors @cs={cs}: {:?}", res.errors);
        let out = &res.outputs[0];
        let mut got = Vec::with_capacity(rows);
        for c in &out.chunks {
            let bi = c.schema.index_of("bucket").unwrap();
            for r in 0..c.len {
                got.push(c.value(r, bi).to_string());
            }
        }
        assert_eq!(got, expect, "case buckets @cs={cs}");
    }
}

#[test]
fn starts_ends_with_chunk_size_independent() {
    // starts_with / ends_with row filters must match a row-wise oracle and be
    // independent of chunk size.
    let rows = 20_000;
    let mut rng = Rng::new(29);
    let mut text = String::from("code\n");
    let mut starts = 0u64;
    let mut ends = 0u64;
    for _ in 0..rows {
        // codes like "JP-1234" / "US-0007" — prefix is a 2-letter country.
        let cc = ["JP", "US", "DE"][rng.below(3) as usize];
        let n = rng.below(10_000);
        let code = format!("{cc}-{n:04}");
        if code.starts_with("JP") {
            starts += 1;
        }
        if code.ends_with("7") {
            ends += 1;
        }
        text.push_str(&code);
        text.push('\n');
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_startsends",
        text.as_bytes(),
    ));
    let p = f.0.display();
    for cs in [1usize, 7, 1024, 8192, rows] {
        let s = run_src(
            &format!("S:\n open {p}\n |? starts_with(code, \"JP\")\n;"),
            cs,
        );
        assert_eq!(s.total_rows_out(), starts, "starts_with @cs={cs}");
        let e = run_src(&format!("E:\n open {p}\n |? ends_with(code, \"7\")\n;"), cs);
        assert_eq!(e.total_rows_out(), ends, "ends_with @cs={cs}");
        assert!(s.errors.is_empty() && e.errors.is_empty());
    }
}

#[test]
fn like_and_glob_chunk_size_independent() {
    // `like` (SQL %/_) and `glob` (*?[...]) row filters must match a row-wise
    // oracle and be chunk-size independent.
    let rows = 20_000;
    let mut rng = Rng::new(31);
    let mut text = String::from("code\n");
    let mut like_jp = 0u64;
    let mut glob_cls = 0u64;
    for _ in 0..rows {
        let cc = ["JP", "US", "DE"][rng.below(3) as usize];
        let n = rng.below(10_000);
        let code = format!("{cc}-{n:04}");
        if code.starts_with("JP-") {
            like_jp += 1; // like "JP-%"
        }
        // glob "[JD]*00" → starts with J or D and ends with "00".
        let first = code.chars().next().unwrap();
        if (first == 'J' || first == 'D') && code.ends_with("00") {
            glob_cls += 1;
        }
        text.push_str(&code);
        text.push('\n');
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_likeglob",
        text.as_bytes(),
    ));
    let p = f.0.display();
    for cs in [1usize, 7, 1024, 8192, rows] {
        let l = run_src(&format!("L:\n open {p}\n |? like(code, \"JP-%\")\n;"), cs);
        assert_eq!(l.total_rows_out(), like_jp, "like @cs={cs}");
        let g = run_src(
            &format!("G:\n open {p}\n |? glob(code, \"[JD]*00\")\n;"),
            cs,
        );
        assert_eq!(g.total_rows_out(), glob_cls, "glob @cs={cs}");
        assert!(l.errors.is_empty() && g.errors.is_empty());
    }
}
