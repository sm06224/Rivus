//! Group-by & describe aggregations against an oracle.
//!
//! Moved verbatim from the former monolithic `stress.rs` (design 26 §26.8.1,
//! mechanical move-only split; logic unchanged).

use super::*;

#[test]
fn array_agg_collects_list_in_source_order_and_is_explode_dual() {
    // §32 / #172: `array_agg:v` collects each group's non-null values into a
    // `List` lane, in source order. `array_agg` is the dual of `explode`:
    // grouping then exploding the list reconstructs the original (g, v) rows.
    let text = "g,v\na,1\nb,2\na,3\nb,4\na,5\n";
    let f = TempCsv(gendata::write_temp_bytes("stress_arragg", text.as_bytes()));
    let p = f.0.display();

    let res = run_src(&format!("G:\n open {p}\n |# g array_agg:v\n;"), 4096);
    let o = res
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some("G"))
        .unwrap();
    let c = &o.chunks[0];
    let (gi, ai) = (
        c.schema.index_of("g").unwrap(),
        c.schema.index_of("array_agg_v").unwrap(),
    );
    // The aggregate column is a List lane.
    assert_eq!(c.schema.fields[ai].dtype, rivus_core::DataType::List);
    let mut got: Vec<(String, String)> = (0..c.len)
        .map(|r| (c.value(r, gi).to_string(), c.value(r, ai).to_string()))
        .collect();
    got.sort();
    // Source order preserved within each group.
    assert_eq!(
        got,
        vec![
            ("a".to_string(), "[1, 3, 5]".to_string()),
            ("b".to_string(), "[2, 4]".to_string()),
        ],
        "array_agg keeps source order per group"
    );

    // Dual: group → array_agg → explode reconstructs the original (g, v) rows.
    let back = run_src(
        &format!("G:\n open {p}\n |# g array_agg:v\n explode array_agg_v\n |> g array_agg_v\n;"),
        4096,
    );
    let mut rows: Vec<(String, String)> = {
        let g = collect_strings(&back, "G", "g");
        let v = collect_strings(&back, "G", "array_agg_v");
        g.into_iter().zip(v).collect()
    };
    rows.sort();
    assert_eq!(
        rows,
        vec![
            ("a".into(), "1".into()),
            ("a".into(), "3".into()),
            ("a".into(), "5".into()),
            ("b".into(), "2".into()),
            ("b".into(), "4".into()),
        ],
        "explode(array_agg) reconstructs the rows (the dual)"
    );
}

#[test]
fn describe_matches_oracle() {
    // One numeric column `v`; `describe` must report count/min/max/mean that
    // match an independent computation, for every chunk size.
    let rows = 10_000;
    let mut rng = Rng::new(1);
    let mut text = String::from("v\n");
    let (mut sum, mut mn, mut mx) = (0i64, i64::MAX, i64::MIN);
    for _ in 0..rows {
        let x = rng.below(1000) as i64;
        text.push_str(&format!("{x}\n"));
        sum += x;
        mn = mn.min(x);
        mx = mx.max(x);
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_desc", text.as_bytes()));
    let p = f.0.display();
    let mean = sum as f64 / rows as f64;

    for cs in [1, 7, 1024, rows] {
        let res = run_src(&format!("D:\n open {p}\n describe\n;"), cs);
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("D"))
            .expect("describe output");
        let c = &o.chunks[0];
        let cell = |col: &str| {
            let ci = c.schema.index_of(col).unwrap();
            c.value(0, ci).to_string()
        };
        assert_eq!(cell("column"), "v", "@cs={cs}");
        assert_eq!(cell("count"), rows.to_string(), "count @cs={cs}");
        assert_eq!(
            cell("min").parse::<f64>().unwrap(),
            mn as f64,
            "min @cs={cs}"
        );
        assert_eq!(
            cell("max").parse::<f64>().unwrap(),
            mx as f64,
            "max @cs={cs}"
        );
        assert!(
            (cell("mean").parse::<f64>().unwrap() - mean).abs() < 1e-6,
            "mean @cs={cs}"
        );
    }
}

#[test]
fn group_aggregates_are_exact() {
    // `|# country sum:age max:age` (+ implicit count) must match an oracle that
    // buckets the regenerated PRNG stream by country.
    use std::collections::BTreeMap;
    let rows = 20_000;
    let seed = 314;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_groupagg", &data));
    let p = f.0.display();

    let countries = ["JP", "US", "DE", "FR", "BR"];
    let mut rng = Rng::new(seed);
    let mut oracle: BTreeMap<String, (i64, f64, f64)> = BTreeMap::new(); // (count,sum,max)
    for _ in 0..rows {
        let age = rng.below(90) as f64;
        let _score = rng.below(10_000);
        let c = countries[rng.below(5) as usize].to_string();
        let _active = rng.below(2);
        let e = oracle.entry(c).or_insert((0, 0.0, f64::NEG_INFINITY));
        e.0 += 1;
        e.1 += age;
        e.2 = e.2.max(age);
    }

    let res = run_src(
        &format!("G:\n open {p}\n |# country sum:age max:age\n;"),
        4096,
    );
    let out = &res.outputs[0];
    let chunk = &out.chunks[0];
    assert_eq!(
        chunk.schema.field_names(),
        vec!["country", "count", "sum_age", "max_age"]
    );
    assert_eq!(chunk.len, oracle.len());
    for row in 0..chunk.len {
        let country = chunk.value(row, 0).to_string();
        let count = chunk.value(row, 1).as_f64().unwrap() as i64;
        let sum = chunk.value(row, 2).as_f64().unwrap();
        let max = chunk.value(row, 3).as_f64().unwrap();
        let (oc, os, om) = oracle[&country];
        assert_eq!(count, oc, "count[{country}]");
        assert_eq!(sum, os, "sum[{country}]");
        assert_eq!(max, om, "max[{country}]");
    }
}

#[test]
fn multi_key_group_matches_oracle() {
    // `|# country active sum:age` groups by the (country, active) tuple. The
    // per-group count and sum must match an independent oracle that buckets the
    // regenerated PRNG stream by the same tuple, and be chunk-size independent.
    use std::collections::BTreeMap;
    let rows = 20_000;
    let seed = 271;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_mkgroup", &data));
    let p = f.0.display();

    // Oracle: replay clean()'s exact PRNG sequence (age, score, country, active).
    let countries = ["JP", "US", "DE", "FR", "BR"];
    let mut rng = Rng::new(seed);
    let mut oracle: BTreeMap<(String, String), (i64, f64)> = BTreeMap::new();
    for _ in 0..rows {
        let age = rng.below(90) as f64;
        let _score = rng.below(10_000);
        let country = countries[rng.below(5) as usize].to_string();
        let active = (rng.below(2) == 1).to_string();
        let e = oracle.entry((country, active)).or_insert((0, 0.0));
        e.0 += 1;
        e.1 += age;
    }

    for cs in [1usize, 7, 1024, rows] {
        let res = run_src(&format!("G:\n open {p}\n |# country active sum:age\n;"), cs);
        let out = &res.outputs[0];
        let chunk = &out.chunks[0];
        assert_eq!(
            chunk.schema.field_names(),
            vec!["country", "active", "count", "sum_age"],
            "schema @cs={cs}"
        );
        assert_eq!(chunk.len, oracle.len(), "group count @cs={cs}");
        for row in 0..chunk.len {
            let country = chunk.value(row, 0).to_string();
            let active = chunk.value(row, 1).to_string();
            let count = chunk.value(row, 2).as_f64().unwrap() as i64;
            let sum = chunk.value(row, 3).as_f64().unwrap();
            let (oc, os) = oracle[&(country.clone(), active.clone())];
            assert_eq!(count, oc, "count[{country},{active}] @cs={cs}");
            assert_eq!(sum, os, "sum[{country},{active}] @cs={cs}");
        }
    }
}

#[test]
fn group_extended_aggregates_are_correct_and_chunk_independent() {
    // std / count_distinct / first / last (plus avg) must be correct and
    // independent of chunk size. Two small groups with known statistics.
    let text = "team,player,score\nA,x,10\nA,y,20\nA,x,30\nB,z,5\nB,z,5\nB,w,15\n";
    let f = TempCsv(gendata::write_temp_bytes("stress_grpext", text.as_bytes()));
    let p = f.0.display();
    let src = format!(
        "G:\n open {p}\n |# team std:score count_distinct:player first:player last:player avg:score\n;"
    );

    // Verify the values once (at a normal chunk size), then assert that smaller
    // chunk sizes produce a byte-identical result row-for-row.
    let base = run_src(&src, 4096);
    let bchunk = &base.outputs[0].chunks[0];
    assert_eq!(
        bchunk.schema.field_names(),
        vec![
            "team",
            "count",
            "std_score",
            "count_distinct_player",
            "first_player",
            "last_player",
            "avg_score",
        ]
    );
    assert_eq!(bchunk.len, 2);
    let cell = |row: usize, col: usize| bchunk.value(row, col).to_string();
    let num = |row: usize, col: usize| bchunk.value(row, col).as_f64().unwrap();
    // Group A: scores 10,20,30 → std 10, avg 20; players x,y,x → distinct 2, first x, last x.
    assert_eq!(cell(0, 0), "A");
    assert_eq!(num(0, 1), 3.0);
    assert!((num(0, 2) - 10.0).abs() < 1e-9);
    assert_eq!(num(0, 3), 2.0);
    assert_eq!(cell(0, 4), "x");
    assert_eq!(cell(0, 5), "x");
    assert!((num(0, 6) - 20.0).abs() < 1e-9);
    // Group B: scores 5,5,15 → sample std 5.7735…, avg 25/3; players z,z,w → distinct 2, first z, last w.
    assert_eq!(cell(1, 0), "B");
    assert!((num(1, 2) - 5.773_502_691_896_257).abs() < 1e-9);
    assert_eq!(num(1, 3), 2.0);
    assert_eq!(cell(1, 4), "z");
    assert_eq!(cell(1, 5), "w");
    assert!((num(1, 6) - 25.0 / 3.0).abs() < 1e-9);

    // Chunk-size independence: every cell matches the base across chunk sizes.
    for cs in [1usize, 2, 5, 64] {
        let r = run_src(&src, cs);
        let c = &r.outputs[0].chunks[0];
        assert_eq!(c.len, bchunk.len, "row count @cs={cs}");
        for row in 0..c.len {
            for col in 0..bchunk.schema.fields.len() {
                assert_eq!(
                    c.value(row, col).to_string(),
                    bchunk.value(row, col).to_string(),
                    "cell[{row}][{col}] @cs={cs}"
                );
            }
        }
    }
}

#[test]
fn group_percentiles_are_correct_and_chunk_independent() {
    // median / p90 over a known group. Group A: 10,20,30,40 → median 25, p90 37
    // (linear interp: rank=0.9*3=2.7 → 30+(40-30)*0.7=37). Group B: 5,100 →
    // median 52.5, p90 90.5. Must be identical across chunk sizes.
    let text = "team,score\nA,10\nA,20\nA,30\nA,40\nB,5\nB,100\n";
    let f = TempCsv(gendata::write_temp_bytes("stress_pct", text.as_bytes()));
    let p = f.0.display();
    let src = format!("G:\n open {p}\n |# team median:score p90:score\n;");

    let base = run_src(&src, 4096);
    let bchunk = &base.outputs[0].chunks[0];
    assert_eq!(
        bchunk.schema.field_names(),
        vec!["team", "count", "median_score", "p90_score"]
    );
    let num = |row: usize, col: usize| bchunk.value(row, col).as_f64().unwrap();
    // Row 0 = A, row 1 = B (BTreeMap key order).
    assert!((num(0, 2) - 25.0).abs() < 1e-9, "A median");
    assert!((num(0, 3) - 37.0).abs() < 1e-9, "A p90");
    assert!((num(1, 2) - 52.5).abs() < 1e-9, "B median");
    assert!((num(1, 3) - 90.5).abs() < 1e-9, "B p90");

    // Chunk-size independence: every cell matches the base.
    for cs in [1usize, 2, 3, 5] {
        let r = run_src(&src, cs);
        let c = &r.outputs[0].chunks[0];
        for row in 0..c.len {
            for col in 0..bchunk.schema.fields.len() {
                assert_eq!(
                    c.value(row, col).to_string(),
                    bchunk.value(row, col).to_string(),
                    "cell[{row}][{col}] @cs={cs}"
                );
            }
        }
    }
}

#[test]
fn decimal_sum_overflow_is_surfaced_and_null_not_silent_f64() {
    // #202: an i128 overflow in a decimal aggregate must NOT silently degrade
    // the column to f64 — that is a wrong money total with no warning, on the
    // lane whose whole contract is exactness. The column stays Decimal, the
    // overflowed group's cell is null (continue-first), and one Recoverable
    // event names the column (never-silent). A sum that fits stays exact and
    // raises nothing (no false positives on clean data).
    let big = "9999999999999999999999999999999999.99"; // ~1e34, scale 2

    // (a) 50 rows fit in i128 (~5e35 * 100 < i128::MAX ~ 1.7e38): exact, quiet.
    let mut fits = String::from("g,v\n");
    for _ in 0..50 {
        fits.push_str(&format!("x,{big}\n"));
    }
    let f1 = TempCsv(gendata::write_temp_bytes(
        "stress_dec_fits",
        fits.as_bytes(),
    ));
    let res = run_src(
        &format!(
            "G:\n open {} (g:str v:decimal(2))\n |# g sum:v\n;",
            f1.0.display()
        ),
        4096,
    );
    let o = res
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some("G"))
        .unwrap();
    let c = &o.chunks[0];
    let si = c.schema.index_of("sum_v").unwrap();
    assert!(matches!(
        c.schema.fields[si].dtype,
        rivus_core::DataType::Decimal { .. }
    ));
    assert_eq!(
        c.value(0, si).to_string(),
        "499999999999999999999999999999999999.50",
        "in-range decimal sum stays exact"
    );
    assert!(
        !res.errors.iter().any(|e| e.message.contains("overflow")),
        "no false-positive overflow on clean data"
    );

    // (b) 200 rows overflow i128 (~2e36 * 100 > i128::MAX): Decimal + null + surfaced.
    let mut ovf = String::from("g,v\n");
    for _ in 0..200 {
        ovf.push_str(&format!("x,{big}\n"));
    }
    let f2 = TempCsv(gendata::write_temp_bytes("stress_dec_ovf", ovf.as_bytes()));
    let res = run_src(
        &format!(
            "G:\n open {} (g:str v:decimal(2))\n |# g sum:v\n;",
            f2.0.display()
        ),
        4096,
    );
    let o = res
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some("G"))
        .unwrap();
    let c = &o.chunks[0];
    let si = c.schema.index_of("sum_v").unwrap();
    assert!(
        matches!(
            c.schema.fields[si].dtype,
            rivus_core::DataType::Decimal { .. }
        ),
        "an overflowed decimal column must stay Decimal, never degrade to f64"
    );
    assert!(
        matches!(c.value(0, si), rivus_core::Value::Null),
        "the overflowed cell is null, not a drifted f64: got {}",
        c.value(0, si)
    );
    let ci = c.schema.index_of("count").unwrap();
    assert_eq!(
        c.value(0, ci).to_string(),
        "200",
        "count survives (continue-first)"
    );
    assert!(
        res.errors
            .iter()
            .any(|e| e.message.contains("overflow") && e.message.contains("'v'")),
        "the overflow must be surfaced naming the column; got: {:?}",
        res.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
    );
}
