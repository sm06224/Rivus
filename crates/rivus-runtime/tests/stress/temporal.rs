//! Temporal lanes: datetime / date / time / duration read, extract, group.
//!
//! Moved verbatim from the former monolithic `stress.rs` (design 26 §26.8.1,
//! mechanical move-only split; logic unchanged).

use super::*;

#[test]
fn datetime_column_parses_and_is_chunk_size_independent() {
    // A `:datetime("yyMMddHHmmss")` column parses fixed-width timestamps into the
    // exact integer-tick lane (design 23). A non-matching cell is continue-first
    // (epoch 0, no fatal). The result must not depend on chunk size.
    let text = "ts,id\n\
                260601143000,1\n\
                991231235959,2\n\
                bad,3\n\
                700101000000,4\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_datetime",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let flow = format!("D:\n open {p} (ts:datetime(\"yyMMddHHmmss\") id:int)\n |> ts id\n;");

    let want_ts = vec![
        "2026-06-01T14:30:00".to_string(), // yy=26 → 2026
        "1999-12-31T23:59:59".to_string(), // yy=99 → 1999 (pivot >68 → 19xx)
        "".to_string(),                    // "bad" → null (parse-fail; design 26)
        "1970-01-01T00:00:00".to_string(), // yy=70 → 1970 (a real epoch instant)
    ];
    for cz in [1usize, 2, 3, 4096] {
        let res = run_src(&flow, cz);
        assert!(
            !res.errors.iter().any(rivus_core::ErrorEvent::is_fatal),
            "datetime parse must never raise a fatal (cz={cz})"
        );
        assert_eq!(
            collect_strings(&res, "D", "ts"),
            want_ts,
            "datetime ISO rendering changed at chunk_size {cz}"
        );
        assert_eq!(
            collect_i64(&res, "D", "id"),
            vec![1, 2, 3, 4],
            "row alignment changed at chunk_size {cz}"
        );
        // The declared lane is DateTime, not a string fallback.
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
            "ts column must be the datetime lane at chunk_size {cz}"
        );
    }
}

#[test]
fn datetime_ddd_locale_and_subsecond_chunk_size_independent() {
    // §29 s3: a `[ja-jp]` format with a validated `ddd` weekday and a 6-digit
    // `nnnnnn` sub-second run. The run derives the Micro tick unit, every digit
    // is preserved (Display renders the full-width fraction), a weekday name
    // that contradicts its date is a counted parse failure (never silently
    // accepted), and none of it may depend on chunk size.
    let text = "ts,id\n\
                2026年06月10日(水) 12:00:00.123456,1\n\
                2026年06月10日(月) 12:00:00.000001,2\n\
                garbage,3\n\
                2026年06月11日(木) 00:00:00.999999,4\n";
    let f = TempCsv(gendata::write_temp_bytes("stress_dt_ja", text.as_bytes()));
    let p = f.0.display();
    let flow = format!(
        "D:\n open {p} (ts:datetime(\"[ja-jp]yyyy年MM月dd日(ddd) HH:mm:ss.nnnnnn\") id:int)\n \
         |> ts (format(ts, \"[ja-jp]ddd\")) as w id\n;"
    );
    let want_ts = vec![
        "2026-06-10T12:00:00.123456".to_string(),
        "".to_string(), // (月) on a Wednesday → weekday-validated parse failure
        "".to_string(), // garbage → parse failure
        "2026-06-11T00:00:00.999999".to_string(),
    ];
    let want_w = vec![
        "水".to_string(),
        "".to_string(),
        "".to_string(),
        "木".to_string(),
    ];
    for cz in [1usize, 2, 3, 4096] {
        let res = run_src(&flow, cz);
        assert!(
            !res.errors.iter().any(rivus_core::ErrorEvent::is_fatal),
            "ja datetime parse must never raise a fatal (cz={cz})"
        );
        assert!(
            !res.errors.is_empty(),
            "the 2 bad cells must surface on the error stream (cz={cz})"
        );
        assert_eq!(collect_strings(&res, "D", "ts"), want_ts, "cz={cz}");
        assert_eq!(collect_strings(&res, "D", "w"), want_w, "cz={cz}");
        assert_eq!(collect_i64(&res, "D", "id"), vec![1, 2, 3, 4], "cz={cz}");
    }
}

#[test]
fn date_column_parses_chunk_size_independent_and_surfaces_bad() {
    // `:date` reads ISO yyyy-MM-dd into the exact i32 epoch-day lane (#58). An
    // invalid date (2024-02-30) is continue-first (epoch 0) AND surfaced on the
    // error stream; an empty cell is "missing" (not counted). Result must not
    // depend on chunk size.
    let text = "id,d\n\
                1,2024-06-03\n\
                2,2024-02-30\n\
                3,\n\
                4,2023-12-25\n";
    let f = TempCsv(gendata::write_temp_bytes("stress_date", text.as_bytes()));
    let p = f.0.display();
    let flow = format!("D:\n open {p} (id:int d:date)\n |> id d\n;");
    let want_d = vec![
        "2024-06-03".to_string(),
        "".to_string(), // invalid → null (parse-fail; design 26)
        "".to_string(), // empty → null (missing; design 26)
        "2023-12-25".to_string(),
    ];
    for cz in [1usize, 2, 3, 4096] {
        let res = run_src(&flow, cz);
        assert!(
            !res.errors.iter().any(rivus_core::ErrorEvent::is_fatal),
            "date parse must never raise a fatal (cz={cz})"
        );
        assert_eq!(
            collect_strings(&res, "D", "d"),
            want_d,
            "date ISO rendering changed at chunk_size {cz}"
        );
        assert_eq!(
            collect_i64(&res, "D", "id"),
            vec![1, 2, 3, 4],
            "alignment @cz={cz}"
        );
        // Exactly one parse failure surfaced (the invalid date; empty not counted).
        // Verbatim phrasing (incl. the "(as date)" lane tag the GUIDE quotes).
        let fails = res
            .errors
            .iter()
            .filter(|e| {
                e.message
                    .contains("in column 'd' (as date) could not be parsed; set to null")
            })
            .count();
        assert_eq!(
            fails, 1,
            "one date parse failure surfaced @cz={cz}: {:?}",
            res.errors
        );
        // The declared lane is Date, not a string fallback.
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("D"))
            .unwrap();
        let ci = o.chunks[0].schema.index_of("d").unwrap();
        assert!(
            matches!(
                o.chunks[0].schema.fields[ci].dtype,
                rivus_core::DataType::Date
            ),
            "d must be the date lane at chunk_size {cz}"
        );
    }
}

#[test]
fn time_column_reads_minmax_and_surfaces_bad() {
    // :time reads HH:mm:ss into the exact i64 tick lane; a bad cell is
    // continue-first (null) AND surfaced; an empty cell is null (not counted);
    // min/max keep the Time lane (render HH:mm:ss) and are chunk-size
    // independent + parallel byte-identical (#58).
    let text = "k,t\n\
                a,09:05:00\n\
                a,23:59:59\n\
                a,nope\n\
                a,\n\
                b,00:00:01\n\
                b,12:30:00\n";
    let f = TempCsv(gendata::write_temp_bytes("stress_time", text.as_bytes()));
    let p = f.0.display();
    let flow = format!("T:\n open {p} (k:str t:time)\n |# k min:t max:t\n;");
    let snapshot = |pref: rivus_runtime::MemoryPref, cz: usize| {
        let g = rivus_parser::parse(&flow).expect("parse");
        std::env::set_var("RIVUS_PARALLEL_MIN_BYTES", "0");
        let res = run(
            &g,
            RunOptions {
                chunk_size: cz,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run");
        std::env::remove_var("RIVUS_PARALLEL_MIN_BYTES");
        // The bad cell (a) is surfaced verbatim; empty is not counted.
        let fails = res
            .errors
            .iter()
            .filter(|e| {
                e.message
                    .contains("in column 't' (as time) could not be parsed; set to null")
            })
            .count();
        assert_eq!(
            fails, 1,
            "one time parse failure surfaced: {:?}",
            res.errors
        );
        let lo = collect_strings(&res, "T", "min_t");
        for s in lo.iter().chain(collect_strings(&res, "T", "max_t").iter()) {
            assert_eq!(
                s.len(),
                8,
                "min/max must render HH:mm:ss (Time lane), got {s:?}"
            );
        }
        let mut rows: Vec<(String, String, String)> = {
            let k = collect_strings(&res, "T", "k");
            let hi = collect_strings(&res, "T", "max_t");
            (0..k.len())
                .map(|i| (k[i].clone(), lo[i].clone(), hi[i].clone()))
                .collect()
        };
        rows.sort();
        rows
    };
    for cz in [1usize, 2, 4096] {
        assert_eq!(
            snapshot(rivus_runtime::MemoryPref::Low, cz),
            snapshot(rivus_runtime::MemoryPref::Fast, cz),
            "time min/max byte-identical serial vs parallel @cz={cz}"
        );
    }
    // Oracle: key b spans 00:00:01..12:30:00; key a's bad/empty are now null
    // and skipped (design 26), so a's min/max is over its two real times.
    assert_eq!(
        snapshot(rivus_runtime::MemoryPref::Low, 4096),
        vec![
            (
                "a".to_string(),
                "09:05:00".to_string(),
                "23:59:59".to_string()
            ),
            (
                "b".to_string(),
                "00:00:01".to_string(),
                "12:30:00".to_string()
            ),
        ],
        "time min/max extreme values"
    );
}

#[test]
fn date_extractors_chunk_size_independent() {
    // weekday (Mon=0..Sun=6), is_weekend, and date(ts) (DateTime→date) are
    // row-wise and chunk-size independent (#58).
    let text = "d\n2024-06-03\n2024-06-08\n2024-06-09\n2023-12-25\n"; // Mon, Sat, Sun, Mon
    let f = TempCsv(gendata::write_temp_bytes("stress_date_fn", text.as_bytes()));
    let p = f.0.display();
    let flow = format!("W:\n open {p} (d:date)\n |> (weekday(d)) as wd (is_weekend(d)) as we\n;");
    for cz in [1usize, 2, 4096] {
        let res = run_src(&flow, cz);
        assert_eq!(
            collect_i64(&res, "W", "wd"),
            vec![0, 5, 6, 0],
            "weekday @cz={cz}"
        );
        assert_eq!(
            collect_strings(&res, "W", "we"),
            vec!["false", "true", "true", "false"],
            "is_weekend @cz={cz}"
        );
    }
    // date(ts) drops the time-of-day and keeps the exact date lane.
    let t2 = "ts\n2024-06-03 14:30:00\n2023-12-25 00:00:00\n";
    let f2 = TempCsv(gendata::write_temp_bytes("stress_date_fn2", t2.as_bytes()));
    let p2 = f2.0.display();
    let flow2 = format!("D:\n open {p2} (ts:datetime)\n |> (date(ts)) as day\n;");
    for cz in [1usize, 2, 4096] {
        let res = run_src(&flow2, cz);
        assert_eq!(
            collect_strings(&res, "D", "day"),
            vec!["2024-06-03", "2023-12-25"],
            "date(ts) @cz={cz}"
        );
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("D"))
            .unwrap();
        let ci = o.chunks[0].schema.index_of("day").unwrap();
        assert!(
            matches!(
                o.chunks[0].schema.fields[ci].dtype,
                rivus_core::DataType::Date
            ),
            "date(ts) must yield the date lane @cz={cz}"
        );
    }
}

#[test]
fn datetime_auto_infer_common_formats() {
    // A bare `:datetime` (no explicit format) auto-infers common shapes per cell:
    // ISO-with-T, ISO-with-space, and bare date all resolve; junk → null.
    let text = "ts\n\
                2026-06-01T14:30:00\n\
                2026-06-01 14:30:00\n\
                2026-06-01\n\
                nope\n";
    let f = TempCsv(gendata::write_temp_bytes("stress_dt_auto", text.as_bytes()));
    let p = f.0.display();
    let flow = format!("D:\n open {p} (ts:datetime)\n |> ts\n;");
    let res = run_src(&flow, 4096);
    assert_eq!(
        collect_strings(&res, "D", "ts"),
        vec![
            "2026-06-01T14:30:00".to_string(),
            "2026-06-01T14:30:00".to_string(),
            "2026-06-01T00:00:00".to_string(),
            "".to_string(), // "nope" → null (parse-fail; design 26)
        ],
    );
}

#[test]
fn datetime_filter_by_literal_same_lane() {
    // `|? ts >= "literal"` parses the literal into the datetime lane and compares
    // instants exactly (design 23) — not the lossy f64 view, and not a string
    // compare. Chunk-size independent.
    let text = "ts,id\n\
                260601143000,1\n\
                260601000000,2\n\
                991231235959,3\n\
                700101120000,4\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_dt_filter",
        text.as_bytes(),
    ));
    let p = f.0.display();
    // Threshold = 2026-06-01 00:00:00. Rows: r0 2026-06-01 14:30 (>=), r1 exactly
    // equal (>=), r2 1999-12-31 (no), r3 1970-01-01 (no).
    let flow = format!(
        "D:\n open {p} (ts:datetime(\"yyMMddHHmmss\") id:int)\n |? ts >= \"260601000000\"\n |> id\n;"
    );
    for cz in [1usize, 2, 3, 4096] {
        assert_eq!(
            collect_i64(&run_src(&flow, cz), "D", "id"),
            vec![1, 2],
            "datetime >= literal changed at chunk_size {cz}"
        );
    }
    // Strict `<` excludes the equal row; `==` keeps only it.
    let lt = format!(
        "D:\n open {p} (ts:datetime(\"yyMMddHHmmss\") id:int)\n |? ts < \"260601000000\"\n |> id\n;"
    );
    assert_eq!(collect_i64(&run_src(&lt, 4096), "D", "id"), vec![3, 4]);
    let eq = format!(
        "D:\n open {p} (ts:datetime(\"yyMMddHHmmss\") id:int)\n |? ts == \"260601000000\"\n |> id\n;"
    );
    assert_eq!(collect_i64(&run_src(&eq, 4096), "D", "id"), vec![2]);
    // An ISO-form literal resolves to the same instant as the compact column.
    let iso = format!(
        "D:\n open {p} (ts:datetime(\"yyMMddHHmmss\") id:int)\n |? ts >= \"2026-06-01\"\n |> id\n;"
    );
    assert_eq!(collect_i64(&run_src(&iso, 4096), "D", "id"), vec![1, 2]);
    // An unparseable literal is continue-first: no instant satisfies an ordering
    // (so `>=` keeps nothing), while `!=` keeps every row (none equals it).
    let bad_ge = format!(
        "D:\n open {p} (ts:datetime(\"yyMMddHHmmss\") id:int)\n |? ts >= \"not-a-date\"\n |> id\n;"
    );
    // An all-filtered flow emits no chunks for the output node.
    let bad = run_src(&bad_ge, 4096);
    let kept: usize = bad
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some("D"))
        .map_or(0, |o| o.chunks.iter().map(|c| c.len).sum());
    assert_eq!(
        kept, 0,
        "`>=` against an unparseable literal must keep no rows"
    );
    let bad_ne = format!(
        "D:\n open {p} (ts:datetime(\"yyMMddHHmmss\") id:int)\n |? ts != \"not-a-date\"\n |> id\n;"
    );
    assert_eq!(
        collect_i64(&run_src(&bad_ne, 4096), "D", "id"),
        vec![1, 2, 3, 4]
    );
}

#[test]
fn datetime_functions_and_daily_groupby() {
    // Field extractors, `trunc`, `format`, and a time-series daily group-by
    // (design 23) — all integer math, so chunk-size independent.
    let text = "ts,v\n\
                260601143000,10\n\
                260601090000,5\n\
                260602120000,7\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_dt_funcs",
        text.as_bytes(),
    ));
    let p = f.0.display();

    // Extractors over row 0 (2026-06-01 14:30:00).
    let ext = format!(
        "D:\n open {p} (ts:datetime(\"yyMMddHHmmss\") v:int)\n \
         |> (year(ts)) as y (month(ts)) as mo (day(ts)) as d (hour(ts)) as h (minute(ts)) as mi (second(ts)) as se\n;"
    );
    for cz in [1usize, 2, 4096] {
        let res = run_src(&ext, cz);
        assert_eq!(collect_i64(&res, "D", "y")[0], 2026, "year (cz={cz})");
        assert_eq!(collect_i64(&res, "D", "mo")[0], 6);
        assert_eq!(collect_i64(&res, "D", "d")[0], 1);
        assert_eq!(collect_i64(&res, "D", "h")[0], 14);
        assert_eq!(collect_i64(&res, "D", "mi")[0], 30);
        assert_eq!(collect_i64(&res, "D", "se")[0], 0);
    }

    // `trunc(ts,"day")` stays on the datetime lane; `format` renders it.
    let tr = format!(
        "D:\n open {p} (ts:datetime(\"yyMMddHHmmss\") v:int)\n \
         |> (format(trunc(ts, \"day\"), \"yyyy-MM-dd\")) as day v\n;"
    );
    assert_eq!(
        collect_strings(&run_src(&tr, 4096), "D", "day"),
        vec![
            "2026-06-01".to_string(),
            "2026-06-01".to_string(),
            "2026-06-02".to_string(),
        ],
    );

    // Daily aggregation: sum(v) grouped by the truncated day.
    let grp = format!(
        "D:\n open {p} (ts:datetime(\"yyMMddHHmmss\") v:int)\n \
         |> (format(trunc(ts, \"day\"), \"yyyy-MM-dd\")) as day v\n \
         |# day sum:v\n;"
    );
    for cz in [1usize, 2, 4096] {
        let res = run_src(&grp, cz);
        let days = collect_strings(&res, "D", "day");
        let sums = collect_i64(&res, "D", "sum_v");
        let mut pairs: Vec<(String, i64)> = days.into_iter().zip(sums).collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                ("2026-06-01".to_string(), 15), // 10 + 5
                ("2026-06-02".to_string(), 7),
            ],
            "daily sum changed at chunk_size {cz}"
        );
    }
}

#[test]
fn datetime_min_max_groupby_keeps_datetime_type() {
    // `min:ts` / `max:ts` over a datetime column must stay on the datetime lane
    // (exact ticks + DateTime type, ISO rendering), not collapse to f64 (#53).
    let text = "g,ts\n\
                a,260601143000\n\
                a,260601090000\n\
                b,260602120000\n\
                b,260602235959\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_dt_minmax",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let flow =
        format!("D:\n open {p} (g:str ts:datetime(\"yyMMddHHmmss\"))\n |# g min:ts max:ts\n;");
    for cz in [1usize, 2, 4096] {
        let res = run_src(&flow, cz);
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("D"))
            .unwrap();
        for col in ["min_ts", "max_ts"] {
            let ci = o.chunks[0].schema.index_of(col).unwrap();
            assert!(
                matches!(
                    o.chunks[0].schema.fields[ci].dtype,
                    rivus_core::DataType::DateTime { .. }
                ),
                "{col} must stay on the datetime lane (cz={cz})"
            );
        }
        // Pair (g, min_ts, max_ts) regardless of group order.
        let gs = collect_strings(&res, "D", "g");
        let mins = collect_strings(&res, "D", "min_ts");
        let maxs = collect_strings(&res, "D", "max_ts");
        let mut rows: Vec<(String, String, String)> = gs
            .into_iter()
            .zip(mins)
            .zip(maxs)
            .map(|((g, mn), mx)| (g, mn, mx))
            .collect();
        rows.sort();
        assert_eq!(
            rows,
            vec![
                (
                    "a".to_string(),
                    "2026-06-01T09:00:00".to_string(),
                    "2026-06-01T14:30:00".to_string()
                ),
                (
                    "b".to_string(),
                    "2026-06-02T12:00:00".to_string(),
                    "2026-06-02T23:59:59".to_string()
                ),
            ],
            "datetime min/max changed at chunk_size {cz}"
        );
    }
}

#[test]
fn duration_read_roundtrip_and_diff() {
    // A `:duration` column reads the human form exactly; `end - start` yields a
    // duration; both render back. Chunk-size independent.
    let text = "label,start,end\n\
                a,260601090000,260601103000\n\
                b,260601120000,260601121530\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_dur_diff",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let flow = format!(
        "D:\n open {p} (label:str start:datetime(\"yyMMddHHmmss\") end:datetime(\"yyMMddHHmmss\"))\n \
         |> label (end - start) as dur\n;"
    );
    for cz in [1usize, 2, 4096] {
        assert_eq!(
            collect_strings(&run_src(&flow, cz), "D", "dur"),
            vec!["01:30:00".to_string(), "00:15:30".to_string()],
            "ts2-ts1 duration changed at chunk_size {cz}"
        );
    }
    // A declared `:duration` column round-trips its human text.
    let dt = "d\n01:30:00\n00:15:30\n2d 00:00:01\n";
    let g = TempCsv(gendata::write_temp_bytes("stress_dur_read", dt.as_bytes()));
    let gp = g.0.display();
    let rd = format!("D:\n open {gp} (d:duration)\n |> d\n;");
    assert_eq!(
        collect_strings(&run_src(&rd, 4096), "D", "d"),
        vec![
            "01:30:00".to_string(),
            "00:15:30".to_string(),
            "2d 00:00:01".to_string()
        ],
    );
}
