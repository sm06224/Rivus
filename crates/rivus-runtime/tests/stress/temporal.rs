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
fn tz_abbreviations_normalize_and_ambiguous_surface() {
    // §29 s3 / #140 (a): unambiguous abbreviations (JST/UTC/…) normalize to the
    // same UTC instant on the reader path; an ambiguous one (CST — US Central /
    // China / Cuba) is a counted parse failure (never guessed). Chunk-size
    // independent.
    let text = "ts,id\n\
                2026-06-10 21:00:00 JST,1\n\
                2026-06-10 12:00:00 UTC,2\n\
                2026-06-10 12:00:00Z,3\n\
                2026-06-10 12:00:00 CST,4\n";
    let f = TempCsv(gendata::write_temp_bytes("stress_tz", text.as_bytes()));
    let p = f.0.display();
    let flow = format!("D:\n open {p} (ts:datetime id:int)\n |> ts id\n;");
    let want = vec![
        "2026-06-10T12:00:00".to_string(), // 21:00 JST = 12:00 UTC
        "2026-06-10T12:00:00".to_string(),
        "2026-06-10T12:00:00".to_string(),
        "".to_string(), // ambiguous CST → null + counted (never-silent)
    ];
    for cz in [1usize, 2, 4096] {
        let res = run_src(&flow, cz);
        assert!(
            !res.errors.is_empty(),
            "the ambiguous-CST cell must surface (cz={cz})"
        );
        assert_eq!(collect_strings(&res, "D", "ts"), want, "cz={cz}");
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
fn date_bin_chunk_size_independent_epoch_and_origin() {
    // `date_bin(ts, dur)` (epoch-aligned) and `date_bin(ts, dur, origin)`
    // (origin-aligned) are row-wise, exact-integer, and chunk-size independent
    // — the resample / gap-fill boundary primitive (#62). Closed-open bins.
    let text = "ts\n\
        2026-01-01T00:07:00\n\
        2026-01-01T00:14:59\n\
        2026-01-01T00:15:00\n\
        2026-01-01T00:22:30\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_date_bin",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let flow = format!(
        "D:\n open {p} (ts:datetime)\n \
         |> (date_bin(ts, \"15m\")) as b (date_bin(ts, \"15m\", \"2026-01-01T00:05:00\")) as o\n;"
    );
    // Epoch-aligned: boundaries at :00/:15/:30; the :15:00 instant opens a new
    // bin (closed-open). Origin :05: boundaries at :05/:20/:35.
    let want_b = vec![
        "2026-01-01T00:00:00",
        "2026-01-01T00:00:00",
        "2026-01-01T00:15:00",
        "2026-01-01T00:15:00",
    ];
    let want_o = vec![
        "2026-01-01T00:05:00",
        "2026-01-01T00:05:00",
        "2026-01-01T00:05:00",
        "2026-01-01T00:20:00",
    ];
    for cz in [1usize, 2, 3, 4096] {
        let res = run_src(&flow, cz);
        assert!(
            !res.errors.iter().any(rivus_core::ErrorEvent::is_fatal),
            "date_bin must never raise a fatal (cz={cz})"
        );
        assert_eq!(
            collect_strings(&res, "D", "b"),
            want_b,
            "epoch bin @cz={cz}"
        );
        assert_eq!(
            collect_strings(&res, "D", "o"),
            want_o,
            "origin bin @cz={cz}"
        );
        // Result stays on the datetime lane (exact ticks, not a text rendering).
        let out = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("D"))
            .unwrap();
        let ci = out.chunks[0].schema.index_of("b").unwrap();
        assert!(
            matches!(
                out.chunks[0].schema.fields[ci].dtype,
                rivus_core::DataType::DateTime { .. }
            ),
            "date_bin must yield the datetime lane @cz={cz}"
        );
    }
    // 2-arg date_bin is definitionally the existing `bucket` (epoch origin).
    let flow_bucket = format!("D:\n open {p} (ts:datetime)\n |> (bucket(ts, \"15m\")) as b\n;");
    for cz in [1usize, 4096] {
        let res = run_src(&flow_bucket, cz);
        assert_eq!(
            collect_strings(&res, "D", "b"),
            want_b,
            "date_bin(ts,dur) == bucket(ts,dur) @cz={cz}"
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

#[test]
fn test_bucket_tumbling_window_evaluation() {
    // Generate a set of timestamps and verify they are bucketed correctly
    // using bucket(ts, "15m") and bucket(ts, "1h").
    let text = "ts\n\
                2026-06-23T12:05:00\n\
                2026-06-23T12:14:59\n\
                2026-06-23T12:15:00\n\
                2026-06-23T12:29:59\n\
                2026-06-23T13:00:00\n\
                1969-12-31T23:59:00\n"; // Negative tick: before epoch (1970-01-01)

    let f = TempCsv(gendata::write_temp_bytes("stress_bucket", text.as_bytes()));
    let p = f.0.display();

    // Evaluate bucket(ts, "15m":duration) and bucket(ts, "1h")
    let flow = format!(
        "
        D:
          open {p}
          |> ts (bucket(ts, \"15m\":duration)) as b15m (bucket(ts, \"1h\")) as b1h
        ;
    "
    );

    let res = run_src(&flow, 1024);
    assert!(res.errors.is_empty());

    let b15m_vals = collect_strings(&res, "D", "b15m");
    assert_eq!(
        b15m_vals,
        vec![
            "2026-06-23T12:00:00".to_string(),
            "2026-06-23T12:00:00".to_string(),
            "2026-06-23T12:15:00".to_string(),
            "2026-06-23T12:15:00".to_string(),
            "2026-06-23T13:00:00".to_string(),
            "1969-12-31T23:45:00".to_string(), // correct floor bucketing
        ]
    );

    let b1h_vals = collect_strings(&res, "D", "b1h");
    assert_eq!(
        b1h_vals,
        vec![
            "2026-06-23T12:00:00".to_string(),
            "2026-06-23T12:00:00".to_string(),
            "2026-06-23T12:00:00".to_string(),
            "2026-06-23T12:00:00".to_string(),
            "2026-06-23T13:00:00".to_string(),
            "1969-12-31T23:00:00".to_string(),
        ]
    );
}

// --- sliding windows via `hops(ts, size, hop)` + explode + `|#` (§30.4 / #60,
// research prototype: sliding = derived grouping KEYS, plural). ---

#[test]
fn sliding_window_hops_explode_group_matches_oracle() {
    // 5 ticks, size=2m hop=1m: each row lands in exactly two windows (the
    // epoch-aligned starts covering it), and the per-window avg/count follow.
    let text = "ts,price\n\
                2024-06-03T09:00:10,100\n\
                2024-06-03T09:00:50,110\n\
                2024-06-03T09:01:30,120\n\
                2024-06-03T09:02:10,130\n\
                2024-06-03T09:04:30,200\n";
    let f = TempCsv(gendata::write_temp_bytes("slide_oracle", text.as_bytes()));
    let p = f.0.display();
    let flow = format!(
        "W:\n open {p} (ts:datetime price:int)\n |> (hops(ts, \"2m\", \"1m\")) as w price\n \
         explode w\n |# w avg:price\n sort w\n;"
    );
    for cz in [1usize, 2, 3, 4096] {
        let res = run_src(&flow, cz);
        assert_eq!(
            collect_strings(&res, "W", "w"),
            vec![
                "2024-06-03T08:59:00",
                "2024-06-03T09:00:00",
                "2024-06-03T09:01:00",
                "2024-06-03T09:02:00",
                "2024-06-03T09:03:00",
                "2024-06-03T09:04:00",
            ],
            "window starts @cz={cz}"
        );
        assert_eq!(
            collect_i64(&res, "W", "count"),
            vec![2, 3, 2, 1, 1, 1],
            "per-window membership @cz={cz}"
        );
        assert_eq!(
            collect_strings(&res, "W", "avg_price"),
            vec!["105", "110", "125", "130", "200", "200"],
            "per-window avg @cz={cz}"
        );
    }
}

#[test]
fn hops_gap_and_degenerate_cases() {
    // hop > size leaves gaps: a tick between windows yields an EMPTY list →
    // explode drops the row (zero windows is a real answer, not an error).
    // hop == size degenerates to tumbling (== bucket).
    let text = "ts,v\n2024-01-01T00:00:30,1\n2024-01-01T00:03:30,2\n";
    let f = TempCsv(gendata::write_temp_bytes("slide_gap", text.as_bytes()));
    let p = f.0.display();
    // size=1m hop=3m: only ticks in the first minute of each 3m hop belong.
    let flow = format!(
        "G:\n open {p} (ts:datetime v:int)\n |> (hops(ts, \"1m\", \"3m\")) as w v\n \
         explode w\n |# w count:v\n sort w\n;"
    );
    let res = run_src(&flow, 4096);
    assert_eq!(
        collect_strings(&res, "G", "w"),
        vec!["2024-01-01T00:00:00", "2024-01-01T00:03:00"],
        "hop>size: each tick in ≤1 window, gap ticks in none"
    );
    // hop == size ≡ bucket: identical keys.
    let tumbling = format!(
        "T:\n open {p} (ts:datetime v:int)\n |> (hops(ts, \"3m\", \"3m\")) as w v\n \
         explode w\n |# w count:v\n sort w\n;"
    );
    let bucketed = format!(
        "T:\n open {p} (ts:datetime v:int)\n |> (bucket(ts, \"3m\")) as w v\n \
         |# w count:v\n sort w\n;"
    );
    assert_eq!(
        collect_strings(&run_src(&tumbling, 4096), "T", "w"),
        collect_strings(&run_src(&bucketed, 4096), "T", "w"),
        "hops(size==hop) must degenerate to bucket"
    );
}

#[test]
fn sliding_window_serial_parallel_chunk_size_byte_identical() {
    // Exact aggregates (count/max) over sliding windows are byte-identical
    // across the serial and parallel paths and chunk sizes (§30.6: windows add
    // no new parallel hazard — the window key is just another group key).
    let rows = 200_000usize;
    let mut text = String::from("ts,v\n");
    for i in 0..rows {
        let (h, m, s) = ((i / 3600) % 24, (i / 60) % 60, i % 60);
        text.push_str(&format!("2024-06-03T{h:02}:{m:02}:{s:02},{}\n", i % 1000));
    }
    let f = TempCsv(gendata::write_temp_bytes("slide_bi", text.as_bytes()));
    let p = f.0.display();
    let run_one = |cz: usize, pref: rivus_runtime::MemoryPref| {
        let flow = format!(
            "B:\n open {p} (ts:datetime v:int)\n |> (hops(ts, \"10m\", \"5m\")) as w v\n \
             explode w\n |# w count:v max:v\n sort w\n;"
        );
        let g = rivus_parser::parse(&flow).expect("parse");
        let res = run(
            &g,
            RunOptions {
                chunk_size: cz,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run");
        let mut lines = Vec::new();
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("B"))
            .expect("output");
        for c in &o.chunks {
            for r in 0..c.len {
                let cells: Vec<String> = (0..c.columns.len())
                    .map(|ci| c.value(r, ci).to_string())
                    .collect();
                lines.push(cells.join("\u{1f}"));
            }
        }
        lines
    };
    let oracle = run_one(1024, rivus_runtime::MemoryPref::Low);
    assert!(!oracle.is_empty());
    for cz in [777usize, 4096] {
        assert_eq!(
            run_one(cz, rivus_runtime::MemoryPref::Fast),
            oracle,
            "sliding window must be serial==parallel==chunk-size @cz={cz}"
        );
    }
}

// --- session windows via `sessionize ts gap "30m" by user` (§36.5 / #60,
// research prototype: session start as the derived key). ---

#[test]
fn sessionize_assigns_session_starts_per_group() {
    // Two interleaved users; gap=30m. aki: 09:00+09:05 share a session,
    // 09:50 and 10:31 each start new ones. ben: 09:02+09:03 share, 10:30 new.
    // Interleaving exercises per-group state (ts regressions ACROSS groups are
    // fine — only within-group order matters).
    let text = "ts,user\n\
                2024-06-03T09:00:00,aki\n\
                2024-06-03T09:05:00,aki\n\
                2024-06-03T09:50:00,aki\n\
                2024-06-03T09:02:00,ben\n\
                2024-06-03T09:03:00,ben\n\
                2024-06-03T10:30:00,ben\n\
                2024-06-03T10:31:00,aki\n";
    let f = TempCsv(gendata::write_temp_bytes("sess_oracle", text.as_bytes()));
    let p = f.0.display();
    let flow = format!(
        "S:\n open {p} (ts:datetime user:str)\n sessionize ts gap \"30m\" by user\n \
         |# user session count:ts\n sort user session\n;"
    );
    // Chunk-size sweep pins the cross-chunk state carry (cz=1 = every row its
    // own chunk) — the session assignment must not depend on chunking.
    for cz in [1usize, 2, 3, 4096] {
        let res = run_src(&flow, cz);
        assert_eq!(
            collect_strings(&res, "S", "session"),
            vec![
                "2024-06-03T09:00:00",
                "2024-06-03T09:50:00",
                "2024-06-03T10:31:00",
                "2024-06-03T09:02:00",
                "2024-06-03T10:30:00",
            ],
            "session starts @cz={cz}"
        );
        assert_eq!(
            collect_i64(&res, "S", "count"),
            vec![2, 1, 1, 2, 1],
            "rows per session @cz={cz}"
        );
        assert!(
            !res.errors
                .iter()
                .any(|e| e.message.contains("out of time order")),
            "ascending per-group input must not surface a regression @cz={cz}"
        );
    }
}

#[test]
fn sessionize_gap_boundary_is_closed() {
    // A gap of exactly `gap` continues the session; strictly greater starts a
    // new one (closed threshold, pinned).
    let text = "ts,v\n\
                2024-01-01T00:00:00,1\n\
                2024-01-01T00:30:00,2\n\
                2024-01-01T01:00:01,3\n";
    let f = TempCsv(gendata::write_temp_bytes("sess_edge", text.as_bytes()));
    let p = f.0.display();
    let flow = format!(
        "S:\n open {p} (ts:datetime v:int)\n sessionize ts gap \"30m\"\n \
         |# session count:v\n sort session\n;"
    );
    let res = run_src(&flow, 4096);
    assert_eq!(
        collect_strings(&res, "S", "session"),
        vec!["2024-01-01T00:00:00", "2024-01-01T01:00:01"],
        "== gap continues; > gap (by 1s) starts a new session"
    );
    assert_eq!(collect_i64(&res, "S", "count"), vec![2, 1]);
}

#[test]
fn sessionize_surfaces_time_regressions_and_null_ts() {
    // Out-of-order rows (within the single implicit group) are counted and
    // surfaced once (never-silent); a null ts yields a null session cell.
    let text = "ts,v\n\
                2024-01-01T10:00:00,1\n\
                2024-01-01T09:00:00,2\n\
                ,3\n\
                2024-01-01T11:00:00,4\n";
    let f = TempCsv(gendata::write_temp_bytes("sess_reg", text.as_bytes()));
    let p = f.0.display();
    let flow =
        format!("S:\n open {p} (ts:datetime v:int)\n sessionize ts gap \"10m\"\n |> v session\n;");
    let res = run_src(&flow, 4096);
    assert_eq!(
        collect_strings(&res, "S", "session"),
        vec![
            "2024-01-01T10:00:00",
            "2024-01-01T10:00:00", // regression: negative gap ≤ gap → same session
            "",                    // null ts → null session
            "2024-01-01T11:00:00",
        ],
    );
    let reg = res
        .errors
        .iter()
        .filter(|e| e.message.contains("out of time order"))
        .count();
    assert_eq!(
        reg, 1,
        "exactly one aggregate regression event: {:?}",
        res.errors
    );
    assert!(
        res.errors.iter().any(|e| e.message.contains("1 row(s)")),
        "the event carries the count: {:?}",
        res.errors
    );
}

// --- time-series shift/difference via `shift col lag|diff|pct_change …` (#65,
// Track C slice 1: backward-only, order-dependent, per-group serial). ---

#[test]
fn shift_lag_diff_pct_change_by_group_matches_oracle() {
    // Two interleaved groups; the shift is per-group in source order and must
    // not depend on chunking (cross-chunk state carry).
    let text = "sym,price\n\
                A,100\n\
                A,110\n\
                B,50\n\
                A,105\n\
                B,55\n";
    let f = TempCsv(gendata::write_temp_bytes("shift_oracle", text.as_bytes()));
    let p = f.0.display();
    let flow = format!(
        "T:\n open {p} (sym:str price:int)\n \
         shift price lag 1 by sym as prev\n \
         shift price diff by sym as delta\n \
         shift price pct_change by sym as ret\n;"
    );
    for cz in [1usize, 2, 3, 4096] {
        let res = run_src(&flow, cz);
        // lag: first row of each group is null.
        assert_eq!(
            collect_strings(&res, "T", "prev"),
            vec!["", "100", "", "110", "50"],
            "lag by group @cz={cz}"
        );
        assert_eq!(
            collect_strings(&res, "T", "delta"),
            vec!["", "10", "", "-5", "5"],
            "diff by group @cz={cz}"
        );
        // pct_change: (110-100)/100 = 0.1, (105-110)/110, (55-50)/50 = 0.1.
        let ret = collect_strings(&res, "T", "ret");
        assert_eq!(ret[0], "", "@cz={cz}");
        assert_eq!(ret[1], "0.1", "@cz={cz}");
        assert!(ret[3].starts_with("-0.0454"), "{} @cz={cz}", ret[3]);
        assert_eq!(ret[4], "0.1", "@cz={cz}");
    }
}

#[test]
fn shift_datetime_diff_is_exact_duration() {
    // A datetime column's `diff` yields an exact `Duration` (i64 ticks, #57) —
    // never routed through f64. Sub-second precision must survive.
    let text = "ts\n\
                2024-06-03T09:00:00.000\n\
                2024-06-03T09:00:01.500\n\
                2024-06-03T09:00:03.750\n";
    let f = TempCsv(gendata::write_temp_bytes("shift_dtdiff", text.as_bytes()));
    let p = f.0.display();
    // Declare millisecond precision (the `nnn` fraction run → ms unit) so the
    // sub-second gap is exact and rides the Duration's millisecond ticks.
    let flow = format!(
        "D:\n open {p} (ts:datetime(\"yyyy-MM-ddTHH:mm:ss.nnn\"))\n          shift ts diff as gap\n |> gap\n;"
    );
    let res = run_src(&flow, 4096);
    assert_eq!(
        collect_strings(&res, "D", "gap"),
        vec!["", "00:00:01.500", "00:00:02.250"],
        "datetime diff → exact Duration with sub-second ticks"
    );
}

#[test]
fn shift_lag_n_and_endpoints() {
    // lag N > 1: the first N rows per group are null; the rest reference N back.
    let text = "v\n1\n2\n3\n4\n5\n";
    let f = TempCsv(gendata::write_temp_bytes("shift_lagn", text.as_bytes()));
    let p = f.0.display();
    let flow = format!("N:\n open {p} (v:int)\n shift v lag 2 as p2\n;");
    for cz in [1usize, 2, 5] {
        let res = run_src(&flow, cz);
        assert_eq!(
            collect_strings(&res, "N", "p2"),
            vec!["", "", "1", "2", "3"],
            "lag 2 endpoints @cz={cz}"
        );
    }
}
