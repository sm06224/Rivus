//! Byte-identity: serial == parallel == chunk-size, across lanes (the #41 guarantee).
//!
//! Moved verbatim from the former monolithic `stress.rs` (design 26 §26.8.1,
//! mechanical move-only split; logic unchanged).

use super::*;

#[test]
fn parallel_array_agg_list_byte_identical() {
    // §32 / #172 GO condition: `array_agg` collects a group's values into a List
    // in SOURCE order, and the parallel partition→merge concatenates partitions
    // in source order — so the list (element order included) is byte-identical
    // serial == parallel == chunk-size. Same discipline as `explode` (order-based,
    // not the f64 #41 trap). `v = row index` makes the element order checkable.
    // >1 MiB + MemoryPref::Fast forces the bounded parallel group path.
    let rows = 150_000usize;
    let groups = ["a", "b", "c", "d"];
    let mut text = String::from("g,v\n");
    for i in 0..rows {
        text.push_str(&format!("{},{}\n", groups[i % groups.len()], i));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_arragg_par",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let flow = format!("G:\n open {p}\n |# g array_agg:v\n;");
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
        // (group, full list text) — the list text encodes the exact element order.
        let mut lines: Vec<String> = o
            .chunks
            .iter()
            .flat_map(|c| {
                let (gi, ai) = (
                    c.schema.index_of("g").unwrap(),
                    c.schema.index_of("array_agg_v").unwrap(),
                );
                (0..c.len).map(move |r| format!("{}\t{}", c.value(r, gi), c.value(r, ai)))
            })
            .collect();
        lines.sort();
        (lines, !res.workers.is_empty())
    };
    let (serial, serial_par) = collect(rivus_runtime::MemoryPref::Low);
    let (parallel, par) = collect(rivus_runtime::MemoryPref::Fast);
    assert!(!serial_par, "low must be serial");
    assert!(par, "fast should engage the bounded parallel group path");
    assert_eq!(
        parallel, serial,
        "array_agg list (element order included) must be byte-identical serial vs parallel"
    );
    // Sanity: the element order is the source order (group a = 0,4,8,…).
    let a = serial.iter().find(|l| l.starts_with("a\t")).unwrap();
    assert!(
        a.starts_with("a\t[0, 4, 8, "),
        "source order in the list: {a}"
    );
}

#[test]
fn parallel_decimal_chunk_size_independent() {
    // The streaming-parallel byte-range reader builds decimal columns via the
    // same ColBuilder as the serial path. This gates that the parallel path is
    // byte-identical to serial AND chunk-size independent for a decimal(2)
    // column. The byte-range path only engages with a file sink, so each run
    // `save`s and we compare the output files byte-for-byte. Force it with a
    // >1 MiB file + MemoryPref::Fast (no env vars → no cross-test races) and
    // assert workers actually engaged.
    let rows = 120_000usize; // ~1.5 MiB of "id,price\n" → crosses the Fast 1 MiB floor
    let mut text = String::from("id,price\n");
    for i in 0..rows {
        // Varied fractional widths to exercise pad / exact / round-half-even.
        let cell = match i % 4 {
            0 => format!("{}.1", i % 1000),   // 1 digit → pad to .x0
            1 => format!("{}.345", i % 1000), // 3 digits → round half-even
            2 => format!("{}", i % 1000),     // integer → .00
            _ => format!("{}.355", i % 1000), // 3 digits → round half-even (up)
        };
        text.push_str(&format!("{i},{cell}\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_par_decimal",
        text.as_bytes(),
    ));
    let p = f.0.display();

    let run_to_file = |cs: usize, pref: rivus_runtime::MemoryPref, out: &std::path::Path| {
        let src = format!(
            "D:\n open {p} (id price:decimal(2))\n |? id >= 1000\n |> id price\n save {}\n;",
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
    let ser_out = TempCsv(gendata::write_temp_bytes("par_decimal_serial", b""));
    run_to_file(1024, rivus_runtime::MemoryPref::Low, &ser_out.0);
    let oracle = std::fs::read_to_string(&ser_out.0).expect("read serial out");
    assert!(oracle.lines().count() > 1000, "oracle unexpectedly small");

    for cs in [1usize, 1000, rows] {
        let par_out = TempCsv(gendata::write_temp_bytes("par_decimal_parallel", b""));
        let res = run_to_file(cs, rivus_runtime::MemoryPref::Fast, &par_out.0);
        assert!(
            !res.workers.is_empty(),
            "expected the byte-range parallel reader to engage @cs={cs}"
        );
        let got = std::fs::read_to_string(&par_out.0).expect("read parallel out");
        // Parts are concatenated in source order, so parallel output is
        // byte-identical to serial (not merely set-equal).
        assert_eq!(got, oracle, "parallel decimal != serial @cs={cs}");
    }
}

#[test]
fn binary_char_field_is_parallel_chunk_size_byte_identical() {
    // `char[N]` decode (§29.4, #139) on the **parallel** binary reader: records
    // are fixed-width, so byte ranges split on record boundaries and the Str
    // lane must come out byte-identical to serial across chunk sizes. Record =
    // i32 id + char[12] name → 16 B; 80_000 records = 1.22 MiB > the Fast floor.
    let rows = 80_000usize;
    let mut bytes = Vec::with_capacity(rows * 16);
    for i in 0..rows {
        bytes.extend_from_slice(&(i as i32).to_le_bytes());
        // 12-byte name with NUL padding kept as value (ratification #137 ③).
        let name = format!("n{:07}\0\0\0\0", i % 1_000_000);
        assert_eq!(name.len(), 12);
        bytes.extend_from_slice(name.as_bytes());
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_par_binchar", &bytes));
    let p = f.0.display();

    let run_to_file = |cs: usize, pref: rivus_runtime::MemoryPref, out: &std::path::Path| {
        let src = format!(
            "B:\n readbin {p} (id:i32 name:char[12])\n |? id >= 1000\n |> id name\n save {}\n;",
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

    let ser_out = TempCsv(gendata::write_temp_bytes("par_binchar_serial", b""));
    run_to_file(1024, rivus_runtime::MemoryPref::Low, &ser_out.0);
    let oracle = std::fs::read_to_string(&ser_out.0).expect("read serial out");
    assert!(oracle.lines().count() > 1000, "oracle unexpectedly small");

    for cs in [1usize, 1000, rows] {
        let par_out = TempCsv(gendata::write_temp_bytes("par_binchar_parallel", b""));
        let res = run_to_file(cs, rivus_runtime::MemoryPref::Fast, &par_out.0);
        assert!(
            !res.workers.is_empty(),
            "expected the record-range parallel binary reader to engage @cs={cs}"
        );
        let got = std::fs::read_to_string(&par_out.0).expect("read parallel out");
        assert_eq!(got, oracle, "binary char[N] parallel != serial @cs={cs}");
    }
}

#[test]
fn union_view_subviews_are_parallel_chunk_size_byte_identical() {
    // A union sub-view (§29.3, s2) is a pure row-wise char slice, so its output
    // must be byte-identical across chunk sizes and the serial/parallel reader
    // split (§29.7). The byte-range parallel path engages only with a file sink,
    // so each run `save`s and the files are compared byte-for-byte. A >1 MiB file
    // + MemoryPref::Fast forces the parallel reader; assert it engaged.
    let rows = 120_000usize; // ~1.56 MiB of 12-char ids → crosses the Fast 1 MiB floor
    let mut text = String::from("id\n");
    for i in 0..rows {
        text.push_str(&format!("{i:012}\n")); // fixed 12-char id
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_union_view",
        text.as_bytes(),
    ));
    let p = f.0.display();

    let run_to_file = |cs: usize, pref: rivus_runtime::MemoryPref, out: &std::path::Path| {
        let src = format!(
            "U:\n open {p} (id:str)\n \
             |> id :string(12) :{{ cls@0..3 dept@3..7 seq@7..12 }}\n \
             |> (id.cls) as cls (id.dept) as dept (id.seq) as seq\n \
             save {}\n;",
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
    let ser_out = TempCsv(gendata::write_temp_bytes("union_view_serial", b""));
    run_to_file(1024, rivus_runtime::MemoryPref::Low, &ser_out.0);
    let oracle = std::fs::read_to_string(&ser_out.0).expect("read serial out");
    assert!(oracle.lines().count() > 1000, "oracle unexpectedly small");
    // Spot-check the char slice on the first data row (id `000000000000`).
    assert_eq!(
        oracle.lines().nth(1),
        Some("000,0000,00000"),
        "sub-view slice wrong on first row"
    );

    for cs in [1usize, 1000, rows] {
        let par_out = TempCsv(gendata::write_temp_bytes("union_view_parallel", b""));
        let res = run_to_file(cs, rivus_runtime::MemoryPref::Fast, &par_out.0);
        assert!(
            !res.workers.is_empty(),
            "expected the byte-range parallel reader to engage @cs={cs}"
        );
        let got = std::fs::read_to_string(&par_out.0).expect("read parallel out");
        assert_eq!(got, oracle, "union view parallel != serial @cs={cs}");
    }
}

#[test]
fn decimal_sum_is_order_independent() {
    // The same multiset of decimals in two different row orders must give the
    // identical exact sum (f64 would drift). This is the property #41 relies on
    // to merge partial decimal sums across parallel workers byte-identically.
    let vals = ["0.10", "0.20", "0.30", "1.15", "3.3333", "99999.99", "0.01"];
    let sum_in = |order: &[usize]| -> String {
        let mut text = String::from("g,amount\n");
        for &i in order {
            text.push_str(&format!("1,{}\n", vals[i]));
        }
        let f = TempCsv(gendata::write_temp_bytes(
            "stress_dec_order",
            text.as_bytes(),
        ));
        let p = f.0.display();
        let res = run_src(
            &format!("G:\n open {p} (g amount:decimal(4))\n |# g sum:amount\n;"),
            512,
        );
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("G"))
            .unwrap();
        let c = &o.chunks[0];
        let si = c.schema.index_of("sum_amount").unwrap();
        c.value(0, si).to_string()
    };
    let a = sum_in(&[0, 1, 2, 3, 4, 5, 6]);
    let b = sum_in(&[6, 5, 4, 3, 2, 1, 0]);
    let c = sum_in(&[3, 0, 6, 2, 5, 1, 4]);
    assert_eq!(a, b, "decimal sum depends on order");
    assert_eq!(a, c, "decimal sum depends on order");
}

#[test]
fn parallel_group_by_matches_serial() {
    // Parallel group-by (#41) must be byte-identical to serial for the safe
    // aggregate set (decimal sum/avg — exact i128; min/max/count/count_distinct/
    // first/last/percentile — associative or buffered). Force parallel with a
    // >1 MiB file + MemoryPref::Fast (no env → no cross-test races); the serial
    // oracle uses MemoryPref::Low.
    let rows = 150_000usize;
    let mut text = String::from("country,amount\n");
    let countries = ["JP", "US", "DE", "FR", "GB"];
    for i in 0..rows {
        let c = countries[i % countries.len()];
        let cents = (i as i128 * 7 % 100_000) + 1;
        text.push_str(&format!("{c},{}.{:02}\n", cents / 100, cents % 100));
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_pgroup", text.as_bytes()));
    let p = f.0.display();
    let flow = format!(
        "G:\n open {p} (country amount:decimal(2))\n \
         |# country sum:amount avg:amount min:amount max:amount \
         count_distinct:amount first:amount last:amount p50:amount\n;"
    );

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
    let (parallel, par_engaged) = collect(rivus_runtime::MemoryPref::Fast);
    assert!(par_engaged, "parallel group-by did not engage");
    assert_eq!(parallel, serial, "parallel group-by != serial");
    // Sanity: the exact decimal sum is present (and exact, not f64-drifted).
    assert!(
        serial.iter().any(|l| l.contains(".00,")),
        "expected exact decimal sums"
    );
}

#[test]
fn f64_sum_group_stays_serial_but_correct() {
    // An f64-column sum/avg is NOT parallel-safe (non-associative), so the engine
    // must keep it serial even under MemoryPref::Fast — and still be correct and
    // chunk-size independent. (min/max/count on f64 ARE safe and may parallelize;
    // here we check the f64 sum path doesn't corrupt results.)
    let rows = 120_000usize;
    let mut text = String::from("g,v\n");
    for i in 0..rows {
        text.push_str(&format!("{},{}.{}\n", i % 4, i % 1000, i % 10));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_f64group",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let flow = format!("G:\n open {p}\n |# g sum:v\n;"); // v inferred f64
    let collect = |pref: rivus_runtime::MemoryPref| -> Vec<String> {
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
                lines.push(format!("{},{}", c.value(r, 0), c.value(r, 1)));
            }
        }
        lines.sort();
        lines
    };
    // Fast and Low must agree (f64 sum is kept serial under both → deterministic).
    assert_eq!(
        collect(rivus_runtime::MemoryPref::Fast),
        collect(rivus_runtime::MemoryPref::Low)
    );
}

#[test]
fn unbounded_group_parallelizes_non_csv_byte_identical() {
    // #50: a non-splittable source (JSONL) can't use the bounded streaming group
    // path, so it stays serial under auto/fast. With the opt-in `Unbounded` tier
    // the engine materializes + partitions to parallelize it — still byte-identical
    // to serial; only memory differs.
    let rows = 60_000usize;
    let countries = ["JP", "US", "DE", "FR"];
    let mut text = String::new();
    for i in 0..rows {
        text.push_str(&format!(
            "{{\"country\":\"{}\",\"amount\":{}}}\n",
            countries[i % countries.len()],
            i % 1000
        ));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_unbounded_jsonl",
        text.as_bytes(),
    ));
    // Rename so the reader picks JSONL from the extension.
    let jpath = f.0.with_extension("jsonl");
    std::fs::rename(&f.0, &jpath).unwrap();
    let _cleanup = TempCsv(jpath.clone());
    let p = jpath.display();
    let flow = format!("G:\n open {p}\n |# country min:amount max:amount count_distinct:amount\n;");
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
    let (serial, serial_par) = collect(rivus_runtime::MemoryPref::Low);
    let (auto, auto_par) = collect(rivus_runtime::MemoryPref::Auto);
    let (unbounded, unbounded_par) = collect(rivus_runtime::MemoryPref::Unbounded);
    // auto/low never materialize a non-splittable group → serial (bounded).
    assert!(!serial_par, "low must be serial");
    assert!(
        !auto_par,
        "auto must NOT silently go unbounded on a JSONL group"
    );
    // unbounded parallelizes it, byte-identical.
    assert!(
        unbounded_par,
        "unbounded should parallelize the JSONL group"
    );
    assert_eq!(unbounded, serial, "unbounded group != serial");
    assert_eq!(auto, serial, "auto group != serial");
}

#[test]
fn parallel_jsonl_group_bounded_byte_identical() {
    // #49: JSONL is now a splittable source — its group-by parallelizes in the
    // bounded byte-range path (no whole-file materialize), byte-identical to
    // serial. Forced with a >1 MiB file + MemoryPref::Fast (no env races).
    let rows = 120_000usize;
    let countries = ["JP", "US", "DE", "FR"];
    let mut text = String::new();
    for i in 0..rows {
        text.push_str(&format!(
            "{{\"country\":\"{}\",\"amount\":{}}}\n",
            countries[i % countries.len()],
            i % 1000
        ));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_jsonl_group",
        text.as_bytes(),
    ));
    let jpath = f.0.with_extension("jsonl");
    std::fs::rename(&f.0, &jpath).unwrap();
    let _cleanup = TempCsv(jpath.clone());
    let p = jpath.display();
    let flow = format!("G:\n open {p}\n |# country min:amount max:amount count_distinct:amount\n;");
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
    let (serial, serial_par) = collect(rivus_runtime::MemoryPref::Low);
    let (parallel, par) = collect(rivus_runtime::MemoryPref::Fast);
    assert!(!serial_par, "low must be serial");
    assert!(
        par,
        "fast should engage the bounded JSONL byte-range group path"
    );
    assert_eq!(parallel, serial, "parallel JSONL group != serial");
}

#[test]
fn parallel_jsonl_stateless_byte_identical() {
    // #49: a JSONL filter+project+save flow parallelizes in the bounded
    // streaming-parallel path (part files → ordered concat), byte-identical to
    // serial. Compares the saved output files.
    let rows = 120_000usize;
    let mut text = String::new();
    for i in 0..rows {
        text.push_str(&format!("{{\"id\":{},\"amount\":{}}}\n", i, i % 1000));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_jsonl_sl",
        text.as_bytes(),
    ));
    let jpath = f.0.with_extension("jsonl");
    std::fs::rename(&f.0, &jpath).unwrap();
    let _cleanup = TempCsv(jpath.clone());
    let p = jpath.display();
    let run_to = |pref: rivus_runtime::MemoryPref, out: &std::path::Path| -> bool {
        let src = format!(
            "F:\n open {p}\n |? amount >= 500\n |> id amount\n save {}\n;",
            out.display()
        );
        let g = rivus_parser::parse(&src).expect("parse");
        let res = run(
            &g,
            RunOptions {
                chunk_size: 4096,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run");
        !res.workers.is_empty()
    };
    let ser = TempCsv(gendata::write_temp_bytes("jsonl_sl_serial", b""));
    let par = TempCsv(gendata::write_temp_bytes("jsonl_sl_par", b""));
    assert!(
        !run_to(rivus_runtime::MemoryPref::Low, &ser.0),
        "low must be serial"
    );
    assert!(
        run_to(rivus_runtime::MemoryPref::Fast, &par.0),
        "fast should engage the JSONL streaming-parallel path"
    );
    let a = std::fs::read_to_string(&ser.0).unwrap();
    let b = std::fs::read_to_string(&par.0).unwrap();
    assert_eq!(a, b, "parallel JSONL stateless != serial");
    assert!(a.lines().count() > 1000);
}

#[test]
fn parallel_jsonl_nested_byte_identical() {
    // §32 s3b: nested Struct/List columns now have a live generation path (the
    // JSON reader). This pins the #41 guarantee for that NEW path — serial ==
    // parallel == chunk-size — end to end: the byte-range parallel reader builds
    // each range's nested columns against the SAME globally-inferred shape, a
    // `where` gathers nested rows (exercises ColumnData::{Struct,List}::gather),
    // and the CSV sink renders the nested cells. Output files are compared
    // byte-for-byte. >1 MiB + MemoryPref::Fast forces the parallel reader.
    let rows = 120_000usize;
    let mut text = String::new();
    let ys = ["aa", "bb", "cc", "dd"];
    for i in 0..rows {
        // A struct column `meta` and a list column `tags` per row.
        text.push_str(&format!(
            "{{\"id\":{i},\"meta\":{{\"x\":{},\"y\":\"{}\"}},\"tags\":[{},{}]}}\n",
            i % 1000,
            ys[i % ys.len()],
            i % 7,
            (i % 7) + 1,
        ));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_jsonl_nested",
        text.as_bytes(),
    ));
    let jpath = f.0.with_extension("jsonl");
    std::fs::rename(&f.0, &jpath).unwrap();
    let _cleanup = TempCsv(jpath.clone());
    let p = jpath.display();

    let run_to = |pref: rivus_runtime::MemoryPref, cs: usize, out: &std::path::Path| -> bool {
        // Filter (gathers nested rows) + project the nested columns + save.
        let src = format!(
            "F:\n open {p}\n |? id >= 1000\n |> id meta tags\n save {}\n;",
            out.display()
        );
        let g = rivus_parser::parse(&src).expect("parse");
        let res = run(
            &g,
            RunOptions {
                chunk_size: cs,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run");
        !res.workers.is_empty()
    };

    // Serial oracle (single-threaded reader).
    let ser = TempCsv(gendata::write_temp_bytes("jsonl_nested_serial", b""));
    assert!(
        !run_to(rivus_runtime::MemoryPref::Low, 4096, &ser.0),
        "low must be serial"
    );
    let oracle = std::fs::read_to_string(&ser.0).expect("read serial out");
    assert!(oracle.lines().count() > 1000, "oracle unexpectedly small");
    // The nested cells render via the sink (struct → `{x: .., y: ..}`, list →
    // `[.., ..]`), quoted as CSV text.
    assert!(
        oracle.lines().nth(1).is_some_and(|l| l.contains("{x:")),
        "expected a rendered struct cell, got {:?}",
        oracle.lines().nth(1)
    );

    // Parallel must be byte-identical to serial (parts concatenated in order).
    let par = TempCsv(gendata::write_temp_bytes("jsonl_nested_par", b""));
    assert!(
        run_to(rivus_runtime::MemoryPref::Fast, 4096, &par.0),
        "fast should engage the JSONL streaming-parallel path"
    );
    let got = std::fs::read_to_string(&par.0).expect("read parallel out");
    assert_eq!(got, oracle, "parallel nested JSONL != serial");

    // Chunk-size independence (serial): the result must not depend on chunk_size.
    for cs in [1usize, 1000, rows] {
        let cz = TempCsv(gendata::write_temp_bytes("jsonl_nested_cz", b""));
        run_to(rivus_runtime::MemoryPref::Low, cs, &cz.0);
        let got = std::fs::read_to_string(&cz.0).expect("read cz out");
        assert_eq!(got, oracle, "nested JSONL changed @cs={cs}");
    }
}

#[test]
fn parallel_jsonl_explode_byte_identical() {
    // §32 s4c: `explode` is a deterministic, stateless, per-chunk row multiplier
    // (expansion = the list's physical order), so a flow that explodes a list and
    // saves must be byte-identical serial == parallel == chunk-size. The
    // byte-range parallel path explodes each range to a part file; the parts
    // concatenate in source order. >1 MiB + Fast forces the parallel reader.
    let rows = 120_000usize;
    let mut text = String::new();
    for i in 0..rows {
        // Mostly 2-element lists; every 13th row an empty list (→ zero output
        // rows) to exercise the empty-list path across partition boundaries.
        let tags = if i % 13 == 0 {
            "[]".to_string()
        } else {
            format!("[{},{}]", i % 100, (i % 100) + 1)
        };
        text.push_str(&format!("{{\"id\":{i},\"tags\":{tags}}}\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_jsonl_explode",
        text.as_bytes(),
    ));
    let jpath = f.0.with_extension("jsonl");
    std::fs::rename(&f.0, &jpath).unwrap();
    let _cleanup = TempCsv(jpath.clone());
    let p = jpath.display();

    let run_to = |pref: rivus_runtime::MemoryPref, cs: usize, out: &std::path::Path| -> bool {
        let src = format!(
            "X:\n open {p}\n explode tags\n |> id tags\n save {}\n;",
            out.display()
        );
        let g = rivus_parser::parse(&src).expect("parse");
        let res = run(
            &g,
            RunOptions {
                chunk_size: cs,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run");
        !res.workers.is_empty()
    };

    let ser = TempCsv(gendata::write_temp_bytes("jsonl_explode_serial", b""));
    assert!(
        !run_to(rivus_runtime::MemoryPref::Low, 4096, &ser.0),
        "low must be serial"
    );
    let oracle = std::fs::read_to_string(&ser.0).expect("read serial out");
    // Each non-empty row contributes 2 rows; ~1/13 contribute 0 → more than rows.
    assert!(oracle.lines().count() > rows, "explode should add rows");

    let par = TempCsv(gendata::write_temp_bytes("jsonl_explode_par", b""));
    assert!(
        run_to(rivus_runtime::MemoryPref::Fast, 4096, &par.0),
        "fast should engage the JSONL streaming-parallel path"
    );
    let got = std::fs::read_to_string(&par.0).expect("read parallel out");
    assert_eq!(got, oracle, "parallel explode != serial");

    for cs in [1usize, 1000, rows] {
        let cz = TempCsv(gendata::write_temp_bytes("jsonl_explode_cz", b""));
        run_to(rivus_runtime::MemoryPref::Low, cs, &cz.0);
        let got = std::fs::read_to_string(&cz.0).expect("read cz out");
        assert_eq!(got, oracle, "explode changed @cs={cs}");
    }
}

#[test]
fn parallel_jsonl_nested_key_group_byte_identical() {
    // §32 s4b: a group-by **key** can be a nested path. The byte-range parallel
    // group path resolves the nested key column per range against the same
    // globally-inferred shape, so a group-by on `user.country` is byte-identical
    // serial == parallel, with parallel-safe aggregates. >1 MiB + Fast forces it.
    let rows = 120_000usize;
    let countries = ["JP", "US", "DE", "FR"];
    let mut text = String::new();
    for i in 0..rows {
        text.push_str(&format!(
            "{{\"user\":{{\"country\":\"{}\"}},\"amount\":{}}}\n",
            countries[i % countries.len()],
            i % 1000
        ));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_jsonl_nkey_group",
        text.as_bytes(),
    ));
    let jpath = f.0.with_extension("jsonl");
    std::fs::rename(&f.0, &jpath).unwrap();
    let _cleanup = TempCsv(jpath.clone());
    let p = jpath.display();
    let flow =
        format!("G:\n open {p}\n |# user.country min:amount max:amount count_distinct:amount\n;");
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
    let (serial, serial_par) = collect(rivus_runtime::MemoryPref::Low);
    let (parallel, par) = collect(rivus_runtime::MemoryPref::Fast);
    assert!(!serial_par, "low must be serial");
    assert!(
        par,
        "fast should engage the bounded JSONL byte-range group path"
    );
    // The output key column is named `user.country` (the path's column name).
    assert!(
        serial
            .iter()
            .all(|l| ["JP", "US", "DE", "FR"].iter().any(|c| l.starts_with(c))),
        "group key should resolve the nested `user.country` value, got {:?}",
        serial.first()
    );
    assert_eq!(parallel, serial, "nested-key group parallel != serial");
}

#[test]
fn parallel_jsonl_path_resolve_byte_identical() {
    // §32 s4: nested-path resolution (`user.age`, `tags[0]`) is a deterministic
    // pure function of the row, so a flow that filters on a struct-field path and
    // projects struct/list paths must be byte-identical serial == parallel ==
    // chunk-size. The byte-range parallel reader resolves each range against the
    // same globally-inferred nested shape; output files are compared byte-for-byte.
    let rows = 120_000usize;
    let mut text = String::new();
    let names = ["aa", "bb", "cc", "dd"];
    for i in 0..rows {
        // `user` struct (age + name) and a `tags` list. Some rows have an empty
        // tags list so `tags[0]` exercises the out-of-range → typed null path.
        let tags = if i % 11 == 0 {
            "[]".to_string()
        } else {
            format!("[{},{}]", i % 100, (i % 100) + 1)
        };
        text.push_str(&format!(
            "{{\"id\":{i},\"user\":{{\"age\":{},\"name\":\"{}\"}},\"tags\":{tags}}}\n",
            i % 90,
            names[i % names.len()],
        ));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_jsonl_path",
        text.as_bytes(),
    ));
    let jpath = f.0.with_extension("jsonl");
    std::fs::rename(&f.0, &jpath).unwrap();
    let _cleanup = TempCsv(jpath.clone());
    let p = jpath.display();

    let run_to = |pref: rivus_runtime::MemoryPref, cs: usize, out: &std::path::Path| -> bool {
        // Filter on a struct-field path; project the struct field and a list
        // index (out-of-range on empty lists → typed null).
        let src = format!(
            "F:\n open {p}\n |? user.age >= 18\n |> id (user.age) as age (tags[0]) as first\n save {}\n;",
            out.display()
        );
        let g = rivus_parser::parse(&src).expect("parse");
        let res = run(
            &g,
            RunOptions {
                chunk_size: cs,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run");
        !res.workers.is_empty()
    };

    let ser = TempCsv(gendata::write_temp_bytes("jsonl_path_serial", b""));
    assert!(
        !run_to(rivus_runtime::MemoryPref::Low, 4096, &ser.0),
        "low must be serial"
    );
    let oracle = std::fs::read_to_string(&ser.0).expect("read serial out");
    assert!(oracle.lines().count() > 1000, "oracle unexpectedly small");
    assert_eq!(oracle.lines().next(), Some("id,age,first"), "header");

    let par = TempCsv(gendata::write_temp_bytes("jsonl_path_par", b""));
    assert!(
        run_to(rivus_runtime::MemoryPref::Fast, 4096, &par.0),
        "fast should engage the JSONL streaming-parallel path"
    );
    let got = std::fs::read_to_string(&par.0).expect("read parallel out");
    assert_eq!(got, oracle, "parallel path-resolve != serial");

    for cs in [1usize, 1000, rows] {
        let cz = TempCsv(gendata::write_temp_bytes("jsonl_path_cz", b""));
        run_to(rivus_runtime::MemoryPref::Low, cs, &cz.0);
        let got = std::fs::read_to_string(&cz.0).expect("read cz out");
        assert_eq!(got, oracle, "path-resolve changed @cs={cs}");
    }
}

#[test]
fn sort_by_nested_column_is_deterministic_text_order() {
    // §32 s3a forward-compat behavior is now LIVE (s3b): sorting by a nested
    // column has no native key, so it orders by the `Value` text form
    // (`make_cmp` / `argsort_single`). Confirm the semantics are sound: a defined
    // total order, deterministic, chunk-size independent, and continue-first (no
    // panic on a nested key). We sort by the `tags` list column and pin that the
    // row order is identical across chunk sizes.
    let mut text = String::new();
    // Distinct list cells whose text forms have a clear lexicographic order.
    let lists = ["[3, 1]", "[1, 2]", "[2, 9]", "[1, 0]", "[10, 0]"];
    for (i, t) in lists.iter().enumerate() {
        text.push_str(&format!("{{\"id\":{i},\"tags\":{t}}}\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_sort_nested",
        text.as_bytes(),
    ));
    let jpath = f.0.with_extension("jsonl");
    std::fs::rename(&f.0, &jpath).unwrap();
    let _cleanup = TempCsv(jpath.clone());
    let p = jpath.display();
    let flow = format!("S:\n open {p}\n sort tags\n |> id tags\n;");

    let order = |cz: usize| -> Vec<i64> {
        let res = run_src(&flow, cz);
        // Continue-first: a nested sort key must never raise a fatal.
        assert!(
            !res.errors.iter().any(rivus_core::ErrorEvent::is_fatal),
            "sort by a nested column must not be fatal (cz={cz})"
        );
        collect_i64(&res, "S", "id")
    };
    let reference = order(4096);
    assert_eq!(reference.len(), lists.len(), "all rows survive the sort");
    // Text-form ascending order over the list cells:
    //   "[1, 0]"(3) < "[1, 2]"(1) < "[10, 0]"(4) < "[2, 9]"(2) < "[3, 1]"(0)
    assert_eq!(
        reference,
        vec![3, 1, 4, 2, 0],
        "nested sort must follow the Value text form's lexicographic order"
    );
    for cz in [1usize, 2, 3, lists.len()] {
        assert_eq!(order(cz), reference, "nested sort order changed @cz={cz}");
    }
}

#[test]
fn parallel_binary_byte_identical() {
    // #49: fixed-width binary is splittable (record-aligned) — its filter and
    // group-by parallelize in the bounded byte-range path, byte-identical to
    // serial. ~150k records (17 B packed) > 1 MiB so MemoryPref::Fast engages it.
    let rows = 150_000;
    let bytes = gendata::bin_clean(rows, 7);
    let f = TempCsv(gendata::write_temp_bytes("stress_bin_par", &bytes));
    let p = f.0.display();
    // Stateless filter+project to a file, and a group-by — both parallel paths.
    for flow in [
        format!(
            "F:\n readbin {p} (id:i32 age:i32 score:f64 active:u8)\n |? age >= 45\n |> id age\n save {{OUT}}\n;"
        ),
        format!(
            "F:\n readbin {p} (id:i32 age:i32 score:f64 active:u8)\n |# active min:age max:age count_distinct:age\n save {{OUT}}\n;"
        ),
    ] {
        let ser = TempCsv(gendata::write_temp_bytes("bin_ser", b""));
        let par = TempCsv(gendata::write_temp_bytes("bin_par", b""));
        let run_to = |pref: rivus_runtime::MemoryPref, out: &std::path::Path| -> bool {
            let src = flow.replace("{OUT}", &out.display().to_string());
            let g = rivus_parser::parse(&src).expect("parse");
            let res = run(
                &g,
                RunOptions {
                    chunk_size: 4096,
                    memory: pref,
                    ..Default::default()
                },
            )
            .expect("run");
            !res.workers.is_empty()
        };
        assert!(!run_to(rivus_runtime::MemoryPref::Low, &ser.0), "low serial");
        assert!(run_to(rivus_runtime::MemoryPref::Fast, &par.0), "fast should parallelize binary");
        let a = std::fs::read_to_string(&ser.0).unwrap();
        let b = std::fs::read_to_string(&par.0).unwrap();
        assert_eq!(a, b, "parallel binary != serial for flow:\n{flow}");
    }
}

#[test]
fn f64_parallel_sum_needs_canonical_order_decimal_is_exact_today() {
    // #45 (measured): f64 addition is non-associative, so a naive partition→merge
    // sum diverges from the serial sum *and* varies with the partition count —
    // which is exactly why the parallel group-by keeps f64 sum/avg/std serial
    // (#41 option 1). A canonical fixed-block fold is a pure function of the value
    // sequence (partition-independent), but adopting it changes the serial value
    // too and needs global-row coordination to run bounded+parallel. Meanwhile the
    // **decimal lane already gives an exact, byte-identical parallel sum today**.
    let naive = |v: &[f64]| v.iter().fold(0.0f64, |a, &x| a + x);
    let part_naive = |v: &[f64], k: usize| {
        let n = v.len();
        (0..k)
            .map(|i| naive(&v[n * i / k..n * (i + 1) / k]))
            .fold(0.0, |a, b| a + b)
    };
    let canonical = |v: &[f64], bs: usize| {
        let mut total = 0.0;
        let mut i = 0;
        while i < v.len() {
            let e = (i + bs).min(v.len());
            total += naive(&v[i..e]);
            i = e;
        }
        total
    };
    // Large magnitudes → the additions actually round.
    let v: Vec<f64> = (0..200_000)
        .map(|i| ((i as f64) * 1.000000123).sin() * 1e9 + 1e15)
        .collect();
    let serial = naive(&v);
    // The problem: naive parallel diverges and is partition-count dependent.
    assert_ne!(part_naive(&v, 2), serial, "demo expects f64 drift");
    assert_ne!(
        part_naive(&v, 2),
        part_naive(&v, 4),
        "partition-count dependent"
    );
    // The canonical fold is a pure function of (values, block size): the same no
    // matter how it is later partitioned — the property a parallel impl must hit.
    assert_eq!(canonical(&v, 256), canonical(&v, 256));

    // What we ship today: route exactness through the decimal lane, whose sum is
    // exact and byte-identical in parallel (no f64 drift at all).
    let mut text = String::from("g,amount\n");
    for i in 0..5000 {
        text.push_str(&format!("1,{}.{:02}\n", i % 1000, i % 100));
    }
    let f = TempCsv(gendata::write_temp_bytes("canon_dec", text.as_bytes()));
    let p = f.0.display();
    let sum = |cs: usize| {
        let res = run_src(
            &format!("G:\n open {p} (g amount:decimal(2))\n |# g sum:amount\n;"),
            cs,
        );
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("G"))
            .unwrap();
        let c = &o.chunks[0];
        let si = c.schema.index_of("sum_amount").unwrap();
        c.value(0, si).to_string()
    };
    assert_eq!(
        sum(1),
        sum(4096),
        "decimal sum is exact & chunk-independent today"
    );
}

#[test]
fn parallel_group_final_mode_matches_serial() {
    // #48: the parallel group-by must derive `final_mode` from its workers'
    // errors (so a fatal halts the run and the CLI exit code matches serial),
    // not hardcode Normal. Fatals are not reachable on this path with valid input
    // (the range source yields empty on open error rather than raising fatal), so
    // this guards the common case: clean input → Normal on BOTH paths (and the
    // fix derives Halted from any fatal error exactly as the serial path does).
    let rows = 130_000usize;
    let countries = ["JP", "US", "DE", "FR"];
    let mut text = String::from("country,amount\n");
    for i in 0..rows {
        text.push_str(&format!(
            "{},{}\n",
            countries[i % countries.len()],
            i % 1000
        ));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_group_mode",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let flow = format!("G:\n open {p}\n |# country min:amount max:amount\n;");
    let mode = |pref: rivus_runtime::MemoryPref| {
        let g = rivus_parser::parse(&flow).expect("parse");
        run(
            &g,
            RunOptions {
                chunk_size: 4096,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run")
        .final_mode
    };
    let serial = mode(rivus_runtime::MemoryPref::Low);
    let parallel = mode(rivus_runtime::MemoryPref::Fast);
    assert_eq!(
        serial,
        rivus_core::Mode::Normal,
        "clean input → Normal (serial)"
    );
    assert_eq!(
        parallel, serial,
        "parallel group-by final_mode must match serial"
    );
}

#[test]
fn date_groupby_parallel_matches_serial() {
    // The byte-range parallel reader builds Date columns with the same parse as
    // the serial reader; a group-by count per date is exact + associative, so it
    // is byte-identical across serial/parallel and chunk size (#58).
    let rows = 6_000;
    let mut rng = Rng::new(58);
    let mut text = String::from("d,v\n");
    let days = ["2024-06-01", "2024-06-02", "2024-06-03", "2023-12-25"];
    for _ in 0..rows {
        let d = days[rng.below(days.len() as u64) as usize];
        text.push_str(&format!("{d},{}\n", rng.below(1000) as i64));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_date_par",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let flow = format!("D:\n open {p} (d:date v:int)\n |# d count max:v\n;");

    let snapshot = |pref: rivus_runtime::MemoryPref| {
        let g = rivus_parser::parse(&flow).expect("parse");
        std::env::set_var("RIVUS_PARALLEL_MIN_BYTES", "0");
        let res = run(
            &g,
            RunOptions {
                chunk_size: 512,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run");
        std::env::remove_var("RIVUS_PARALLEL_MIN_BYTES");
        let mut rows: Vec<(String, i64, i64)> = {
            let days = collect_strings(&res, "D", "d");
            let cnt = collect_i64(&res, "D", "count");
            let mx = collect_i64(&res, "D", "max_v");
            (0..days.len())
                .map(|i| (days[i].clone(), cnt[i], mx[i]))
                .collect()
        };
        rows.sort();
        rows
    };
    assert_eq!(
        snapshot(rivus_runtime::MemoryPref::Low),
        snapshot(rivus_runtime::MemoryPref::Fast),
        "date group-by must be byte-identical serial vs parallel"
    );
}

#[test]
fn date_minmax_keeps_date_type_and_parallel_matches_serial() {
    // min/max on a date column keep the Date lane (render yyyy-MM-dd, not the
    // raw epoch-day) and — being exact integer extremes — are byte-identical
    // serial vs parallel and across chunk size (#58).
    let rows = 6_000;
    let mut rng = Rng::new(581);
    let mut text = String::from("k,d\n");
    let days = ["2024-06-01", "2024-01-15", "2023-12-25", "2024-02-29"];
    // Guarantee both keys contain the extremes so the oracle is deterministic
    // (min = 2023-12-25, max = 2024-06-01) regardless of the random fill.
    for k in ["a", "b"] {
        text.push_str(&format!("{k},2023-12-25\n{k},2024-06-01\n"));
    }
    for _ in 0..rows {
        let k = if rng.below(2) == 0 { "a" } else { "b" };
        let d = days[rng.below(days.len() as u64) as usize];
        text.push_str(&format!("{k},{d}\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_date_mm", text.as_bytes()));
    let p = f.0.display();
    let flow = format!("M:\n open {p} (k:str d:date)\n |# k min:d max:d\n;");

    let snapshot = |pref: rivus_runtime::MemoryPref| {
        let g = rivus_parser::parse(&flow).expect("parse");
        std::env::set_var("RIVUS_PARALLEL_MIN_BYTES", "0");
        let res = run(
            &g,
            RunOptions {
                chunk_size: 512,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run");
        std::env::remove_var("RIVUS_PARALLEL_MIN_BYTES");
        // The min/max columns must render as ISO dates (Date lane preserved).
        for col in ["min_d", "max_d"] {
            for s in collect_strings(&res, "M", col) {
                assert!(
                    s.len() == 10 && s.as_bytes()[4] == b'-',
                    "{col} must render as yyyy-MM-dd (Date lane), got {s:?}"
                );
            }
        }
        let mut rows: Vec<(String, String, String)> = {
            let k = collect_strings(&res, "M", "k");
            let lo = collect_strings(&res, "M", "min_d");
            let hi = collect_strings(&res, "M", "max_d");
            (0..k.len())
                .map(|i| (k[i].clone(), lo[i].clone(), hi[i].clone()))
                .collect()
        };
        rows.sort();
        rows
    };
    let serial = snapshot(rivus_runtime::MemoryPref::Low);
    assert_eq!(
        serial,
        snapshot(rivus_runtime::MemoryPref::Fast),
        "date min/max must be byte-identical serial vs parallel"
    );
    // Oracle: both keys span the full date set, so min/max are the true extremes.
    assert_eq!(
        serial,
        vec![
            (
                "a".to_string(),
                "2023-12-25".to_string(),
                "2024-06-01".to_string()
            ),
            (
                "b".to_string(),
                "2023-12-25".to_string(),
                "2024-06-01".to_string()
            ),
        ],
        "date min/max extreme values"
    );
}

#[test]
fn auto_inferred_temporal_lanes_parallel_byte_identical() {
    // Item-1 (review #94 follow-up): pin the *inference* invariant for the
    // UNDECLARED temporal lanes. `datetime_auto_inferred_without_declaration_bug_b`
    // only checks a 2-row serial datetime infer; this sweeps date / time /
    // datetime auto-inference (no schema declared) for
    //   (a) the resolved lanes + numeric precedence (an 8-digit integer column
    //       that *looks* date-ish — and would match the `yyyyMMdd` datetime auto
    //       format — must stay I64, never mis-infer as a temporal lane), and
    //   (b) byte-identity serial(Low) vs parallel(Fast) and across chunk sizes,
    //       since the byte-range parallel reader infers via the same Flags merge.

    // --- (a) lanes + numeric-precedence guard (tiny, serial, deterministic) ---
    let types = "d,t,ts,n,v\n\
                 2024-06-03,09:05:00,2024-06-03T14:30:00,20240601,7\n\
                 2023-12-25,23:59:59,2023-12-25T00:00:00,20231225,8\n";
    let tf = TempCsv(gendata::write_temp_bytes(
        "auto_temporal_types",
        types.as_bytes(),
    ));
    let tp = tf.0.display();
    let tres = run_src(&format!("A:\n open {tp}\n |> d t ts n v\n;"), 4096); // no schema
    let to = tres
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some("A"))
        .unwrap();
    let dtype = |name: &str| {
        let ci = to.chunks[0].schema.index_of(name).unwrap();
        to.chunks[0].schema.fields[ci].dtype
    };
    assert!(
        matches!(dtype("d"), rivus_core::DataType::Date),
        "undeclared yyyy-MM-dd should infer the date lane, got {:?}",
        dtype("d")
    );
    assert!(
        matches!(dtype("t"), rivus_core::DataType::Time),
        "undeclared HH:mm:ss should infer the time lane, got {:?}",
        dtype("t")
    );
    assert!(
        matches!(dtype("ts"), rivus_core::DataType::DateTime { .. }),
        "undeclared ISO datetime should infer the datetime lane, got {:?}",
        dtype("ts")
    );
    assert!(
        matches!(dtype("n"), rivus_core::DataType::I64),
        "an 8-digit integer column must stay I64 (numeric precedence), not a \
         temporal lane — got {:?}",
        dtype("n")
    );
    assert!(
        matches!(dtype("v"), rivus_core::DataType::I64),
        "plain integer column should infer I64, got {:?}",
        dtype("v")
    );

    // --- (b) serial vs parallel byte-identity on auto-inferred temporal cols ---
    // >1 MiB file + MemoryPref::Fast forces the byte-range parallel reader (no
    // env vars → no cross-test races). Group-by on the auto-inferred date key,
    // with min/max of the auto-inferred datetime + time columns (all exact +
    // associative → parallel-safe).
    let rows = 150_000usize;
    let mut rng = Rng::new(94);
    let dates = ["2024-06-01", "2024-06-02", "2023-12-25", "2024-02-29"];
    let times = ["09:05:00", "00:00:00", "23:59:59", "12:30:00"];
    let stamps = [
        "2024-06-01T14:30:00",
        "2024-06-02T09:00:00",
        "2023-12-25T00:00:00",
        "2024-02-29T23:59:59",
    ];
    let mut text = String::from("d,t,ts,n,v\n");
    for _ in 0..rows {
        let i = rng.below(dates.len() as u64) as usize;
        let n = 20_240_000i64 + rng.below(900) as i64; // 8-digit, must stay I64
        text.push_str(&format!(
            "{},{},{},{n},{}\n",
            dates[i],
            times[i],
            stamps[i],
            rng.below(1000) as i64
        ));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "auto_temporal_par",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let flow = format!(
        "G:\n open {p}\n |# d count min:ts max:ts min:t max:t\n;" // undeclared keys/aggs
    );

    let collect = |pref: rivus_runtime::MemoryPref, cz: usize| -> (Vec<String>, bool) {
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
        assert!(
            !res.errors.iter().any(rivus_core::ErrorEvent::is_fatal),
            "auto-infer group-by must never raise a fatal (pref={pref:?}, cz={cz})"
        );
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

    let (serial, _) = collect(rivus_runtime::MemoryPref::Low, 4096);
    let (parallel, par_engaged) = collect(rivus_runtime::MemoryPref::Fast, 4096);
    assert!(
        par_engaged,
        "parallel reader did not engage on a >1 MiB undeclared-temporal file"
    );
    assert_eq!(
        parallel, serial,
        "auto-inferred temporal group-by must be byte-identical serial vs parallel"
    );
    // The min/max columns render as the temporal lanes (not raw integer ticks).
    assert!(
        serial.iter().all(|l| l.contains('T') && l.contains(':')),
        "min/max ts/t must render as datetime/time text, got e.g. {:?}",
        serial.first()
    );
    // Chunk-size independence of the inference + aggregation (serial oracle).
    for cz in [1usize, 7, 512] {
        let (rows_cz, _) = collect(rivus_runtime::MemoryPref::Low, cz);
        assert_eq!(
            rows_cz, serial,
            "auto-inferred temporal group-by changed @cz={cz}"
        );
    }
}

#[test]
fn cast_datetime_failures_sum_serial_eq_parallel() {
    // BUG-D never-silent contract: an expression `cast` to datetime surfaces a
    // per-column failure summary. In parallel each worker emits its partition's
    // partial and the counts must SUM to the serial total (same contract as
    // parse_failures / validate reject), and the cast output stays byte-identical
    // serial vs parallel. Forced parallel with a >1 MiB file + MemoryPref::Fast +
    // a file sink (no env vars → no cross-test races; cf. parallel_decimal).
    let rows = 120_000usize; // ~2.5 MiB → crosses the Fast 1 MiB floor
    let mut text = String::from("id,ts\n");
    let mut fails = 0u64;
    for i in 0..rows {
        if i % 50 == 0 {
            text.push_str(&format!("{i},BAD\n")); // unparseable → null + counted
            fails += 1;
        } else {
            text.push_str(&format!("{i},2026-06-01T00:00:00\n")); // valid ISO
        }
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_castpar", text.as_bytes()));
    let p = f.0.display();
    let run_to_file = |pref: rivus_runtime::MemoryPref, out: &std::path::Path| {
        let src = format!(
            "C:\n open {p} (id:int ts:str)\n cast ts:datetime\n |> id ts\n save {}\n;",
            out.display()
        );
        let g = rivus_parser::parse(&src).expect("parse");
        run(
            &g,
            RunOptions {
                chunk_size: 4096,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run")
    };
    let sum_fails = |res: &rivus_runtime::RunResult| -> u64 {
        res.errors
            .iter()
            .filter(|e| e.message.contains("could not be cast to datetime"))
            .filter_map(|e| e.message.split_whitespace().next()?.parse::<u64>().ok())
            .sum()
    };
    let ser_out = TempCsv(gendata::write_temp_bytes("castpar_serial", b""));
    let ser = run_to_file(rivus_runtime::MemoryPref::Low, &ser_out.0);
    assert_eq!(sum_fails(&ser), fails, "serial cast-failure total");

    let par_out = TempCsv(gendata::write_temp_bytes("castpar_parallel", b""));
    let par = run_to_file(rivus_runtime::MemoryPref::Fast, &par_out.0);
    assert!(
        !par.workers.is_empty(),
        "byte-range parallel reader must engage"
    );
    // Per-worker partials must sum to the serial total (never-silent).
    assert_eq!(
        sum_fails(&par),
        fails,
        "parallel cast-failure counts must sum to the total"
    );
    // Output is byte-identical serial vs parallel (parts concatenated in order).
    let a = std::fs::read_to_string(&ser_out.0).expect("read serial");
    let b = std::fs::read_to_string(&par_out.0).expect("read parallel");
    assert_eq!(a, b, "cast output identical serial vs parallel");
}

#[test]
fn pred_cast_failures_sum_serial_eq_parallel() {
    // BUG-D Slice A-2: a cast failure inside a `|?` predicate is surfaced on the
    // scalar path. In parallel each worker emits its partition's partial; the
    // counts must SUM to the serial total, and the filtered output stays
    // byte-identical serial vs parallel. (>1 MiB + MemoryPref::Fast + sink; no env.)
    let rows = 120_000usize;
    let mut text = String::from("id,ts\n");
    let mut fails = 0u64;
    for i in 0..rows {
        if i % 50 == 0 {
            text.push_str(&format!("{i},BAD\n")); // unparseable → null → excluded + counted
            fails += 1;
        } else {
            text.push_str(&format!("{i},2026-06-01T00:00:00\n")); // valid, passes the filter
        }
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_predcastpar",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let run_to_file = |pref: rivus_runtime::MemoryPref, out: &std::path::Path| {
        let src = format!(
            "C:\n open {p} (id:int ts:str)\n |? ts:datetime > \"2020-01-01T00:00:00\"\n |> id\n save {}\n;",
            out.display()
        );
        let g = rivus_parser::parse(&src).expect("parse");
        run(
            &g,
            RunOptions {
                chunk_size: 4096,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run")
    };
    let sum_fails = |res: &rivus_runtime::RunResult| -> u64 {
        res.errors
            .iter()
            .filter(|e| e.message.contains("could not be cast"))
            .filter_map(|e| e.message.split_whitespace().next()?.parse::<u64>().ok())
            .sum()
    };
    let ser_out = TempCsv(gendata::write_temp_bytes("predcast_serial", b""));
    let ser = run_to_file(rivus_runtime::MemoryPref::Low, &ser_out.0);
    assert_eq!(
        sum_fails(&ser),
        fails,
        "serial predicate cast-failure total"
    );

    let par_out = TempCsv(gendata::write_temp_bytes("predcast_parallel", b""));
    let par = run_to_file(rivus_runtime::MemoryPref::Fast, &par_out.0);
    assert!(
        !par.workers.is_empty(),
        "byte-range parallel reader must engage"
    );
    assert_eq!(
        sum_fails(&par),
        fails,
        "parallel predicate cast-failure counts must sum to the total"
    );
    let a = std::fs::read_to_string(&ser_out.0).expect("read serial");
    let b = std::fs::read_to_string(&par_out.0).expect("read parallel");
    assert_eq!(a, b, "filtered output identical serial vs parallel");
}

#[test]
fn validate_reject_parallel_summary_counts_sum_to_total() {
    // In parallel each byte-range/partition worker emits its own validate
    // summary; the counts must SUM to the true total (never-silent), while the
    // dropped rows stay byte-identical to serial (#83). A single coordinator-
    // merged count is a §24 follow-up; this pins the current contract.
    let n = 4_000usize;
    let mut text = String::from("id,age\n");
    let mut fails = 0u64;
    for i in 0..n {
        let age: i64 = if i % 5 == 0 { -1 } else { 30 }; // every 5th row fails
        if age < 0 {
            fails += 1;
        }
        text.push_str(&format!("{i},{age}\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_validate_par",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let flow = format!("V:\n open {p} (id:int age:int)\n |! age >= 0 reject\n |> id\n;");
    let run_pref = |pref| {
        let g = rivus_parser::parse(&flow).expect("parse");
        std::env::set_var("RIVUS_PARALLEL_MIN_BYTES", "0");
        let res = run(
            &g,
            RunOptions {
                chunk_size: 256,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run");
        std::env::remove_var("RIVUS_PARALLEL_MIN_BYTES");
        res
    };
    let par = run_pref(rivus_runtime::MemoryPref::Fast);
    // Sum the per-worker reject summaries — the total must be exact.
    let total: u64 = par
        .errors
        .iter()
        .filter(|e| e.message.contains("failed") && e.message.contains("(reject)"))
        .filter_map(|e| e.message.split_whitespace().next()?.parse::<u64>().ok())
        .sum();
    assert_eq!(total, fails, "parallel reject counts must sum to the total");
    // Dropped rows are byte-identical to serial.
    let ser = run_pref(rivus_runtime::MemoryPref::Low);
    assert_eq!(
        collect_i64(&par, "V", "id"),
        collect_i64(&ser, "V", "id"),
        "reject rows identical serial vs parallel"
    );
}

#[test]
fn datetime_parallel_matches_serial_and_chunk_size() {
    // The byte-range parallel reader builds datetime columns with the same
    // `DtSpec` as the serial reader, so a datetime read + filter + daily
    // group-by must be byte-identical across serial/parallel and chunk size
    // (design 23 step 3c; integer ticks are exact + associative).
    let rows = 20_000;
    let mut rng = Rng::new(11);
    let mut text = String::from("ts,v\n");
    // Three days in 2026-06; a compact yyMMddHHmmss timestamp per row.
    let days = ["260601", "260602", "260603"];
    for _ in 0..rows {
        let day = days[rng.below(days.len() as u64) as usize];
        let hh = rng.below(24);
        let mm = rng.below(60);
        let ss = rng.below(60);
        let v = rng.below(1000) as i64;
        text.push_str(&format!("{day}{hh:02}{mm:02}{ss:02},{v}\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_dt_par", text.as_bytes()));
    let p = f.0.display();
    let flow = format!(
        "D:\n open {p} (ts:datetime(\"yyMMddHHmmss\") v:int)\n \
         |? ts >= \"260602000000\"\n \
         |> (format(trunc(ts, \"day\"), \"yyyy-MM-dd\")) as day v\n \
         |# day sum:v max:v\n;"
    );

    // Collect (day, sum_v, max_v) as a sorted, chunk/strategy-independent key.
    let snapshot = |pref: rivus_runtime::MemoryPref, cz: usize| {
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
        let days = collect_strings(&res, "D", "day");
        let sums = collect_i64(&res, "D", "sum_v");
        let maxs = collect_i64(&res, "D", "max_v");
        let mut rows: Vec<(String, i64, i64)> = days
            .into_iter()
            .zip(sums)
            .zip(maxs)
            .map(|((d, s), m)| (d, s, m))
            .collect();
        rows.sort();
        rows
    };

    let reference = snapshot(rivus_runtime::MemoryPref::Low, 4096);
    // Only the days >= 2026-06-02 survive the filter.
    assert_eq!(
        reference.iter().map(|r| r.0.clone()).collect::<Vec<_>>(),
        vec!["2026-06-02".to_string(), "2026-06-03".to_string()],
    );
    for pref in [
        rivus_runtime::MemoryPref::Low,
        rivus_runtime::MemoryPref::Fast,
    ] {
        for cz in [1usize, 7, 256, 4096] {
            assert_eq!(
                snapshot(pref, cz),
                reference,
                "datetime group-by diverged at pref={pref:?} chunk_size={cz}"
            );
        }
    }
}

#[test]
fn duration_groupby_parallel_matches_serial() {
    // `sum`/`avg`/`min`/`max` of a duration (from `end - start`) are exact i64
    // and associative → identical across serial/parallel and chunk size. #57.
    let rows = 20_000;
    let mut rng = Rng::new(13);
    let mut text = String::from("g,start,end\n");
    let groups = ["a", "b", "c"];
    for _ in 0..rows {
        let g = groups[rng.below(groups.len() as u64) as usize];
        // start somewhere in the day, end = start + [1..3600] seconds.
        let sh = rng.below(20);
        let sm = rng.below(60);
        let dsec = 1 + rng.below(3600) as i64;
        let start_s = sh as i64 * 3600 + sm as i64 * 60;
        let end_s = start_s + dsec;
        // yyMMddHHmmss on 2026-06-01, HH:MM:SS derived from the second count.
        let stamp = |s: i64| format!("260601{:02}{:02}{:02}", s / 3600, (s % 3600) / 60, s % 60);
        text.push_str(&format!("{g},{},{}\n", stamp(start_s), stamp(end_s)));
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_dur_par", text.as_bytes()));
    let p = f.0.display();
    let flow = format!(
        "D:\n open {p} (g:str start:datetime(\"yyMMddHHmmss\") end:datetime(\"yyMMddHHmmss\"))\n \
         |> g (end - start) as dur\n \
         |# g sum:dur avg:dur min:dur max:dur\n;"
    );
    let snapshot = |pref: rivus_runtime::MemoryPref, cz: usize| {
        let gph = rivus_parser::parse(&flow).expect("parse");
        let res = run(
            &gph,
            RunOptions {
                chunk_size: cz,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run");
        let g = collect_strings(&res, "D", "g");
        let cols: Vec<Vec<String>> = ["sum_dur", "avg_dur", "min_dur", "max_dur"]
            .iter()
            .map(|c| collect_strings(&res, "D", c))
            .collect();
        let mut rows: Vec<(String, String, String, String, String)> = (0..g.len())
            .map(|i| {
                (
                    g[i].clone(),
                    cols[0][i].clone(),
                    cols[1][i].clone(),
                    cols[2][i].clone(),
                    cols[3][i].clone(),
                )
            })
            .collect();
        rows.sort();
        rows
    };
    let reference = snapshot(rivus_runtime::MemoryPref::Low, 4096);
    assert_eq!(reference.len(), 3, "three groups");
    for pref in [
        rivus_runtime::MemoryPref::Low,
        rivus_runtime::MemoryPref::Fast,
    ] {
        for cz in [1usize, 7, 256, 4096] {
            assert_eq!(
                snapshot(pref, cz),
                reference,
                "duration agg diverged @pref={pref:?} cz={cz}"
            );
        }
    }
}
