//! IO formats: binary, JSONL, JSON array, TSV, gzip, zstd round-trips.
//!
//! Moved verbatim from the former monolithic `stress.rs` (design 26 §26.8.1,
//! mechanical move-only split; logic unchanged).

use super::*;

#[test]
fn binary_source_matches_oracle() {
    // Fixed-width binary records (C struct dump): i32 id, i32 age, f64 score,
    // u8 active. Decoding must produce the same filter result as an oracle that
    // replays the generator's PRNG, across chunk sizes.
    let rows = 50_000;
    let seed = 7;
    let bytes = gendata::bin_clean(rows, seed);
    let f = TempCsv(gendata::write_temp_bytes("stress_bin", &bytes));
    let p = f.0.display();

    let mut rng = Rng::new(seed);
    let mut ge = 0u64;
    for _ in 0..rows {
        let age = rng.below(90);
        let _score = rng.below(10_000);
        let _active = rng.below(2);
        if age >= 45 {
            ge += 1;
        }
    }

    for cs in [1, 1000, 8192] {
        let src =
            format!("F:\n readbin {p} (id:i32 age:i32 score:f64 active:u8)\n |? age >= 45\n;");
        let res = run_src(&src, cs);
        assert_eq!(res.total_rows_out(), ge, "binary filter chunk_size={cs}");
        assert!(res.errors.is_empty(), "clean binary should not error");
    }
}

#[test]
fn binary_big_endian_decodes() {
    // Two packed big-endian records: (i32 id, i32 age).
    let mut bytes = Vec::new();
    for (id, age) in [(1i32, 50i32), (2, 10)] {
        bytes.extend_from_slice(&id.to_be_bytes());
        bytes.extend_from_slice(&age.to_be_bytes());
    }
    let f = TempCsv(gendata::write_temp_bytes("be", &bytes));
    let res = run_src(
        &format!(
            "F:\n readbin {} be (id:i32 age:i32)\n |? age >= 20\n;",
            f.0.display()
        ),
        4096,
    );
    assert_eq!(res.total_rows_out(), 1); // only age 50 survives
}

#[test]
fn binary_c_alignment_decodes() {
    // C `struct { u8 flag; i32 v; }`: flag@0, 3 pad bytes, v@4, record size 8.
    let mut bytes = Vec::new();
    for (flag, v) in [(1u8, 100i32), (0u8, 200i32)] {
        bytes.push(flag);
        bytes.extend_from_slice(&[0, 0, 0]); // alignment padding
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    let f = TempCsv(gendata::write_temp_bytes("aligned", &bytes));
    // With `aligned`, the reader skips the padding and reads v at offset 4.
    let res = run_src(
        &format!(
            "F:\n readbin {} aligned (flag:u8 v:i32)\n |? v >= 150\n;",
            f.0.display()
        ),
        4096,
    );
    assert_eq!(res.total_rows_out(), 1); // only v=200 survives
}

#[test]
fn binary_char_field_decodes_to_text() {
    // `char[N]` (§29.4): N raw bytes decoded as UTF-8, padding kept as value
    // (§29.5-3). Record: i32 id + char[8] name. Chunk-size independent; UTF-8
    // multi-byte is decoded whole.
    let names: [&[u8]; 3] = [
        b"Alice\0\0\0",
        b"Bob\0\0\0\0\0",
        "\u{3042}\u{3044}\0\0".as_bytes(),
    ];
    let mut bytes = Vec::new();
    for (i, name) in names.iter().enumerate() {
        assert_eq!(name.len(), 8, "fixture record must be 8 bytes");
        bytes.extend_from_slice(&((i as i32) + 1).to_le_bytes());
        bytes.extend_from_slice(name);
    }
    let f = TempCsv(gendata::write_temp_bytes("bin_char", &bytes));
    let p = f.0.display();
    for cs in [1usize, 2, 4096] {
        let res = run_src(
            &format!("F:\n readbin {p} (id:i32 name:char[8])\n |> id name\n;"),
            cs,
        );
        assert!(
            res.errors.is_empty(),
            "char[N] decode should not error @cs={cs}"
        );
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("F"))
            .unwrap();
        let mut got = Vec::new();
        for c in &o.chunks {
            let ci = c.schema.index_of("name").unwrap();
            for r in 0..c.len {
                got.push(c.value(r, ci).to_string());
            }
        }
        assert_eq!(got.len(), 3, "@cs={cs}");
        // Text prefix decoded; trailing NUL padding kept as value (not trimmed).
        assert_eq!(got[0].trim_end_matches('\0'), "Alice", "@cs={cs}");
        assert_eq!(got[1].trim_end_matches('\0'), "Bob", "@cs={cs}");
        assert_eq!(
            got[2].trim_end_matches('\0'),
            "\u{3042}\u{3044}",
            "@cs={cs}"
        );
        assert!(got[0].contains('\0'), "padding kept as value @cs={cs}");
    }
}

#[test]
fn jsonl_source_matches_oracle() {
    // JSON Lines source: filter on a numeric field must match an oracle that
    // replays the generator's PRNG, across chunk sizes. `.jsonl` extension
    // selects the JSON reader automatically.
    let rows = 40_000;
    let seed = 55;
    let data = gendata::jsonl_clean(rows, seed);
    // write_temp names files `.csv`; rename to `.jsonl` so `open` selects the
    // JSON reader by extension.
    let raw = gendata::write_temp("stress_jsonl", &data);
    let mut jpath = raw.clone();
    jpath.set_extension("jsonl");
    std::fs::rename(&raw, &jpath).unwrap();
    let _cleanup = TempCsv(jpath.clone());

    let mut rng = Rng::new(seed);
    let mut ge = 0u64;
    for _ in 0..rows {
        let age = rng.below(90);
        let _score = rng.below(10_000);
        let _country = rng.below(5);
        let _active = rng.below(2);
        if age >= 50 {
            ge += 1;
        }
    }

    for cs in [1, 1000, 8192] {
        let src = format!("F:\n open {}\n |? age >= 50\n;", jpath.display());
        let res = run_src(&src, cs);
        assert_eq!(res.total_rows_out(), ge, "jsonl filter chunk_size={cs}");
        assert!(res.errors.is_empty(), "clean jsonl should not error");
    }
}

#[test]
fn json_array_source_matches_oracle() {
    // A large top-level JSON array of objects (multi-line) must filter to the
    // same count as an oracle replaying the generator's PRNG.
    let rows = 30_000;
    let seed = 88;
    let lines = gendata::jsonl_clean(rows, seed);
    let array = format!("[\n{}\n]", lines.trim_end().replace('\n', ",\n"));
    let raw = gendata::write_temp("stress_jsonarr", &array);
    let mut jpath = raw.clone();
    jpath.set_extension("json");
    std::fs::rename(&raw, &jpath).unwrap();
    let _cleanup = TempCsv(jpath.clone());

    let mut rng = Rng::new(seed);
    let mut ge = 0u64;
    for _ in 0..rows {
        let age = rng.below(90);
        let _score = rng.below(10_000);
        let _country = rng.below(5);
        let _active = rng.below(2);
        if age >= 50 {
            ge += 1;
        }
    }

    for cs in [1, 1000, 8192] {
        let res = run_src(
            &format!("F:\n open {}\n |? age >= 50\n;", jpath.display()),
            cs,
        );
        assert_eq!(res.total_rows_out(), ge, "json array chunk_size={cs}");
        assert!(res.errors.is_empty(), "clean json array should not error");
    }
}

#[test]
fn csv_to_jsonl_roundtrip_preserves_data() {
    // open CSV -> save JSONL -> open JSONL: the same filter must yield the same
    // count, proving the source/sink format pair round-trips (numbers, strings,
    // bools all survive).
    let rows = 5_000;
    let seed = 3;
    let csv = TempCsv(gendata::write_temp("rt_csv", &gendata::clean(rows, seed)));
    let mut jpath = csv.0.clone();
    jpath.set_extension("jsonl");
    let _jguard = TempCsv(jpath.clone());

    // Convert CSV -> JSONL (explicit `as jsonl`).
    run_src(
        &format!(
            "C:\n open {}\n save {} as jsonl\n;",
            csv.0.display(),
            jpath.display()
        ),
        4096,
    );

    let want = run_src(
        &format!("C:\n open {}\n |? age >= 45\n;", csv.0.display()),
        4096,
    )
    .total_rows_out();
    let got = run_src(
        &format!("J:\n open {}\n |? age >= 45\n;", jpath.display()),
        4096,
    )
    .total_rows_out();
    assert!(want > 0 && want < rows as u64);
    assert_eq!(
        want, got,
        "CSV->JSONL->read must preserve the filtered count"
    );
}

#[test]
fn csv_to_json_array_roundtrips_and_is_valid() {
    // open CSV -> save a single JSON array (.json) -> re-open it: the JSON
    // reader accepts the array, and the filtered count round-trips. Also assert
    // the file is one bracketed array (starts `[`, ends `]`), not NDJSON.
    let rows = 3_000;
    let csv = TempCsv(gendata::write_temp("rt_jsoncsv", &gendata::clean(rows, 5)));
    let mut jpath = csv.0.clone();
    jpath.set_extension("json");
    let _jguard = TempCsv(jpath.clone());

    // `.json` extension implies a JSON array (no `as` needed).
    run_src(
        &format!(
            "C:\n open {}\n save {}\n;",
            csv.0.display(),
            jpath.display()
        ),
        4096,
    );

    let text = std::fs::read_to_string(&jpath).unwrap();
    let t = text.trim_end();
    assert!(
        t.starts_with('['),
        "JSON array must start with [: {:.40}",
        t
    );
    assert!(t.ends_with(']'), "JSON array must end with ]");
    // A JSON array joins objects with `},{` — NDJSON would have none.
    assert!(t.contains("},{"), "expected array-joined objects");

    let want = run_src(
        &format!("C:\n open {}\n |? age >= 45\n;", csv.0.display()),
        4096,
    )
    .total_rows_out();
    let got = run_src(
        &format!("J:\n open {}\n |? age >= 45\n;", jpath.display()),
        4096,
    )
    .total_rows_out();
    assert!(want > 0 && want < rows as u64);
    assert_eq!(want, got, "CSV->JSON-array->read must preserve the count");
}

#[test]
fn tsv_read_filter_project_chunk_size_independent() {
    // A `.tsv` source must split on tabs, infer per-column types (so the numeric
    // filter works), and stay chunk-size independent — exactly like CSV.
    let rows = 20_000;
    let mut rng = Rng::new(7);
    let mut text = String::from("name\tage\tcity\n");
    let mut expect = 0u64;
    for _ in 0..rows {
        let age = rng.below(90);
        text.push_str(&format!("user\t{age}\tNYC\n"));
        if age >= 40 {
            expect += 1;
        }
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_tsv", text.as_bytes()));
    let p = f.0.display();
    // The path has no `.tsv` extension, so force the delimiter with `as tsv`.
    for cs in [1, 7, 1024, 8192, rows] {
        let res = run_src(
            &format!("T:\n open {p} as tsv\n |? age >= 40\n |> name age\n;"),
            cs,
        );
        assert_eq!(res.total_rows_out(), expect, "tsv filter @cs={cs}");
        assert!(
            res.errors.is_empty(),
            "tsv errors @cs={cs}: {:?}",
            res.errors
        );
    }
}

#[cfg(feature = "gzip")]
#[test]
fn gzip_csv_matches_uncompressed_oracle() {
    use std::io::Write;

    // Build a CSV, gzip it, and assert that reading the `.csv.gz` filters to the
    // same rows as an independent oracle — across chunk sizes (so the single-pass
    // reader's sample-buffer + stream split is exercised at every boundary).
    let rows = 6_000usize;
    let mut text = String::from("id,age\n");
    let mut ge = 0u64;
    let mut rng = Rng::new(11);
    for i in 0..rows {
        let age = rng.below(100);
        text.push_str(&format!("{i},{age}\n"));
        if age >= 50 {
            ge += 1;
        }
    }

    // Write a real .gz fixture with flate2 (available under the gzip feature).
    let dir = std::env::temp_dir();
    let path = dir.join(format!("rivus_gz_{}.csv.gz", std::process::id()));
    {
        let f = std::fs::File::create(&path).unwrap();
        let mut enc = flate2::write::GzEncoder::new(f, flate2::Compression::default());
        enc.write_all(text.as_bytes()).unwrap();
        enc.finish().unwrap();
    }
    let _guard = TempCsv(path.clone());
    let p = path.display();

    for cs in [1usize, 7, 1024, rows] {
        let res = run_src(&format!("G:\n open {p}\n |? age >= 50\n;"), cs);
        assert_eq!(res.total_rows_out(), ge, "gzip filter @cs={cs}");
        assert!(res.errors.is_empty(), "gzip errors @cs={cs}");
    }
}

#[cfg(feature = "zstd")]
#[test]
fn zstd_csv_matches_uncompressed_oracle() {
    // Same shape as the gzip oracle but for `.zst`: a zstd-encoded CSV must
    // filter to the same rows as an independent oracle, across chunk sizes. The
    // fixture is written with the `zstd` crate (an encode-only dev-dependency);
    // the runtime decodes it with the pure-Rust `ruzstd`.
    let rows = 6_000usize;
    let mut text = String::from("id,age\n");
    let mut ge = 0u64;
    let mut rng = Rng::new(17);
    for i in 0..rows {
        let age = rng.below(100);
        text.push_str(&format!("{i},{age}\n"));
        if age >= 50 {
            ge += 1;
        }
    }

    let dir = std::env::temp_dir();
    let path = dir.join(format!("rivus_zst_{}.csv.zst", std::process::id()));
    let comp = zstd::stream::encode_all(text.as_bytes(), 0).unwrap();
    std::fs::write(&path, &comp).unwrap();
    let _guard = TempCsv(path.clone());
    let p = path.display();

    for cs in [1usize, 7, 1024, rows] {
        let res = run_src(&format!("Z:\n open {p}\n |? age >= 50\n;"), cs);
        assert_eq!(res.total_rows_out(), ge, "zstd filter @cs={cs}");
        assert!(res.errors.is_empty(), "zstd errors @cs={cs}");
    }
}

#[test]
fn route_save_partitions_deterministically_and_byte_identically() {
    // §28.7 route (#143): Hive layout + template + flat, the null-key
    // sentinel, escape injectivity (`a/b` → `a%2Fb`); per-file bytes are
    // chunk-size independent and serial == parallel (the parallel path routes
    // the merged stream through the same `route` core).
    let text = "id,country,score\n1,JP,10\n2,US,\n3,a/b,30\n4,JP,10\n5,a%2Fb,30\n";
    let f = TempCsv(gendata::write_temp_bytes("stress_route", text.as_bytes()));
    let p = f.0.display();
    let dir = std::env::temp_dir().join(format!("rivus_route_{}", std::process::id()));
    let base = dir.display();
    let read = |rel: &str| std::fs::read_to_string(dir.join(rel)).unwrap_or_default();

    // Hive layout by a string key (escape) and by an int key (blank → null).
    let hive = format!(
        "R:\n open {p} (id:int country:str score:int)\n save \"{base}/h\" by country score\n;"
    );
    let mut snaps: Vec<String> = Vec::new();
    for cz in [1usize, 2, 4096] {
        let _ = std::fs::remove_dir_all(&dir);
        let res = run_src(&hive, cz);
        assert_eq!(res.final_mode, rivus_core::Mode::Normal, "@cz={cz}");
        let jp = read("h/country=JP/score=10/part.csv");
        assert_eq!(jp, "id,country,score\n1,JP,10\n4,JP,10\n", "@cz={cz}");
        let nullp = read("h/country=US/score=__HIVE_DEFAULT_PARTITION__/part.csv");
        assert_eq!(nullp, "id,country,score\n2,US,\n", "@cz={cz}");
        let esc = read("h/country=a%2Fb/score=30/part.csv");
        assert_eq!(esc, "id,country,score\n3,a/b,30\n", "@cz={cz}");
        // Injectivity (#143 ②): `%` itself escapes, so the literal key
        // `a%2Fb` can never collide with the escaped form of `a/b`.
        let pct = read("h/country=a%252Fb/score=30/part.csv");
        assert_eq!(pct, "id,country,score\n5,a%2Fb,30\n", "@cz={cz}");
        snaps.push(format!("{jp}|{nullp}|{esc}|{pct}"));
    }
    assert!(
        snaps.windows(2).all(|w| w[0] == w[1]),
        "per-file bytes must be chunk-size independent"
    );

    // Serial vs parallel: identical per-file bytes (template form).
    let tmpl = format!(
        "R:\n open {p} (id:int country:str score:int)\n |> id country\n save \"{base}/t/{{country}}.csv\"\n;"
    );
    let bytes_for = |pref: rivus_runtime::MemoryPref| {
        let _ = std::fs::remove_dir_all(&dir);
        let g = rivus_parser::parse(&tmpl).expect("parse");
        std::env::set_var("RIVUS_PARALLEL_MIN_BYTES", "0");
        run(
            &g,
            RunOptions {
                chunk_size: 2,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run");
        std::env::remove_var("RIVUS_PARALLEL_MIN_BYTES");
        format!("{}|{}", read("t/JP.csv"), read("t/a%2Fb.csv"))
    };
    let serial = bytes_for(rivus_runtime::MemoryPref::Low);
    assert_eq!(
        serial, "id,country\n1,JP\n4,JP\n|id,country\n3,a/b\n",
        "template naming + content"
    );
    assert_eq!(
        serial,
        bytes_for(rivus_runtime::MemoryPref::Fast),
        "route bytes must be serial == parallel"
    );

    // Streaming bounded-memory writer (route follow-up): a tiny fd budget
    // forces evict+reopen on (nearly) every row, yet the per-file bytes stay
    // identical to the default (large-budget) run — header written once,
    // append on reopen, JSON `[`/`]` closed exactly once.
    for codec_save in ["{country}.csv", "{country}.jsonl", "{country}.json"] {
        let flow = format!(
            "R:\n open {p} (id:int country:str score:int)\n |> id country\n save \"{base}/sw/{codec_save}\"\n;"
        );
        let read_all = |sub: &str| -> Vec<(String, String)> {
            let d = dir.join(sub);
            let mut v: Vec<(String, String)> = std::fs::read_dir(&d)
                .map(|rd| {
                    rd.filter_map(|e| e.ok())
                        .map(|e| {
                            (
                                e.file_name().to_string_lossy().into_owned(),
                                std::fs::read_to_string(e.path()).unwrap(),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            v.sort();
            v
        };
        let _ = std::fs::remove_dir_all(&dir);
        std::env::set_var("RIVUS_ROUTE_FD_BUDGET", "1"); // force eviction
        run_src(&flow, 1);
        std::env::remove_var("RIVUS_ROUTE_FD_BUDGET");
        let evicted = read_all("sw");
        let _ = std::fs::remove_dir_all(&dir);
        run_src(&flow, 4096); // default large budget, single chunk
        let plain = read_all("sw");
        assert_eq!(
            evicted, plain,
            "evict/reopen must be byte-identical for {codec_save}"
        );
        assert!(!evicted.is_empty(), "wrote files for {codec_save}");
    }

    // Computed placeholder keys (s4c, #143 ①): each expression is its own
    // anonymous key, evaluated per row (chunk-size independent like the rest).
    for cz in [1usize, 4096] {
        let _ = std::fs::remove_dir_all(&dir);
        run_src(
            &format!(
                "R:\n open {p} (id:int country:str score:int)\n |> id country\n save \"{base}/x/{{substr(country,1,1)}}.csv\"\n;"
            ),
            cz,
        );
        assert_eq!(
            read("x/J.csv"),
            "id,country\n1,JP\n4,JP\n",
            "computed key @cz={cz}"
        );
        assert_eq!(read("x/U.csv"), "id,country\n2,US\n", "@cz={cz}");
    }

    // Eval-failure surfacing is strategy-independent (review #146): the
    // parallel collector path reports the same Recoverable as serial.
    for pref in [
        rivus_runtime::MemoryPref::Low,
        rivus_runtime::MemoryPref::Fast,
    ] {
        let _ = std::fs::remove_dir_all(&dir);
        let g2 = rivus_parser::parse(&format!(
            "R:\n open {p} (id:int country:str score:int)\n |> id\n save \"{base}/pe/{{$_[9]}}.csv\"\n;"
        ))
        .expect("parse");
        std::env::set_var("RIVUS_PARALLEL_MIN_BYTES", "0");
        let res = run(
            &g2,
            RunOptions {
                chunk_size: 2,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run");
        std::env::remove_var("RIVUS_PARALLEL_MIN_BYTES");
        assert!(
            res.errors
                .iter()
                .any(|e| e.message.contains("could not be evaluated")),
            "computed-key eval fails must surface @{pref:?}: {:?}",
            res.errors
        );
        assert_eq!(
            read("pe/__HIVE_DEFAULT_PARTITION__.csv"),
            "id\n1\n2\n3\n4\n5\n",
            "all rows to the sentinel @{pref:?}"
        );
    }

    // Traversal guard (review #145): a `.`/`..` key value escapes instead of
    // walking out of the declared output tree.
    let _ = std::fs::remove_dir_all(&dir);
    let evil = "id,country\n1,..\n2,.\n";
    let g = TempCsv(gendata::write_temp_bytes(
        "stress_route_dots",
        evil.as_bytes(),
    ));
    let gp = g.0.display();
    run_src(
        &format!(
            "R:\n open {gp} (id:int country:str)\n save \"{base}/t2/{{country}}/data.csv\"\n;"
        ),
        4096,
    );
    assert_eq!(
        read("t2/%2E%2E/data.csv"),
        "id,country\n1,..\n",
        "`..` must escape, not traverse"
    );
    assert_eq!(read("t2/%2E/data.csv"), "id,country\n2,.\n");
    assert!(!dir.join("data.csv").exists(), "no write outside the tree");

    // Flat layout: `v.ext` names under the base.
    let _ = std::fs::remove_dir_all(&dir);
    let flat = format!(
        "R:\n open {p} (id:int country:str score:int)\n |> id country\n save \"{base}/f\" by country as flat\n;"
    );
    run_src(&flat, 4096);
    assert_eq!(read("f/US.csv"), "id,country\n2,US\n", "flat naming");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn route_save_parallel_merge_streams_byte_identically() {
    // #143 ③ follow-up: the parallel-merge path streams the merged chunks
    // through the same `RouteWriter` as the serial operator (chunk-wise, no
    // whole-stream gather). An `ls | read` flow has no single-file size, so the
    // autotuner defers to the engine, which takes the in-memory collector path
    // — the real parallel-merge route write. With chunk_size=1 the 16 handle
    // rows split across workers (multi-worker merge); with a large chunk_size
    // the single-partition path (`flush_parallel_sinks`) runs. Both must be
    // byte-identical, per partition file, to the forced-serial run — across
    // different chunk sizes (the chunk-size independence contract).
    let base = std::env::temp_dir().join(format!("rivus_route_par_{}", std::process::id()));
    let files = base.join("in");
    let out = base.join("out");
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&files).unwrap();
    let countries = ["JP", "US", "DE", "FR"];
    for i in 0..16usize {
        let mut s = String::from("id,country\n");
        for r in 0..3usize {
            s.push_str(&format!("{},{}\n", i * 3 + r, countries[(i + r) % 4]));
        }
        std::fs::write(files.join(format!("f{i:02}.csv")), s).unwrap();
    }
    let flow = format!(
        "R:\n ls \"{}/*.csv\"\n read as csv\n save \"{}/{{country}}.csv\"\n;",
        files.display(),
        out.display()
    );
    let snap = |out: &std::path::Path| -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = std::fs::read_dir(out)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .map(|e| {
                        (
                            e.file_name().to_string_lossy().into_owned(),
                            std::fs::read_to_string(e.path()).unwrap(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        v.sort();
        v
    };
    let run_pref = |pref: rivus_runtime::MemoryPref, cz: usize| -> (Vec<(String, String)>, usize) {
        let _ = std::fs::remove_dir_all(&out);
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
        assert_eq!(res.final_mode, rivus_core::Mode::Normal, "@cz={cz}");
        (snap(&out), res.workers.len())
    };
    let (serial, _) = run_pref(rivus_runtime::MemoryPref::Low, 4096);
    assert_eq!(
        serial.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        ["DE.csv", "FR.csv", "JP.csv", "US.csv"],
        "partition set"
    );
    assert_eq!(
        serial.iter().map(|(_, c)| c.lines().count()).sum::<usize>(),
        4 + 48,
        "4 headers + all 48 rows"
    );
    let (par_multi, nworkers) = run_pref(rivus_runtime::MemoryPref::Fast, 1);
    assert_eq!(
        serial, par_multi,
        "multi-worker parallel merge must be byte-identical to serial"
    );
    // Guard against the parallel leg silently going serial (vacuous test): with
    // 2..=8 cpus, 16 handle chunks split across ≥ 2 workers. Outside that range
    // (1 cpu, or > 8 where 16 < 2×threads) the engine legitimately runs the
    // single-partition path and the byte assertion above still holds.
    let threads = std::thread::available_parallelism().map_or(1, |t| t.get());
    if (2..=8).contains(&threads) {
        assert!(
            nworkers >= 2,
            "expected the multi-worker merge path with {threads} cpus, got {nworkers} workers"
        );
    }
    let (par_single, _) = run_pref(rivus_runtime::MemoryPref::Fast, 4096);
    assert_eq!(
        serial, par_single,
        "single-partition parallel merge must be byte-identical to serial"
    );
    let _ = std::fs::remove_dir_all(&base);
}

// --- Apache Parquet reader (feature `parquet`, SUPPLY-CHAIN selected adapter,
// read-only slice). The fixture is written with the same vetted crate. ---

#[cfg(feature = "parquet")]
#[test]
fn parquet_typed_lanes_and_nulls_match_oracle() {
    use parquet::basic::{
        Compression, ConvertedType, LogicalType, Repetition, Type as PhysicalType,
    };
    use parquet::data_type::{ByteArray, ByteArrayType, DoubleType, Int64Type};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::types::Type as PqType;
    use std::sync::Arc;

    // Write a snappy-compressed fixture: i64, nullable utf8, nullable f64,
    // timestamp-millis.
    let mut path = std::env::temp_dir();
    path.push(format!("rivus_pq_lanes_{}.parquet", std::process::id()));
    let schema = Arc::new(
        PqType::group_type_builder("schema")
            .with_fields(vec![
                Arc::new(
                    PqType::primitive_type_builder("id", PhysicalType::INT64)
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    PqType::primitive_type_builder("name", PhysicalType::BYTE_ARRAY)
                        .with_converted_type(ConvertedType::UTF8)
                        .with_repetition(Repetition::OPTIONAL)
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    PqType::primitive_type_builder("score", PhysicalType::DOUBLE)
                        .with_repetition(Repetition::OPTIONAL)
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    PqType::primitive_type_builder("ts", PhysicalType::INT64)
                        .with_logical_type(Some(LogicalType::timestamp(
                            true,
                            parquet::basic::TimeUnit::MILLIS,
                        )))
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                ),
            ])
            .build()
            .unwrap(),
    );
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let file = std::fs::File::create(&path).unwrap();
    let mut w = SerializedFileWriter::new(file, schema, props).unwrap();
    {
        let mut rg = w.next_row_group().unwrap();
        let mut c = rg.next_column().unwrap().unwrap();
        c.typed::<Int64Type>()
            .write_batch(&[1, 2, 3], None, None)
            .unwrap();
        c.close().unwrap();
        let mut c = rg.next_column().unwrap().unwrap();
        c.typed::<ByteArrayType>()
            .write_batch(
                &[ByteArray::from("aki"), ByteArray::from("cho")],
                Some(&[1, 0, 1]),
                None,
            )
            .unwrap();
        c.close().unwrap();
        let mut c = rg.next_column().unwrap().unwrap();
        c.typed::<DoubleType>()
            .write_batch(&[93.46, 50.0], Some(&[1, 1, 0]), None)
            .unwrap();
        c.close().unwrap();
        let mut c = rg.next_column().unwrap().unwrap();
        c.typed::<Int64Type>()
            .write_batch(
                &[1717406400123i64, 1717406401000, 1717406402500],
                None,
                None,
            )
            .unwrap();
        c.close().unwrap();
        rg.close().unwrap();
    }
    w.close().unwrap();
    let f = TempCsv(path.clone());
    let p = f.0.display();

    // Chunk-size sweep: the row-group → chunk slicing must not change results.
    for cz in [1usize, 2, 4096] {
        let res = run_src(&format!("P:\n open {p}\n;"), cz);
        assert!(
            res.errors.is_empty(),
            "clean read @cz={cz}: {:?}",
            res.errors
        );
        assert_eq!(collect_i64(&res, "P", "id"), vec![1, 2, 3], "@cz={cz}");
        assert_eq!(
            collect_strings(&res, "P", "name"),
            vec!["aki", "", "cho"],
            "null name renders empty @cz={cz}"
        );
        assert_eq!(
            collect_strings(&res, "P", "score"),
            vec!["93.46", "50", ""],
            "f64 lane + null @cz={cz}"
        );
        // The timestamp column rides the DateTime lane at milli resolution.
        assert_eq!(
            collect_strings(&res, "P", "ts"),
            vec![
                "2024-06-03T09:20:00.123",
                "2024-06-03T09:20:01.000",
                "2024-06-03T09:20:02.500",
            ],
            "timestamp-millis → datetime lane @cz={cz}"
        );
        // Typed lanes flow into the engine: a numeric filter works end-to-end.
        let res = run_src(&format!("P:\n open {p}\n |? id >= 2\n |> id name\n;"), cz);
        assert_eq!(collect_i64(&res, "P", "id"), vec![2, 3], "@cz={cz}");
    }
}

#[cfg(feature = "parquet")]
#[test]
fn parquet_nested_schema_is_refused_with_guidance() {
    use parquet::basic::{Repetition, Type as PhysicalType};
    use parquet::data_type::Int64Type;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::types::Type as PqType;
    use std::sync::Arc;

    // A file whose root has a nested group column → Fatal naming the column.
    let mut path = std::env::temp_dir();
    path.push(format!("rivus_pq_nested_{}.parquet", std::process::id()));
    let schema = Arc::new(
        PqType::group_type_builder("schema")
            .with_fields(vec![
                Arc::new(
                    PqType::primitive_type_builder("id", PhysicalType::INT64)
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    PqType::group_type_builder("user")
                        .with_repetition(Repetition::OPTIONAL)
                        .with_fields(vec![Arc::new(
                            PqType::primitive_type_builder("age", PhysicalType::INT64)
                                .with_repetition(Repetition::OPTIONAL)
                                .build()
                                .unwrap(),
                        )])
                        .build()
                        .unwrap(),
                ),
            ])
            .build()
            .unwrap(),
    );
    let file = std::fs::File::create(&path).unwrap();
    let mut w =
        SerializedFileWriter::new(file, schema, Arc::new(WriterProperties::builder().build()))
            .unwrap();
    {
        let mut rg = w.next_row_group().unwrap();
        let mut c = rg.next_column().unwrap().unwrap();
        c.typed::<Int64Type>()
            .write_batch(&[1], None, None)
            .unwrap();
        c.close().unwrap();
        let mut c = rg.next_column().unwrap().unwrap();
        c.typed::<Int64Type>()
            .write_batch(&[7], Some(&[2]), None)
            .unwrap();
        c.close().unwrap();
        rg.close().unwrap();
    }
    w.close().unwrap();
    let f = TempCsv(path.clone());
    let p = f.0.display();
    let res = run_src(&format!("P:\n open {p}\n;"), 4096);
    assert!(
        res.errors.iter().any(|e| {
            e.is_fatal() && e.message.contains("'user'") && e.message.contains("nested")
        }),
        "nested column must be a Fatal naming the column: {:?}",
        res.errors
    );
}

#[cfg(not(feature = "parquet"))]
#[test]
fn parquet_without_the_feature_refuses_the_plan_pre_run() {
    // The default (zero-dependency) build must refuse a Parquet plan before
    // running — never a silent empty read (same shape as regex/gzip).
    let g = rivus_parser::parse("P:\n open data.parquet\n;").expect("parse is always std-only");
    let err = run(&g, RunOptions::default()).expect_err("must refuse pre-run");
    let msg = err.to_string();
    assert!(
        msg.contains("`parquet` feature") && msg.contains("--features parquet"),
        "teaches the rebuild: {msg}"
    );
}
