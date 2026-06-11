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
