//! Provenance (`with source`) — §28.6 slice 2-②.
//!
//! Pins the runtime activation of `with source`: the origin handle is stamped on
//! every chunk and the `source.uri` / `source.scheme` accessor materializes it,
//! `null` when provenance is off (continue-first), and — the byte-identity gate
//! — the stamped handle is identical across serial, byte-range parallel, and
//! chunk size (each worker derives the same handle from the same path).

use super::*;

/// `source.uri` materializes the opened path on every row; `source.scheme`
/// resolves the (deterministic) transport scheme.
#[test]
fn source_uri_and_scheme_materialize() {
    let f = TempCsv(gendata::write_temp_bytes(
        "prov_uri",
        b"id,name\n1,a\n2,b\n3,c\n",
    ));
    let p = f.0.display().to_string();
    let res = run_src(
        &format!("P:\n open {p} with source\n |> id (source.uri) as src (source.scheme) as sch\n;"),
        2,
    );
    let src = collect_strings(&res, "P", "src");
    let sch = collect_strings(&res, "P", "sch");
    assert_eq!(src.len(), 3, "one src per data row");
    assert!(
        src.iter().all(|s| *s == p),
        "every row's source.uri must be the opened path {p:?}, got {src:?}"
    );
    assert!(
        sch.iter().all(|s| s == "file"),
        "a local path resolves scheme=file, got {sch:?}"
    );
}

/// Without `with source`, the accessor is `null` for every row (continue-first):
/// the column renders empty and is never the path.
#[test]
fn provenance_off_yields_null_source() {
    let f = TempCsv(gendata::write_temp_bytes(
        "prov_off",
        b"id,name\n1,a\n2,b\n",
    ));
    let p = f.0.display().to_string();
    let res = run_src(&format!("P:\n open {p}\n |> id (source.uri) as src\n;"), 4);
    let src = collect_strings(&res, "P", "src");
    assert_eq!(src.len(), 2);
    assert!(
        src.iter().all(|s| s.is_empty()),
        "no provenance → null (empty) source column, got {src:?}"
    );
}

/// `with filename` materializes a `filename` column (= `source.uri`) at the end
/// of each chunk (the §27.1 sugar), and the `source.uri` accessor still works
/// alongside it.
#[test]
fn with_filename_materializes_column() {
    let f = TempCsv(gendata::write_temp_bytes(
        "prov_filename",
        b"id,name\n1,a\n2,b\n",
    ));
    let p = f.0.display().to_string();
    let res = run_src(
        &format!(
            "P:\n open {p} (id:int name:str) with filename\n |> id name filename (source.uri) as src\n;"
        ),
        4,
    );
    let fname = collect_strings(&res, "P", "filename");
    let src = collect_strings(&res, "P", "src");
    assert_eq!(fname.len(), 2, "one filename per data row");
    assert!(
        fname.iter().all(|s| *s == p),
        "the materialized filename column must be the path, got {fname:?}"
    );
    assert!(
        src.iter().all(|s| *s == p),
        "source.uri must work under with filename too, got {src:?}"
    );
}

/// Collision rule (§27.1): when the data already has a `filename` column, the
/// materialized provenance column is `filename_r` (the join rule) and the data
/// column is preserved unchanged.
#[test]
fn with_filename_collision_uses_filename_r() {
    let f = TempCsv(gendata::write_temp_bytes(
        "prov_collide",
        b"filename,v\nx,1\ny,2\n",
    ));
    let p = f.0.display().to_string();
    let res = run_src(
        &format!("P:\n open {p} (filename:str v:int) with filename\n |> filename filename_r\n;"),
        4,
    );
    let orig = collect_strings(&res, "P", "filename");
    let prov = collect_strings(&res, "P", "filename_r");
    assert_eq!(
        orig,
        vec!["x", "y"],
        "the data filename column is preserved"
    );
    assert!(
        prov.iter().all(|s| *s == p),
        "the provenance column is filename_r = path, got {prov:?}"
    );
}

/// Byte-identity gate (#41 / §28.6): the stamped provenance is identical across
/// serial (Low) and the byte-range parallel reader (Fast), and across chunk
/// size. A >1 MiB file + `MemoryPref::Fast` forces the parallel reader (no env
/// vars → no cross-test races); each run `save`s and the output files are
/// compared byte-for-byte.
#[test]
fn provenance_parallel_byte_identical() {
    let rows = 120_000usize; // > 1 MiB → crosses the Fast parallel floor
    let mut text = String::from("id,name\n");
    for i in 0..rows {
        text.push_str(&format!("{i},n{}\n", i % 97));
    }
    let f = TempCsv(gendata::write_temp_bytes("prov_par", text.as_bytes()));
    let p = f.0.display().to_string();

    let run_to_file = |cs: usize, pref: rivus_runtime::MemoryPref, out: &std::path::Path| {
        // Filter (engages the byte-range parallel path) + a provenance column.
        let src = format!(
            "D:\n open {p} with source\n |? id >= 1000\n |> id (source.uri) as src\n save {}\n;",
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

    // Serial oracle.
    let ser_out = TempCsv(gendata::write_temp_bytes("prov_serial", b""));
    run_to_file(1024, rivus_runtime::MemoryPref::Low, &ser_out.0);
    let oracle = std::fs::read_to_string(&ser_out.0).expect("read serial out");
    assert!(oracle.lines().count() > 1000, "oracle unexpectedly small");
    // Provenance is actually ON: every data line carries the path (not empty),
    // so the byte-identity below is a real equality, not a vacuous all-null one.
    assert!(
        oracle.lines().skip(1).all(|l| l.ends_with(&p)),
        "every data row must carry source.uri = {p:?}"
    );

    for cs in [1usize, 1000, rows] {
        let par_out = TempCsv(gendata::write_temp_bytes("prov_parallel", b""));
        let res = run_to_file(cs, rivus_runtime::MemoryPref::Fast, &par_out.0);
        assert!(
            !res.workers.is_empty(),
            "expected the byte-range parallel reader to engage @cs={cs}"
        );
        let got = std::fs::read_to_string(&par_out.0).expect("read parallel out");
        assert_eq!(got, oracle, "parallel provenance != serial @cs={cs}");
    }
}

/// The materialized `filename` column (slice 2-②b) is byte-identical across
/// serial and the byte-range parallel reader, and across chunk size — each
/// worker appends the same column with the same value from the same path.
#[test]
fn with_filename_parallel_byte_identical() {
    let rows = 120_000usize; // > 1 MiB → crosses the Fast parallel floor
    let mut text = String::from("id,name\n");
    for i in 0..rows {
        text.push_str(&format!("{i},n{}\n", i % 97));
    }
    let f = TempCsv(gendata::write_temp_bytes("prov_fn_par", text.as_bytes()));
    let p = f.0.display().to_string();

    let run_to = |cs: usize, pref: rivus_runtime::MemoryPref, out: &std::path::Path| {
        let src = format!(
            "D:\n open {p} with filename\n |? id >= 1000\n save {}\n;",
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

    let ser_out = TempCsv(gendata::write_temp_bytes("prov_fn_serial", b""));
    run_to(1024, rivus_runtime::MemoryPref::Low, &ser_out.0);
    let oracle = std::fs::read_to_string(&ser_out.0).expect("read serial out");
    // The materialized column is present (header ends with it) and carries the
    // path on every data row.
    assert!(
        oracle.lines().next().unwrap().ends_with("filename"),
        "header must end with the materialized filename column"
    );
    assert!(
        oracle.lines().skip(1).all(|l| l.ends_with(&p)),
        "every data row must end with filename = {p:?}"
    );

    for cs in [1usize, 1000, rows] {
        let par_out = TempCsv(gendata::write_temp_bytes("prov_fn_parallel", b""));
        let res = run_to(cs, rivus_runtime::MemoryPref::Fast, &par_out.0);
        assert!(
            !res.workers.is_empty(),
            "expected the byte-range parallel reader to engage @cs={cs}"
        );
        let got = std::fs::read_to_string(&par_out.0).expect("read parallel out");
        assert_eq!(got, oracle, "parallel with-filename != serial @cs={cs}");
    }
}
