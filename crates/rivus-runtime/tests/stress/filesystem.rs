//! Filesystem integration (design 27). Slice 1: the `filename` provenance
//! column (`open … with filename`).

use super::*;

#[test]
fn with_filename_appends_source_path_column() {
    let f = TempCsv(gendata::write_temp_bytes(
        "fs_fname",
        b"id,v\n1,a\n2,b\n3,c\n",
    ));
    let p = f.0.display();
    let path = f.0.to_string_lossy().to_string();
    let flow = format!("F:\n open {p} (id:int v:str) with filename\n |> id filename\n;");
    for cz in [1usize, 2, 4096] {
        // Every row carries its source path in `filename`.
        assert_eq!(
            collect_strings(&run_src(&flow, cz), "F", "filename"),
            vec![path.clone(), path.clone(), path.clone()],
            "filename column = source path on every row @cz={cz}",
        );
        // The data columns are unchanged.
        assert_eq!(collect_i64(&run_src(&flow, cz), "F", "id"), vec![1, 2, 3]);
    }
}

#[test]
fn without_with_filename_no_extra_column() {
    // Zero regression: no `filename` column unless asked.
    let f = TempCsv(gendata::write_temp_bytes("fs_nofname", b"id,v\n1,a\n"));
    let p = f.0.display();
    let res = run_src(&format!("F:\n open {p} (id:int v:str)\n |> id v\n;"), 4096);
    let o = res
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some("F"))
        .unwrap();
    assert!(
        o.chunks[0].schema.index_of("filename").is_none(),
        "no filename column without `with filename`",
    );
}

#[test]
fn with_filename_round_trips_through_to_source() {
    let f = TempCsv(gendata::write_temp_bytes("fs_rt", b"id,v\n1,a\n"));
    let p = f.0.display();
    let src = format!("F:\n open {p} (id:int v:str) with filename\n |> id\n;");
    let g = rivus_parser::parse(&src).expect("parse");
    let regen = g.to_source();
    assert!(
        regen.contains("with filename"),
        "to_source must re-emit `with filename`: {regen}",
    );
    // Re-parsing the regenerated source is stable.
    let g2 = rivus_parser::parse(&regen).expect("reparse");
    assert_eq!(g2.to_source(), regen, "round-trip is a fixed point");
}

#[test]
fn with_filename_on_json_is_a_clear_error() {
    // `with filename` is CSV/TSV-only for now (slice 1); a JSON source rejects it
    // with an actionable message rather than silently ignoring it.
    let err = rivus_parser::parse("F: open data.jsonl with filename ;").unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("CSV") || msg.contains("filename"),
        "clear error for `with filename` on JSON: {msg}",
    );
}
