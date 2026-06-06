//! DuckDB parity oracle (DuckDB-ETL migration track, theme 1).
//!
//! The migration blocker is that **null handling must not change output row
//! counts** vs DuckDB. This harness runs the same null-semantics queries in
//! Rivus and in DuckDB and asserts the row counts agree.
//!
//! DuckDB is an **external** oracle: the test shells out to the `duckdb` CLI
//! only when it is on `PATH`, and **skips** (does not fail) otherwise — so the
//! default build and CI gate need **no third-party dependency** (zero-dep
//! default is preserved). To run it live: install the `duckdb` CLI (official
//! release binary) and `cargo test --test duckdb_parity`.
//!
//! Each case writes CSV input(s), evaluates the Rivus flow, and compares
//! `total_rows_out()` against DuckDB's `COUNT(*)` of the equivalent SQL.

use rivus_runtime::{run, RunOptions};
use std::process::Command;

/// Is the `duckdb` CLI available on PATH?
fn duckdb_available() -> bool {
    Command::new("duckdb")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Row count of `SELECT count(*) FROM (<inner>)` via the DuckDB CLI.
fn duckdb_count(inner_sql: &str) -> u64 {
    let sql = format!("SELECT count(*) AS n FROM ({inner_sql}) t;");
    let out = Command::new("duckdb")
        .args(["-csv", "-c", &sql])
        .output()
        .expect("run duckdb");
    assert!(
        out.status.success(),
        "duckdb failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let s = String::from_utf8_lossy(&out.stdout);
    // Output is a 2-line CSV: header `n`, then the count.
    s.lines()
        .nth(1)
        .and_then(|l| l.trim().parse().ok())
        .unwrap_or_else(|| panic!("unexpected duckdb output: {s:?}"))
}

/// Rivus row count of a flow's (single) output.
fn rivus_count(flow: &str) -> u64 {
    let g = rivus_parser::parse(flow).expect("parse");
    run(&g, RunOptions::default())
        .expect("run")
        .total_rows_out()
}

struct Tmp(std::path::PathBuf);
impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
fn write_tmp(tag: &str, body: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("rivus_ddb_{tag}_{}.csv", std::process::id()));
    std::fs::write(&p, body).unwrap();
    p
}

#[test]
fn parity_null_filter_join_group_counts() {
    if !duckdb_available() {
        eprintln!("duckdb not on PATH — skipping live parity oracle (zero-dep gate unaffected)");
        return;
    }

    // --- Case 1: filter — `age >= 0` must exclude null ages (not treat them 0).
    let people = write_tmp("people", "id,age\n1,25\n2,\n3,0\n4,40\n");
    let _g1 = Tmp(people.clone());
    let pp = people.display();
    let r = rivus_count(&format!(
        "F: open {pp} (id:int age:int) |? age >= 0 |> id\n;"
    ));
    let d = duckdb_count(&format!(
        "SELECT id FROM read_csv('{pp}', header=true, columns={{'id':'INT','age':'INT'}}) WHERE age >= 0"
    ));
    assert_eq!(r, d, "filter null parity: rivus={r} duckdb={d}");

    // --- Case 2: inner join — a null join key must not match (no count inflation).
    let lf = write_tmp("ljoin", "id,k\n1,a\n2,\n3,b\n");
    let rf = write_tmp("rjoin", "k,v\na,10\n,99\nb,20\n");
    let _g2 = Tmp(lf.clone());
    let _g3 = Tmp(rf.clone());
    let (lp, rp) = (lf.display(), rf.display());
    let r = rivus_count(&format!(
        "L: open {lp} (id:int k:str) ;\nR: open {rp} (k:str v:int) ;\nJ: L & R on k |> id v\n;"
    ));
    let d = duckdb_count(&format!(
        "SELECT l.id FROM read_csv('{lp}', header=true, columns={{'id':'INT','k':'VARCHAR'}}) l \
         JOIN read_csv('{rp}', header=true, columns={{'k':'VARCHAR','v':'INT'}}) r ON l.k = r.k"
    ));
    assert_eq!(r, d, "inner-join null-key parity: rivus={r} duckdb={d}");

    // --- Case 3: group-by — null keys fold into one group, kept (COUNT(*) rows).
    let gf = write_tmp("group", "g,v\na,1\n,2\n,3\nb,4\n");
    let _g4 = Tmp(gf.clone());
    let gp = gf.display();
    let r = rivus_count(&format!("G: open {gp} (g:str v:int) |# g count\n;"));
    let d = duckdb_count(&format!(
        "SELECT g FROM read_csv('{gp}', header=true, columns={{'g':'VARCHAR','v':'INT'}}) GROUP BY g"
    ));
    assert_eq!(
        r, d,
        "group-by null-key parity (one null group): rivus={r} duckdb={d}"
    );

    // --- Case 4: maintainer's real ETL (scaled) — extract rows whose 34-char id
    // has "0059" at positions 22-25, from data carrying a blank / `***.**` /
    // garbage `0-.-2` value and a broken (wrong-arity) row. The migration blocker
    // is **count parity**: a value that won't parse must not drop the row. Rivus
    // keeps such rows (val → null); DuckDB matches when `val` is read as text and
    // the broken row is skipped (`ignore_errors`). Both → 5 extracted rows.
    let id_tag = |tag: &str| format!("{}{}{}", "A".repeat(21), tag, "B".repeat(9));
    let m = id_tag("0059");
    let x = id_tag("1234");
    let body = format!(
        "ts,id,val\n\
         260601120000,{m},123.4500\n260601120001,{m},\n260601120002,{m},***.**\n\
         260601120003,{m},0-.-2\n260601120004,{x},500.0\n260601120005,{m},-999999.9999\n\
         260601120006,brokenrow,with,too,many\n"
    );
    let ef = write_tmp("etl", &body);
    let _g5 = Tmp(ef.clone());
    let ep = ef.display();
    let r = rivus_count(&format!(
        "E: open {ep} (ts:datetime(\"yyMMddHHmmss\") id:str val:f64) |? substr(id, 22, 4) == \"0059\" |> id\n;"
    ));
    let d = duckdb_count(&format!(
        "SELECT id FROM read_csv('{ep}', header=true, \
         columns={{'ts':'VARCHAR','id':'VARCHAR','val':'VARCHAR'}}, ignore_errors=true) \
         WHERE substr(id, 22, 4) = '0059'"
    ));
    assert_eq!(
        r, d,
        "real-ETL id-substring extract count parity: rivus={r} duckdb={d}"
    );
}
