//! Real-ETL shaped regression cases (maintainer's actual workloads, scaled
//! down). The first: a `yyMMddHHmmss` timestamp, a 34-char ASCII id, and a
//! numeric value column carrying real dirt — a simple blank (missing), an
//! explicit `***.**` missing marker, garbage like `0-.-2` (corruption), and a
//! broken (wrong-arity) row — from which we extract only the rows whose id has
//! `0059` at positions 22-25 (the meaningful subset). Continue-first: the dirt
//! never halts the run; unparseable values become `null`.

use super::*;

/// A 34-char ASCII id with `tag` (4 chars) at positions 22-25 (1-based):
/// 21 chars + tag + 9 chars.
fn id_with_tag(tag: &str) -> String {
    format!("{}{}{}", "A".repeat(21), tag, "B".repeat(9))
}

#[test]
fn real_etl_extract_by_id_substring_with_dirty_values() {
    let m = id_with_tag("0059"); // matches the id[22..26] == "0059" filter
    let x = id_with_tag("1234"); // does not match
    let text = format!(
        "ts,id,val\n\
         260601120000,{m},123.4500\n\
         260601120001,{m},\n\
         260601120002,{m},***.**\n\
         260601120003,{m},0-.-2\n\
         260601120004,{x},500.0\n\
         260601120005,{m},-999999.9999\n\
         260601120006,brokenrow,with,too,many\n"
    );
    let f = TempCsv(gendata::write_temp_bytes("real_etl", text.as_bytes()));
    let p = f.0.display();
    let flow = format!(
        "E:\n open {p} (ts:datetime(\"yyMMddHHmmss\") id:str val:f64)\n \
         |? substr(id, 22, 4) == \"0059\"\n |> id val\n;"
    );
    for cz in [1usize, 2, 4096] {
        let res = run_src(&flow, cz);
        // Exactly the five `0059` rows survive — the `1234` row is filtered out,
        // the broken (wrong-arity) row is skipped (continue-first), and a null
        // `val` never drops its row (DuckDB count parity: a value that won't
        // parse must not change the row count).
        assert_eq!(
            collect_strings(&res, "E", "id").len(),
            5,
            "extract exactly the id[22..26]==0059 rows @cz={cz}",
        );
        // Valid numbers kept; blank / ***.** / 0-.-2 all became null (empty).
        assert_eq!(
            collect_strings(&res, "E", "val"),
            vec!["123.45", "", "", "", "-999999.9999"],
            "dirty values → null, valid numbers preserved @cz={cz}",
        );
        // Continue-first: garbage + a broken row never raise a fatal.
        assert!(
            !res.errors.iter().any(rivus_core::ErrorEvent::is_fatal),
            "dirty real-ETL input must never fatal @cz={cz}",
        );
        // The two non-empty unparseable `val` cells (`***.**`, `0-.-2`) surface
        // as a single "set to null" summary reporting **2 values**; the simple
        // blank is missing, not counted. And the broken row is surfaced as a
        // skipped malformed row (never-silent, continue-first).
        let fail_events: Vec<_> = res
            .errors
            .iter()
            .filter(|e| e.message.contains("could not be parsed; set to null"))
            .collect();
        assert_eq!(
            fail_events.len(),
            1,
            "one val parse-failure summary @cz={cz}"
        );
        assert!(
            fail_events[0].message.contains("2 value(s)"),
            "the 2 non-empty garbage values surface (blank not counted) @cz={cz}: {}",
            fail_events[0].message,
        );
        assert!(
            res.errors
                .iter()
                .any(|e| e.message.contains("malformed row")),
            "the broken (wrong-arity) row is surfaced as skipped @cz={cz}",
        );
    }
}
