//! Plan Validation Gate specs (#191/#195/#200 — the "errors that teach" pass).
//!
//! The gate runs plan-time (before dispatch) and refuses with guidance instead
//! of letting a typo become a silent wrong answer at runtime. These specs pin
//! both sides of the honesty rule (§32.1): a *declared* schema turns an unknown
//! column into a plan error with a did-you-mean hint, while an *inferred*
//! schema stays out of the gate's reach (the runtime warns handle it).

use super::*;

fn build_err(src: &str) -> String {
    let graph = rivus_parser::parse(src).expect("parse");
    match run(&graph, RunOptions::default()) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected the plan gate to refuse: {src}"),
    }
}

#[test]
fn unknown_column_with_declared_schema_is_plan_error_with_hint() {
    // #191: `aeg` vs a declared (name, age) schema — refused before running,
    // with an OSA did-you-mean (transposition = 1 edit) and the column list.
    let text = "name,age\nAlice,70\nBob,30\n";
    let f = TempCsv(gendata::write_temp_bytes("gate_typo", text.as_bytes()));
    let p = f.0.display();
    let e = build_err(&format!(
        "S:\n open {p} (name:str age:int)\n |? aeg >= 60\n;"
    ));
    assert!(e.contains("unknown column 'aeg'"), "defect named: {e}");
    assert!(e.contains("did you mean 'age'"), "did-you-mean hint: {e}");
    assert!(
        e.contains("name, age"),
        "available columns must be listed: {e}"
    );
}

#[test]
fn unknown_column_with_inferred_schema_stays_runtime_policy() {
    // Honesty rule (§32.1): no declared schema → the static schema is unknown
    // → the gate must NOT guess. The flow still runs (runtime policy applies).
    let text = "name,age\nAlice,70\nBob,30\n";
    let f = TempCsv(gendata::write_temp_bytes("gate_inferred", text.as_bytes()));
    let p = f.0.display();
    let graph = rivus_parser::parse(&format!("S:\n open {p}\n |? aeg >= 60\n;")).expect("parse");
    let res = run(&graph, RunOptions::default());
    assert!(
        res.is_ok(),
        "an inferred-schema flow must still run (runtime policy, not a plan error): {res:?}"
    );
}

#[test]
fn empty_program_is_a_plan_error_not_ok() {
    // #195: `rivus check` used to report `ok: 0 node(s)` for an empty program.
    let graph = rivus_parser::parse("").expect("empty parse is fine");
    let e = rivus_runtime::plan_validate(&graph)
        .expect_err("empty program must be refused")
        .to_string();
    assert!(e.contains("no flow found"), "teaches the mistake: {e}");
}

#[test]
fn route_hook_is_rejected_up_front_not_silently_ignored() {
    // #200: `on error: route X` parses but is not wired — refusing beats a no-op.
    let text = "name,age\nAlice,70\n";
    let f = TempCsv(gendata::write_temp_bytes("gate_route", text.as_bytes()));
    let p = f.0.display();
    let e = build_err(&format!("X:\n open {p}\n on error: route Errs;\n;"));
    assert!(e.contains("route Errs"), "names the hook: {e}");
    assert!(e.contains("not yet implemented"), "honest wording: {e}");
}

#[test]
fn log_hook_narrates_on_the_error_stream() {
    // #200 (the wired half): `on error: log "…"` emits an Info `log: …` event
    // when a matching error fires. An unparseable `age` cell provides the error.
    let text = "name,age\nAlice,seventy\nBob,30\n";
    let f = TempCsv(gendata::write_temp_bytes("gate_log", text.as_bytes()));
    let p = f.0.display();
    let res = run_src(
        &format!("X:\n open {p} (name:str age:int)\n on error: log \"age went bad\";\n;"),
        4096,
    );
    assert!(
        res.errors
            .iter()
            .any(|e| e.message.contains("log: age went bad")),
        "the log hook must narrate on the error stream: {:?}",
        res.errors
    );
}

#[test]
fn bare_agg_word_in_group_teaches_func_col_form() {
    // A bare `count` in `|# d count max:v` parses as a *phantom group key* (it
    // collides with the always-emitted count column). With a declared schema
    // the gate refuses and teaches the `func:col` form instead.
    let text = "d,v\na,1\nb,2\n";
    let f = TempCsv(gendata::write_temp_bytes("gate_baregg", text.as_bytes()));
    let p = f.0.display();
    let e = build_err(&format!(
        "D:\n open {p} (d:str v:int)\n |# d count max:v\n;"
    ));
    assert!(e.contains("unknown column 'count'"), "defect named: {e}");
    assert!(
        e.contains("`count` is always emitted") && e.contains("func:col"),
        "teaches the aggregate form: {e}"
    );
}

#[test]
fn provenance_filename_column_passes_the_gate() {
    // `with filename` materializes a `filename` column (§28.6) — the static
    // schema must carry it so a downstream projection is not a false positive.
    let text = "id,name\n1,a\n2,b\n";
    let f = TempCsv(gendata::write_temp_bytes("gate_prov", text.as_bytes()));
    let p = f.0.display();
    let res = run_src(
        &format!("P:\n open {p} (id:int name:str) with filename\n |> id filename\n;"),
        4096,
    );
    let names = collect_strings(&res, "P", "filename");
    assert_eq!(names.len(), 2, "filename column must materialize");
}
