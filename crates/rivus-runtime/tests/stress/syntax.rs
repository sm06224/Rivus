//! Flow syntax: fan-out merge, value holes, named-flow reuse, validators.
//!
//! Moved verbatim from the former monolithic `stress.rs` (design 26 §26.8.1,
//! mechanical move-only split; logic unchanged).

use super::*;

#[test]
fn fanout_merge_conserves_rows() {
    let rows = 20_000;
    let data = gendata::clean(rows, 99);
    let f = TempCsv(gendata::write_temp("stress_fanout", &data));
    let p = f.0.display();

    // Partition by age into 3 disjoint, exhaustive branches, then merge: the
    // merged row count must equal the clean input row count exactly.
    let src = format!(
        "Src:\n open {p}\n \
          -> A: |? age >= 60 ;\n \
          -> B: |? age >= 30 ;\n \
          -> C: |? age <  30 ;\n;\n\
         M:\n A + B + C\n;"
    );
    let res = run_src(&src, 4096);
    let merged = res
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some("M"))
        .expect("M output");
    let merged_rows: usize = merged.chunks.iter().map(|c| c.len).sum();
    // A(age>=60) + B(age>=30) + C(age<30) overlaps on [60,90): A⊂B. So the
    // conservation check is: B ∪ C == all rows, and A is a subset of B.
    // Here we assert the total equals |B|+|C|+|A| = rows + |A|.
    let a = run_src(&format!("F:\n open {p}\n |? age >= 60\n;"), 4096).total_rows_out() as usize;
    assert_eq!(merged_rows, rows + a, "fan-out/merge row conservation");
}

#[test]
fn resource_literal_computed_column_renders_uri() {
    // A `resource("uri")` literal evaluates end-to-end (parse -> IR -> eval) to a
    // Resource-lane column whose cells render the uri (§28.1 / slice 1b-②).
    let text = "name\naki\nben\n";
    let f = TempCsv(gendata::write_temp_bytes("res_lit", text.as_bytes()));
    let p = f.0.display();
    let src = format!("F:\n open {p}\n |> (resource(\"file:///data/a.csv\")) as src\n;");
    let res = run_src(&src, 4096);
    let out = res
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some("F"))
        .expect("F output");
    let mut rows = 0;
    for c in &out.chunks {
        // The projection keeps only the computed column, on the Resource lane.
        assert_eq!(c.columns.len(), 1, "projection keeps only the computed col");
        assert_eq!(c.columns[0].dtype().to_string(), "resource");
        for r in 0..c.len {
            assert_eq!(c.value(r, 0).to_string(), "file:///data/a.csv");
            rows += 1;
        }
    }
    assert_eq!(rows, 2);
}

/// Collect a column's per-row `Value::to_string()` across all chunks of the
/// output labeled `label` (used to inspect the datetime lane's ISO rendering).
#[test]
fn unbound_value_hole_is_surfaced_never_silent() {
    // A `$x` hole reaching execution with no binding must be surfaced (never
    // silent) — not just evaluate to null and drop rows quietly (§25.3).
    let text = "name,age\nalice,30\nbob,15\n";
    let f = TempCsv(gendata::write_temp_bytes("unbound_hole", text.as_bytes()));
    let p = f.0.display();
    let res = run_src(
        &format!("T:\n open {p}\n |? age >= $min\n |> name\n;"),
        4096,
    );
    let surfaced = res
        .errors
        .iter()
        .filter(|e| e.message.contains("value hole $min is unbound"))
        .count();
    assert_eq!(
        surfaced, 1,
        "unbound hole not surfaced once: {:?}",
        res.errors
    );
    // Continue-first: it is recoverable, not fatal.
    assert!(!res.errors.iter().any(rivus_core::ErrorEvent::is_fatal));
}

#[test]
fn bound_value_hole_is_observationally_identical_to_inline_literal() {
    // End-to-end (§25.3): `| clean min=20` over `clean: … |? age >= $min`
    // produces the same output as writing `|? age >= 20` inline — the bound
    // hole desugars to the literal byte-identically.
    let text = "name,age\nalice,30\nbob,15\ncarol,42\ndan,19\n";
    let f = TempCsv(gendata::write_temp_bytes("bound_hole", text.as_bytes()));
    let p = f.0.display();
    let applied = run_src(
        &format!("clean:\n open {p}\n |? age >= $min\n |> name age\n;\nR:\n open {p}\n | clean min=20\n;"),
        4096,
    );
    let inline = run_src(
        &format!("R:\n open {p}\n |? age >= 20\n |> name age\n;"),
        4096,
    );
    assert_eq!(
        collect_strings(&applied, "R", "name"),
        collect_strings(&inline, "R", "name"),
        "bound `$min` differs from the inline literal"
    );
    assert_eq!(
        collect_strings(&applied, "R", "name"),
        vec!["alice", "carol"]
    );
}

#[test]
fn named_flow_apply_is_observationally_identical_to_inline() {
    // End-to-end (§25.4): `R: open f | clean` produces the *same* output as
    // writing `clean`'s transforms inline in R — the desugar is byte-identical.
    let text = "name,age\nalice,30\nbob,15\ncarol,42\ndan,19\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "named_flow_apply",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let applied = run_src(
        &format!("clean:\n open {p}\n |? age >= 20\n |> name age\n;\nR:\n open {p}\n | clean\n;"),
        4096,
    );
    let inline = run_src(
        &format!("R:\n open {p}\n |? age >= 20\n |> name age\n;"),
        4096,
    );
    assert_eq!(
        collect_strings(&applied, "R", "name"),
        collect_strings(&inline, "R", "name"),
        "`| clean` names differ from inline"
    );
    assert_eq!(
        collect_i64(&applied, "R", "age"),
        collect_i64(&inline, "R", "age"),
        "`| clean` ages differ from inline"
    );
    // The kept set is the adults (filter applied through `| clean`).
    assert_eq!(
        collect_strings(&applied, "R", "name"),
        vec!["alice", "carol"]
    );
}

#[test]
fn validate_dispositions_surface_and_dispose_chunk_size_independent() {
    // `|! pred warn|reject|halt` (#83 §24): a row failing `pred` is disposed of
    // per the disposition and ALWAYS surfaced (never silent). warn keeps every
    // row, reject drops the failing rows; the failure count is chunk-size
    // independent and reject is byte-identical serial vs parallel; halt is fatal.
    let text = "id,age\n1,25\n2,-5\n3,40\n4,200\n5,18\n"; // ages -5 and 200 fail
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_validate",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let warn = format!("V:\n open {p} (id:int age:int)\n |! age >= 0, age <= 120 warn\n |> id\n;");
    let reject =
        format!("V:\n open {p} (id:int age:int)\n |! age >= 0, age <= 120 reject\n |> id\n;");
    for cz in [1usize, 2, 4096] {
        // warn: all 5 rows survive; exactly one summary counting the 2 failures.
        let res = run_src(&warn, cz);
        assert_eq!(res.total_rows_out(), 5, "warn keeps every row @cz={cz}");
        let warned = res
            .errors
            .iter()
            .filter(|e| e.message.contains("2 row(s) failed") && e.message.contains("(warn)"))
            .count();
        assert_eq!(
            warned, 1,
            "one warn summary of 2 @cz={cz}: {:?}",
            res.errors
        );

        // reject: the 2 failing rows are dropped (id 1,3,5 survive) and surfaced.
        let res2 = run_src(&reject, cz);
        assert_eq!(
            collect_i64(&res2, "V", "id"),
            vec![1, 3, 5],
            "reject drops failing @cz={cz}"
        );
        assert!(
            res2.errors
                .iter()
                .any(|e| e.message.contains("2 row(s) failed") && e.message.contains("(reject)")),
            "reject must surface the drop @cz={cz}: {:?}",
            res2.errors
        );
    }

    // reject is byte-identical serial vs parallel (row-wise predicate).
    let rows_for = |pref: rivus_runtime::MemoryPref| {
        let g = rivus_parser::parse(&reject).expect("parse");
        std::env::set_var("RIVUS_PARALLEL_MIN_BYTES", "0");
        let res = run(
            &g,
            RunOptions {
                chunk_size: 2,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run");
        std::env::remove_var("RIVUS_PARALLEL_MIN_BYTES");
        collect_i64(&res, "V", "id")
    };
    assert_eq!(
        rows_for(rivus_runtime::MemoryPref::Low),
        rows_for(rivus_runtime::MemoryPref::Fast),
        "reject byte-identical serial vs parallel"
    );

    // halt: a failing row halts the run (fatal on the error stream).
    let halt = format!("V:\n open {p} (id:int age:int)\n |! age <= 120 halt\n |> id\n;");
    let res3 = run_src(&halt, 4096);
    assert_eq!(
        res3.final_mode,
        rivus_core::Mode::Halted,
        "halt must halt the run"
    );
    assert!(
        res3.errors.iter().any(rivus_core::ErrorEvent::is_fatal),
        "halt must raise a fatal: {:?}",
        res3.errors
    );
}

#[test]
fn regex_flow_is_refused_without_the_feature_and_filters_with_it() {
    // §29.5-6 s4 never-silent gate: a flow using `~` / regexp() must never
    // quietly evaluate every test to false in a build without the `regex`
    // feature — the engine refuses the plan before running, with guidance.
    // With the feature, the infix filters like regexp() and is chunk-size
    // independent.
    let text = "id,code\n1,JP-1234\n2,US-9999\n3,JP-77\n";
    let f = TempCsv(gendata::write_temp_bytes("stress_regex", text.as_bytes()));
    let p = f.0.display();
    let src = format!("R:\n open {p} (id:int code:str)\n |? code ~ '^JP-\\d{{4}}$'\n |> id\n;");
    #[cfg(not(feature = "regex"))]
    {
        let g = rivus_parser::parse(&src).expect("parse");
        let err = run(
            &g,
            RunOptions {
                chunk_size: 4096,
                ..Default::default()
            },
        )
        .expect_err("a feature-less build must refuse a regex plan, not run it");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("regex"),
            "the refusal must name the missing feature: {msg}"
        );
    }
    #[cfg(feature = "regex")]
    for cz in [1usize, 2, 4096] {
        let res = run_src(&src, cz);
        assert_eq!(
            collect_i64(&res, "R", "id"),
            vec![1],
            "`~` must match exactly JP-1234 @cz={cz}"
        );
    }
}

#[test]
fn positional_reference_reads_schema_order_and_out_of_range_is_counted() {
    // `$_[i]` (§29.5-6 s4): 0-based schema order, chunk-size independent;
    // an out-of-range index → null + counted (continue-first, never silent).
    let text = "id,age\n1,25\n2,8\n3,40\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_positional",
        text.as_bytes(),
    ));
    let p = f.0.display();
    for cz in [1usize, 2, 4096] {
        let res = run_src(
            &format!("P:\n open {p} (id:int age:int)\n |? $_[1] >= 20\n |> id\n;"),
            cz,
        );
        assert_eq!(
            collect_i64(&res, "P", "id"),
            vec![1, 3],
            "$_[1] must read the age column @cz={cz}"
        );
    }
    let res = run_src(
        &format!("P:\n open {p} (id:int age:int)\n |> id ($_[9]) as ghost\n;"),
        4096,
    );
    assert_eq!(res.total_rows_out(), 3, "rows continue with a null ghost");
    assert!(
        res.errors.iter().any(|e| e.message.contains("ghost")),
        "out-of-range $_[9] must be surfaced, never silent: {:?}",
        res.errors
    );
}

#[test]
fn validate_bundle_runs_each_contract_in_order() {
    // `|! { … }` (§29.5-6 s4) lowers to chained contracts: each entry disposes
    // and surfaces independently (warn keeps, reject drops), chunk-size
    // independent — same behaviour as writing the contracts separately.
    let text = "id,age\n1,25\n2,-5\n3,40\n4,200\n5,18\n";
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_validate_bundle",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let src = format!(
        "V:\n open {p} (id:int age:int)\n |! {{ age >= 0 warn; age <= 120 reject }}\n |> id\n;"
    );
    for cz in [1usize, 2, 4096] {
        let res = run_src(&src, cz);
        // warn keeps id 2 (age -5); reject drops id 4 (age 200).
        assert_eq!(
            collect_i64(&res, "V", "id"),
            vec![1, 2, 3, 5],
            "bundle dispositions @cz={cz}"
        );
        let warned = res
            .errors
            .iter()
            .any(|e| e.message.contains("1 row(s) failed") && e.message.contains("(warn)"));
        let rejected = res
            .errors
            .iter()
            .any(|e| e.message.contains("1 row(s) failed") && e.message.contains("(reject)"));
        assert!(
            warned && rejected,
            "each contract surfaces its own count @cz={cz}: {:?}",
            res.errors
        );
    }
}

// ----- Executable bug specs (BUG-A/B/C, docs/TEST-AUDIT.md). These assert the
// INTENDED behaviour and currently fail, so they are #[ignore]d to keep the gate
// green; the fix un-ignores its spec. Fixes are PLANNED only (per the request).
