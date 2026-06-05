//! Exact decimal lane: declared `(c:decimal)` read and grouping.
//!
//! Moved verbatim from the former monolithic `stress.rs` (design 26 §26.8.1,
//! mechanical move-only split; logic unchanged).

use super::*;

#[test]
fn declared_decimal_chunk_size_independent() {
    // A decimal(2) column read from text with varied fractional widths. The
    // value (text → unscaled i128) and its rendering must be identical across
    // chunk sizes, and exact (never via f64): 0.1→0.10, 12.345→12.34 (round
    // half-even), 7→7.00.
    let rows = 4_000usize;
    let mut text = String::from("id,price\n");
    let mut expected: Vec<String> = Vec::with_capacity(rows);
    for i in 0..rows {
        // Cycle through forms that exercise pad, exact, and round-half-even.
        let (cell, want) = match i % 4 {
            0 => ("0.1".to_string(), "0.10"),     // pad up to scale 2
            1 => ("12.345".to_string(), "12.34"), // 3-digit → round half-even (4 even)
            2 => ("7".to_string(), "7.00"),       // integer → padded
            _ => ("12.355".to_string(), "12.36"), // round half-even (5 odd → up)
        };
        text.push_str(&format!("{i},{cell}\n"));
        expected.push(want.to_string());
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_decimal", text.as_bytes()));
    let p = f.0.display();
    let mut prev: Option<Vec<String>> = None;
    for cs in [1usize, 7, 1024, rows] {
        let res = run_src(
            &format!("D:\n open {p} (id price:decimal(2))\n |> id price\n;"),
            cs,
        );
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("D"))
            .unwrap();
        // Collect (id, rendered price) in id order.
        let mut got: Vec<(usize, String)> = Vec::with_capacity(rows);
        for c in &o.chunks {
            let ii = c.schema.index_of("id").unwrap();
            let pi = c.schema.index_of("price").unwrap();
            assert_eq!(
                c.schema.fields[pi].dtype,
                rivus_core::DataType::Decimal { scale: 2 },
                "price is not decimal(2) @cs={cs}"
            );
            for r in 0..c.len {
                let id = c.value(r, ii).to_string().parse::<usize>().unwrap();
                got.push((id, c.value(r, pi).to_string()));
            }
        }
        got.sort_by_key(|(id, _)| *id);
        let rendered: Vec<String> = got.into_iter().map(|(_, s)| s).collect();
        // Exact expected values (proves text→i128 is exact, half-even rounding).
        assert_eq!(rendered, expected, "decimal values wrong @cs={cs}");
        // Chunk-size independence: identical across every chunk size.
        if let Some(p) = &prev {
            assert_eq!(&rendered, p, "decimal output changed across chunk size");
        }
        prev = Some(rendered);
    }
}

#[test]
fn decimal_group_aggregation_exact_and_chunk_size_independent() {
    // A decimal(2) column whose f64 sum would drift (many .01-step values). The
    // group sum/avg/min/max must be EXACT (i128) and identical across chunk sizes
    // — the associativity that lets decimal aggregates parallelize byte-identically
    // (#41). Two groups by parity of id.
    let rows = 10_000usize;
    let mut text = String::from("grp,amount\n");
    // Independent i128 oracles of the per-group unscaled sum / min / max (cents).
    let mut sum_cents = [0i128, 0i128];
    let mut min_cents = [i128::MAX, i128::MAX];
    let mut max_cents = [i128::MIN, i128::MIN];
    for i in 0..rows {
        let g = i % 2;
        let cents = (i as i128 % 1000) + 1; // 0.01 .. 10.00
        text.push_str(&format!("{g},{}.{:02}\n", cents / 100, cents % 100));
        sum_cents[g] += cents;
        min_cents[g] = min_cents[g].min(cents);
        max_cents[g] = max_cents[g].max(cents);
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_dec_group",
        text.as_bytes(),
    ));
    let p = f.0.display();
    let cents_str = |c: i128| format!("{}.{:02}", c / 100, c % 100);
    let want_sum = |g: usize| cents_str(sum_cents[g]);

    let mut prev: Option<Vec<String>> = None;
    for cs in [1usize, 7, 1024, rows] {
        let res = run_src(
            &format!(
                "G:\n open {p} (grp amount:decimal(2))\n |# grp sum:amount min:amount max:amount\n;"
            ),
            cs,
        );
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("G"))
            .unwrap();
        let mut rows_out: Vec<(String, String, String, String)> = Vec::new();
        for c in &o.chunks {
            let gi = c.schema.index_of("grp").unwrap();
            let si = c.schema.index_of("sum_amount").unwrap();
            let mni = c.schema.index_of("min_amount").unwrap();
            let mxi = c.schema.index_of("max_amount").unwrap();
            assert_eq!(
                c.schema.fields[si].dtype,
                rivus_core::DataType::Decimal { scale: 2 },
                "sum is not exact decimal @cs={cs}"
            );
            for r in 0..c.len {
                rows_out.push((
                    c.value(r, gi).to_string(),
                    c.value(r, si).to_string(),
                    c.value(r, mni).to_string(),
                    c.value(r, mxi).to_string(),
                ));
            }
        }
        rows_out.sort();
        // Exact sums (vs the i128 oracle), and min/max.
        for (g, sum, min, max) in &rows_out {
            let gi: usize = g.parse().unwrap();
            assert_eq!(sum, &want_sum(gi), "decimal sum wrong @cs={cs} grp={g}");
            assert_eq!(min, &cents_str(min_cents[gi]), "min @cs={cs} grp={g}");
            assert_eq!(max, &cents_str(max_cents[gi]), "max @cs={cs} grp={g}");
        }
        let flat: Vec<String> = rows_out.iter().map(|t| format!("{t:?}")).collect();
        if let Some(pv) = &prev {
            assert_eq!(&flat, pv, "decimal group output changed across chunk size");
        }
        prev = Some(flat);
    }
}
