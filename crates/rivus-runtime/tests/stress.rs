//! Correctness-at-scale stress tests.
//!
//! These assert that the engine stays *correct* under the same three regimes
//! the benchmarks measure for *speed*: large clean data, error-heavy input, and
//! mixed-type columns. They run as part of `cargo test` (smaller row counts
//! than the benches so CI stays fast) and are the regression guard for every
//! optimization that follows.

use rivus_runtime::gendata::{self, Rng};
use rivus_runtime::{run, RunOptions};

struct TempCsv(std::path::PathBuf);
impl Drop for TempCsv {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn run_src(src: &str, chunk_size: usize) -> rivus_runtime::RunResult {
    let graph = rivus_parser::parse(src).expect("parse");
    run(
        &graph,
        RunOptions {
            chunk_size,
            ..Default::default()
        },
    )
    .expect("run")
}

/// Independent oracle: count clean rows with age >= threshold by regenerating
/// the exact same PRNG sequence used by `gendata::clean`.
fn expected_clean_ge(rows: usize, seed: u64, threshold: u64) -> u64 {
    let mut rng = Rng::new(seed);
    let mut n = 0;
    for _ in 0..rows {
        let age = rng.below(90);
        let _score = rng.below(10_000);
        let _country = rng.below(5);
        let _active = rng.below(2);
        if age >= threshold {
            n += 1;
        }
    }
    n
}

#[test]
fn headerless_csv_positional_columns_chunk_size_independent() {
    // No header row: columns are named c0, c1, c2 and the FIRST line is data.
    let rows = 20_000;
    let mut rng = Rng::new(3);
    let mut text = String::new();
    let mut expect = 0u64;
    for _ in 0..rows {
        let age = rng.below(90);
        text.push_str(&format!("user,x,{age}\n"));
        if age >= 45 {
            expect += 1;
        }
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_nh", text.as_bytes()));
    let p = f.0.display();
    for cs in [1, 7, 1024, 8192, rows] {
        let res = run_src(
            &format!("H:\n open {p} noheader\n |? c2 >= 45\n |> c0 c2\n;"),
            cs,
        );
        assert_eq!(res.total_rows_out(), expect, "noheader filter @cs={cs}");
        assert!(res.errors.is_empty());
    }
}

#[test]
fn declared_schema_renames_and_types_chunk_size_independent() {
    // A header file with columns a,b,c. Declare names (id, code, age) and force
    // `code` to str so leading zeros survive (it would otherwise infer i64).
    let rows = 5_000;
    let mut text = String::from("a,b,c\n");
    let mut kept = 0u64;
    for i in 0..rows {
        let age = (i % 90) as u64;
        text.push_str(&format!("{i},0{i:05},{age}\n")); // code has a leading zero
        if age >= 45 {
            kept += 1;
        }
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_decl", text.as_bytes()));
    let p = f.0.display();
    for cs in [1, 7, 1024, rows] {
        // Declared names are used by the predicate/projection; `code:str` keeps
        // the leading zero intact.
        let res = run_src(
            &format!("D:\n open {p} (id code:str age)\n |? age >= 45\n |> code\n;"),
            cs,
        );
        assert_eq!(res.total_rows_out(), kept, "declared filter @cs={cs}");
        // Every emitted `code` must still start with '0' (kept as a string).
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("D"))
            .unwrap();
        for c in &o.chunks {
            let ci = c.schema.index_of("code").unwrap();
            assert_eq!(c.schema.fields[ci].dtype, rivus_core::DataType::Str);
            for r in 0..c.len {
                assert!(
                    c.value(r, ci).to_string().starts_with('0'),
                    "leading zero lost"
                );
            }
        }
    }
}

#[test]
fn inline_cast_numeric_compare_on_string_column() {
    // `age` is declared str (so a bare compare would be lexical: "100" < "20").
    // `age:int >= N` casts to numeric, so the result matches a numeric oracle and
    // is chunk-size independent.
    let rows = 8_000;
    let mut rng = Rng::new(2);
    let mut text = String::from("name,age\n");
    let mut ge = 0u64;
    for _ in 0..rows {
        let age = rng.below(1000);
        text.push_str(&format!("u,{age}\n"));
        if age >= 500 {
            ge += 1;
        }
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_cast", text.as_bytes()));
    let p = f.0.display();
    for cs in [1, 7, 1024, rows] {
        let res = run_src(
            &format!("C:\n open {p} (name age:str)\n |? age:int >= 500\n;"),
            cs,
        );
        assert_eq!(res.total_rows_out(), ge, "cast compare @cs={cs}");
    }
}

#[test]
fn inner_hash_join_matches_oracle() {
    // Left: users (id, name). Right: orders (id, amount), many-to-one. The inner
    // join row count must equal an independent count of matching pairs, and be
    // chunk-size independent.
    let users = 2_000usize;
    let mut u = String::from("id,name\n");
    for i in 0..users {
        u.push_str(&format!("{i},user{i}\n"));
    }
    // Each order has an id in [0, users*2); ~half match a user.
    let orders = 6_000usize;
    let mut o = String::from("id,amount\n");
    let mut rng = Rng::new(5);
    let mut expected = 0u64;
    for _ in 0..orders {
        let id = rng.below((users * 2) as u64);
        o.push_str(&format!("{id},10\n"));
        if (id as usize) < users {
            expected += 1; // one matching user → one joined row (one-to-many)
        }
    }
    let uf = TempCsv(gendata::write_temp_bytes("join_u", u.as_bytes()));
    let of = TempCsv(gendata::write_temp_bytes("join_o", o.as_bytes()));
    let (up, op) = (uf.0.display(), of.0.display());

    for cs in [1, 7, 1024, orders] {
        let src = format!("U: open {up} ;\nO: open {op} ;\nJ: U & O on id |> name amount\n;");
        let res = run_src(&src, cs);
        assert_eq!(res.total_rows_out(), expected, "join rows @cs={cs}");
    }
}

#[test]
fn multi_key_inner_join_matches_oracle() {
    // Join on a (country, region) tuple. Left rows whose tuple matches a right
    // row join; a left row with the same country but a different region must NOT
    // match (the composite key matters). Row count and the joined `sales` value
    // are checked against an independent oracle, chunk-size independent.
    use std::collections::HashMap;
    // Left: one row per (country, region) with a name.
    let lefts = [
        ("JP", "east", "a"),
        ("JP", "west", "b"),
        ("US", "east", "c"),
        ("US", "south", "d"), // no right match (region differs)
    ];
    // Right: (country, region) -> sales. JP/east and US/east match; JP/north is
    // an orphan; US/south is absent.
    let rights = [
        ("JP", "east", 100i64),
        ("US", "east", 200),
        ("JP", "north", 9),
    ];
    let mut l = String::from("country,region,name\n");
    for (c, r, n) in lefts {
        l.push_str(&format!("{c},{r},{n}\n"));
    }
    let mut o = String::from("country,region,sales\n");
    for (c, r, s) in rights {
        o.push_str(&format!("{c},{r},{s}\n"));
    }
    // Oracle: inner join on (country, region).
    let mut rmap: HashMap<(&str, &str), i64> = HashMap::new();
    for (c, r, s) in rights {
        rmap.insert((c, r), s);
    }
    let mut expected: Vec<(String, String, String, i64)> = Vec::new();
    for (c, r, n) in lefts {
        if let Some(&s) = rmap.get(&(c, r)) {
            expected.push((c.into(), r.into(), n.into(), s));
        }
    }
    expected.sort();

    let lf = TempCsv(gendata::write_temp_bytes("mkjoin_l", l.as_bytes()));
    let of = TempCsv(gendata::write_temp_bytes("mkjoin_o", o.as_bytes()));
    let (lp, op) = (lf.0.display(), of.0.display());

    for cs in [1usize, 2, 1024] {
        let src = format!(
            "L: open {lp} ;\nO: open {op} ;\nJ: L & O on country region |> country region name sales\n;"
        );
        let res = run_src(&src, cs);
        let out = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("J"))
            .unwrap();
        let mut got: Vec<(String, String, String, i64)> = Vec::new();
        for c in &out.chunks {
            let (ci, ri, ni, si) = (
                c.schema.index_of("country").unwrap(),
                c.schema.index_of("region").unwrap(),
                c.schema.index_of("name").unwrap(),
                c.schema.index_of("sales").unwrap(),
            );
            for r in 0..c.len {
                got.push((
                    c.value(r, ci).to_string(),
                    c.value(r, ri).to_string(),
                    c.value(r, ni).to_string(),
                    c.value(r, si).as_f64().unwrap() as i64,
                ));
            }
        }
        got.sort();
        assert_eq!(got, expected, "multi-key inner join @cs={cs}");
    }
}

#[test]
fn left_join_keeps_unmatched_left_rows() {
    // Left: users 0..users. Right: orders whose id is in [0, users*2), so only
    // some users have a matching order. A LEFT join must emit:
    //   (matched pairs) + (one padded row per user with no order at all),
    // and be chunk-size independent. The padded rows carry amount = 0 (i64
    // default), so summing `amount` over the left join equals summing over the
    // inner join — an independent oracle that also checks the default padding.
    let users = 1_500usize;
    let mut u = String::from("id,name\n");
    for i in 0..users {
        u.push_str(&format!("{i},user{i}\n"));
    }
    let orders = 4_000usize;
    let mut o = String::from("id,amount\n");
    let mut rng = Rng::new(9);
    let mut matched_pairs = 0u64;
    let mut matched_users = vec![false; users];
    for _ in 0..orders {
        let id = rng.below((users * 2) as u64);
        o.push_str(&format!("{id},10\n"));
        if (id as usize) < users {
            matched_pairs += 1;
            matched_users[id as usize] = true;
        }
    }
    let unmatched_users = matched_users.iter().filter(|m| !**m).count() as u64;
    // LEFT join rows = matched pairs + one padded row per never-matched user.
    let expected_rows = matched_pairs + unmatched_users;

    let uf = TempCsv(gendata::write_temp_bytes("ljoin_u", u.as_bytes()));
    let of = TempCsv(gendata::write_temp_bytes("ljoin_o", o.as_bytes()));
    let (up, op) = (uf.0.display(), of.0.display());

    for cs in [1, 7, 1024, orders] {
        let src = format!("U: open {up} ;\nO: open {op} ;\nJ: U &left O on id |> name amount\n;");
        let res = run_src(&src, cs);
        assert_eq!(
            res.total_rows_out(),
            expected_rows,
            "left join rows @cs={cs}"
        );

        // Sum of amount = 10 per matched pair, 0 for padded rows.
        let o_out = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("J"))
            .unwrap();
        let mut sum = 0i64;
        let mut padded = 0u64;
        for c in &o_out.chunks {
            let ai = c.schema.index_of("amount").unwrap();
            for r in 0..c.len {
                let v = c.value(r, ai).to_string().parse::<i64>().unwrap_or(0);
                sum += v;
                if v == 0 {
                    padded += 1;
                }
            }
        }
        assert_eq!(sum, matched_pairs as i64 * 10, "amount sum @cs={cs}");
        assert_eq!(padded, unmatched_users, "padded (amount=0) rows @cs={cs}");
        assert!(res.errors.is_empty(), "left join errors @cs={cs}");
    }
}

#[test]
fn string_functions_chunk_size_independent() {
    // contains(city, "y") filter + upper(name) projection must match an oracle.
    let rows = 6_000usize;
    let mut text = String::from("name,city\n");
    let cities = ["york", "la", "yyz", "sfo"];
    let mut kept = 0u64;
    for i in 0..rows {
        let city = cities[i % cities.len()];
        text.push_str(&format!("u{i},{city}\n"));
        if city.contains('y') {
            kept += 1;
        }
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_strfn", text.as_bytes()));
    let p = f.0.display();
    for cs in [1, 7, 1024, rows] {
        let res = run_src(
            &format!("S:\n open {p}\n |? contains(city, \"y\")\n |> (upper(name)) as up\n;"),
            cs,
        );
        assert_eq!(res.total_rows_out(), kept, "contains filter @cs={cs}");
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("S"))
            .unwrap();
        for c in &o.chunks {
            let ci = c.schema.index_of("up").unwrap();
            for r in 0..c.len {
                let v = c.value(r, ci).to_string();
                assert_eq!(v, v.to_uppercase(), "upper() not applied");
            }
        }
    }
}

#[test]
fn replace_split_concat_chunk_size_independent() {
    // replace / split_part / concat over a path-like column. Each output row is
    // checked against an independent oracle, and the result must be chunk-size
    // independent (these lower to row-wise eval inside a computed projection).
    let rows = 4_000usize;
    let mut text = String::from("id,path\n");
    for i in 0..rows {
        // paths like "/a/b<i>/c" so split_part(path,"/",3) = "b<i>".
        text.push_str(&format!("{i},/a/b{i}/c\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_strfn2", text.as_bytes()));
    let p = f.0.display();
    for cs in [1usize, 7, 1024, rows] {
        let res = run_src(
            &format!(
                "S:\n open {p}\n |> id (replace(path, \"/\", \"-\")) as r (split_part(path, \"/\", 3)) as seg (concat(id, \"@\", path)) as tag\n;"
            ),
            cs,
        );
        assert_eq!(res.total_rows_out(), rows as u64, "rows @cs={cs}");
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("S"))
            .unwrap();
        for c in &o.chunks {
            let ii = c.schema.index_of("id").unwrap();
            let ri = c.schema.index_of("r").unwrap();
            let si = c.schema.index_of("seg").unwrap();
            let ti = c.schema.index_of("tag").unwrap();
            for r in 0..c.len {
                let id = c.value(r, ii).to_string();
                assert_eq!(
                    c.value(r, ri).to_string(),
                    format!("-a-b{id}-c"),
                    "replace @cs={cs}"
                );
                assert_eq!(
                    c.value(r, si).to_string(),
                    format!("b{id}"),
                    "split @cs={cs}"
                );
                assert_eq!(
                    c.value(r, ti).to_string(),
                    format!("{id}@/a/b{id}/c"),
                    "concat @cs={cs}"
                );
            }
        }
        assert!(res.errors.is_empty(), "errors @cs={cs}");
    }
}

#[test]
fn numeric_and_coalesce_funcs_chunk_size_independent() {
    // abs/round/floor/ceil over a signed-decimal column, and coalesce over a
    // sometimes-blank text column. Each output is checked against an independent
    // oracle and must be chunk-size independent.
    let rows = 4_000usize;
    let mut text = String::from("id,v,name\n");
    let mut vs: Vec<f64> = Vec::with_capacity(rows);
    for i in 0..rows {
        // deterministic signed decimals in [-50.0, 49.5] stepping by 0.5
        let v = (i as f64 % 200.0) * 0.5 - 50.0;
        vs.push(v);
        let name = if i % 3 == 0 {
            String::new()
        } else {
            format!("n{i}")
        };
        text.push_str(&format!("{i},{v},{name}\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_numfn", text.as_bytes()));
    let p = f.0.display();
    for cs in [1usize, 7, 1024, rows] {
        let res = run_src(
            &format!(
                "N:\n open {p}\n |> id (abs(v)) as a (round(v)) as r (floor(v)) as fl (ceil(v)) as ce (coalesce(name, \"NA\")) as nm\n;"
            ),
            cs,
        );
        let out = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("N"))
            .unwrap();
        for c in &out.chunks {
            let ii = c.schema.index_of("id").unwrap();
            let (ai, ri, fi, ci, ni) = (
                c.schema.index_of("a").unwrap(),
                c.schema.index_of("r").unwrap(),
                c.schema.index_of("fl").unwrap(),
                c.schema.index_of("ce").unwrap(),
                c.schema.index_of("nm").unwrap(),
            );
            for row in 0..c.len {
                let id = c.value(row, ii).to_string().parse::<usize>().unwrap();
                let v = vs[id];
                assert_eq!(c.value(row, ai).as_f64().unwrap(), v.abs(), "abs @cs={cs}");
                assert_eq!(
                    c.value(row, ri).as_f64().unwrap(),
                    v.round(),
                    "round @cs={cs}"
                );
                assert_eq!(
                    c.value(row, fi).as_f64().unwrap(),
                    v.floor(),
                    "floor @cs={cs}"
                );
                assert_eq!(
                    c.value(row, ci).as_f64().unwrap(),
                    v.ceil(),
                    "ceil @cs={cs}"
                );
                let want_nm = if id % 3 == 0 {
                    "NA".to_string()
                } else {
                    format!("n{id}")
                };
                assert_eq!(c.value(row, ni).to_string(), want_nm, "coalesce @cs={cs}");
            }
        }
        assert!(res.errors.is_empty(), "errors @cs={cs}");
    }
}

#[test]
fn dropna_and_fill_chunk_size_independent() {
    // city is blank on every 3rd row. dropna city drops those; fill city
    // replaces them. Both must be exact and chunk-size independent.
    let rows = 9_000usize;
    let mut text = String::from("id,city\n");
    let mut nonblank = 0u64;
    for i in 0..rows {
        if i % 3 == 0 {
            text.push_str(&format!("{i},\n")); // blank city
        } else {
            text.push_str(&format!("{i},town\n"));
            nonblank += 1;
        }
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_na", text.as_bytes()));
    let p = f.0.display();
    for cs in [1, 7, 1024, rows] {
        let dn = run_src(&format!("D:\n open {p} (id city:str)\n dropna city\n;"), cs);
        assert_eq!(dn.total_rows_out(), nonblank, "dropna @cs={cs}");

        // fill keeps all rows; none should be blank afterwards.
        let fl = run_src(
            &format!("D:\n open {p} (id city:str)\n fill city \"NA\"\n;"),
            cs,
        );
        assert_eq!(fl.total_rows_out(), rows as u64, "fill keeps rows @cs={cs}");
        let o = fl
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("D"))
            .unwrap();
        for c in &o.chunks {
            let ci = c.schema.index_of("city").unwrap();
            for r in 0..c.len {
                assert!(
                    !c.value(r, ci).to_string().is_empty(),
                    "blank survived fill"
                );
            }
        }
    }
}

#[test]
fn fill_ffill_bfill_chunk_size_independent() {
    // A column of runs of blanks between a few anchors, plus a leading and a
    // trailing blank (which ffill/bfill respectively cannot resolve). ffill
    // carries the previous value forward across chunk boundaries; bfill carries
    // the next value back across them. Both results must be exact and identical
    // regardless of chunk_size — the regression guard for the cross-chunk carry.
    let raw = ["", "", "a", "", "", "b", "", "c", "", "", "", "d", ""];
    let rows = raw.len();
    let mut text = String::from("id,tag\n");
    for (i, v) in raw.iter().enumerate() {
        text.push_str(&format!("{i},{v}\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_fill_dir",
        text.as_bytes(),
    ));
    let p = f.0.display();

    // Independent oracles.
    let mut ff = vec![String::new(); rows];
    let mut last = String::new();
    for i in 0..rows {
        if raw[i].is_empty() {
            ff[i] = last.clone();
        } else {
            ff[i] = raw[i].to_string();
            last = raw[i].to_string();
        }
    }
    let mut bf = vec![String::new(); rows];
    let mut next = String::new();
    for i in (0..rows).rev() {
        if raw[i].is_empty() {
            bf[i] = next.clone();
        } else {
            bf[i] = raw[i].to_string();
            next = raw[i].to_string();
        }
    }

    let collect = |res: &rivus_runtime::RunResult| -> Vec<String> {
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("F"))
            .unwrap();
        let mut out = Vec::new();
        for c in &o.chunks {
            let ci = c.schema.index_of("tag").unwrap();
            for r in 0..c.len {
                out.push(c.value(r, ci).to_string());
            }
        }
        out
    };

    for cs in [1usize, 2, 3, 5, rows] {
        let fwd = run_src(
            &format!("F:\n open {p} (id tag:str)\n fill tag ffill\n;"),
            cs,
        );
        assert_eq!(collect(&fwd), ff, "ffill @cs={cs}");
        assert!(fwd.errors.is_empty(), "ffill errors @cs={cs}");

        let back = run_src(
            &format!("F:\n open {p} (id tag:str)\n fill tag bfill\n;"),
            cs,
        );
        assert_eq!(collect(&back), bf, "bfill @cs={cs}");
        assert!(back.errors.is_empty(), "bfill errors @cs={cs}");
    }
}

#[test]
fn right_and_full_outer_join_match_oracle() {
    // Users 0..users; orders carry an id in [0, users*2). So some users have no
    // order (unmatched left) and some orders reference a non-existent user
    // (unmatched right). Build independent oracles for the row counts of a RIGHT
    // and a FULL outer join, and assert chunk-size independence.
    let users = 1_200usize;
    let mut u = String::from("id,name\n");
    for i in 0..users {
        u.push_str(&format!("{i},user{i}\n"));
    }
    let norders = 3_500usize;
    let mut o = String::from("id,amount\n");
    let mut rng = Rng::new(13);
    let mut matched_pairs = 0u64; // (user, order) matches
    let mut orphan_orders = 0u64; // orders with no user
    let mut matched_users = vec![false; users];
    for _ in 0..norders {
        let id = rng.below((users * 2) as u64);
        o.push_str(&format!("{id},10\n"));
        if (id as usize) < users {
            matched_pairs += 1;
            matched_users[id as usize] = true;
        } else {
            orphan_orders += 1;
        }
    }
    let unmatched_users = matched_users.iter().filter(|m| !**m).count() as u64;

    // RIGHT join = every order row: matched pairs + orphan orders (one each).
    let right_rows = matched_pairs + orphan_orders;
    // FULL join = matched pairs + unmatched users + orphan orders.
    let full_rows = matched_pairs + unmatched_users + orphan_orders;

    let uf = TempCsv(gendata::write_temp_bytes("rjoin_u", u.as_bytes()));
    let of = TempCsv(gendata::write_temp_bytes("rjoin_o", o.as_bytes()));
    let (up, op) = (uf.0.display(), of.0.display());

    for cs in [1, 7, 1024, norders] {
        let r = run_src(
            &format!("U: open {up} ;\nO: open {op} ;\nJ: U &right O on id |> id name amount\n;"),
            cs,
        );
        assert_eq!(r.total_rows_out(), right_rows, "right join rows @cs={cs}");
        assert!(r.errors.is_empty(), "right join errors @cs={cs}");

        let f = run_src(
            &format!("U: open {up} ;\nO: open {op} ;\nJ: U &full O on id |> id name amount\n;"),
            cs,
        );
        assert_eq!(f.total_rows_out(), full_rows, "full join rows @cs={cs}");

        // Every output row must carry a non-empty `id` (key-preservation: an
        // orphan order with no user still keeps its id in the key column).
        let o_out = f
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("J"))
            .unwrap();
        for c in &o_out.chunks {
            let ci = c.schema.index_of("id").unwrap();
            for row in 0..c.len {
                assert!(
                    !c.value(row, ci).to_string().is_empty(),
                    "full join lost a key @cs={cs}"
                );
            }
        }
    }
}

#[test]
fn fill_mean_median_chunk_size_independent() {
    // score is blank on every 4th row; the rest are a known numeric sequence.
    // `fill score mean|median` must replace blanks with the column statistic of
    // the non-empty cells, keep the non-empty cells unchanged, and be identical
    // across chunk_size (the statistic is computed over the whole buffered
    // column, a pipeline-breaker like sort).
    let rows = 4_000usize;
    let mut text = String::from("id,score\n");
    let mut present: Vec<f64> = Vec::new();
    for i in 0..rows {
        if i % 4 == 0 {
            text.push_str(&format!("{i},\n")); // blank score
        } else {
            let s = (i % 100) as f64; // deterministic spread 0..99
            text.push_str(&format!("{i},{s}\n"));
            present.push(s);
        }
    }
    // Oracle statistics over the present (non-blank) values.
    let mean = present.iter().sum::<f64>() / present.len() as f64;
    let mut sorted = present.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let rank = 0.5 * (sorted.len() - 1) as f64;
    let (lo, hi, frac) = (
        rank.floor() as usize,
        rank.ceil() as usize,
        rank - rank.floor(),
    );
    let median = sorted[lo] + (sorted[hi] - sorted[lo]) * frac;

    let f = TempCsv(gendata::write_temp_bytes(
        "stress_fillstat",
        text.as_bytes(),
    ));
    let p = f.0.display();

    // Sum of the filled column = sum(present) + (#blanks * statistic). Checking
    // the sum (not exact strings) keeps the oracle robust to float formatting.
    let nblank = (rows / 4) as f64;
    let present_sum: f64 = present.iter().sum();

    let col_sum = |res: &rivus_runtime::RunResult| -> f64 {
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("F"))
            .unwrap();
        let mut sum = 0f64;
        let mut blanks = 0u64;
        for c in &o.chunks {
            let ci = c.schema.index_of("score").unwrap();
            for r in 0..c.len {
                let v = c.value(r, ci).to_string();
                assert!(!v.trim().is_empty(), "blank survived fill");
                sum += v.parse::<f64>().unwrap();
                blanks += 0; // (kept for clarity; blanks already replaced)
            }
        }
        let _ = blanks;
        sum
    };

    for cs in [1usize, 7, 1024, rows] {
        let m = run_src(
            &format!("F:\n open {p} (id score:str)\n fill score mean\n;"),
            cs,
        );
        assert!(
            (col_sum(&m) - (present_sum + nblank * mean)).abs() < 1e-6,
            "fill mean sum @cs={cs}"
        );
        assert!(m.errors.is_empty(), "mean errors @cs={cs}");

        let md = run_src(
            &format!("F:\n open {p} (id score:str)\n fill score median\n;"),
            cs,
        );
        assert!(
            (col_sum(&md) - (present_sum + nblank * median)).abs() < 1e-6,
            "fill median sum @cs={cs}"
        );
    }
}

#[test]
fn describe_matches_oracle() {
    // One numeric column `v`; `describe` must report count/min/max/mean that
    // match an independent computation, for every chunk size.
    let rows = 10_000;
    let mut rng = Rng::new(1);
    let mut text = String::from("v\n");
    let (mut sum, mut mn, mut mx) = (0i64, i64::MAX, i64::MIN);
    for _ in 0..rows {
        let x = rng.below(1000) as i64;
        text.push_str(&format!("{x}\n"));
        sum += x;
        mn = mn.min(x);
        mx = mx.max(x);
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_desc", text.as_bytes()));
    let p = f.0.display();
    let mean = sum as f64 / rows as f64;

    for cs in [1, 7, 1024, rows] {
        let res = run_src(&format!("D:\n open {p}\n describe\n;"), cs);
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("D"))
            .expect("describe output");
        let c = &o.chunks[0];
        let cell = |col: &str| {
            let ci = c.schema.index_of(col).unwrap();
            c.value(0, ci).to_string()
        };
        assert_eq!(cell("column"), "v", "@cs={cs}");
        assert_eq!(cell("count"), rows.to_string(), "count @cs={cs}");
        assert_eq!(
            cell("min").parse::<f64>().unwrap(),
            mn as f64,
            "min @cs={cs}"
        );
        assert_eq!(
            cell("max").parse::<f64>().unwrap(),
            mx as f64,
            "max @cs={cs}"
        );
        assert!(
            (cell("mean").parse::<f64>().unwrap() - mean).abs() < 1e-6,
            "mean @cs={cs}"
        );
    }
}

#[test]
fn large_clean_filter_is_exact() {
    let rows = 50_000;
    let seed = 42;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_clean", &data));
    let p = f.0.display();

    // Run across several chunk sizes: the result must be identical regardless
    // of chunk granularity (chunk-size independence).
    let expected = expected_clean_ge(rows, seed, 45);
    for cs in [1, 7, 1024, 8192, rows] {
        let src = format!("F:\n open {p}\n |? age >= 45\n;");
        let res = run_src(&src, cs);
        assert_eq!(res.total_rows_out(), expected, "chunk_size={cs}");
        assert!(res.errors.is_empty(), "clean data should not error");
    }
}

#[test]
fn take_caps_rows_chunk_size_independent() {
    let rows = 50_000;
    let seed = 42;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_take", &data));
    let p = f.0.display();

    let matched = expected_clean_ge(rows, seed, 45);
    // Limit below and above the number of matches; result is min(N, matched),
    // and must not depend on chunk granularity (a chunk may straddle the cut).
    for n in [
        0u64,
        1,
        123,
        matched.saturating_sub(1),
        matched,
        matched + 1000,
    ] {
        let want = n.min(matched);
        for cs in [1, 7, 1024, 8192, rows] {
            let src = format!("F:\n open {p}\n |? age >= 45\n take {n}\n;");
            let res = run_src(&src, cs);
            assert_eq!(res.total_rows_out(), want, "take {n} @ chunk_size={cs}");
            assert!(res.errors.is_empty(), "clean data should not error");
        }
    }
}

/// Collect an integer column across all chunks of the output labeled `label`.
fn collect_i64(res: &rivus_runtime::RunResult, label: &str, col: &str) -> Vec<i64> {
    let mut out = Vec::new();
    let o = res
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some(label))
        .expect("labeled output");
    for c in &o.chunks {
        if let Some(ci) = c.schema.index_of(col) {
            for r in 0..c.len {
                out.push(c.value(r, ci).as_f64().unwrap() as i64);
            }
        }
    }
    out
}

#[test]
fn sort_orders_rows_chunk_size_independent() {
    let rows = 20_000;
    let seed = 7;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_sort", &data));
    let p = f.0.display();

    // Oracle: regenerate the age multiset and sort it independently.
    let mut rng = Rng::new(seed);
    let mut want_asc = Vec::with_capacity(rows);
    for _ in 0..rows {
        let age = rng.below(90) as i64;
        let _score = rng.below(10_000);
        let _country = rng.below(5);
        let _active = rng.below(2);
        want_asc.push(age);
    }
    want_asc.sort_unstable();
    let mut want_desc = want_asc.clone();
    want_desc.reverse();

    // The sorted output must equal the oracle exactly, for every chunk size.
    for cs in [1, 7, 1024, 8192, rows] {
        let asc = run_src(&format!("S:\n open {p}\n sort age\n;"), cs);
        assert_eq!(collect_i64(&asc, "S", "age"), want_asc, "asc @cs={cs}");

        let desc = run_src(&format!("S:\n open {p}\n sort age desc\n;"), cs);
        assert_eq!(collect_i64(&desc, "S", "age"), want_desc, "desc @cs={cs}");
    }
}

#[test]
fn multi_key_sort_orders_by_each_key_chunk_size_independent() {
    // `sort team score desc` orders by team ascending, then by score descending
    // within a team. Build rows with deliberate team ties so the secondary key
    // is exercised; compare against an independent Rust sort, every chunk size.
    let rows = 12_000usize;
    let mut rng = Rng::new(23);
    let mut text = String::from("team,score\n");
    let mut tuples: Vec<(i64, i64)> = Vec::with_capacity(rows); // (team, score)
    for _ in 0..rows {
        let team = rng.below(5) as i64; // few teams → many ties
        let score = rng.below(1000) as i64;
        text.push_str(&format!("{team},{score}\n"));
        tuples.push((team, score));
    }
    // Oracle: team asc, then score desc.
    let mut want = tuples.clone();
    want.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));

    let f = TempCsv(gendata::write_temp_bytes("stress_msort", text.as_bytes()));
    let p = f.0.display();
    for cs in [1usize, 7, 1024, rows] {
        let res = run_src(&format!("S:\n open {p}\n sort team score desc\n;"), cs);
        let out = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("S"))
            .unwrap();
        let mut got: Vec<(i64, i64)> = Vec::with_capacity(rows);
        for c in &out.chunks {
            let (ti, si) = (
                c.schema.index_of("team").unwrap(),
                c.schema.index_of("score").unwrap(),
            );
            for r in 0..c.len {
                got.push((
                    c.value(r, ti).as_f64().unwrap() as i64,
                    c.value(r, si).as_f64().unwrap() as i64,
                ));
            }
        }
        assert_eq!(got, want, "multi-key sort @cs={cs}");
    }
}

#[test]
fn distinct_dedups_chunk_size_independent() {
    let rows = 20_000;
    let seed = 11;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_distinct", &data));
    let p = f.0.display();

    // `country` is one of five fixed values; with 20k rows all five appear, so
    // `distinct country` yields exactly 5 rows regardless of chunk size.
    for cs in [1, 7, 1024, 8192, rows] {
        let res = run_src(&format!("D:\n open {p}\n distinct country\n;"), cs);
        assert_eq!(res.total_rows_out(), 5, "distinct country @cs={cs}");
        assert!(res.errors.is_empty());
    }

    // Whole-row distinct: the surviving count must be identical across chunk
    // sizes (first-occurrence dedup is order-deterministic, not chunk-bound).
    let baseline = run_src(&format!("D:\n open {p}\n distinct\n;"), 4096).total_rows_out();
    assert!(baseline > 0 && baseline <= rows as u64);
    for cs in [1, 7, 8192, rows] {
        let res = run_src(&format!("D:\n open {p}\n distinct\n;"), cs);
        assert_eq!(
            res.total_rows_out(),
            baseline,
            "whole-row distinct @cs={cs}"
        );
    }
}

#[test]
fn computed_columns_are_exact_chunk_size_independent() {
    let rows = 20_000;
    let seed = 5;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_calc", &data));
    let p = f.0.display();

    // `(age * 2 + 1)` must equal the arithmetic on the source `age`, exactly and
    // for every chunk size. Carry `age` through so we can check element-wise.
    for cs in [1, 7, 1024, 8192, rows] {
        let res = run_src(&format!("C:\n open {p}\n |> age (age * 2 + 1) as v\n;"), cs);
        let age = collect_i64(&res, "C", "age");
        let v = collect_i64(&res, "C", "v");
        assert_eq!(age.len(), rows, "row count @cs={cs}");
        assert_eq!(v.len(), rows, "computed row count @cs={cs}");
        for (a, got) in age.iter().zip(&v) {
            assert_eq!(*got, a * 2 + 1, "computed value @cs={cs}");
        }
    }
}

#[test]
fn error_heavy_skips_and_continues() {
    let rows = 40_000;
    let data = gendata::error_heavy(rows, 0.5, 7);
    let f = TempCsv(gendata::write_temp("stress_err", &data));
    let p = f.0.display();

    // Roughly half the rows are malformed; the run must still succeed, surface a
    // recoverable error about skipped rows, and never go fatal.
    let src = format!("F:\n open {p}\n |? age >= 0\n;");
    let res = run_src(&src, 4096);

    assert!(
        res.errors.iter().any(|e| e.message.contains("malformed")),
        "expected a recoverable malformed-row error"
    );
    assert!(
        !res.errors.iter().any(rivus_core::ErrorEvent::is_fatal),
        "error-heavy input must not be fatal (continue-first)"
    );
    let out = res.total_rows_out();
    assert!(out > 0 && out < rows as u64, "kept {out} of {rows}");
}

#[test]
fn mixed_types_degrades_to_string_lane() {
    let rows = 30_000;
    // Pure-int column: inference picks i64, predicate is numeric.
    let pure = gendata::mixed_types(rows, 0.0, 1);
    let fp = TempCsv(gendata::write_temp("stress_pure", &pure));
    let res_pure = run_src(
        &format!("F:\n open {}\n |? value >= 50\n;", fp.0.display()),
        4096,
    );
    assert!(res_pure.errors.is_empty());

    // Mixed column: some cells are non-numeric, so inference falls back to Str
    // and the comparison runs on the string lane — it must still run, not crash.
    let mixed = gendata::mixed_types(rows, 0.3, 1);
    let fm = TempCsv(gendata::write_temp("stress_mixed", &mixed));
    let res_mixed = run_src(
        &format!("F:\n open {}\n |? value >= 50\n;", fm.0.display()),
        4096,
    );
    // Both runs complete; the mixed run produces a (string-comparison) result
    // without going fatal.
    assert!(!res_mixed
        .errors
        .iter()
        .any(rivus_core::ErrorEvent::is_fatal));
}

#[test]
fn string_filter_matches_oracle() {
    // Filter on a string column (country == "JP") must match an independent
    // count, exercising the borrowed-&str predicate fast path across chunk
    // sizes. Also checks `!=` for the complementary count.
    let rows = 40_000;
    let seed = 123;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_strfilter", &data));
    let p = f.0.display();

    // Oracle: replay the generator's PRNG to count JP rows.
    let mut rng = Rng::new(seed);
    let countries = ["JP", "US", "DE", "FR", "BR"];
    let mut jp = 0u64;
    for _ in 0..rows {
        let _age = rng.below(90);
        let _score = rng.below(10_000);
        let c = countries[rng.below(5) as usize];
        let _active = rng.below(2);
        if c == "JP" {
            jp += 1;
        }
    }

    for cs in [1, 1000, 8192] {
        let eq = run_src(&format!("F:\n open {p}\n |? country == \"JP\"\n;"), cs);
        assert_eq!(eq.total_rows_out(), jp, "== chunk_size={cs}");
        let ne = run_src(&format!("F:\n open {p}\n |? country != \"JP\"\n;"), cs);
        assert_eq!(ne.total_rows_out(), rows as u64 - jp, "!= chunk_size={cs}");
    }
}

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
fn group_aggregates_are_exact() {
    // `|# country sum:age max:age` (+ implicit count) must match an oracle that
    // buckets the regenerated PRNG stream by country.
    use std::collections::BTreeMap;
    let rows = 20_000;
    let seed = 314;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_groupagg", &data));
    let p = f.0.display();

    let countries = ["JP", "US", "DE", "FR", "BR"];
    let mut rng = Rng::new(seed);
    let mut oracle: BTreeMap<String, (i64, f64, f64)> = BTreeMap::new(); // (count,sum,max)
    for _ in 0..rows {
        let age = rng.below(90) as f64;
        let _score = rng.below(10_000);
        let c = countries[rng.below(5) as usize].to_string();
        let _active = rng.below(2);
        let e = oracle.entry(c).or_insert((0, 0.0, f64::NEG_INFINITY));
        e.0 += 1;
        e.1 += age;
        e.2 = e.2.max(age);
    }

    let res = run_src(
        &format!("G:\n open {p}\n |# country sum:age max:age\n;"),
        4096,
    );
    let out = &res.outputs[0];
    let chunk = &out.chunks[0];
    assert_eq!(
        chunk.schema.field_names(),
        vec!["country", "count", "sum_age", "max_age"]
    );
    assert_eq!(chunk.len, oracle.len());
    for row in 0..chunk.len {
        let country = chunk.value(row, 0).to_string();
        let count = chunk.value(row, 1).as_f64().unwrap() as i64;
        let sum = chunk.value(row, 2).as_f64().unwrap();
        let max = chunk.value(row, 3).as_f64().unwrap();
        let (oc, os, om) = oracle[&country];
        assert_eq!(count, oc, "count[{country}]");
        assert_eq!(sum, os, "sum[{country}]");
        assert_eq!(max, om, "max[{country}]");
    }
}

#[test]
fn multi_key_group_matches_oracle() {
    // `|# country active sum:age` groups by the (country, active) tuple. The
    // per-group count and sum must match an independent oracle that buckets the
    // regenerated PRNG stream by the same tuple, and be chunk-size independent.
    use std::collections::BTreeMap;
    let rows = 20_000;
    let seed = 271;
    let data = gendata::clean(rows, seed);
    let f = TempCsv(gendata::write_temp("stress_mkgroup", &data));
    let p = f.0.display();

    // Oracle: replay clean()'s exact PRNG sequence (age, score, country, active).
    let countries = ["JP", "US", "DE", "FR", "BR"];
    let mut rng = Rng::new(seed);
    let mut oracle: BTreeMap<(String, String), (i64, f64)> = BTreeMap::new();
    for _ in 0..rows {
        let age = rng.below(90) as f64;
        let _score = rng.below(10_000);
        let country = countries[rng.below(5) as usize].to_string();
        let active = (rng.below(2) == 1).to_string();
        let e = oracle.entry((country, active)).or_insert((0, 0.0));
        e.0 += 1;
        e.1 += age;
    }

    for cs in [1usize, 7, 1024, rows] {
        let res = run_src(&format!("G:\n open {p}\n |# country active sum:age\n;"), cs);
        let out = &res.outputs[0];
        let chunk = &out.chunks[0];
        assert_eq!(
            chunk.schema.field_names(),
            vec!["country", "active", "count", "sum_age"],
            "schema @cs={cs}"
        );
        assert_eq!(chunk.len, oracle.len(), "group count @cs={cs}");
        for row in 0..chunk.len {
            let country = chunk.value(row, 0).to_string();
            let active = chunk.value(row, 1).to_string();
            let count = chunk.value(row, 2).as_f64().unwrap() as i64;
            let sum = chunk.value(row, 3).as_f64().unwrap();
            let (oc, os) = oracle[&(country.clone(), active.clone())];
            assert_eq!(count, oc, "count[{country},{active}] @cs={cs}");
            assert_eq!(sum, os, "sum[{country},{active}] @cs={cs}");
        }
    }
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

#[test]
fn group_extended_aggregates_are_correct_and_chunk_independent() {
    // std / count_distinct / first / last (plus avg) must be correct and
    // independent of chunk size. Two small groups with known statistics.
    let text = "team,player,score\nA,x,10\nA,y,20\nA,x,30\nB,z,5\nB,z,5\nB,w,15\n";
    let f = TempCsv(gendata::write_temp_bytes("stress_grpext", text.as_bytes()));
    let p = f.0.display();
    let src = format!(
        "G:\n open {p}\n |# team std:score count_distinct:player first:player last:player avg:score\n;"
    );

    // Verify the values once (at a normal chunk size), then assert that smaller
    // chunk sizes produce a byte-identical result row-for-row.
    let base = run_src(&src, 4096);
    let bchunk = &base.outputs[0].chunks[0];
    assert_eq!(
        bchunk.schema.field_names(),
        vec![
            "team",
            "count",
            "std_score",
            "count_distinct_player",
            "first_player",
            "last_player",
            "avg_score",
        ]
    );
    assert_eq!(bchunk.len, 2);
    let cell = |row: usize, col: usize| bchunk.value(row, col).to_string();
    let num = |row: usize, col: usize| bchunk.value(row, col).as_f64().unwrap();
    // Group A: scores 10,20,30 → std 10, avg 20; players x,y,x → distinct 2, first x, last x.
    assert_eq!(cell(0, 0), "A");
    assert_eq!(num(0, 1), 3.0);
    assert!((num(0, 2) - 10.0).abs() < 1e-9);
    assert_eq!(num(0, 3), 2.0);
    assert_eq!(cell(0, 4), "x");
    assert_eq!(cell(0, 5), "x");
    assert!((num(0, 6) - 20.0).abs() < 1e-9);
    // Group B: scores 5,5,15 → sample std 5.7735…, avg 25/3; players z,z,w → distinct 2, first z, last w.
    assert_eq!(cell(1, 0), "B");
    assert!((num(1, 2) - 5.773_502_691_896_257).abs() < 1e-9);
    assert_eq!(num(1, 3), 2.0);
    assert_eq!(cell(1, 4), "z");
    assert_eq!(cell(1, 5), "w");
    assert!((num(1, 6) - 25.0 / 3.0).abs() < 1e-9);

    // Chunk-size independence: every cell matches the base across chunk sizes.
    for cs in [1usize, 2, 5, 64] {
        let r = run_src(&src, cs);
        let c = &r.outputs[0].chunks[0];
        assert_eq!(c.len, bchunk.len, "row count @cs={cs}");
        for row in 0..c.len {
            for col in 0..bchunk.schema.fields.len() {
                assert_eq!(
                    c.value(row, col).to_string(),
                    bchunk.value(row, col).to_string(),
                    "cell[{row}][{col}] @cs={cs}"
                );
            }
        }
    }
}

#[test]
fn rename_and_drop_are_chunk_size_independent() {
    // `rename` changes only column names; `drop` removes columns. Both are
    // stateless, so the result must not depend on chunk size. Verify the output
    // schema and that the kept values survive across chunk sizes.
    let rows = 20_000;
    let mut rng = Rng::new(11);
    let mut text = String::from("name,age,city\n");
    let mut ages: Vec<u64> = Vec::with_capacity(rows);
    for _ in 0..rows {
        let age = rng.below(90);
        ages.push(age);
        text.push_str(&format!("user,{age},NYC\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_rendrop", text.as_bytes()));
    let p = f.0.display();
    // rename age -> years, then drop city: output columns must be [name, years].
    let src = format!("R:\n open {p}\n rename age years\n drop city\n;");
    for cs in [1usize, 7, 1024, 8192, rows] {
        let res = run_src(&src, cs);
        assert!(res.errors.is_empty(), "errors @cs={cs}: {:?}", res.errors);
        let out = &res.outputs[0];
        let total: usize = out.chunks.iter().map(|c| c.len).sum();
        assert_eq!(total, rows, "row count @cs={cs}");
        let first = &out.chunks[0];
        assert_eq!(
            first.schema.field_names(),
            vec!["name", "years"],
            "schema @cs={cs}"
        );
    }
    // Spot-check values: the `years` column equals the original ages, in order.
    let res = run_src(&src, 4096);
    let out = &res.outputs[0];
    let mut got = Vec::with_capacity(rows);
    for c in &out.chunks {
        let yi = c.schema.index_of("years").unwrap();
        for r in 0..c.len {
            got.push(c.value(r, yi).as_f64().unwrap() as u64);
        }
    }
    assert_eq!(got, ages, "renamed column values preserved in order");
}

#[test]
fn reorder_is_chunk_size_independent() {
    // `reorder city age` moves those columns to the front; the rest follow in
    // original order. A permutation — types/values preserved, row count and
    // schema independent of chunk size.
    let rows = 12_000;
    let mut rng = Rng::new(17);
    let mut text = String::from("id,name,age,city\n");
    let mut ages: Vec<u64> = Vec::with_capacity(rows);
    for i in 0..rows {
        let age = rng.below(90);
        ages.push(age);
        text.push_str(&format!("{i},user,{age},NYC\n"));
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_reorder", text.as_bytes()));
    let p = f.0.display();
    let src = format!("R:\n open {p}\n reorder city age\n;");
    for cs in [1usize, 7, 1024, rows] {
        let res = run_src(&src, cs);
        assert!(res.errors.is_empty(), "errors @cs={cs}");
        let out = &res.outputs[0];
        let total: usize = out.chunks.iter().map(|c| c.len).sum();
        assert_eq!(total, rows, "row count @cs={cs}");
        assert_eq!(
            out.chunks[0].schema.field_names(),
            vec!["city", "age", "id", "name"],
            "reordered schema @cs={cs}"
        );
    }
    // `age` values survive the permutation, in order.
    let res = run_src(&src, 4096);
    let out = &res.outputs[0];
    let mut got = Vec::with_capacity(rows);
    for c in &out.chunks {
        let ai = c.schema.index_of("age").unwrap();
        for r in 0..c.len {
            got.push(c.value(r, ai).as_f64().unwrap() as u64);
        }
    }
    assert_eq!(got, ages, "reordered column values preserved in order");
}

#[test]
fn cast_verb_retypes_columns_chunk_size_independent() {
    // `code` is declared str (keeps leading zeros); `cast code:int` re-types it,
    // dropping the zeros. The cast result and the column dtype must be exact and
    // chunk-size independent.
    let rows = 5_000usize;
    let mut text = String::from("id,code\n");
    for i in 0..rows {
        text.push_str(&format!("{i},0{i:04}\n")); // leading-zero code
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_cast_verb",
        text.as_bytes(),
    ));
    let p = f.0.display();
    for cs in [1usize, 7, 1024, rows] {
        let res = run_src(
            &format!("C:\n open {p} (id code:str)\n cast code:int\n;"),
            cs,
        );
        let out = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("C"))
            .unwrap();
        // The `code` column is now i64, value == id (leading zeros stripped).
        assert_eq!(
            out.chunks[0].schema.fields[out.chunks[0].schema.index_of("code").unwrap()].dtype,
            rivus_core::DataType::I64,
            "code dtype @cs={cs}"
        );
        let mut got = Vec::with_capacity(rows);
        for c in &out.chunks {
            let ci = c.schema.index_of("code").unwrap();
            for r in 0..c.len {
                got.push(c.value(r, ci).as_f64().unwrap() as i64);
            }
        }
        let want: Vec<i64> = (0..rows as i64).collect();
        assert_eq!(got, want, "cast values @cs={cs}");
        assert!(res.errors.is_empty(), "errors @cs={cs}");
    }
}

#[test]
fn case_when_is_chunk_size_independent() {
    // `case when … then … else … end` computed column buckets each row by its
    // age band, identically across chunk sizes.
    let rows = 20_000;
    let mut rng = Rng::new(13);
    let mut text = String::from("name,age\n");
    let mut expect: Vec<&str> = Vec::with_capacity(rows);
    for _ in 0..rows {
        let age = rng.below(90);
        text.push_str(&format!("user,{age}\n"));
        expect.push(if age >= 60 {
            "senior"
        } else if age >= 18 {
            "adult"
        } else {
            "minor"
        });
    }
    let f = TempCsv(gendata::write_temp_bytes("stress_case", text.as_bytes()));
    let p = f.0.display();
    let src = format!(
        "C:\n open {p}\n |> name (case when age >= 60 then \"senior\" when age >= 18 then \"adult\" else \"minor\" end) as bucket\n;"
    );
    for cs in [1usize, 7, 1024, 8192, rows] {
        let res = run_src(&src, cs);
        assert!(res.errors.is_empty(), "errors @cs={cs}: {:?}", res.errors);
        let out = &res.outputs[0];
        let mut got = Vec::with_capacity(rows);
        for c in &out.chunks {
            let bi = c.schema.index_of("bucket").unwrap();
            for r in 0..c.len {
                got.push(c.value(r, bi).to_string());
            }
        }
        assert_eq!(got, expect, "case buckets @cs={cs}");
    }
}

#[test]
fn group_percentiles_are_correct_and_chunk_independent() {
    // median / p90 over a known group. Group A: 10,20,30,40 → median 25, p90 37
    // (linear interp: rank=0.9*3=2.7 → 30+(40-30)*0.7=37). Group B: 5,100 →
    // median 52.5, p90 90.5. Must be identical across chunk sizes.
    let text = "team,score\nA,10\nA,20\nA,30\nA,40\nB,5\nB,100\n";
    let f = TempCsv(gendata::write_temp_bytes("stress_pct", text.as_bytes()));
    let p = f.0.display();
    let src = format!("G:\n open {p}\n |# team median:score p90:score\n;");

    let base = run_src(&src, 4096);
    let bchunk = &base.outputs[0].chunks[0];
    assert_eq!(
        bchunk.schema.field_names(),
        vec!["team", "count", "median_score", "p90_score"]
    );
    let num = |row: usize, col: usize| bchunk.value(row, col).as_f64().unwrap();
    // Row 0 = A, row 1 = B (BTreeMap key order).
    assert!((num(0, 2) - 25.0).abs() < 1e-9, "A median");
    assert!((num(0, 3) - 37.0).abs() < 1e-9, "A p90");
    assert!((num(1, 2) - 52.5).abs() < 1e-9, "B median");
    assert!((num(1, 3) - 90.5).abs() < 1e-9, "B p90");

    // Chunk-size independence: every cell matches the base.
    for cs in [1usize, 2, 3, 5] {
        let r = run_src(&src, cs);
        let c = &r.outputs[0].chunks[0];
        for row in 0..c.len {
            for col in 0..bchunk.schema.fields.len() {
                assert_eq!(
                    c.value(row, col).to_string(),
                    bchunk.value(row, col).to_string(),
                    "cell[{row}][{col}] @cs={cs}"
                );
            }
        }
    }
}

#[test]
fn starts_ends_with_chunk_size_independent() {
    // starts_with / ends_with row filters must match a row-wise oracle and be
    // independent of chunk size.
    let rows = 20_000;
    let mut rng = Rng::new(29);
    let mut text = String::from("code\n");
    let mut starts = 0u64;
    let mut ends = 0u64;
    for _ in 0..rows {
        // codes like "JP-1234" / "US-0007" — prefix is a 2-letter country.
        let cc = ["JP", "US", "DE"][rng.below(3) as usize];
        let n = rng.below(10_000);
        let code = format!("{cc}-{n:04}");
        if code.starts_with("JP") {
            starts += 1;
        }
        if code.ends_with("7") {
            ends += 1;
        }
        text.push_str(&code);
        text.push('\n');
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_startsends",
        text.as_bytes(),
    ));
    let p = f.0.display();
    for cs in [1usize, 7, 1024, 8192, rows] {
        let s = run_src(
            &format!("S:\n open {p}\n |? starts_with(code, \"JP\")\n;"),
            cs,
        );
        assert_eq!(s.total_rows_out(), starts, "starts_with @cs={cs}");
        let e = run_src(&format!("E:\n open {p}\n |? ends_with(code, \"7\")\n;"), cs);
        assert_eq!(e.total_rows_out(), ends, "ends_with @cs={cs}");
        assert!(s.errors.is_empty() && e.errors.is_empty());
    }
}

#[test]
fn like_and_glob_chunk_size_independent() {
    // `like` (SQL %/_) and `glob` (*?[...]) row filters must match a row-wise
    // oracle and be chunk-size independent.
    let rows = 20_000;
    let mut rng = Rng::new(31);
    let mut text = String::from("code\n");
    let mut like_jp = 0u64;
    let mut glob_cls = 0u64;
    for _ in 0..rows {
        let cc = ["JP", "US", "DE"][rng.below(3) as usize];
        let n = rng.below(10_000);
        let code = format!("{cc}-{n:04}");
        if code.starts_with("JP-") {
            like_jp += 1; // like "JP-%"
        }
        // glob "[JD]*00" → starts with J or D and ends with "00".
        let first = code.chars().next().unwrap();
        if (first == 'J' || first == 'D') && code.ends_with("00") {
            glob_cls += 1;
        }
        text.push_str(&code);
        text.push('\n');
    }
    let f = TempCsv(gendata::write_temp_bytes(
        "stress_likeglob",
        text.as_bytes(),
    ));
    let p = f.0.display();
    for cs in [1usize, 7, 1024, 8192, rows] {
        let l = run_src(&format!("L:\n open {p}\n |? like(code, \"JP-%\")\n;"), cs);
        assert_eq!(l.total_rows_out(), like_jp, "like @cs={cs}");
        let g = run_src(
            &format!("G:\n open {p}\n |? glob(code, \"[JD]*00\")\n;"),
            cs,
        );
        assert_eq!(g.total_rows_out(), glob_cls, "glob @cs={cs}");
        assert!(l.errors.is_empty() && g.errors.is_empty());
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
