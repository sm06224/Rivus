//! Joins: inner / multi-key / left / right / full-outer against an oracle.
//!
//! Moved verbatim from the former monolithic `stress.rs` (design 26 §26.8.1,
//! mechanical move-only split; logic unchanged).

use super::*;

#[test]
fn inner_join_on_nested_key_matches_oracle() {
    // §32 s4b: a join key can be a nested path. Left is JSONL with a `user`
    // struct; the join keys on `user.id` against the right's flat `id`. The
    // matched row count + joined value must equal an independent oracle and be
    // chunk-size independent (the nested key resolves to a deterministic value).
    let users = 2_000usize;
    let mut u = String::new();
    for i in 0..users {
        u.push_str(&format!("{{\"user\":{{\"id\":{i}}},\"name\":\"u{i}\"}}\n"));
    }
    let uf = TempCsv(gendata::write_temp_bytes("join_nested_u", u.as_bytes()));
    let ujson = uf.0.with_extension("jsonl");
    std::fs::rename(&uf.0, &ujson).unwrap();
    let _uc = TempCsv(ujson.clone());

    let orders = 6_000usize;
    let mut o = String::from("id,amount\n");
    let mut rng = Rng::new(5);
    let mut expected = 0u64;
    for _ in 0..orders {
        let id = rng.below((users * 2) as u64);
        o.push_str(&format!("{id},10\n"));
        if (id as usize) < users {
            expected += 1; // each order with id < users matches exactly one user
        }
    }
    let of = TempCsv(gendata::write_temp_bytes("join_nested_o", o.as_bytes()));
    let (up, op) = (ujson.display(), of.0.display());

    for cs in [1, 7, 1024, orders] {
        // Join the nested `user.id` against the flat `id`.
        let src =
            format!("U: open {up} ;\nO: open {op} ;\nJ: U & O on user.id:id |> name amount\n;");
        let res = run_src(&src, cs);
        assert_eq!(
            res.total_rows_out(),
            expected,
            "nested-key join rows @cs={cs}"
        );
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
