//! #237 merge preconditions (R1/R2, レビュー兼指揮 2026-07-12): committed
//! byte-identity guards for the parallel read paths.
//!
//! - **R1**: `.csv.gz` / `.jsonl.gz` — serial == parallel over the compressed
//!   read→join→group path (the 0.86×/0.90× headline paths), rows compared
//!   **in order** (no sort), so the parallel group emit order is asserted too.
//!   Fixtures carry dirty data at the parallel seams: a malformed row, a bad
//!   cell, and a file missing a column (union-by-name widening).
//! - **R2**: the parallel read→join→sink path emits **left-row order** —
//!   asserted against a hand-computed expected byte sequence (no sort), with a
//!   duplicate right key pinning the multi-match (right build insertion)
//!   order. This is the order guard for the slice-15 Fx hash-table swap.

use super::*;

/// A temp directory of fixture files, removed on drop.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!("rivus_{tag}_{}_{n}", std::process::id()));
        std::fs::create_dir_all(&p).expect("mkdir");
        TempDir(p)
    }
    fn file(&self, name: &str) -> std::path::PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(feature = "gzip")]
fn write_gz(path: &std::path::Path, text: &str) {
    use std::io::Write as _;
    let f = std::fs::File::create(path).expect("create gz");
    let mut enc = flate2::write::GzEncoder::new(f, flate2::Compression::default());
    enc.write_all(text.as_bytes()).expect("gz write");
    enc.finish().expect("gz finish");
}

/// Run `flow`, capture statement `label`, and return every output row as one
/// tab-joined string — **in emit order** (never sorted; order IS the assertion)
/// — plus the surfaced strategy note.
fn collect_rows(
    flow: &str,
    label: &str,
    pref: rivus_runtime::MemoryPref,
    chunk_size: usize,
) -> (Vec<String>, Option<String>) {
    let g = rivus_parser::parse(flow).expect("parse");
    let res = run(
        &g,
        RunOptions {
            chunk_size,
            memory: pref,
            ..Default::default()
        },
    )
    .expect("run");
    let o = res
        .outputs
        .iter()
        .find(|o| o.label.as_deref() == Some(label))
        .expect("captured output");
    let rows = o
        .chunks
        .iter()
        .flat_map(|c| {
            (0..c.len).map(move |r| {
                (0..c.columns.len())
                    .map(|i| c.value(r, i).to_string())
                    .collect::<Vec<_>>()
                    .join("\t")
            })
        })
        .collect();
    (rows, res.strategy)
}

/// R1 fixture: three files over `order_id,region,amount,category` with dirty
/// data — file 01 has a wrong-arity row and an unparsable `amount` cell, file
/// 02 lacks the `amount` column entirely (union-by-name adds it back as null).
/// CSV texts; the JSONL twin mirrors the same rows/dirt.
fn r1_csv_texts() -> [String; 3] {
    let mut f0 = String::from("order_id,region,amount,category\n");
    for i in 0..2000 {
        f0.push_str(&format!("{},r{},{},c{}\n", i, i % 7, i * 3 % 1000, i % 5));
    }
    let mut f1 = String::from("order_id,region,amount,category\n");
    for i in 2000..4000 {
        if i == 2500 {
            f1.push_str("9999,r1\n"); // wrong arity: skipped, counted in pass 1
        }
        if i == 3000 {
            f1.push_str("3000,r2,notanumber,c0\n"); // amount → null + report
            continue;
        }
        f1.push_str(&format!("{},r{},{},c{}\n", i, i % 7, i * 3 % 1000, i % 5));
    }
    let mut f2 = String::from("order_id,region,category\n"); // no `amount`
    for i in 4000..6000 {
        f2.push_str(&format!("{},r{},c{}\n", i, i % 7, i % 5));
    }
    [f0, f1, f2]
}

fn r1_jsonl_texts() -> [String; 3] {
    let mut f0 = String::new();
    for i in 0..2000 {
        f0.push_str(&format!(
            "{{\"order_id\":{},\"region\":\"r{}\",\"amount\":{},\"category\":\"c{}\"}}\n",
            i,
            i % 7,
            i * 3 % 1000,
            i % 5
        ));
    }
    let mut f1 = String::new();
    for i in 2000..4000 {
        if i == 2500 {
            f1.push_str("{\"order_id\":9999,\"region\":\n"); // malformed: skipped
        }
        f1.push_str(&format!(
            "{{\"order_id\":{},\"region\":\"r{}\",\"amount\":{},\"category\":\"c{}\"}}\n",
            i,
            i % 7,
            i * 3 % 1000,
            i % 5
        ));
    }
    let mut f2 = String::new(); // no `amount` key anywhere
    for i in 4000..6000 {
        f2.push_str(&format!(
            "{{\"order_id\":{},\"region\":\"r{}\",\"category\":\"c{}\"}}\n",
            i,
            i % 7,
            i % 5
        ));
    }
    [f0, f1, f2]
}

fn regions_csv() -> String {
    // r5/r6 unmatched on purpose (left join keeps them with a null country).
    "region,country\nr0,JP\nr1,US\nr2,DE\nr3,FR\nr4,BR\n".to_string()
}

/// The shared R1 body: serial (Low) is the oracle; parallel (Unbounded, which
/// engages the per-file group driver at any size) must reproduce it row-for-row
/// at more than one chunk size.
fn assert_gz_group_identity(dir: &TempDir, glob: &str, fmt: &str) {
    let threads = std::thread::available_parallelism()
        .map(|t| t.get())
        .unwrap_or(1);
    if threads < 2 {
        eprintln!("skipping: single-core runner cannot exercise the parallel path");
        return;
    }
    let rp = gendata::write_temp("pgz_regions", &regions_csv());
    let _rguard = TempCsv(rp.clone());
    let flow = format!(
        "R: open {} (region:str country:str) ;\n\
         S: ls \"{}/{}\" read as {} cast amount :int ;\n\
         J: S &left R on region\n\
            |# country region sum:amount count:order_id ;",
        rp.display(),
        dir.0.display(),
        glob,
        fmt
    );
    for cs in [7usize, 4096] {
        let (oracle, s_strat) = collect_rows(&flow, "J", rivus_runtime::MemoryPref::Low, cs);
        assert!(
            !oracle.is_empty(),
            "oracle must have groups (fixture sanity)"
        );
        assert!(
            s_strat.is_none_or(|s| !s.contains("parallel read group-by")),
            "Low must stay serial"
        );
        let (par, p_strat) = collect_rows(&flow, "J", rivus_runtime::MemoryPref::Unbounded, cs);
        assert_eq!(
            p_strat.as_deref(),
            Some("parallel read group-by (per-file workers)"),
            "the parallel group driver must actually engage (else this guard tests nothing)"
        );
        // Row-for-row, in emit order: this pins output bytes AND the group
        // emit order (composite-key order via seal()) in one assertion.
        assert_eq!(par, oracle, "serial == parallel (in order) @cs={cs}");
    }
}

/// R1a: `.csv.gz` serial == parallel byte-identity (compressed group path).
#[cfg(feature = "gzip")]
#[test]
fn gz_csv_group_serial_parallel_byte_identical() {
    let dir = TempDir::new("pgz_csv");
    for (i, text) in r1_csv_texts().iter().enumerate() {
        write_gz(&dir.file(&format!("part_0{i}.csv.gz")), text);
    }
    assert_gz_group_identity(&dir, "*.csv.gz", "csv");
}

/// R1b: `.jsonl.gz` serial == parallel byte-identity (compressed group path).
#[cfg(feature = "gzip")]
#[test]
fn gz_jsonl_group_serial_parallel_byte_identical() {
    let dir = TempDir::new("pgz_jsonl");
    for (i, text) in r1_jsonl_texts().iter().enumerate() {
        write_gz(&dir.file(&format!("part_0{i}.jsonl.gz")), text);
    }
    assert_gz_group_identity(&dir, "*.jsonl.gz", "jsonl");
}

/// R3 (Stage C, design/41 §5): plain-CSV group flows open **speculatively**
/// (schema from a row sample, no pass-1 scan). This guard pins, against the
/// serial canonical oracle:
/// - byte-identity in emit order when a file **contradicts** its sample (an
///   unparsable `amount` beyond the sample window at cs=7 → local canonical
///   re-run of that file while every other partial is kept, union widened
///   I64→Str), and when nothing contradicts (cs=4096 samples whole files);
/// - the malformed-row reports (in-stream arity counting must equal pass 1's
///   count per file — never-silent survives the skipped scan);
/// - that speculation actually engages (strategy suffix), else this tests
///   nothing.
#[test]
fn stage_c_speculative_group_serial_parallel_byte_identical() {
    let threads = std::thread::available_parallelism()
        .map(|t| t.get())
        .unwrap_or(1);
    if threads < 2 {
        eprintln!("skipping: single-core runner cannot exercise the parallel path");
        return;
    }
    let dir = TempDir::new("stagec");
    for (i, text) in r1_csv_texts().iter().enumerate() {
        std::fs::write(dir.file(&format!("part_0{i}.csv")), text).unwrap();
    }
    let rp = gendata::write_temp("stagec_regions", &regions_csv());
    let _rguard = TempCsv(rp.clone());
    let flow = format!(
        "R: open {} (region:str country:str) ;\n\
         S: ls \"{}/*.csv\" read as csv cast amount :int ;\n\
         J: S &left R on region\n\
            |# country region sum:amount count:order_id ;",
        rp.display(),
        dir.0.display(),
    );
    let run_once = |pref: rivus_runtime::MemoryPref,
                    cs: usize|
     -> (Vec<String>, Option<String>, Vec<String>) {
        let g = rivus_parser::parse(&flow).expect("parse");
        let res = run(
            &g,
            RunOptions {
                chunk_size: cs,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run");
        let o = res
            .outputs
            .iter()
            .find(|o| o.label.as_deref() == Some("J"))
            .expect("captured output");
        let rows = o
            .chunks
            .iter()
            .flat_map(|c| {
                (0..c.len).map(move |r| {
                    (0..c.columns.len())
                        .map(|i| c.value(r, i).to_string())
                        .collect::<Vec<_>>()
                        .join("\t")
                })
            })
            .collect();
        let mut bad: Vec<String> = res
            .errors
            .iter()
            .filter(|e| e.message.contains("malformed"))
            .map(|e| e.message.clone())
            .collect();
        bad.sort();
        (rows, res.strategy, bad)
    };
    // cs=7: the sample window is 7 rows, so file 01's `notanumber` cell
    // contradicts mid-stream (local re-run). cs=4096: every file fits the
    // sample, zero contradictions (pure speculation win).
    for cs in [7usize, 4096] {
        let (oracle, s_strat, s_bad) = run_once(rivus_runtime::MemoryPref::Low, cs);
        assert!(!oracle.is_empty(), "oracle must have groups");
        assert!(
            s_strat.is_none_or(|s| !s.contains("parallel read group-by")),
            "Low must stay serial"
        );
        let (par, p_strat, p_bad) = run_once(rivus_runtime::MemoryPref::Unbounded, cs);
        assert_eq!(
            p_strat.as_deref(),
            Some("parallel read group-by (per-file workers, speculative open)"),
            "Stage C must actually engage (else this guard tests nothing)"
        );
        assert_eq!(par, oracle, "serial == parallel (in order) @cs={cs}");
        assert_eq!(
            p_bad, s_bad,
            "malformed-row reports must survive the skipped pass-1 scan @cs={cs}"
        );
    }
}

/// R3b (Stage C, design/41 §5): a contradiction that widens a column between
/// NUMERIC lanes (i64→f64 here, via a `1.5` beyond the sample window) is not
/// Display-exact above 2^53, so the driver must bail to the serial canonical
/// path instead of keeping partials — correctness over speed, and the output
/// still matches the oracle exactly.
#[test]
fn stage_c_numeric_widening_bails_to_serial() {
    let threads = std::thread::available_parallelism()
        .map(|t| t.get())
        .unwrap_or(1);
    if threads < 2 {
        eprintln!("skipping: single-core runner cannot exercise the parallel path");
        return;
    }
    let dir = TempDir::new("stagec_f64");
    let mut f0 = String::from("order_id,region,amount\n");
    for i in 0..500 {
        f0.push_str(&format!("{i},r{},{}\n", i % 3, i * 3 % 100));
    }
    let mut f1 = String::from("order_id,region,amount\n");
    for i in 500..1000 {
        f1.push_str(&format!("{i},r{},{}\n", i % 3, i * 3 % 100));
    }
    f1.push_str("1000,r1,1.5\n"); // beyond the 7-row sample: i64 → f64 widening
    std::fs::write(dir.file("part_00.csv"), &f0).unwrap();
    std::fs::write(dir.file("part_01.csv"), &f1).unwrap();
    let rp = gendata::write_temp("stagec_f64_regions", &regions_csv());
    let _rguard = TempCsv(rp.clone());
    // No cast on amount: `sum:amount` alone would already fail the C-eq gate,
    // so cast to :float — value-safe — and let the union widening trigger the
    // re-run-time bail instead (the case this guard is about).
    let flow = format!(
        "R: open {} (region:str country:str) ;\n\
         S: ls \"{}/*.csv\" read as csv cast amount :float ;\n\
         J: S &left R on region\n\
            |# country region count:order_id ;",
        rp.display(),
        dir.0.display(),
    );
    let (oracle, _) = collect_rows(&flow, "J", rivus_runtime::MemoryPref::Low, 7);
    assert!(!oracle.is_empty(), "oracle must have groups");
    let (par, p_strat) = collect_rows(&flow, "J", rivus_runtime::MemoryPref::Unbounded, 7);
    assert!(
        p_strat
            .as_deref()
            .is_none_or(|s| !s.contains("parallel read group-by")),
        "numeric widening must abandon the parallel driver (got {p_strat:?})"
    );
    assert_eq!(par, oracle, "the serial fallback must match the oracle");
}

/// R2: the parallel read→join→sink path writes **left-row order** — file uri
/// order, then row order within each file, then right-match (build insertion)
/// order for a duplicated right key. The expected bytes are hand-computed with
/// no sorting anywhere; a hash-order leak in the probe table (slice 15's Fx
/// swap) or in segment concatenation would break this exactly.
#[test]
fn parallel_sink_join_emits_left_row_order_without_sort() {
    let threads = std::thread::available_parallelism()
        .map(|t| t.get())
        .unwrap_or(1);
    if threads < 2 {
        eprintln!("skipping: single-core runner cannot exercise the parallel path");
        return;
    }
    let dir = TempDir::new("psink");
    // Left files: uri order is part_00 then part_01.
    let mut f0 = String::from("order_id,region\n");
    for i in 0..500 {
        f0.push_str(&format!("{},r{}\n", i, i % 3));
    }
    let mut f1 = String::from("order_id,region\n");
    for i in 500..1000 {
        // r9 never matches: left join keeps the row with a null country.
        let region = if i % 100 == 0 { 9 } else { i % 3 };
        f1.push_str(&format!("{},r{}\n", i, region));
    }
    std::fs::write(dir.file("part_00.csv"), &f0).unwrap();
    std::fs::write(dir.file("part_01.csv"), &f1).unwrap();
    // Right side: r1 appears TWICE — each matching left row must emit both
    // right rows in build (insertion) order: US then USA.
    let rp = gendata::write_temp(
        "psink_regions",
        "region,country\nr0,JP\nr1,US\nr2,DE\nr1,USA\n",
    );
    let _rguard = TempCsv(rp.clone());
    let out = dir.file("joined_out.csv");

    // Expected bytes, computed independently with no sorting: header, then for
    // each file in uri order, each row in order, each right match in right-row
    // order (null country = empty cell).
    let rights = |region: &str| -> Vec<&str> {
        match region {
            "r0" => vec!["JP"],
            "r1" => vec!["US", "USA"],
            "r2" => vec!["DE"],
            _ => vec![""], // unmatched: kept by the left join, null-padded
        }
    };
    let mut expected = String::from("order_id,region,country\n");
    for (i, region_of) in [
        (0..500).collect::<Vec<i64>>(),
        (500..1000).collect::<Vec<i64>>(),
    ]
    .into_iter()
    .enumerate()
    {
        for id in region_of {
            let region = if i == 1 && id % 100 == 0 {
                "r9".to_string()
            } else {
                format!("r{}", id % 3)
            };
            for c in rights(&region) {
                expected.push_str(&format!("{id},{region},{c}\n"));
            }
        }
    }

    let flow = format!(
        "R: open {} (region:str country:str) ;\n\
         S: ls \"{}/*.csv\" read as csv ;\n\
         J: S &left R on region save {} ;",
        rp.display(),
        dir.0.display(),
        out.display()
    );
    let run_once = |pref: rivus_runtime::MemoryPref| -> (String, Option<String>) {
        let g = rivus_parser::parse(&flow).expect("parse");
        let res = run(
            &g,
            RunOptions {
                chunk_size: 64,
                memory: pref,
                ..Default::default()
            },
        )
        .expect("run");
        let bytes = std::fs::read_to_string(&out).expect("sink file");
        let _ = std::fs::remove_file(&out);
        (bytes, res.strategy)
    };

    let (serial, s_strat) = run_once(rivus_runtime::MemoryPref::Low);
    assert!(
        s_strat.is_none_or(|s| !s.contains("parallel read sink")),
        "Low must stay serial"
    );
    assert_eq!(serial, expected, "serial sink must be exact left-row order");

    let (parallel, p_strat) = run_once(rivus_runtime::MemoryPref::Unbounded);
    assert_eq!(
        p_strat.as_deref(),
        Some("parallel read sink (per-file segments)"),
        "the parallel sink driver must actually engage (else this guard tests nothing)"
    );
    assert_eq!(
        parallel, expected,
        "parallel sink must be byte-identical, in left-row order, without sort"
    );
}
