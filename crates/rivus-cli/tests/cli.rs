//! CLI program-input plumbing: a flow can be supplied as a file, inline via
//! `-c`, or piped on stdin (heredoc style). These tests drive the built
//! binary so they cover real argument parsing and stdin wiring.

use std::io::Write;
use std::process::{Command, Stdio};

const BIN: &str = env!("CARGO_BIN_EXE_rivus");

/// `check -c '<program>'` parses an inline program and reports node/edge counts.
#[test]
fn check_inline_command() {
    let out = Command::new(BIN)
        .args([
            "check",
            "-c",
            "U: open users.csv |? age >= 20 |> name age ;",
        ])
        .output()
        .expect("spawn rivus");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.starts_with("ok:"), "unexpected stdout: {stdout}");
}

/// `check -` reads the program from stdin (the heredoc path).
#[test]
fn check_from_stdin() {
    let mut child = Command::new(BIN)
        .args(["check", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rivus");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"U: open users.csv |? age >= 20 ;\n")
        .unwrap();
    let out = child.wait_with_output().expect("wait rivus");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).starts_with("ok:"));
}

/// Passing both `-c` and a path is a usage error (exit code 2).
#[test]
fn inline_and_path_conflict_errors() {
    let out = Command::new(BIN)
        .args(["check", "-c", "U: open x.csv ;", "extra.riv"])
        .output()
        .expect("spawn rivus");
    assert_eq!(out.status.code(), Some(2));
}

/// End-to-end: `run -c` over a real CSV, with `save stdout` leaving clean data
/// on stdout (visualization goes to stderr).
#[test]
fn run_inline_to_stdout() {
    let dir = std::env::temp_dir();
    let csv = dir.join(format!("rivus_cli_test_{}.csv", std::process::id()));
    std::fs::write(&csv, "name,age\nalice,30\nbob,15\ncarol,42\n").unwrap();

    let prog = format!(
        "U: open {} as csv |? age >= 20 |> name age save stdout as csv ;",
        csv.display()
    );
    let out = Command::new(BIN)
        .args(["run", "-c", &prog])
        .output()
        .expect("spawn rivus");
    let _ = std::fs::remove_file(&csv);

    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("alice"), "stdout: {stdout}");
    assert!(stdout.contains("carol"), "stdout: {stdout}");
    assert!(!stdout.contains("bob"), "filtered row leaked: {stdout}");
}

/// `gen <shape>` writes deterministic, seeded data to stdout: same seed →
/// byte-identical output; the `clean` shape is a valid CSV that Rivus can read
/// back; an unknown shape is a usage error.
#[test]
fn gen_is_deterministic_and_self_describing() {
    let run_gen = |args: &[&str]| {
        Command::new(BIN)
            .args(args)
            .output()
            .expect("spawn rivus gen")
    };

    // Same seed twice → identical bytes.
    let a = run_gen(&["gen", "clean", "--rows", "500", "--seed", "7"]);
    let b = run_gen(&["gen", "clean", "--rows", "500", "--seed", "7"]);
    assert!(a.status.success());
    assert_eq!(a.stdout, b.stdout, "same seed must be byte-identical");

    // A different seed changes the data.
    let c = run_gen(&["gen", "clean", "--rows", "500", "--seed", "8"]);
    assert_ne!(a.stdout, c.stdout, "different seed should differ");

    // `clean` has a header + exactly `rows` data lines.
    let text = String::from_utf8(a.stdout).unwrap();
    assert!(text.starts_with("id,name,age,score,country,active\n"));
    assert_eq!(text.lines().count(), 501, "header + 500 rows");

    // Unknown shape → usage error (exit 2).
    let bad = run_gen(&["gen", "wat"]);
    assert_eq!(bad.status.code(), Some(2));

    // jsonl shape emits one object per line.
    let j = run_gen(&["gen", "jsonl", "--rows", "3", "--seed", "1"]);
    let jtext = String::from_utf8(j.stdout).unwrap();
    assert_eq!(jtext.lines().count(), 3);
    assert!(jtext
        .lines()
        .all(|l| l.starts_with('{') && l.ends_with('}')));
}

/// The parallel byte-range reader must produce the *same* stdout — same rows,
/// same order — as the serial path, including for a `save -` (stdout) sink.
/// `RIVUS_PARALLEL_MIN_BYTES=0` forces the parallel path on a small file;
/// `RIVUS_NO_PARALLEL=1` forces serial. The two outputs must be byte-identical.
#[test]
fn parallel_stdout_matches_serial() {
    // Build a CSV big enough to split into several byte ranges.
    let dir = std::env::temp_dir();
    let csv = dir.join(format!("rivus_par_{}.csv", std::process::id()));
    let mut text = String::from("id,name,age\n");
    for i in 0..50_000 {
        text.push_str(&format!("{i},user{i},{}\n", 18 + (i % 70)));
    }
    std::fs::write(&csv, &text).unwrap();

    let prog = format!(
        "F: open {} |? age >= 50 |> id name age save - ;",
        csv.display()
    );
    let run = |force_serial: bool| {
        let mut cmd = Command::new(BIN);
        cmd.args(["run", "-c", &prog]);
        if force_serial {
            cmd.env("RIVUS_NO_PARALLEL", "1");
        } else {
            cmd.env("RIVUS_PARALLEL_MIN_BYTES", "0"); // force parallel
        }
        let out = cmd.output().expect("spawn rivus");
        assert!(
            out.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        out.stdout
    };

    let serial = run(true);
    let parallel = run(false);
    let _ = std::fs::remove_file(&csv);

    assert_eq!(
        serial, parallel,
        "parallel stdout differs from serial (rows/order must match)"
    );
    // Sanity: header + at least one data row survived the filter.
    let s = String::from_utf8(serial).unwrap();
    assert!(s.starts_with("id,name,age\n"));
    assert!(s.lines().count() > 1);
}
