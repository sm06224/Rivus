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
