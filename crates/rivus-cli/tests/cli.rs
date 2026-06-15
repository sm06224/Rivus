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

/// `rivus fmt -c …` reformats to canonical source on stdout and preserves the
/// `#{ … }#` / `# …` comment trivia (§25.7). Formatting is idempotent.
#[test]
fn fmt_preserves_comments_and_is_idempotent() {
    let prog = "F:\n #{ note }#\n open d.csv\n # adults\n |? age >= 20\n |> name age\n;";
    let out = Command::new(BIN)
        .args(["fmt", "-c", prog])
        .output()
        .expect("spawn rivus");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let once = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(once.contains("#{ note }#"), "block comment lost:\n{once}");
    assert!(once.contains("# adults"), "line comment lost:\n{once}");
    // Re-formatting the formatted output is a fixed point.
    let out2 = Command::new(BIN)
        .args(["fmt", "-c", once.trim_end()])
        .output()
        .expect("spawn rivus");
    assert!(out2.status.success());
    assert_eq!(
        once,
        String::from_utf8_lossy(&out2.stdout),
        "fmt not idempotent"
    );
}

/// A 2-way `->` tee now formats faithfully (round-trips), emitting the inline
/// `-> Label:` form rather than the old lossy placeholder.
#[test]
fn fmt_formats_a_tee_branch() {
    let prog = "U:\n open u.csv\n -> A: |? age >= 20 ;\n -> B: |? age < 20 ;\n;";
    let out = Command::new(BIN)
        .args(["fmt", "-c", prog])
        .output()
        .expect("spawn rivus");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("-> A:") && s.contains("-> B:"),
        "branch not rendered:\n{s}"
    );
    assert!(!s.contains("..."), "lossy placeholder leaked:\n{s}");
}

/// A single `->` branch (fan-out of one) now round-trips too (the renderer no
/// longer absorbs a single-output parent into the child chain).
#[test]
fn fmt_formats_a_single_branch() {
    let prog = "U:\n open u.csv\n -> Only: |? age >= 20 ;\n;";
    let out = Command::new(BIN)
        .args(["fmt", "-c", prog])
        .output()
        .expect("spawn rivus");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("-> Only:"));
}

/// fmt stays honest: a construct the canonical renderer cannot yet reproduce
/// losslessly — an anonymous, unlabeled scope (to_source only emits labeled
/// scopes) — is refused with a non-zero exit and the source left untouched,
/// rather than silently rewritten away.
#[test]
fn fmt_refuses_construct_it_cannot_round_trip() {
    let prog = ": open d.csv |? age >= 20 ;";
    let out = Command::new(BIN)
        .args(["fmt", "-c", prog])
        .output()
        .expect("spawn rivus");
    assert_eq!(out.status.code(), Some(1));
    assert!(out.stdout.is_empty(), "should not emit a rewritten program");
    assert!(String::from_utf8_lossy(&out.stderr).contains("cannot yet faithfully round-trip"));
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

/// `--json` emits machine-readable JSONL telemetry to stderr, while stdout
/// stays clean data. Every stderr line must be a valid JSON object, including a
/// final `summary`, and the data on stdout must be unaffected by the flag.
#[test]
fn telemetry_json_emits_jsonl_to_stderr() {
    let dir = std::env::temp_dir();
    let csv = dir.join(format!("rivus_tele_{}.csv", std::process::id()));
    std::fs::write(&csv, "id,age\n1,30\n2,55\n3,72\n").unwrap();

    let prog = format!("F: open {} |? age >= 50 |> id age save - ;", csv.display());
    let out = Command::new(BIN)
        .args(["run", "-c", &prog, "--json"])
        .output()
        .expect("spawn rivus");
    let _ = std::fs::remove_file(&csv);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // stdout: clean CSV (header + the two matching rows), no telemetry noise.
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert_eq!(
        stdout, "id,age\n2,55\n3,72\n",
        "stdout must stay clean data"
    );

    // stderr: every non-empty line is a JSON object; a `summary` line exists;
    // at least one `node` line carries the expected counter keys.
    let stderr = String::from_utf8(out.stderr).unwrap();
    let mut saw_summary = false;
    let mut saw_node = false;
    for line in stderr.lines().filter(|l| !l.trim().is_empty()) {
        assert!(
            line.starts_with('{') && line.ends_with('}'),
            "not a JSON object: {line}"
        );
        if line.contains("\"event\":\"summary\"") {
            saw_summary = true;
            assert!(line.contains("\"final_mode\":\"normal\""));
        }
        if line.contains("\"event\":\"node\"") {
            saw_node = true;
            assert!(line.contains("\"rows_out\":"));
            assert!(line.contains("\"kind\":"));
        }
    }
    assert!(saw_summary, "missing summary line in: {stderr}");
    assert!(saw_node, "missing node line in: {stderr}");
}

/// `--telemetry-addr HOST:PORT` streams the JSONL telemetry to a TCP socket
/// instead of stderr. A listener thread captures it; stdout stays clean data.
#[test]
fn telemetry_addr_streams_jsonl_over_tcp() {
    use std::io::Read;
    use std::net::TcpListener;

    let dir = std::env::temp_dir();
    let csv = dir.join(format!("rivus_teleaddr_{}.csv", std::process::id()));
    std::fs::write(&csv, "id,age\n1,30\n2,55\n3,72\n").unwrap();

    // Bind to an ephemeral port and hand the address to the CLI.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let handle = std::thread::spawn(move || {
        let (mut conn, _) = listener.accept().unwrap();
        let mut buf = String::new();
        conn.read_to_string(&mut buf).unwrap();
        buf
    });

    let prog = format!("F: open {} |? age >= 50 |> id age save - ;", csv.display());
    let out = Command::new(BIN)
        .args(["run", "-c", &prog, "--telemetry-addr", &addr])
        .output()
        .expect("spawn rivus");
    let _ = std::fs::remove_file(&csv);
    assert!(out.status.success());

    // stdout stays clean data.
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert_eq!(stdout, "id,age\n2,55\n3,72\n");

    // The socket received valid JSONL with a summary line.
    let received = handle.join().unwrap();
    let mut saw_summary = false;
    for line in received.lines().filter(|l| !l.trim().is_empty()) {
        assert!(
            line.starts_with('{') && line.ends_with('}'),
            "bad line: {line}"
        );
        if line.contains("\"event\":\"summary\"") {
            saw_summary = true;
        }
    }
    assert!(saw_summary, "no summary over socket: {received}");
    // Telemetry went to the socket, so stderr carries no JSONL.
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(
        !stderr.contains("\"event\""),
        "stderr leaked telemetry: {stderr}"
    );
}

/// `--serve ADDR` launches the live dashboard: `GET /` returns the HTML,
/// `GET /snapshot` returns JSON, `GET /events` streams ≥1 SSE frame — while
/// stdout stays clean data. Drives the real binary on a fixed loopback port.
#[test]
fn serve_dashboard_responds_over_http() {
    use std::io::{Read, Write as _};
    use std::net::TcpStream;
    use std::time::Duration;

    let dir = std::env::temp_dir();
    let csv = dir.join(format!("rivus_serve_{}.csv", std::process::id()));
    // Enough rows that the run lasts long enough to serve a few requests.
    let mut data = String::from("id,age\n");
    for i in 0..120_000u64 {
        data.push_str(&format!("{i},{}\n", i % 100));
    }
    std::fs::write(&csv, data).unwrap();
    let out = dir.join(format!("rivus_serve_{}.out", std::process::id()));

    // Pick a port unlikely to clash in CI.
    let addr = "127.0.0.1:8788";
    let prog = format!(
        "F: open {} |? age >= 50 |> id age save {} ;",
        csv.display(),
        out.display()
    );
    let mut child = Command::new(BIN)
        .args(["run", "-c", &prog, "--serve", addr])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn rivus --serve");

    // Give the server a moment to bind.
    std::thread::sleep(Duration::from_millis(300));

    let http_get = |path: &str| -> Option<String> {
        let mut s = TcpStream::connect(addr).ok()?;
        s.set_read_timeout(Some(Duration::from_millis(800))).ok();
        write!(
            s,
            "GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"
        )
        .ok()?;
        let mut buf = String::new();
        let _ = s.read_to_string(&mut buf); // timeout is fine for SSE
        Some(buf)
    };

    let root = http_get("/").unwrap_or_default();
    assert!(root.contains("200 OK"), "GET / status: {:.60}", root);
    assert!(root.contains("<!doctype html>"), "GET / should serve HTML");

    let events = http_get("/events").unwrap_or_default();
    assert!(
        events.contains("text/event-stream"),
        "GET /events should be an SSE stream: {:.80}",
        events
    );
    assert!(
        events.contains("\"rows_seen\""),
        "GET /events should carry a snapshot frame"
    );

    let status = child.wait().expect("run completes");
    assert!(status.success(), "served run should succeed");

    // stdout stayed clean (the sink wrote to a file, not stdout).
    let mut stdout = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .ok();
    assert!(
        stdout.is_empty(),
        "stdout must stay clean under --serve: {stdout:.60}"
    );

    let _ = std::fs::remove_file(&csv);
    let _ = std::fs::remove_file(&out);
}

/// #36 (A2 exposure): a parallel run's `--json` summary carries a
/// `worker_breakdown` array (per-worker rows_out / busy), not just the count.
/// We force the streaming-parallel reader with `RIVUS_PARALLEL_MIN_BYTES=0`.
#[test]
fn json_summary_exposes_worker_breakdown_on_parallel_runs() {
    // A file with enough rows to split into ≥2 byte ranges, and a real `save`
    // sink (parallel needs a file sink).
    let mut body = String::from("id,age\n");
    for i in 0..20_000u32 {
        body.push_str(&format!("{i},{}\n", i % 90));
    }
    let csv = std::env::temp_dir().join(format!("rivus_wbreak_{}.csv", std::process::id()));
    std::fs::write(&csv, &body).unwrap();
    let mut out = csv.clone();
    out.set_extension("wbreak.out.csv");
    let prog = format!(
        "F:\n open {}\n |? age >= 30\n |> id age\n save {}\n;",
        csv.display(),
        out.display()
    );

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_rivus"))
        .args(["run", "-c", &prog, "--json"])
        .env("RIVUS_PARALLEL_MIN_BYTES", "0")
        .env_remove("RIVUS_NO_PARALLEL")
        .output()
        .expect("run rivus --json");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let summary = stderr
        .lines()
        .find(|l| l.contains("\"event\":\"summary\""))
        .expect("a summary line on stderr");
    assert!(
        summary.contains("\"workers\":"),
        "parallel summary must report a worker count: {summary}"
    );
    assert!(
        summary.contains("\"worker_breakdown\":[{\"worker\":0,"),
        "parallel summary must expose the per-worker breakdown: {summary}"
    );
    let _ = std::fs::remove_file(&csv);
    let _ = std::fs::remove_file(&out);
}

// ─── §31 stage 1: `.riv.md` Literate ──────────────────────────────────────

/// A unique temp `.riv.md` path for this process + tag (avoids cross-test races).
fn tmp_md(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("rivus_md_{}_{}.riv.md", tag, std::process::id()))
}

/// `run <doc.riv.md>` extracts the executable program from the ```flow fence(s),
/// ignores frontmatter/prose, and runs it — data reaches stdout via the sink.
#[test]
fn riv_md_run_extracts_flow_and_ignores_prose() {
    let dir = std::env::temp_dir();
    let csv = dir.join(format!("rivus_md_data_{}.csv", std::process::id()));
    std::fs::write(&csv, "name,age\nalice,30\nbob,15\ncarol,42\n").unwrap();
    let md = tmp_md("run");
    let doc = format!(
        "---\ntitle: adults\nchunk_size: 2\nneeds: [read:data.csv]\n---\n\n\
         # Heading\n\nThis prose is inert.\n\n\
         ```flow\n#| name: adults\nU: open {} as csv |? age >= 20 |> name age save stdout as csv ;\n```\n",
        csv.display()
    );
    std::fs::write(&md, &doc).unwrap();

    let out = Command::new(BIN)
        .args(["run"])
        .arg(&md)
        .output()
        .expect("spawn rivus");
    let _ = std::fs::remove_file(&csv);
    let _ = std::fs::remove_file(&md);

    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("alice") && stdout.contains("carol"),
        "stdout: {stdout}"
    );
    assert!(!stdout.contains("bob"), "filtered row leaked: {stdout}");
    // Prose / frontmatter must never reach the executed program or output.
    assert!(
        !stdout.contains("Heading") && !stdout.contains("title"),
        "prose leaked: {stdout}"
    );
}

/// `fmt <doc.riv.md>` reformats only the ```flow body and round-trips prose,
/// frontmatter and `#|` options verbatim; it is idempotent.
#[test]
fn riv_md_fmt_preserves_prose_and_is_idempotent() {
    let md = tmp_md("fmt");
    let doc = "---\ntitle: demo\nchunk_size: 4096\n---\n\n\
               # 見出し\n\ninert prose\n\n\
               ```flow\n#| name: a\nU: open d.csv |? age >= 20 |> name age ;\n```\n";
    std::fs::write(&md, doc).unwrap();

    let out = Command::new(BIN)
        .args(["fmt"])
        .arg(&md)
        .output()
        .expect("spawn rivus");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let once = String::from_utf8_lossy(&out.stdout).to_string();
    // Frontmatter, prose and the `#|` option survive; the flow body is canonicalized.
    assert!(once.contains("title: demo"), "frontmatter lost:\n{once}");
    assert!(
        once.contains("# 見出し") && once.contains("inert prose"),
        "prose lost:\n{once}"
    );
    assert!(once.contains("#| name: a"), "cell option lost:\n{once}");
    assert!(
        once.contains("|? $_.age >= 20"),
        "flow not canonicalized:\n{once}"
    );

    // Idempotent: writing the formatted doc back and reformatting is a fixed point.
    std::fs::write(&md, &once).unwrap();
    let out2 = Command::new(BIN)
        .args(["fmt"])
        .arg(&md)
        .output()
        .expect("spawn rivus");
    let _ = std::fs::remove_file(&md);
    assert!(out2.status.success());
    assert_eq!(
        once,
        String::from_utf8_lossy(&out2.stdout),
        "fmt not idempotent"
    );
}

/// A `.riv.md` with no ```flow fence is a never-silent error (untagged / other
/// fences are inert and cannot be executed).
#[test]
fn riv_md_without_flow_fence_errors() {
    let md = tmp_md("noflow");
    std::fs::write(&md, "# just prose\n\n```\nopen NOT_RUN.csv ;\n```\n").unwrap();
    let out = Command::new(BIN)
        .args(["check"])
        .arg(&md)
        .output()
        .expect("spawn rivus");
    let _ = std::fs::remove_file(&md);
    assert!(!out.status.success(), "should fail with no flow fence");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("flow"),
        "stderr should mention the missing flow fence"
    );
}

/// The (R) chunk-size cascade is result-invariant (§31.3): the frontmatter hint
/// and a CLI override produce byte-identical output (serial byte-identity across
/// chunk sizes), and `--chunk-size` takes precedence without changing bytes.
#[test]
fn riv_md_chunk_size_cascade_is_result_invariant() {
    let dir = std::env::temp_dir();
    let csv = dir.join(format!("rivus_md_cascade_{}.csv", std::process::id()));
    let mut body = String::from("name,age\n");
    for i in 0..50 {
        body.push_str(&format!("u{i},{}\n", 20 + (i % 5)));
    }
    std::fs::write(&csv, &body).unwrap();
    let md = tmp_md("cascade");
    let doc = format!(
        "---\nchunk_size: 3\n---\n\n```flow\nU: open {} as csv |? age >= 22 |> name age save stdout as csv ;\n```\n",
        csv.display()
    );
    std::fs::write(&md, &doc).unwrap();

    let from_fm = Command::new(BIN)
        .args(["run"])
        .arg(&md)
        .output()
        .expect("spawn rivus");
    let from_cli = Command::new(BIN)
        .args(["run"])
        .arg(&md)
        .args(["--chunk-size", "64"])
        .output()
        .expect("spawn rivus");
    let _ = std::fs::remove_file(&csv);
    let _ = std::fs::remove_file(&md);

    assert!(from_fm.status.success() && from_cli.status.success());
    assert_eq!(
        from_fm.stdout, from_cli.stdout,
        "chunk-size is an (R) hint — output bytes must not depend on it"
    );
}
