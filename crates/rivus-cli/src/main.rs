//! `rivus` — the command-line shell / runner for the Rivus stream runtime.
//!
//! Subcommands:
//!   rivus run <file.riv>      parse, execute, and visualize a flow
//!   rivus explain <file.riv>  show the DAG IR + regenerated source
//!   rivus check <file.riv>    parse only (report errors)
//!
//! Program input (any subcommand):
//!   <file.riv>                read the program from a file
//!   -c, --command <STRING>    take the program inline, as a string argument
//!   - | stdin                 read the program from standard input (heredoc)
//!
//! Flags:
//!   --chunk-size <N>          rows per chunk emitted by sources (default 4096)
//!   --no-opt                  disable the IR optimizer (run/explain)

mod serve;
mod viz;

use rivus_runtime::{gendata, run, run_with_progress, MemoryPref, RunOptions, RuntimeSnapshot};
use std::io::{IsTerminal, Read, Write};
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        usage();
        return ExitCode::from(2);
    }

    let mut cmd = args[1].as_str();
    if matches!(cmd, "-h" | "--help" | "help") {
        usage();
        return ExitCode::SUCCESS;
    }

    // `rivus gen` — self-hosted, deterministic data generation (dogfooding), so
    // benches and demos need no external awk/python. Writes to stdout.
    if cmd == "gen" {
        return run_gen(&args[2..]);
    }

    // Bare Unix-filter form: `rivus '|? age >= 20 |> name age'` (no subcommand).
    // If arg 1 is a transform-only program rather than a known subcommand, run
    // it as a stdin→stdout filter. Flags still parse from arg 2.
    let mut path: Option<String> = None;
    let mut inline: Option<String> = None;
    if !matches!(cmd, "run" | "explain" | "check" | "fmt") && is_transform_only(cmd) {
        inline = Some(args[1].clone());
        cmd = "run";
    }
    // Track an *explicit* `--chunk-size` so the §31 config cascade can apply
    // `frontmatter ← CLI`: the CLI wins when given, otherwise a `.riv.md`
    // frontmatter hint (an (R) resource hint, result-invariant) supplies the
    // default (§31.3). `None` here means "not set on the command line".
    let mut chunk_size_cli: Option<usize> = None;
    let mut optimize = true;
    // `rivus fmt … --write`/`-w`: rewrite the source file in place instead of
    // printing the canonical form to stdout.
    let mut fmt_write = false;
    let mut telemetry_json = false;
    let mut telemetry_addr: Option<String> = None;
    // `--serve [ADDR]`: launch the live HTTP/SSE dashboard (Pillar B). The
    // optional address defaults to an ephemeral loopback port.
    let mut serve_addr: Option<String> = None;
    // `--open`: also open the `--serve` dashboard URL in the system browser.
    let mut open_browser = false;
    // `--tui`: repaint a live ANSI dashboard on stderr as the run streams.
    let mut tui = false;
    // `--memory low|auto|fast|unbounded` (Pillar C): reader memory/speed strategy.
    let mut memory = MemoryPref::default();
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--no-opt" => optimize = false,
            "--write" | "-w" => fmt_write = true,
            // Emit machine-readable JSONL telemetry to stderr (Observability
            // spec §19: base for editor/GUI). `--telemetry json` or `--json`.
            "--json" => telemetry_json = true,
            "--telemetry" => {
                i += 1;
                match args.get(i).map(|s| s.as_str()) {
                    Some("json") => telemetry_json = true,
                    Some("ascii") | None => {}
                    Some(other) => {
                        eprintln!("error: --telemetry expects 'json' or 'ascii', got '{other}'");
                        return ExitCode::from(2);
                    }
                }
            }
            // Stream the JSONL telemetry to a TCP socket (HOST:PORT) instead of
            // stderr — a live feed for an external viewer/GUI. Implies --json.
            "--telemetry-addr" => {
                i += 1;
                match args.get(i) {
                    Some(a) => {
                        telemetry_addr = Some(a.clone());
                        telemetry_json = true;
                    }
                    None => {
                        eprintln!("error: --telemetry-addr requires a HOST:PORT");
                        return ExitCode::from(2);
                    }
                }
            }
            // Live dashboard: `--serve` (ephemeral loopback port) or
            // `--serve HOST:PORT`. The next arg is the address only if it isn't
            // another flag.
            "--tui" => tui = true,
            "--memory" => {
                i += 1;
                match args.get(i).and_then(|s| MemoryPref::parse(s)) {
                    Some(m) => memory = m,
                    None => {
                        eprintln!("error: --memory expects low|auto|fast|unbounded");
                        return ExitCode::from(2);
                    }
                }
            }
            "--serve" => {
                let addr = match args.get(i + 1) {
                    Some(a) if !a.starts_with("--") => {
                        i += 1;
                        a.clone()
                    }
                    _ => "127.0.0.1:0".to_string(),
                };
                serve_addr = Some(addr);
            }
            "--open" => open_browser = true,
            "--chunk-size" => {
                i += 1;
                match args.get(i).and_then(|v| v.parse::<usize>().ok()) {
                    Some(n) if n >= 1 => chunk_size_cli = Some(n),
                    _ => {
                        eprintln!("error: --chunk-size requires a positive integer");
                        return ExitCode::from(2);
                    }
                }
            }
            "-c" | "--command" => {
                i += 1;
                match args.get(i) {
                    Some(s) => inline = Some(s.clone()),
                    None => {
                        eprintln!("error: {} requires a program string", args[i - 1]);
                        return ExitCode::from(2);
                    }
                }
            }
            other => {
                if path.is_none() {
                    path = Some(other.to_string());
                } else {
                    eprintln!("error: unexpected argument '{other}'");
                    return ExitCode::from(2);
                }
            }
        }
        i += 1;
    }

    // Resolve the program text and a human-facing label from exactly one of:
    // an inline `-c` string, stdin (`-`/`stdin`), or a file path. `prog_stdin`
    // marks the case where the *program* came from stdin (so it can't also be
    // the data source for the filter shorthand below).
    let (label, mut source, prog_stdin) = match (inline, path) {
        (Some(_), Some(p)) => {
            eprintln!("error: give a program with -c OR a path '{p}', not both");
            return ExitCode::from(2);
        }
        (Some(text), None) => ("<command>".to_string(), text, false),
        (None, Some(p)) if p == "-" || p == "stdin" => {
            let mut text = String::new();
            if let Err(e) = std::io::stdin().read_to_string(&mut text) {
                eprintln!("error: cannot read program from stdin: {e}");
                return ExitCode::FAILURE;
            }
            ("<stdin>".to_string(), text, true)
        }
        (None, Some(p)) => match std::fs::read_to_string(&p) {
            Ok(s) => (p, s, false),
            Err(e) => {
                eprintln!("error: cannot read '{p}': {e}");
                return ExitCode::FAILURE;
            }
        },
        (None, None) => {
            eprintln!("error: no program given (pass <file.riv>, -c <string>, or - for stdin)");
            usage();
            return ExitCode::from(2);
        }
    };

    // §31: a `.riv.md` path is a Rivus Literate document (frontmatter + prose +
    // ```flow fences). Parse it into a document so the executable flow can be
    // extracted, the (R) config cascade applied, and `fmt` can reformat only the
    // flow bodies while preserving prose. A `.riv` / -c / stdin program is plain
    // flow (the REPL form) exactly as before.
    let literate_doc = if is_literate_path(&label) {
        match rivus_parser::literate::parse_literate(&source) {
            Ok(doc) => Some(doc),
            Err(e) => {
                eprintln!("parse error in {label}: {e}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        None
    };

    // Config cascade `frontmatter ← CLI` (§31.3): a `--chunk-size` on the command
    // line wins; otherwise a `.riv.md` frontmatter `chunk_size:` (an (R) hint,
    // result-invariant) supplies the default; otherwise the engine default.
    let fm_chunk_size = literate_doc
        .as_ref()
        .and_then(|d| frontmatter_usize(d, "chunk_size"));
    let chunk_size = chunk_size_cli
        .or(fm_chunk_size)
        .unwrap_or_else(|| RunOptions::default().chunk_size);

    // Unix-filter shorthand (plain flow only): a transform-only program (one
    // that starts with a pipe `|…` or a transform verb, i.e. has no source/scope)
    // is wrapped to read CSV from stdin and write CSV to stdout. So this just
    // works:   cat data.csv | rivus run -c '|? age >= 20 |> name age'
    if literate_doc.is_none() && !prog_stdin && is_transform_only(&source) {
        let has_sink = source.contains("save ")
            || source.contains("writecsv")
            || source.contains("writejson")
            || source.contains("print");
        let sink = if has_sink { "" } else { " save stdout as csv" };
        source = format!("Pipe: open stdin {}{} ;", source.trim(), sink);
    }

    // Lower to the IR. A `.riv.md` lowers via its concatenated flow bodies (one
    // document → one `PlanGraph`, §31.2); a `.riv` parses directly.
    let parse_result = match &literate_doc {
        Some(doc) if !doc.has_flow() => Err(rivus_core::RivusError::Parse(
            "no ```flow fence found: a .riv.md document needs at least one executable \
             ```flow block (untagged or other-language fences are inert display)"
                .to_string(),
        )),
        Some(doc) => rivus_parser::parse(&doc.flow_source()),
        None => rivus_parser::parse(&source),
    };
    let parsed = match parse_result {
        Ok(g) => g,
        Err(e) => {
            eprintln!("parse error in {label}: {e}");
            return ExitCode::FAILURE;
        }
    };

    match cmd {
        "fmt" => {
            // Format = parse → IR → canonical source (§25.8: fmt is IR-based).
            // Comment trivia is preserved through the IR (§25.7), so the
            // author's notes survive. For a `.riv.md` document, fmt reformats
            // only the ```flow cell bodies and round-trips prose / frontmatter
            // verbatim (§31.5).
            let formatted = match &literate_doc {
                Some(doc) => match fmt_literate(doc) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("error: {e}");
                        return ExitCode::FAILURE;
                    }
                },
                None => {
                    let formatted = parsed.to_source();
                    // Honesty gate: the canonical renderer is faithful for linear
                    // flows, merge/join scopes and `->` branch fan-out, but a few
                    // constructs (e.g. an anonymous, unlabeled scope) are not yet
                    // reproduced losslessly. Re-parse the result and refuse to
                    // emit anything we can't round-trip, rather than silently
                    // rewrite it into something different.
                    if !fmt_faithful(&parsed, &formatted) {
                        eprintln!(
                            "error: `rivus fmt` cannot yet faithfully round-trip this \
                             program (it uses a construct the canonical renderer does \
                             not yet reproduce losslessly); left the source unchanged"
                        );
                        return ExitCode::FAILURE;
                    }
                    formatted
                }
            };
            if fmt_write {
                if label == "<command>" || label == "<stdin>" {
                    eprintln!("error: `rivus fmt --write` needs a file path (not -c / stdin)");
                    return ExitCode::from(2);
                }
                if let Err(e) = std::fs::write(&label, &formatted) {
                    eprintln!("error: cannot write '{label}': {e}");
                    return ExitCode::FAILURE;
                }
                eprintln!("formatted {label}");
            } else {
                print!("{formatted}");
            }
            ExitCode::SUCCESS
        }
        "check" => {
            println!(
                "ok: {} node(s), {} edge(s)",
                parsed.nodes.len(),
                parsed.edges.len()
            );
            ExitCode::SUCCESS
        }
        "explain" => {
            // The graph to visualize: optimized when the optimizer is on
            // (explain's value is the *post-optimization* DAG), else as parsed.
            let viz_graph = if optimize {
                rivus_optimizer::optimize(parsed.clone()).0
            } else {
                parsed.clone()
            };
            if fmt_write {
                // §31.4: `explain --write` embeds a generated, output-only
                // Mermaid DAG into the `.riv.md` document's sentinel-fenced
                // region — idempotent, preserving prose / frontmatter / flow.
                if literate_doc.is_none() {
                    eprintln!(
                        "error: `rivus explain --write` needs a .riv.md document (the Mermaid \
                         DAG is embedded into the document)"
                    );
                    return ExitCode::from(2);
                }
                if label == "<command>" || label == "<stdin>" {
                    eprintln!("error: `rivus explain --write` needs a file path (not -c / stdin)");
                    return ExitCode::from(2);
                }
                let updated = upsert_generated_region(&source, &generated_region(&viz_graph));
                if let Err(e) = std::fs::write(&label, &updated) {
                    eprintln!("error: cannot write '{label}': {e}");
                    return ExitCode::FAILURE;
                }
                eprintln!("wrote generated DAG region to {label}");
                return ExitCode::SUCCESS;
            }
            print!("{}", viz::render_explain(&parsed));
            if optimize {
                let (opt, report) = rivus_optimizer::optimize(parsed.clone());
                print!("{}", viz::render_optimization(&report, &opt));
            }
            // Also emit the embeddable Mermaid DAG, so `explain` is a generator
            // even without --write (copy into a `.riv.md`, or pipe it on).
            print!("\n{}", generated_region(&viz_graph));
            ExitCode::SUCCESS
        }
        "run" => {
            // Human-facing visualization goes to STDERR so that a `save stdout`
            // sink leaves STDOUT as clean data for shell pipes (`… | rivus run
            // flow.riv | …`). Interactive terminals still show stderr. With
            // `--json`, stderr is machine-readable JSONL instead — so the banner,
            // opt-report and live progress are suppressed to keep it clean.
            if !telemetry_json {
                eprintln!("\u{2550}\u{2550} Rivus \u{2550}\u{2550}  flow: {label}\n");
            }
            let (graph, report) = if optimize {
                rivus_optimizer::optimize(parsed)
            } else {
                (parsed, rivus_optimizer::OptReport::default())
            };
            if !telemetry_json && !report.is_empty() {
                eprint!("{}", viz::render_opt_report(&report));
                eprintln!();
            }
            // Live dashboard: run the flow on a worker thread that publishes
            // snapshots to a Hub, and serve the embedded HTML/SSE UI here.
            if let Some(addr) = &serve_addr {
                return run_served(&graph, addr, chunk_size, memory, open_browser);
            }
            // `--tui`: repaint a live ANSI frame on stderr each tick.
            if tui {
                return run_tui(&graph, chunk_size, memory);
            }
            // Live progress only when stderr is a terminal (keep logs/pipes
            // clean) and not in JSONL mode.
            let progress = !telemetry_json && std::io::stderr().is_terminal();
            // Sink-less flows are previews: cap captured rows so `rivus run
            // 'open big.csv'` shows the head instantly in bounded memory. A
            // `save` sink overrides this and writes every row.
            match run(
                &graph,
                RunOptions {
                    chunk_size,
                    progress,
                    max_capture: Some(1000),
                    memory,
                },
            ) {
                Ok(res) => {
                    if telemetry_json {
                        let jsonl = viz::render_telemetry_jsonl(&graph, &res);
                        if let Some(addr) = &telemetry_addr {
                            // Stream to the socket; fall back to stderr on a
                            // connection error so telemetry is never silently lost.
                            if let Err(e) = send_telemetry(addr, &jsonl) {
                                eprintln!(
                                    "warning: telemetry to '{addr}' failed ({e}); writing to stderr"
                                );
                                eprint!("{jsonl}");
                            }
                        } else {
                            eprint!("{jsonl}");
                        }
                    } else {
                        eprint!("{}", viz::render_run(&graph, &res));
                    }
                    // A fatal error on the stream means the graph halted.
                    if res.final_mode == rivus_core::Mode::Halted {
                        return ExitCode::FAILURE;
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("runtime error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        other => {
            eprintln!("error: unknown command '{other}'");
            usage();
            ExitCode::from(2)
        }
    }
}

/// Send the rendered JSONL telemetry to a TCP `HOST:PORT` (std-only,
/// `std::net`). Connects, writes the whole buffer, and closes — a one-shot feed
/// for a live viewer. Errors propagate so the caller can fall back to stderr.
fn send_telemetry(addr: &str, jsonl: &str) -> std::io::Result<()> {
    use std::io::Write;
    use std::net::TcpStream;
    let mut stream = TcpStream::connect(addr)?;
    stream.write_all(jsonl.as_bytes())?;
    stream.flush()
}

/// `rivus run … --serve [ADDR]`: run the flow on a worker thread that publishes
/// live snapshots to a [`serve::Hub`], while this thread serves the embedded
/// HTML/SSE dashboard. Falls back to a plain run if the address can't be bound.
/// Best-effort launch of the system browser at `url` (`--open`). Detached and
/// non-fatal: a missing opener (e.g. headless server) just prints the URL as
/// usual. Zero-dependency — shells out to the platform's standard opener.
fn open_in_browser(url: &str) {
    let (cmd, args): (&str, Vec<&str>) = if cfg!(target_os = "macos") {
        ("open", vec![url])
    } else if cfg!(target_os = "windows") {
        ("cmd", vec!["/C", "start", "", url])
    } else {
        ("xdg-open", vec![url])
    };
    let _ = std::process::Command::new(cmd)
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}

fn run_served(
    graph: &rivus_ir::PlanGraph,
    addr: &str,
    chunk_size: usize,
    memory: MemoryPref,
    open: bool,
) -> ExitCode {
    let (listener, local) = match serve::bind(addr) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("warning: --serve cannot bind '{addr}' ({e}); running without the dashboard");
            return match run(
                graph,
                RunOptions {
                    chunk_size,
                    progress: false,
                    max_capture: Some(1000),
                    memory,
                },
            ) {
                Ok(res) if res.final_mode == rivus_core::Mode::Halted => ExitCode::FAILURE,
                Ok(_) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("runtime error: {e}");
                    ExitCode::FAILURE
                }
            };
        }
    };
    eprintln!("\u{2550}\u{2550} Rivus live \u{2550}\u{2550}  dashboard: http://{local}/  (Ctrl-C to stop)");
    if open {
        // The listener is already bound, so a connection now is queued by the OS
        // even though the accept loop starts below.
        open_in_browser(&format!("http://{local}/"));
    }

    let hub = serve::Hub::new(viz::render_graph_json(graph));
    let worker_hub = std::sync::Arc::clone(&hub);
    // Clone the graph for the worker thread (PlanGraph: Clone).
    let g = graph.clone();
    let worker = std::thread::spawn(move || {
        let mut hook = |s: &RuntimeSnapshot| worker_hub.publish(viz::render_snapshot_json(s));
        let res = run_with_progress(
            &g,
            RunOptions {
                chunk_size,
                progress: false,
                max_capture: Some(1000),
                memory,
            },
            Some(&mut hook),
        );
        worker_hub.finish();
        res.map(|r| r.final_mode)
            .unwrap_or(rivus_core::Mode::Halted)
    });

    // Serve the dashboard on this thread until the run finishes.
    serve::serve(listener, hub);
    match worker.join() {
        Ok(rivus_core::Mode::Halted) => ExitCode::FAILURE,
        Ok(_) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}

/// `rivus run … --tui`: repaint a live ANSI dashboard frame on stderr each tick
/// (Pillar B, B1). Uses the same progress hook as `--serve`; the run stays on
/// the serial path so frames are coherent.
fn run_tui(graph: &rivus_ir::PlanGraph, chunk_size: usize, memory: MemoryPref) -> ExitCode {
    use std::io::Write as _;
    let to_tty = std::io::stderr().is_terminal();
    let mut hook = |s: &RuntimeSnapshot| {
        let frame = viz::render_snapshot_frame(s);
        let mut err = std::io::stderr().lock();
        if to_tty {
            // Clear screen + home cursor, then paint the frame.
            let _ = write!(err, "\x1b[2J\x1b[H{frame}");
        } else {
            let _ = writeln!(err, "{frame}");
        }
        let _ = err.flush();
    };
    match run_with_progress(
        graph,
        RunOptions {
            chunk_size,
            progress: false,
            max_capture: Some(1000),
            memory,
        },
        Some(&mut hook),
    ) {
        Ok(res) if res.final_mode == rivus_core::Mode::Halted => ExitCode::FAILURE,
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("runtime error: {e}");
            ExitCode::FAILURE
        }
    }
}

/// `rivus gen <shape> [--rows N] [--seed S] [--ratio R]` — write deterministic,
/// seeded benchmark/demo data to stdout. Self-hosted so dogfooding needs no
/// external awk/python. Shapes mirror `gendata`:
///   clean       well-formed `id,name,age,score,country,active` CSV
///   error-heavy ~`ratio` malformed rows (default 0.1) — continue-first stress
///   mixed       ~`ratio` type-mixed cells (default 0.1)
///   jsonl       one flat JSON object per line
fn run_gen(args: &[String]) -> ExitCode {
    let mut shape: Option<&str> = None;
    let mut rows: usize = 1000;
    let mut seed: u64 = 42;
    let mut ratio: f64 = 0.1;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--rows" | "-n" => {
                i += 1;
                match args.get(i).and_then(|v| v.parse().ok()) {
                    Some(n) => rows = n,
                    None => return gen_arg_err("--rows requires a non-negative integer"),
                }
            }
            "--seed" => {
                i += 1;
                match args.get(i).and_then(|v| v.parse().ok()) {
                    Some(s) => seed = s,
                    None => return gen_arg_err("--seed requires an integer"),
                }
            }
            "--ratio" => {
                i += 1;
                match args.get(i).and_then(|v| v.parse::<f64>().ok()) {
                    Some(r) if (0.0..=1.0).contains(&r) => ratio = r,
                    _ => return gen_arg_err("--ratio requires a number in 0.0..=1.0"),
                }
            }
            other if shape.is_none() && !other.starts_with('-') => shape = Some(other),
            other => return gen_arg_err(&format!("unexpected argument '{other}'")),
        }
        i += 1;
    }
    let bytes: Vec<u8> = match shape {
        Some("clean") | None => gendata::clean(rows, seed).into_bytes(),
        Some("error-heavy") => gendata::error_heavy(rows, ratio, seed).into_bytes(),
        Some("mixed") => gendata::mixed_types(rows, ratio, seed).into_bytes(),
        Some("jsonl") => gendata::jsonl_clean(rows, seed).into_bytes(),
        Some(other) => {
            return gen_arg_err(&format!(
                "unknown shape '{other}' (clean|error-heavy|mixed|jsonl)"
            ))
        }
    };
    match std::io::stdout().write_all(&bytes) {
        Ok(()) => ExitCode::SUCCESS,
        // A closed downstream pipe (`rivus gen … | head`) is not an error.
        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("gen: cannot write to stdout: {e}");
            ExitCode::FAILURE
        }
    }
}

fn gen_arg_err(msg: &str) -> ExitCode {
    eprintln!("error: {msg}");
    eprintln!("usage: rivus gen <clean|error-heavy|mixed|jsonl> [--rows N] [--seed S] [--ratio R]");
    ExitCode::from(2)
}

/// A transform-only program (no source/scope): starts with a pipe operator or a
/// transform verb. Such a program is wrapped as a stdin→stdout CSV filter.
fn is_transform_only(src: &str) -> bool {
    let s = src.trim_start();
    if s.starts_with('|') {
        return true;
    }
    let first = s.split_whitespace().next().unwrap_or("");
    matches!(
        first,
        "where" | "take" | "limit" | "head" | "sort" | "distinct" | "describe" | "dropna" | "fill"
    )
}

/// Sentinel that opens the `explain`-generated region in a `.riv.md` (§31.4).
/// Matched by prefix so the trailing note can change without breaking upsert.
const GEN_BEGIN_PREFIX: &str = "<!-- rivus:begin";
/// Sentinel that closes the generated region.
const GEN_END: &str = "<!-- rivus:end -->";

/// Build the `explain`-generated region (§31.4): a sentinel-fenced, output-only
/// Mermaid DAG. The block is regenerated from the IR each time and never parsed
/// back (it lives in inert prose / a ```mermaid fence), so editing inside it is
/// overwritten. Ends with exactly one newline for idempotent upsert.
fn generated_region(graph: &rivus_ir::PlanGraph) -> String {
    let mut s = String::new();
    s.push_str(GEN_BEGIN_PREFIX);
    s.push_str(" generated by `rivus explain --write`; edits inside are overwritten -->\n");
    s.push_str("```mermaid\n");
    s.push_str(&viz::render_mermaid(graph));
    s.push_str("```\n");
    s.push_str(GEN_END);
    s.push('\n');
    s
}

/// Insert or replace the generated region in a `.riv.md` document, idempotently
/// (§31.4). When a `<!-- rivus:begin … -->…<!-- rivus:end -->` span exists, its
/// content is replaced in place (hand-written prose around it is preserved);
/// otherwise the region is appended after a blank line. Running it twice on the
/// same IR is a fixed point.
fn upsert_generated_region(src: &str, region: &str) -> String {
    if let (Some(b), Some(e)) = (src.find(GEN_BEGIN_PREFIX), src.rfind(GEN_END)) {
        let e_end = e + GEN_END.len();
        if e_end >= b {
            let mut out = String::with_capacity(src.len() + region.len());
            out.push_str(&src[..b]);
            out.push_str(region.trim_end_matches('\n'));
            out.push_str(&src[e_end..]);
            return out;
        }
    }
    let mut out = src.trim_end_matches('\n').to_string();
    out.push_str("\n\n");
    out.push_str(region);
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out
}

/// Is this input path a Rivus Literate document (§31)? `.riv.md` is the canonical
/// extension; a plain `.md` is also treated as Literate so documents authored in
/// a Markdown editor work. Anything else (`.riv`, `-c`, stdin) is plain flow.
fn is_literate_path(label: &str) -> bool {
    label.ends_with(".riv.md") || label.ends_with(".md")
}

/// Read a `usize` frontmatter value for the config cascade (§31.3), e.g.
/// `chunk_size`. Returns `None` when absent or not a positive integer.
fn frontmatter_usize(doc: &rivus_parser::literate::LiterateDoc, key: &str) -> Option<usize> {
    doc.frontmatter_pairs()
        .into_iter()
        .find(|(k, _)| k == key)
        .and_then(|(_, v)| v.parse::<usize>().ok())
        .filter(|&n| n >= 1)
}

/// Does the canonical source round-trip back to a structurally identical IR?
/// The honesty gate for `fmt`: refuse to rewrite anything the renderer cannot
/// reproduce losslessly (§25.8) rather than silently change the program.
fn fmt_faithful(parsed: &rivus_ir::PlanGraph, formatted: &str) -> bool {
    match rivus_parser::parse(formatted) {
        Ok(re) => {
            re.nodes.len() == parsed.nodes.len()
                && re
                    .nodes
                    .iter()
                    .zip(parsed.nodes.iter())
                    .all(|(a, b)| a.op.kind_str() == b.op.kind_str())
        }
        Err(_) => false,
    }
}

/// Format a `.riv.md` document (§31.5): reformat each ```flow cell body via the
/// IR (parse → `to_source`) with the same honesty gate as plain `fmt`, and
/// re-render the document so prose, frontmatter and `#|` options round-trip
/// verbatim. Cells are reformatted independently; a cell that does not parse on
/// its own (e.g. it relies on a name defined in another cell) or does not
/// round-trip faithfully leaves fmt refusing — never a silent rewrite.
fn fmt_literate(doc: &rivus_parser::literate::LiterateDoc) -> Result<String, String> {
    use rivus_parser::literate::Segment;
    let mut out = doc.clone();
    for seg in &mut out.segments {
        if let Segment::Flow(cell) = seg {
            let parsed = rivus_parser::parse(&cell.body)
                .map_err(|e| format!("`rivus fmt` cannot parse a ```flow cell on its own: {e}"))?;
            let formatted = parsed.to_source();
            if !fmt_faithful(&parsed, &formatted) {
                return Err(
                    "`rivus fmt` cannot yet faithfully round-trip a ```flow cell (it uses a \
                     construct the canonical renderer does not reproduce losslessly); left the \
                     document unchanged"
                        .to_string(),
                );
            }
            cell.body = formatted.trim_end_matches('\n').to_string();
        }
    }
    Ok(out.render())
}

fn usage() {
    eprintln!(
        "rivus — flow-oriented, DAG-native stream runtime\n\n\
         USAGE:\n\
         \x20 rivus run     <program> [--chunk-size N] [--no-opt] [--memory low|auto|fast|unbounded] [--json|--telemetry-addr HOST:PORT|--tui|--serve [ADDR]]  run a flow\n\
         \x20 rivus explain <program> [--no-opt]                     show DAG IR + optimizer report\n\
         \x20 rivus check   <program>                                parse only\n\
         \x20 rivus fmt     <program> [--write|-w]                   reformat to canonical source (preserves #{{ }}# comments)\n\
         \x20 rivus gen      <shape> [--rows N --seed S --ratio R]    write seeded data to stdout\n\n\
         PROGRAM (any of):\n\
         \x20 <file.riv>                 read the program from a file\n\
         \x20 -c, --command <STRING>     pass the program inline as a string\n\
         \x20 - | stdin                  read the program from stdin (heredoc)\n\n\
         EXAMPLES:\n\
         \x20 rivus run flow.riv\n\
         \x20 rivus run -c 'U: open users.csv |? age >= 20 |> name age ;'\n\
         \x20 rivus run - <<'RIV'\n\
         \x20     U: open users.csv |? age >= 20 ;\n\
         \x20 RIV\n\n\
         UNIX FILTER (transform-only program → reads CSV from stdin, writes stdout):\n\
         \x20 cat data.csv | rivus '|? age >= 20 |> name age'\n\
         \x20 cat data.csv | rivus 'describe'\n\n\
         DATA GENERATION (deterministic, seeded — for benches/demos, no awk needed):\n\
         \x20 rivus gen clean --rows 1000000 > data.csv\n\
         \x20 rivus gen clean --rows 1000000 | rivus '|? age >= 50 |> name age'\n"
    );
}
