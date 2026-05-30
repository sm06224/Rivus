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

mod viz;

use rivus_runtime::{run, RunOptions};
use std::io::{IsTerminal, Read};
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        usage();
        return ExitCode::from(2);
    }

    let cmd = args[1].as_str();
    if matches!(cmd, "-h" | "--help" | "help") {
        usage();
        return ExitCode::SUCCESS;
    }

    let mut path: Option<String> = None;
    let mut inline: Option<String> = None;
    let mut chunk_size = RunOptions::default().chunk_size;
    let mut optimize = true;
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--no-opt" => optimize = false,
            "--chunk-size" => {
                i += 1;
                match args.get(i).and_then(|v| v.parse::<usize>().ok()) {
                    Some(n) if n >= 1 => chunk_size = n,
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
    // an inline `-c` string, stdin (`-`/`stdin`), or a file path.
    let (label, source) = match (inline, path) {
        (Some(_), Some(p)) => {
            eprintln!("error: give a program with -c OR a path '{p}', not both");
            return ExitCode::from(2);
        }
        (Some(text), None) => ("<command>".to_string(), text),
        (None, Some(p)) if p == "-" || p == "stdin" => {
            let mut text = String::new();
            if let Err(e) = std::io::stdin().read_to_string(&mut text) {
                eprintln!("error: cannot read program from stdin: {e}");
                return ExitCode::FAILURE;
            }
            ("<stdin>".to_string(), text)
        }
        (None, Some(p)) => match std::fs::read_to_string(&p) {
            Ok(s) => (p, s),
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

    let parsed = match rivus_parser::parse(&source) {
        Ok(g) => g,
        Err(e) => {
            eprintln!("parse error in {label}: {e}");
            return ExitCode::FAILURE;
        }
    };

    match cmd {
        "check" => {
            println!(
                "ok: {} node(s), {} edge(s)",
                parsed.nodes.len(),
                parsed.edges.len()
            );
            ExitCode::SUCCESS
        }
        "explain" => {
            print!("{}", viz::render_explain(&parsed));
            if optimize {
                let (opt, report) = rivus_optimizer::optimize(parsed.clone());
                print!("{}", viz::render_optimization(&report, &opt));
            }
            ExitCode::SUCCESS
        }
        "run" => {
            // Human-facing visualization goes to STDERR so that a `save stdout`
            // sink leaves STDOUT as clean data for shell pipes (`… | rivus run
            // flow.riv | …`). Interactive terminals still show stderr.
            eprintln!("\u{2550}\u{2550} Rivus \u{2550}\u{2550}  flow: {label}\n");
            let (graph, report) = if optimize {
                rivus_optimizer::optimize(parsed)
            } else {
                (parsed, rivus_optimizer::OptReport::default())
            };
            if !report.is_empty() {
                eprint!("{}", viz::render_opt_report(&report));
                eprintln!();
            }
            // Live progress only when stderr is a terminal (keep logs/pipes clean).
            let progress = std::io::stderr().is_terminal();
            match run(
                &graph,
                RunOptions {
                    chunk_size,
                    progress,
                },
            ) {
                Ok(res) => {
                    eprint!("{}", viz::render_run(&graph, &res));
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

fn usage() {
    eprintln!(
        "rivus — flow-oriented, DAG-native stream runtime\n\n\
         USAGE:\n\
         \x20 rivus run     <program> [--chunk-size N] [--no-opt]    run and visualize a flow\n\
         \x20 rivus explain <program> [--no-opt]                     show DAG IR + optimizer report\n\
         \x20 rivus check   <program>                                parse only\n\n\
         PROGRAM (any of):\n\
         \x20 <file.riv>                 read the program from a file\n\
         \x20 -c, --command <STRING>     pass the program inline as a string\n\
         \x20 - | stdin                  read the program from stdin (heredoc)\n\n\
         EXAMPLES:\n\
         \x20 rivus run flow.riv\n\
         \x20 rivus run -c 'U: open users.csv |? age >= 20 |> name age ;'\n\
         \x20 rivus run - <<'RIV'\n\
         \x20     U: open users.csv |? age >= 20 ;\n\
         \x20 RIV\n"
    );
}
