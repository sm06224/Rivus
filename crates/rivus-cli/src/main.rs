//! `rivus` — the command-line shell / runner for the Rivus stream runtime.
//!
//! Subcommands:
//!   rivus run <file.riv>      parse, execute, and visualize a flow
//!   rivus explain <file.riv>  show the DAG IR + regenerated source
//!   rivus check <file.riv>    parse only (report errors)
//!
//! Flags:
//!   --chunk-size <N>          rows per chunk emitted by sources (default 4096)
//!   --no-opt                  disable the IR optimizer (run/explain)

mod viz;

use rivus_runtime::{run, RunOptions};
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

    let Some(path) = path else {
        eprintln!("error: missing <file.riv>");
        usage();
        return ExitCode::from(2);
    };

    let source = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: cannot read '{path}': {e}");
            return ExitCode::FAILURE;
        }
    };

    let parsed = match rivus_parser::parse(&source) {
        Ok(g) => g,
        Err(e) => {
            eprintln!("parse error in '{path}': {e}");
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
            eprintln!("\u{2550}\u{2550} Rivus \u{2550}\u{2550}  flow: {path}\n");
            let (graph, report) = if optimize {
                rivus_optimizer::optimize(parsed)
            } else {
                (parsed, rivus_optimizer::OptReport::default())
            };
            if !report.is_empty() {
                eprint!("{}", viz::render_opt_report(&report));
                eprintln!();
            }
            match run(&graph, RunOptions { chunk_size }) {
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
         \x20 rivus run     <file.riv> [--chunk-size N] [--no-opt]   run and visualize a flow\n\
         \x20 rivus explain <file.riv> [--no-opt]                    show DAG IR + optimizer report\n\
         \x20 rivus check   <file.riv>                               parse only\n"
    );
}
