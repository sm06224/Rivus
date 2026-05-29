//! End-to-end pipeline benchmarks.
//!
//! Each benchmark measures the *full* path a user feels: read CSV from disk →
//! parse Unified Flow source → build DAG IR → execute chunked → collect result.
//! Scenarios deliberately stress the three regimes called out for Rivus:
//!
//!   - **large**     : hundreds of thousands of clean rows
//!   - **error-heavy**: a large fraction of malformed rows (continue-first cost)
//!   - **mixed**      : mixed-type columns forcing string-lane fallback
//!
//! Throughput is reported in rows/s via `Throughput::Elements`.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rivus_runtime::gendata;
use rivus_runtime::{run, RunOptions};
use std::hint::black_box;
use std::path::PathBuf;

const ROWS: usize = 200_000;
const SEED: u64 = 0x1234_5678_9ABC_DEF0; // fixed seed for reproducibility

/// Parse + run a Rivus program, returning the result so the optimizer can't
/// elide the work.
fn run_source(src: &str) -> rivus_runtime::RunResult {
    let graph = rivus_parser::parse(src).expect("parse");
    run(&graph, RunOptions { chunk_size: 8192 }).expect("run")
}

/// Parse + optimize + run.
fn run_source_opt(src: &str) -> rivus_runtime::RunResult {
    let graph = rivus_parser::parse(src).expect("parse");
    let (graph, _) = rivus_optimizer::optimize(graph);
    run(&graph, RunOptions { chunk_size: 8192 }).expect("run")
}

struct Fixture {
    path: PathBuf,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn bench_large(c: &mut Criterion) {
    let data = gendata::clean(ROWS, SEED);
    let fx = Fixture {
        path: gendata::write_temp("clean", &data),
    };
    let p = fx.path.display();

    let mut g = c.benchmark_group("large");
    g.sample_size(20);
    g.throughput(Throughput::Elements(ROWS as u64));

    g.bench_function("filter_only", |b| {
        let src = format!("F:\n open {p}\n |? age >= 45\n;");
        b.iter(|| black_box(run_source(&src)));
    });

    // String-keyed filter exercises the borrowed-&str predicate fast path
    // (no String allocation per row). No projection, so parse cost is constant.
    g.bench_function("string_filter", |b| {
        let src = format!("F:\n open {p}\n |? country == \"JP\"\n;");
        b.iter(|| black_box(run_source(&src)));
    });

    g.bench_function("filter_project_group", |b| {
        let src = format!(
            "F:\n open {p}\n |? age >= 30\n |> name age country\n;\n\
             Pop:\n open {p}\n |# country\n;"
        );
        b.iter(|| black_box(run_source(&src)));
    });

    g.finish();
}

fn bench_error_heavy(c: &mut Criterion) {
    let mut g = c.benchmark_group("error_heavy");
    g.sample_size(20);
    g.throughput(Throughput::Elements(ROWS as u64));

    for ratio in [0.0_f64, 0.25, 0.5] {
        let data = gendata::error_heavy(ROWS, ratio, SEED);
        let fx = Fixture {
            path: gendata::write_temp("err", &data),
        };
        let p = fx.path.display();
        let src = format!("F:\n open {p}\n |? age >= 30\n |> name age\n;");
        g.bench_with_input(
            BenchmarkId::from_parameter(format!("bad={:.0}%", ratio * 100.0)),
            &src,
            |b, src| b.iter(|| black_box(run_source(src))),
        );
        // keep fx alive across the closure
        drop(fx);
    }
    g.finish();
}

fn bench_mixed(c: &mut Criterion) {
    let mut g = c.benchmark_group("mixed_types");
    g.sample_size(20);
    g.throughput(Throughput::Elements(ROWS as u64));

    for ratio in [0.0_f64, 0.1, 0.5] {
        let data = gendata::mixed_types(ROWS, ratio, SEED);
        let fx = Fixture {
            path: gendata::write_temp("mix", &data),
        };
        let p = fx.path.display();
        // value>=50 compares numerically when pure-int, lexically when mixed.
        let src = format!("F:\n open {p}\n |? value >= 50\n;");
        g.bench_with_input(
            BenchmarkId::from_parameter(format!("mix={:.0}%", ratio * 100.0)),
            &src,
            |b, src| b.iter(|| black_box(run_source(src))),
        );
        drop(fx);
    }
    g.finish();
}

fn bench_fanout(c: &mut Criterion) {
    let data = gendata::clean(ROWS, SEED);
    let fx = Fixture {
        path: gendata::write_temp("clean_fanout", &data),
    };
    let p = fx.path.display();

    let mut g = c.benchmark_group("fanout");
    g.sample_size(20);
    g.throughput(Throughput::Elements(ROWS as u64));

    // One source tees into three filtered branches, then merges (fan-out clone).
    g.bench_function("branch3_merge", |b| {
        let src = format!(
            "Src:\n open {p}\n \
              -> A: |? age >= 60 ;\n \
              -> B: |? age >= 30 ;\n \
              -> C: |? age <  30 ;\n;\n\
             M:\n A + B + C\n;"
        );
        b.iter(|| black_box(run_source(&src)));
    });

    g.finish();
}

fn bench_optimizer(c: &mut Criterion) {
    let data = gendata::clean(ROWS, SEED);
    let fx = Fixture {
        path: gendata::write_temp("clean_opt", &data),
    };
    let p = fx.path.display();

    // Two scopes read the same file; dedup_sources should merge the read.
    let src = format!(
        "A:\n open {p}\n |? age >= 30\n |> name age\n;\n\
         B:\n open {p}\n |# country\n;"
    );

    let mut g = c.benchmark_group("optimizer");
    g.sample_size(20);
    g.throughput(Throughput::Elements(ROWS as u64));
    g.bench_function("two_reads_raw", |b| b.iter(|| black_box(run_source(&src))));
    g.bench_function("two_reads_dedup", |b| {
        b.iter(|| black_box(run_source_opt(&src)))
    });

    // Fusion: filter -> project. Fused gathers only the projected columns once.
    let fp = format!("F:\n open {p}\n |? age >= 30\n |> name age\n;");
    g.bench_function("filter_project_raw", |b| {
        b.iter(|| black_box(run_source(&fp)))
    });
    g.bench_function("filter_project_fused", |b| {
        b.iter(|| black_box(run_source_opt(&fp)))
    });

    // Execution-heavy: a chain of 4 filters + projection. Unfused, each filter
    // gathers ALL six columns (incl. two string columns) for survivors; fused
    // does one scan and gathers only the single projected column once.
    let chain = format!(
        "F:\n open {p}\n |? age >= 10\n |? age >= 20\n |? age < 80\n |? score >= 0\n |> name\n;"
    );
    g.bench_function("filter_chain_raw", |b| {
        b.iter(|| black_box(run_source(&chain)))
    });
    g.bench_function("filter_chain_fused", |b| {
        b.iter(|| black_box(run_source_opt(&chain)))
    });

    // Projection pushdown: project to a single numeric column. Optimized, the
    // reader builds ONLY `age` — the two string columns (name, country) and the
    // rest are never parsed or allocated.
    let proj = format!("F:\n open {p}\n |? age >= 30\n |> age\n;");
    g.bench_function("project_pushdown_raw", |b| {
        b.iter(|| black_box(run_source(&proj)))
    });
    g.bench_function("project_pushdown_opt", |b| {
        b.iter(|| black_box(run_source_opt(&proj)))
    });
    g.finish();
}

criterion_group!(
    benches,
    bench_large,
    bench_error_heavy,
    bench_mixed,
    bench_fanout,
    bench_optimizer
);
criterion_main!(benches);
