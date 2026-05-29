# Rivus Benchmarks

End-to-end benchmarks for the chunk execution engine. Each measures the full
path a user feels: **read CSV from disk → parse Unified Flow source → build DAG
IR → execute chunked → collect result**. Data is generated deterministically
(seeded SplitMix64, no `rand`) by `rivus_runtime::gendata`, covering the three
regimes Rivus must handle well:

- **large** — hundreds of thousands of clean rows
- **error-heavy** — a large fraction of malformed rows (continue-first cost)
- **mixed** — mixed-type columns forcing string-lane fallback

```sh
cargo bench -p rivus-runtime            # full statistical run
cargo bench -p rivus-runtime -- --test  # fast smoke run (CI gate)
```

All scenarios use `ROWS = 200_000`, `chunk_size = 8192`. Throughput is rows/s.

## Baseline — Phase 0 (interpreter, std-only CSV reader)

Machine: Intel Xeon @ 2.80␣GHz, 4␣vCPU. `cargo 1.94`, release profile
(`opt-level=3`, `lto=thin`). Median of 20 samples.

| scenario | columns | time (median) | throughput |
|---|---|---:|---:|
| `large/filter_only` | 6 | 349 ms | **0.57 M rows/s** |
| `large/filter_project_group` (2 sources) | 6 | 515 ms | 0.39 M rows/s |
| `error_heavy/bad=0%` | 6 | 301 ms | 0.67 M rows/s |
| `error_heavy/bad=25%` | 6 | 213 ms | 0.94 M rows/s |
| `error_heavy/bad=50%` | 6 | 143 ms | 1.40 M rows/s |
| `mixed_types/mix=0%` | 2 | 47 ms | **4.22 M rows/s** |
| `mixed_types/mix=10%` | 2 | 62 ms | 3.22 M rows/s |
| `mixed_types/mix=50%` | 2 | 66 ms | 3.05 M rows/s |
| `fanout/branch3_merge` | 6 | 353 ms | 0.57 M rows/s |

### Reading the baseline

- **The CSV reader dominates.** A 2-column workload runs at 4.2␣M rows/s but a
  6-column workload at only 0.57␣M rows/s — a ~7× gap that tracks column count,
  not predicate work. The reader (`csv.rs`) is the hotspot:
  1. every cell is materialized as an owned `String` (≈1.2␣M allocations for
     200k×6), stored in `Vec<Vec<String>>`;
  2. type inference scans **all** cells (`i64`, then `f64`, then `bool` parse
     attempts);
  3. `build_column` then re-parses every cell a third time.
- **Error-heavy gets *faster* as `bad%` rises** — malformed rows are cheaply
  skipped before any column is built, so fewer valid rows means less work. The
  continue-first path has no pathological cost; correctness is held by
  `tests/stress.rs::error_heavy_skips_and_continues`.
- **Mixed-type fallback costs ~1.4×** (47␣ms → 66␣ms) when 50% of cells force
  the `Str` lane instead of `i64`. Graceful, not catastrophic.

## Phase 0.1 — two-pass, allocation-light CSV reader

Replaced the reader's `Vec<Vec<String>>` materialization (≈1.2 M owned-`String`
allocations for 200k×6) with a two-pass parser: pass 1 splits into **borrowed
`&str` slices** and infers types while scanning; pass 2 parses directly into
**pre-sized typed column buffers**. Only genuine string columns allocate
per-cell; unquoted records split with zero allocation.

| scenario | Phase 0 | Phase 0.1 | speedup |
|---|---:|---:|---:|
| `large/filter_only` | 0.57 M | **1.54 M rows/s** | **2.7×** |
| `large/filter_project_group` | 0.39 M | 0.82 M rows/s | 2.1× |
| `error_heavy/bad=0%` | 0.67 M | 1.64 M rows/s | 2.5× |
| `error_heavy/bad=25%` | 0.94 M | 2.06 M rows/s | 2.2× |
| `error_heavy/bad=50%` | 1.40 M | 2.80 M rows/s | 2.0× |
| `mixed_types/mix=0%` | 4.22 M | 5.49 M rows/s | 1.3× |
| `mixed_types/mix=10%` | 3.22 M | 4.18 M rows/s | 1.3× |
| `mixed_types/mix=50%` | 3.05 M | 3.87 M rows/s | 1.3× |
| `fanout/branch3_merge` | 0.57 M | 1.20 M rows/s | 2.1× |

The column-count gap is largely closed: 6-column workloads jumped ~2–2.7×,
confirming per-cell allocation was the dominant cost. Correctness held across
all of `tests/stress.rs` and the new `csv` unit tests (chunk-size independence,
malformed-row skipping, quoted fields, mixed-type fallback).

## Phase 0.2 — optimizer: source de-duplication (CSE)

First IR optimizer pass (`rivus-optimizer`, zero external deps). `dedup_sources`
merges identical `open <path>` reads into one source that fans out to every
consumer — a semantics-preserving DAG rewrite, surfaced via `rivus explain`.

| scenario | raw | deduped | speedup |
|---|---:|---:|---:|
| `optimizer/two_reads` (same file in 2 scopes) | 0.86 M | **1.41 M rows/s** | **1.64×** |

The win scales with the number of duplicate reads (N reads → 1). Correctness is
gated by `tests/optimizer_equiv.rs`, which asserts the optimized graph produces
byte-identical outputs to the unoptimized one. The CLI runs the optimizer by
default (`--no-opt` to disable) and prints the applied rules.

## Phase 0.3 — operator fusion (filter chains + projection)

`fuse_linear` collapses a linear chain of `Filter` nodes and an optional trailing
`Project` into one `FilterProject` node: predicates are evaluated in a single row
scan and only the projected columns are gathered once.

| scenario | raw | fused | result |
|---|---:|---:|---|
| `optimizer/filter_project` (1 filter → project) | 116 ms | 117 ms | **neutral** |
| `optimizer/filter_chain` (4 filters → project) | 143 ms | **119 ms** | **1.20×** |

**Honest reading:** after the Phase-0.1 CSV fix, *parsing dominates* the
single-filter end-to-end, so fusing one filter+project is perf-neutral. The win
appears when **execution** is non-trivial: a 4-filter chain unfused does four
full-column gathers (copying both string columns each time); fused, it does one
scan and a single-column gather → ~1.20×. Fusion is also a prerequisite for
projection pushdown and the eventual SIMD kernels. Correctness is gated by
`tests/optimizer_equiv.rs` (optimized == unoptimized, byte-for-byte).

## Phase 0.4 — projection pushdown into the reader

`project_pushdown` annotates a CSV source with the set of columns its consumers
actually read (predicate columns ∪ projected columns), when every consumer is a
`FilterProject{fields: Some}` (so nothing downstream can reference a pruned
column). The reader then **never parses or allocates** the other columns.

| scenario | raw | pushed-down | speedup |
|---|---:|---:|---:|
| `optimizer/project_pushdown` (`open \| filter age \| project age`) | 113 ms | **53 ms** | **2.14×** |

This is the biggest single win after the CSV rewrite, because it attacks the
now-dominant cost directly: the projected query needs only `age` (i64), so the
two string columns (`name`, `country`) and the rest are never built — eliminating
their per-cell allocation and parsing entirely (0.57 M → 200k/53ms ≈ **3.8 M
rows/s** end-to-end). Safety + equivalence are gated by
`tests/optimizer_equiv.rs::fusion_and_pushdown_preserve_results`.

The optimizer pipeline now runs **dedup → fuse → pushdown**, each visible in
`rivus explain`.

## Phase 0.5 — parallel CSV parsing (std threads) + inference fast-path

The reader splits a large file into line-aligned slices and parses them across
`std::thread::scope` workers (no dependencies; sequential below 512 KB). Phase 1
infers each slice's column types in parallel and reduces them; phase 2 builds
each slice's columns in parallel and concatenates in order (row order preserved
→ byte-identical output). Inference also gained a fast path: while a column is
still all-integer, the redundant `f64` parse is skipped.

Measured (4 vCPU), vs the Phase-0.4 serial reader:

| scenario | serial | parallel | change |
|---|---:|---:|---|
| `mixed_types/mix=0%` (2 cols) | 36 ms | **19.9 ms** | **~1.8×** (10.0 M rows/s) |
| `optimizer/project_pushdown_raw` | 113 ms | 92 ms | −19% |
| `large/filter_only` (6 cols) | 130 ms | 111 ms | −15% (1.81 M rows/s) |
| `error_heavy/bad=0%` | 122 ms | 105 ms | −14% |

**Honest reading:** narrow data scales nearly linearly with cores; wide,
string-heavy data only gains ~15% because the per-cell `String` allocations for
the two text columns **contend on the global allocator** across threads. That is
a precise pointer to the next target — arena / `ArrayRef` string storage — not
more parser tuning. Correctness across the parallel path is held by
`tests/stress.rs` (exact counts, chunk-size independence, error-heavy, fan-out)
and `tests/optimizer_equiv.rs`, all run at row counts that trigger parallelism.

## Phase 0.6 — arena string columns (offsets + bytes)

`Column::Str` changed from `Vec<String>` (one heap allocation per cell) to an
Arrow-like `StrColumn` = one contiguous byte buffer + per-row `u32` offsets. A
cell is a `&str` slice; building a column is two growing `Vec`s with **zero
per-cell allocation**. This removes exactly the cross-thread allocator
contention Phase 0.5 identified, so parallel parsing finally scales on wide,
string-heavy data.

Measured (4 vCPU), vs Phase-0.5:

| scenario | before | after | change | cumulative vs baseline |
|---|---:|---:|---|---:|
| `large/filter_only` (6 cols) | 111 ms | **51 ms** | **−54%** | **~6.8×** (349→51 ms) |
| `error_heavy/bad=0%` | 105 ms | 48 ms | −54% | ~6.3× |
| `mixed_types/mix=0%` (1 str col) | 19.9 ms | 18.5 ms | −6% | ~2.5× |

Wide data is now ~3.9 M rows/s end-to-end. The `unsafe { from_utf8_unchecked }`
in `StrColumn::get` is guarded by the type's invariant (only `&str` is ever
appended) and locked by a multibyte round-trip test
(`str_column_roundtrips_including_multibyte`). All stress + equivalence tests
stay green.

### Optimization backlog (driven by these numbers)

1. ~~CSV reader, single-pass, zero owned-`String`~~ — ✅ Phase 0.1.
2. ~~Avoid double source reads~~ — ✅ Phase 0.2 (`dedup_sources`).
3. ~~Operator fusion (filter chains → project)~~ — ✅ Phase 0.3.
4. ~~Projection pushdown into the CSV reader~~ — ✅ Phase 0.4.
5. ~~Parallel CSV parsing + inference fast-path~~ — ✅ Phase 0.5.
6. ~~Arena string columns (offsets + bytes)~~ — ✅ Phase 0.6.
7. **Filter pushdown** into the reader (skip building rows that won't survive).
8. **Parallel pipeline execution** (chunk-split operators across workers) — doc 05.
9. **Zero-copy `&str` predicate eval** (compare against the arena without
   materializing a `String` per row) for string-keyed filters/joins.

Every optimization PR must attach its before/after row from this table and must
keep `tests/stress.rs` green (correctness is the gate, speed is the reward).
