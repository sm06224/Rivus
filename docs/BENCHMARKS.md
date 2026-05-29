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

### Optimization backlog (driven by these numbers)

1. ~~**CSV reader, single-pass, zero owned-`String`**~~ — ✅ done (Phase 0.1).
2. ~~**Avoid double source reads**~~ — ✅ done (Phase 0.2, `dedup_sources`).
3. **Operator fusion** (filter→project) to drop intermediate chunks (doc 08).
4. **Projection pushdown** into the CSV reader: skip building unused columns.
5. **Vectorized / SIMD predicate kernels** for `i64`/`f64` lanes (doc 09);
   asm-level tuning where a bench proves the win.
6. **Reduce gather copies** (esp. string columns) via Arrow `ArrayRef` sharing.
3. **Vectorized / SIMD predicate kernels** for the `i64`/`f64` lanes (Phase 1→2,
   design doc 09); asm-level tuning where a bench proves the win.
4. **Reduce fan-out clone cost** via Arrow `ArrayRef` refcount sharing (doc 03).

Every optimization PR must attach its before/after row from this table and must
keep `tests/stress.rs` green (correctness is the gate, speed is the reward).
