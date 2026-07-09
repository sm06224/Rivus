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

## Phase 0.7 — zero-copy `&str` predicate evaluation

Predicate evaluation gained borrowed fast paths for the common `Field CMP
Literal` shape: a string comparison reads the arena column as `&str` and a
numeric comparison reads the lane directly, so neither allocates a `Value` (no
`String` per row for string-keyed filters). Mixed/null operands fall back to the
owned-`Value` interpreter, so results are identical.

| scenario | before | after | change |
|---|---:|---:|---|
| `large/string_filter` (`country == "JP"`, 6 cols, no projection) | 58.9 ms | 56.2 ms | −5% |

Modest because this case is parse-bound; the win is removing two `String`
allocations per row, which scales with selectivity and string-heavy,
filter-dominated pipelines. Correctness (incl. `!=` and chunk-size independence)
is locked by `tests/stress.rs::string_filter_matches_oracle`.

## Phase 0.8 — vectorized numeric predicate kernel

The interpreter (`eval.rs`) walked the `Expr` tree *and resolved each field by
name on every row* (`O(rows × fields)` lookups). For a conjunction of
`field <cmp> number`, `kernel.rs` now **compiles once** — resolving each field
to a column index + numeric rhs — then evaluates with per-column typed loops.
Non-numeric / OR / string predicates fall back to the interpreter (identical
results, gated by stress + equivalence tests).

Clean before/after (kernel off via env vs on), `optimizer/filter_chain_raw`
(4 numeric filters + project, 200k rows):

| | time | throughput |
|---|---:|---:|
| interpreter (kernel off) | 71.6 ms | 2.79 M rows/s |
| **vectorized kernel (on)** | **47.5 ms** | **4.21 M rows/s** | 

**~1.5× on multi-predicate filters** — the per-row name lookup ×N preds was the
killer. Single-predicate, parse/write-bound tasks (e.g. `bench/compare.sh`) are
unchanged: the kernel targets *execution*-bound filtering, not I/O. Closing the
remaining DuckDB gap on I/O-bound tasks needs parallel *pipeline* execution
(item 9), which the external comparison already flagged.

## Phase 0.9 — parallel pipeline execution

The whole stateless pipeline now runs multi-threaded, not just parsing. When a
flow has a single file source and no stateful op (group/join), the engine parses
the source once, splits the chunks into contiguous partitions, runs identical
stateless sub-DAGs on worker threads, and merges in source order (file sinks are
written exactly once after the merge). Group/join/multi-source flows stay
serial. Correctness is held by the existing stress + equivalence suites, which
exercise the parallel path at small chunk sizes (`chunk_size=1` → thousands of
chunks) and still match their oracles exactly.

Measured (4 vCPU):

| scenario | serial | parallel | change |
|---|---:|---:|---|
| `bench/compare.sh` 1M e2e (filter→project→write) | 0.277 s | **0.178 s** | **1.55×** (5.6 M rows/s) |
| `large/filter_only` (200k, 6 cols) | ~50 ms | 39.8 ms | −23% |
| `huge/filter_only_2M` (2M rows) | ~650 ms | **357 ms** | **−45% (~1.8×)** |

The win **grows with data size** (more chunks to spread): ~1.3× at 200k, ~1.8×
at 2M. Against the external baseline, optimized Rivus went 3.7 → **5.6 M rows/s**
on the compare task — still behind DuckDB but materially closer, with the
remaining gap now in parsing/IO parallelism rather than execution.

### Optimization backlog (driven by these numbers)

1. ~~CSV reader, single-pass, zero owned-`String`~~ — ✅ Phase 0.1.
2. ~~Avoid double source reads~~ — ✅ Phase 0.2 (`dedup_sources`).
3. ~~Operator fusion (filter chains → project)~~ — ✅ Phase 0.3.
4. ~~Projection pushdown into the CSV reader~~ — ✅ Phase 0.4.
5. ~~Parallel CSV parsing + inference fast-path~~ — ✅ Phase 0.5.
6. ~~Arena string columns (offsets + bytes)~~ — ✅ Phase 0.6.
7. ~~Zero-copy `&str` predicate eval~~ — ✅ Phase 0.7.
8. ~~Vectorized numeric predicate kernel~~ — ✅ Phase 0.8.
9. ~~Parallel pipeline execution~~ — ✅ Phase 0.9.
10. **Filter pushdown** into the reader (skip building rows that won't survive).
11. **Parallel sink writing** + memory-mapped source reads (close the rest of
    the DuckDB gap, which is now in parse/IO rather than execution).

### Cumulative (200k rows, end-to-end read→parse→run)

`open | filter | project age` : **0.57 → ~3.8 M rows/s** (baseline → Phase 0.7),
with every step individually measured and semantics-preserving (correctness
gated by `tests/stress.rs` + `tests/optimizer_equiv.rs`).

### Binary source (C struct dump) — `readbin`

Besides CSV, Rivus reads fixed-width binary records (a C struct dump):
`readbin path (id:i32 age:i32 score:f64 active:u8)`. Fields are packed in
declaration order, little-endian, and decode straight into columnar lanes —
**no text parsing at all**.

| scenario (200k rows) | time | throughput |
|---|---:|---:|
| `binary/filter_only` (`readbin … \|? age>=45`) | 10.7 ms | **~18.7 M rows/s** |
| `large/filter_only` (CSV, 6 cols) | 52 ms | ~3.8 M rows/s |

~5× the comparable CSV path. Correctness (incl. chunk-size independence) is
locked by `tests/stress.rs::binary_source_matches_oracle`. Integer widths ride
the `i64` lane, floats the `f64` lane, `bool` is one byte; `u64` above
`i64::MAX` wraps (documented until a `u64` lane exists). Trailing partial
records are reported on the error stream and ignored (continue-first).

### External comparison (anti-NIH grounding)

Same logical task — read a 1,000,000-row CSV, filter `age >= 45`, project `name`,
write the result — across Rivus and established tools (`bench/compare.sh`, best
of 3; tools used only if present). This grounds Rivus's numbers against
collective-wisdom engines rather than judging it in isolation.

| tool | time | throughput | vs rivus (opt) |
|---|---:|---:|---|
| **rivus (optimized)** | 0.270 s | 3.71 M rows/s | — |
| rivus (`--no-opt`) | 0.353 s | 2.84 M rows/s | |
| awk (mawk) | 0.310 s | 3.22 M rows/s | rivus **1.15× faster** |
| **duckdb 1.1.3** | 0.135 s | 7.39 M rows/s | duckdb **~2× faster** |
| python (stdlib csv) | 1.232 s | 0.81 M rows/s | rivus **~4.6× faster** |

**Honest standing:** optimized Rivus already beats `awk` and is ~4.6× faster
than a hand-written Python loop — but a world-class vectorized, multi-threaded
engine (DuckDB) is ~2× ahead. That 2× is the north star: it is explained almost
entirely by (a) DuckDB executing the *whole* query multi-threaded while Rivus so
far only parallelizes parsing, and (b) vectorized/SIMD predicate kernels. Both
are on the backlog (items 8 + a SIMD kernel pass) — the comparison turns them
from "nice to have" into a measured target. Projection pushdown is what gets
Rivus from 0.353 → 0.270 s (it builds only `name`/`age`).

Run it yourself: `bench/compare.sh [ROWS] [RUNS]`.

### Scale validation (2,000,000 rows)

Confirms the parallel parser + arena strings hold at millions of rows (no
pathological blow-up); `cargo bench -- huge/` (10 samples):

| scenario | time | throughput |
|---|---:|---:|
| `huge/filter_only_2M` (build all 6 cols) | ~650 ms | ~3.1 M rows/s |
| `huge/filter_project_age_2M` (pushdown → only `age`) | ~336 ms | ~6.0 M rows/s |

Wide-column throughput dips slightly vs 200k (memory bandwidth + string
allocation at scale); the projected path stays high because it builds one
column. This is the signal for the next items: filter pushdown (build fewer
rows) and parallel pipeline execution.

Every optimization PR must attach its before/after row from this table and must
keep `tests/stress.rs` green (correctness is the gate, speed is the reward).

### Streaming ingestion — bounded memory at any file size

The CSV source and sinks stream (read in chunks, write as results flow), so
resident memory is independent of input size. Measured peak RSS (`os.wait4`
`ru_maxrss`) on a 1.1 GB / 48 M-row CSV, release build, single thread:

| pipeline | input | peak RSS | time | throughput |
|---|---:|---:|---:|---:|
| `open \|? age>=50 \|> name age save out.csv` (serial) | 1.1 GB | **10.1 MiB** | 14.4 s | ~3.3 M rows/s |
| same, **streaming-parallel** (4 cores) | 1.1 GB | **10.2 MiB** | **3.0 s** | **~16 M rows/s** |
| `open big.csv` (bare, no sink → **preview**) | 1.1 GB | **10.0 MiB** | **0.00 s** | instant |
| `open \|? age>=50` (no sink → **preview**) | 1.1 GB | **10.4 MiB** | **0.00 s** | instant |

A sink-less `rivus run` is a **preview**: the CSV source sample-infers its
schema from the first chunk and the engine stops after the row cap (default
1000), so eyeballing a 15 GB file is instant and flat-memory. Adding
`save out.csv` switches to full global-inference streaming over every row.

**Streaming-parallel** (files > 256 MiB with a file sink): the schema is
inferred once over newline-aligned byte ranges *in parallel*, then each worker
streams its range through an identical stateless sub-DAG and writes a **part
file**; the parts are concatenated in source order (CSV headers de-duplicated).
**2.7× on 4 cores while staying at ~10 MiB** — bounded memory is preserved
because nothing is buffered (an earlier `collector`-based attempt hit 690 MiB;
that regression is why workers stream to part files instead). A byte-identical
oracle test (`engine::tests::streaming_parallel_matches_serial`) gates it.

Before streaming, `open` alone read the whole file into a `String` and parsed
every column up front (~2–3 GB resident for this input, with a long stall and no
output). Now memory is flat at chunk scale and rows flow immediately; an
interactive run shows a live `… N rows  T s  R rows/s` line on stderr.

Files above 256 MiB use the streaming-parallel reader; the chunk-partition
parallel parser (which materializes the whole input to split it) is kept for
small files only.

### External comparison — vs DuckDB / awk / Python (1.1 GB, 48 M rows)

Same task as above (`filter age>=50`, project `name,age`, write CSV), release
build, 4 vCPU, peak RSS via `os.wait4`:

| tool | command | time | peak RSS |
|---|---|---:|---:|
| **Rivus** | `open … \|? age>=50 \|> name age save out.csv` | **3.0 s** | **10.2 MiB** |
| DuckDB | `COPY (SELECT name,age … WHERE age>=50) TO …` | 4.4 s | 406.8 MiB |
| gawk | `awk -F, 'NR>1&&$3>=50{print $2","$3}'` | 11.5 s | — |
| Python | stdlib `csv` reader/writer | 30.9 s | 10.1 MiB |

All four produce the same 22.4 M output rows. **Rivus is ~1.45× faster than DuckDB
while using ~40× less memory** (10 MiB vs 407 MiB — DuckDB parallelizes the
whole query but buffers; Rivus streams), and is **3.8× faster than awk**
and **~7× faster than Python**. This is the headline target met: for everyday
streaming ETL, Rivus is a credible replacement for reaching to DuckDB/Python —
same speed, a fraction of the footprint. Reproduce with `bench/compare.sh`.

Filter pushdown into the reader is what takes Rivus past DuckDB here: the
optimizer lifts `age>=50` onto the CSV source, so the reader skips *building*
the `name` column for the ~half of rows the predicate drops (4.1 s → 3.0 s).
The downstream FilterProject stays authoritative, so output is byte-identical
(gated by `tests/optimizer_equiv.rs`).

### SWAR delimiter scan — landed (overturns the earlier negative result)

`split_offsets` now scans 8 bytes at a time with a dependency-free SWAR
(SIMD-within-a-register) word loop — `core::arch`-free, `unsafe`-free, host-endian
independent — fusing the `"`-detection and `,`-location that the scalar path did
as two separate passes over each line.

**Why the earlier "no win" was wrong (two reasons).**

1. *Measurement methodology.* The earlier note measured **end-to-end wall** on a
   `filter+project+write` path (1.13 vs 1.14 s) where IO + column-build + write are
   fixed costs that dilute the parse node, and the runs were **not interleaved**, so
   machine noise (±0.05 s) swamped the signal. Measuring the **`open` node `busy_ms`
   in interleaved base/SWAR pairs** isolates the parse and exposes a clear win.
2. *A real bug in the naive mask.* The classic `(x-0x01..)&!x&0x80..` zero-byte
   trick is only reliable as a **boolean** ("any match?"). Its per-byte high bits
   are corrupted by subtraction **borrows** — a zero (matching) lane followed by a
   `0x01` lane false-positives — so using it to **locate** delimiters mis-splits some
   records (e.g. `1,-49.5,n1`). Caught by a 4000-line parity test
   (`swar_split_stress_lines`) that the stress suite's chunk-size oracle also flags.
   The landed version uses a **carry-free** exact mask: `(b & 0x7F) + 0x7F` stays
   ≤ `0xFE`, so no carry crosses a byte boundary — per-lane exact, location-safe.

**Result (5 M rows / 171 MiB, release, interleaved base/SWAR pairs, `open` node
`busy_ms`):**

| workload (serial) | scalar `open` | SWAR `open` | speedup |
|---|---:|---:|---:|
| `open … save` (all rows built) | ~2120 ms | ~1903 ms | **~10 %** |
| `open … |? age>=50 |> name age save` | ~1417 ms | ~1168 ms | **~17 %** |

| default parallel (4 cpu) | scalar | SWAR | speedup |
|---|---:|---:|---:|
| `open … save`, `open` node `busy_ms` | ~1069 ms | ~1017 ms | **~5 %** |
| `open … save`, end-to-end wall | ~2.15 s | ~1.91 s | **~11 %** |

Every interleaved pair showed SWAR faster (no overlap). **Byte-identical** output
confirmed by md5 on both workloads (`b45986b8…`, `a19f4f2e…`) and by the full
equivalence/stress suites; zero third-party deps unchanged. The win is larger when
the split is a larger fraction of the parse (the filter path skips building dropped
rows, so the per-line scan dominates more). Next field-scan lever: faster int/float
parse — measured to be cheap here (building 2 vs 6 columns barely moved the time, so
the **scan**, not the per-cell parse, was the cost), so it is *not* pursued without a
profile that shows parse as hot.

### Parse read method — measured negative result (`fill_buf` not adopted)

After the SWAR split landed, checked whether the line *read* itself is worth
optimizing. Microbenchmark over the 5 M-row / 171 MiB file (warm cache, read only,
no parsing):

| read method | time |
|---|---:|
| `read_line` (copy + UTF-8 validate) | ~195 ms |
| `read_until` (copy, no validate) | ~147 ms |
| `fill_buf` (no copy, scan in place) | ~137 ms |

So the whole *read* is only ~195 ms — roughly **10 % of one parse pass** (`open` is
~1.9 s for this file). Switching the reader to `fill_buf` would save the ~48 ms
UTF-8 validation + ~10 ms copy, i.e. **~6 % of `open`** — not worth rewriting the
hot streaming loop (with its byte-range/limit, BOM, and straddling-line edge
cases) for. `open` is dominated by the **two-pass split + per-cell value
parse/build** (already split-optimized by SWAR), which is inherent to the
bounded-memory, chunk-size-independent two-pass design. Per the "measure before
adopting" rule (cf. #39, #45), `fill_buf` is **not adopted**; a real parse win now
needs either breaking the two-pass (a memory trade-off) or SIMD value parsing — a
dedicated, measured effort.

### Columnar / selection-vector (#40) — measured: gather is negligible, deferred

#40 proposes a columnar core whose lever (per #39) is reducing the **gather**
(materializing surviving rows into new columns) that dominates the *filter
kernel's internal* time. Measured the fused `filter+project` node (which performs
that gather) against parse, 5 M rows, serial:

| flow (`open … |? age>=X |> name age save`) | fused (filter+gather) | open (parse) |
|---|---:|---:|
| `age>=50` (~mid selectivity) | ~15 ms | ~908 ms |
| `age>=18` (~high selectivity) | ~27 ms | ~943 ms |
| `age>=89` (~low selectivity) | ~0.6 ms | ~833 ms |

So the gather a columnar rewrite targets is **~0.5–27 ms — under 3 % of parse, and
a fraction of a percent of the whole pipeline**. #39 was right that gather
dominates the *filter kernel's* cost, but the filter kernel is itself negligible
next to parse (and the now-optimized save). A columnar/selection-vector rewrite is
a large architectural change with **near-zero measured payoff for these
workloads**, so per the measure-before-adopting rule it is **deferred** until a
gather-dominated workload appears (e.g. very wide projections, repeated
re-gathering, or an alternate execution backend where the row-wise gather is the
bottleneck).

**Where parse time actually goes (measured).** Interleaved `open` over 5 M rows is
**~1430 ms whether it builds all 6 columns (no projection) or 1 (`|> id`)** — i.e.
per-column value building (parse + string copy) is *not* the dominant cost (an
earlier note guessing it was is corrected here). The cost is the per-row work done
regardless of projection: the **read + full-line split, run in both passes** of the
two-pass reader (the split scans every field to find record boundaries even for
unkept columns; the split itself is already SWAR-optimized). So **SIMD value
parsing is also low-payoff**, and gather (#40) is negligible. The one lever that
would meaningfully cut parse is **eliminating the second pass** (split once, not
twice) — but that trades either chunk-size-independent typing (sampled/single-pass
inference) or memory (buffer the parsed values), and was measured no-win in
warm-cache earlier; it is a deliberate design trade-off for the maintainer, not an
autonomous change. Net: parse is at its **floor for the bounded, chunk-size-
independent two-pass design**; save is optimized; the DuckDB gap is structural.

### Exact decimal filter — no silent rounding, faster *and* correct (#44)

The decimal lane's filter comparison used the f64 view (`u as f64 / 10^scale`),
which loses precision once `|unscaled| > 2^53` and re-introduced the float error
the lane exists to eliminate. The **accounting contract** (design 21) is stronger
than "more accurate f64": a decimal comparison must **never silently round**
either operand. So the literal is preserved as the *exact decimal it was written
as* (a numeric literal with a fractional part lexes to `Value::Dec` at its natural
scale, never via `f64`), and the compare runs at `max(column_scale, literal_scale)`
as `i128` — the same `Decimal::partial_cmp` rule on the kernel and interpreter, so
they stay byte-identical. The per-row work is **hoisted**: the literal lifts to the
common scale once, and each cell is a single `i128` compare whenever the literal's
scale ≤ the column's (the common case; `factor == 1`).

This fixes a real contract violation in the first cut, which quantized the literal
to the column scale (round half-even): `amount > 19.995` then wrongly became
`amount > 20.00` and dropped `20.00`. The exact path keeps `20.00` and matches
nothing for `amount == 0.305` on a `decimal(2)` column — no rounding either way.

Measured on a `decimal(2)` column, `|? amount > 500.00`, 5 M rows, serial,
interleaved old/new pairs (`fused` node `busy_ms`):

| decimal filter | `fused` busy_ms |
|---|---:|
| old — f64 view (`u as f64 / pow`, per-cell convert + divide + f64 compare) | ~45 ms |
| new — exact decimal (hoisted lift, one `i128` compare per cell) | **~22 ms** |

So the exact, no-rounding path is **~2× faster** (one integer compare replaces a
per-cell int→float convert + float divide), not merely cost-neutral — and it is
also *correct* for large values: a `decimal(0)` column with `9007199254740993`
(`2^53 + 1`, not f64-representable) is kept by `> 9007199254740992` on both the
kernel and interpreter, where the f64 view wrongly dropped it. Gated by
`optimizer_equiv::decimal_filter_is_exact_i128`, `decimal_filter_no_silent_rounding`
(sub-cent literals, kernel == interpreter), and `decimal_filter_boundaries_exact`.

### Parallel group-by — byte-identical partition→merge (#41, option 1)

Group-by was serial (the parallel scheduler only handled stateless map/filter).
Each **byte-range streaming worker** now runs its range through the pre-group ops
into a *partial* group state (the same `plan_parallel` + `csv_range_source` the
streaming-parallel reader uses), and the partials merge in source order — taken
only when every aggregate is byte-identical under partition→merge: `min`/`max`/
`count`/`count_distinct`/`first`/`last`/percentile (associative or buffered+sorted)
and `sum`/`avg` **on a decimal column** (exact i128, associative — the reason the
decimal lane was built). `std` and `sum`/`avg` on f64/integer columns are *not*
associative and stay serial (the scheduler checks the group-input schema, so a
pre-group `cast` to decimal counts; the pre-group ops are an allowlist).

Because each worker holds only its current chunk and its partial group state, peak
memory is **O(group cardinality), input-size independent** — the bounded-memory
guarantee is kept on the parallel path. Measured peak RSS (`ru_maxrss`) and
end-to-end wall (4 cpus, release):

| group-by | peak RSS (6 M rows / 53 MiB, 2 groups) | wall (3 M rows, group→sum/avg/min/max) |
|---|---:|---:|
| serial (`--memory low`) | 6 MiB | ~1.35 s |
| parallel, first cut (materialized whole input) | **145 MiB** | — |
| parallel, byte-range streaming (landed) | **6 MiB** | **~0.97 s** (~1.4×) |

So streaming brings the parallel path back to the serial **6 MiB** (vs 145 MiB for
the materialized first cut — O(input)), with no loss of speed. Output is
**byte-identical** to serial (md5-equal saved CSV; exact decimal sums like
`12500000.00`, not f64-drifted). Gated by `stress::parallel_group_by_matches_serial`
(parallel == serial across the safe set, workers engaged),
`f64_sum_group_stays_serial_but_correct` (unsafe path stays serial), and
`operators::agg_merge_tests` (partition→merge == single-pass).

**JSONL too (#49).** JSON Lines is now a splittable, bounded-streaming source: it
reads in O(1)-per-chunk memory (no whole-file slurp) and its group-by takes the
same bounded byte-range parallel path. On a 48 MiB / 2 M-row JSONL group-by (2
groups) peak RSS is **6 MiB serial and 6 MiB parallel** (input-size independent),
byte-identical to serial (`stress::parallel_jsonl_group_bounded_byte_identical`).
Fixed-width **binary** is splittable too (record-aligned ranges, no boundary scan)
and streams bounded — its filter/project and group-by parallelize the same way
(`stress::parallel_binary_byte_identical`). The bounded path now covers CSV +
JSONL + binary; only genuinely non-splittable sources (compressed) need the opt-in
`--memory unbounded` (#50).

### f64 parallel aggregation — canonical reduction (measured assessment, #45)

Question: should plain `f64` `sum`/`avg`/`std` parallelize in the group-by (today
they stay serial — #41 option 1)? Measured the options on a 200k-element f64 stream
with magnitudes large enough to actually round (`stress::f64_parallel_sum_…`):

| reduction | result |
|---|---|
| serial naive left-fold | the reference value |
| **naive partition→merge** | diverges by ~`5–17e6` **and varies with the partition count** → not byte-identical |
| **canonical fixed-block fold** | a *pure function of (values, block size)* → partition-independent; but its value differs from the serial naive fold (relative ~`1e-15`) |

So a canonical reduction (serial *and* parallel fold over global-row-order fixed
blocks) **can** make f64 byte-identical — at two real costs: (a) it changes the
serial value too (every f64 sum/avg/std shifts by ~ULPs), and (b) running it
*bounded + parallel* for grouped aggregation needs global-row coordination (a
row-count pre-pass to give each byte-range worker its global start row, plus a
≤block-size carry to merge the blocks that straddle a worker boundary) — otherwise
it degrades to O(rows-per-group) buffering, breaking the bounded-memory guarantee.

**Recommendation (measured): keep f64 `sum`/`avg`/`std` serial (#41 option 1) and
route exactness through the decimal lane**, which *already* delivers an exact,
byte-identical, bounded, parallel `sum`/`avg` today (`:decimal` / `--exact`; i128
is associative, so no canonical tree is needed). The canonical-tree work for plain
f64 is deferred to a dedicated PR, justified only if a real workload needs parallel
f64 aggregation that can't use the decimal lane — at which point the global-row
coordination above is the design. This mirrors the #39 discipline: a clever
mechanism is not adopted until a measurement shows it earns its complexity.

**Follow-up measurement (2026-07, → `docs/design/37`, awaiting ratification):** an
isolated prototype of the canonical fixed-block tree (not wired into the engine)
adds two findings that may move the recommendation. (1) *Parallel canonical is
bit-identical to serial canonical across P=2/4/8* — achieved only when each worker
returns its **vector of block-sums** (not a single scalar), so the folded tree is
the exact same shape regardless of partition count. (2) *The canonical block-sum is
markedly more accurate than the shipping naive left-fold*: against a Kahan-Babuška
reference on n=1M over 40 seeds, canonical is strictly more accurate in **39/40**
(mean `naive_err / canonical_err` = **70.5×**), because blocked summation grows
error as O(log n) vs the naive fold's O(n). Parallel canonical is ~1.5–3× faster
than the serial fold at n≥1M. So the ratification question sharpens to: *accept a
one-time ~1-ULP shift of every f64 sum/avg/std (at the adopting version boundary),
in exchange for parallel byte-identity **and** a standing accuracy improvement?*
The value shift does not touch `--exact`/decimal users (i128 is unchanged). See
`docs/design/37` §37.6 for the two questions posed to the maintainer.

### Columnar CSV write — format from the lane, stream the output (save ~2.2×)

Output writing was the **second cost** in the 1 GB profile (save 6.9 s vs parse
12.6 s). The hot loop allocated **twice per cell** — `chunk.value(row, c)` (an owned
`Value`, copying every string cell) then `csv_escape(..)` (a fresh `String`) — and
`write_csv_file` built the *entire* output in one `String` before writing. Replace
both with `write_cell`, which formats each cell **straight from its typed column
lane** into one reused line buffer (numeric/bool/decimal lanes never need quoting,
so they go verbatim; only a string cell containing the delimiter/`"`/newline is
quoted), and stream `write_csv_file` through a `BufWriter` (bounded memory, no
whole-output `String`).

Measured on `open … save` (5 M rows, serial, interleaved old/new pairs, the `save`
node `busy_ms`): **~2902 ms → ~1334 ms (~2.2×)**, every pair faster. Output is
**byte-identical** (md5-equal on the serial *and* parallel part-file paths) and the
full equivalence/stress suites stay green; zero third-party deps unchanged.

The **JSONL / JSON** sinks got the same treatment (`write_json_cell` formats from
the column lane; `write_jsonl_file` streams through a `BufWriter`): JSONL `save`
~2780 → ~2190 ms (~1.27× — smaller than CSV because the per-cell cost was already
one allocation, and `json_string` escaping dominates string-heavy output),
byte-identical (md5-equal).

### vs grep — literal line-match vs semantic filter (5 M rows, 171 MiB)

Data generated self-hosted with `rivus gen clean --rows 5000000` (no awk).
Release build, 3 runs each, warm cache.

**Task A — select rows where `country == "JP"`** (grep's home turf: a literal
substring of the raw line, no parsing):

| tool | command | time | rows |
|---|---|---:|---:|
| grep | `grep -c ",JP," data.csv` | **0.27 s** | 1 001 296 |
| Rivus | `open … \|? country == "JP" save -` | 3.1 s | 1 001 296 |

grep is **~11× faster** here, and that is expected and correct: grep scans raw
bytes and never parses CSV, infers types, builds typed columns, or
re-serializes — Rivus does all of that (and twice, for streaming type
inference). For "find lines containing a literal", reach for grep.

**Task B — numeric filter `age >= 50`, project `name age`** (grep *cannot*
express this: it matches bytes, not numeric ranges — you'd need a hand-built
alternation over every matching value):

| tool | command | time | rows |
|---|---|---:|---:|
| grep | — (not expressible) | — | — |
| Rivus | `open … \|? age >= 50 \|> name age save -` | **2.0 s** | 2 222 037 |

(Row count matches the `awk 'NR>1&&$3>=50'` oracle.) Note Task B is *faster*
than Task A despite scanning the same file: filter pushdown skips building the
dropped rows' columns and projection materializes only `name,age`, so less work
than the full-row `save -` in Task A.

**Takeaway:** grep wins decisively on literal line-selection — different tool,
different job. Rivus's value is *typed, semantic* selection (numeric/boolean
predicates, casts, computed columns, joins, aggregates) over the same data at
streaming, bounded memory. The two compose: `grep ,JP, data.csv | rivus '|?
age >= 50 |> name age'` pre-filters bytes with grep, then does the typed work.

### Search-pattern matrix — grep / ripgrep vs Rivus (5 M rows, 171 MiB)

Self-generated (`rivus gen clean --rows 5000000`), release, median of 3, warm
cache. DuckDB rows omitted — its CLI binary could not be fetched in the sandbox
(network policy); the script in `bench/search.sh` fills them in where available.

| pattern | grep | ripgrep | Rivus | Rivus expr |
|---|---:|---:|---:|---:|
| literal `,JP,` | **0.27 s** | 0.33 s | 3.0 s | `contains(country,"JP")` |
| prefix `name=aki…` | 0.74 s | **0.34 s** | 2.5 s | `starts_with(name,"aki")` / `like(name,"aki%")` |
| IN-set `country∈{JP,DE,BR}` | 0.74 s | **0.34 s** | 2.6 s | `country=="JP" or … or …` |

**grep/ripgrep win every literal/anchored/alternation pattern by 4–10×** — and
that is the right outcome: they scan raw bytes and never parse CSV, infer types,
build typed columns, or re-serialize. For "which lines match this pattern",
reach for ripgrep.

Where Rivus is the right tool is the part grep *can't express*: typed, semantic
predicates over parsed fields — numeric ranges (`age >= 50`), casts, computed
columns, `case when`, joins, group aggregates — at streaming, bounded memory,
with the same engine then doing projection / aggregation / output. And the two
compose: `rg ',JP,' data.csv | rivus '|? age >= 50 |# active avg:score'` lets
ripgrep pre-filter bytes and Rivus do the typed work on the survivors.

The gap also points at the optimization that would move these numbers: Rivus
does a **two-pass** streaming read (inference, then build). A pushed-down
*string* prefilter (today only numeric predicates push into the reader) would
let a `contains`/`like`/`starts_with` skip building dropped rows — the same
trick that already took the numeric filter past DuckDB. Tracked in the backlog.


### Regex + DuckDB — the high wall (5 M rows, 171 MiB)

Self-generated (`rivus gen clean --rows 5000000`), release (Rivus built
`--features regex`), median of 3, warm cache, **both writing the full projected
result to stdout** (`COPY … TO '/dev/stdout'` for DuckDB). DuckDB 1.1.3 CLI.

With the 8 MiB threshold now wired into the engine (this file is 171 MiB, so it
takes the byte-range streaming-parallel reader; re-measured 2026-05-31, best of 3):

| pattern | DuckDB | Rivus (serial) | Rivus (parallel) | note |
|---|---:|---:|---:|---|
| regex `^aki[0-9]+$` | **0.34 s** | 2.02 s | 0.54 s | `regexp(name,…)` vs `regexp_matches`, compiled-once |
| IN-set `country∈{JP,DE,BR}` | **0.41 s** | 2.08 s | 0.66 s | DuckDB `IN` vs Rivus `or`-chain |
| numeric `age >= 50` → project | **0.36 s** | 1.59 s | 0.43 s | grep can't express; Rivus filter-pushdown |

**The wall is now ~1.2–1.6×, down from 6–10×.** The byte-range streaming reader
gives a clean ~3× over serial here (171 MiB: numeric 1.59 s → 0.43 s, regex
2.02 s → 0.54 s, IN-set 2.08 s → 0.66 s), byte-identical to the serial path
(`RIVUS_NO_PARALLEL=1`). Numeric is now within ~1.2× of DuckDB; the string-set /
regex shapes ~1.6×. The remaining difference is the **CSV read path**, not the
predicate engine (rust-lang regex matches at DuckDB's RE2-class speed): Rivus
does a *two-pass* streaming read (infer types, then build typed columns) where
DuckDB reads once into vectors. (On a 380 MiB file the same numeric query is
0.91 s parallel vs 3.33 s serial — the win grows with file size.)

> **Note (2026-05-31):** the parallel speedup above only materialized once the
> 8 MiB threshold was *actually wired into the engine*. The earlier "lower to
> 8 MiB" change edited only the docs — the engine const stayed at 256 MiB, so
> 8–256 MiB files silently used the in-memory chunk-partition path, which
> materialized the whole file and ran *slower than serial* (171 MiB numeric:
> 1.7 s in-memory vs 1.5 s serial). `try_parallel` now reads the threshold from
> `parallel_min_bytes()` (default 8 MiB, `RIVUS_PARALLEL_MIN_BYTES`-overridable).

Read-throughput levers, by remaining impact:
1. ✅ **Parallel reads for stdout sinks** — done. The byte-range reader used to
   bail to serial on a `save -` sink; it now assembles ordered parts to stdout.
2. ✅ **Lower the parallel threshold to 8 MiB — *and wire it into the engine*** —
   done. Mid-size files (8–256 MiB) now take the streaming-parallel reader
   instead of the slower in-memory path. `RIVUS_PARALLEL_MIN_BYTES`-overridable
   (default 8 MiB); `RIVUS_NO_PARALLEL=1` forces serial.
3. ❌ **Single-pass retain-buffer reader** — *evaluated and dropped*: measured
   slower than two-pass on a warm cache (see the Pillar C section below). The
   real single-thread→multi-thread win is the byte-range parallel reader.
4. 📋 **mmap + overlap decode with IO**; reuse per-chunk buffers.

DuckDB still buffers (~400 MiB RSS on the 1.1 GB set earlier) where Rivus
streams at ~10 MiB, so the honest framing stays "Rivus trades some speed for
bounded memory and a zero-dependency default" — and the roadmap goal is to close
the read-throughput gap until that trade is near-free. ripgrep remains the right
tool for "match lines in a file"; Rivus composes with it (`rg … | rivus …`).

### String prefilter pushdown (Epic #30 / Pillar C C4(i), 5 M rows, 171 MiB)

`filter_pushdown` now also lifts **literal-substring** predicates
(`contains` / `starts_with` / `ends_with` / `==` / the literal run of `like`)
into the reader as a ripgrep-style raw-line byte pre-scan: a line lacking the
needle is skipped *before* it's split into fields. It's a **superset** filter
(the downstream `FilterProject` re-checks every survivor, so the result is
byte-identical — a substring landing in the wrong column is still rejected), and
it costs no extra memory.

Measured: `… |? contains(country, "JP") |> id name age save -`, serial
(best of 3, 171 MiB, ~1 M matching rows):

| | wall | rows out |
|---|---:|---:|
| without prefilter (`--no-opt`) | 3.45 s | 1,001,313 |
| with string prefilter | **1.70 s** | 1,001,313 |

**~2.0× on the serial path** — the win is skipping the split+build of the ~80%
of rows that can't match. Result is identical (count matches DuckDB's
`country LIKE '%JP%'`). The skipped-row count is surfaced as A1 telemetry
(`prefilter skipped N row(s) at the reader`). The byte-range *parallel* reader
doesn't apply the string pre-scan yet (it stays on the numeric prefilter); that
extension is tracked for a later slice.

### Adaptive execution strategy (Epic #30 / Pillar C, #33) — and a dropped idea

Pillar C closes the "両立ループ" (visibility → strategy → speed): a std-only host
probe (`rivus_runtime::analytics::Analytics::probe` — logical CPUs, available
RAM from `/proc/meminfo`; both overridable with `RIVUS_CPUS` / `RIVUS_RAM_BYTES`
for deterministic tests) feeds an autotuner (`choose_strategy`) that picks the
execution strategy and **surfaces the decision** on `RunResult.strategy` (shown
in the `--json` summary as `"strategy"`). The user knob is
`--memory low|auto|fast`:

- `low` — force the single-thread bounded reader (lowest resource use).
- `auto` (default) — parallelize when ≥2 CPUs **and** the input clears the
  byte-range threshold (8 MiB); small inputs stay serial.
- `fast` — same, with a more aggressive threshold (1 MiB).

All three return **byte-identical** results (guaranteed by
`streaming_parallel_matches_serial` and the new
`memory_strategy_is_result_invariant_and_surfaced` test).

Measured (288 MB clean CSV, `|? age >= 20 |> name age save out.csv`, 4 cpus,
warm cache, best of 4):

| `--memory` | strategy chosen | wall | rows out |
|---|---|---:|---:|
| `low` | forced serial (two-pass) | 3.53 s | 6,223,068 |
| `auto` (default) | byte-range parallel | **1.13 s** | 6,223,068 |
| `fast` | byte-range parallel | 1.13 s | 6,223,068 |

**~3.1× faster on the default path, byte-identical output.** The decision is
self-describing, e.g.
`"memory=auto: 288130173 B ≥ 8388608 B, 4 cpus → parallel"`.

> **Dropped idea — single-pass retain-buffer reader (honest negative result).**
> The roadmap listed "single-pass inference (drop the second scan)" as the
> largest single-thread gap. We prototyped it: read the data region into memory
> once, infer globally over the buffer, then build columns from the buffer (no
> second disk scan), gated to files within a RAM budget (which is why an earlier
> draft probed available RAM). It is byte-identical and
> chunk-size independent — but it was **measured *slower*** than the two-pass
> reader on a warm cache (4.0 s vs 3.4 s on the file above): holding every line as
> an owned `String` creates allocation/memory pressure, while the "second scan"
> it eliminates is a nearly-free re-read from the OS page cache. Per the project
> law ("faster is never asserted without a measured number"), we did **not** ship
> it. The genuinely measured single-thread→multi-thread win is the byte-range
> parallel reader, so Pillar C's adaptive decision is **serial vs parallel**, not
> a single-pass reader swap. (A single-pass reader could still pay off on
> cold-cache / network filesystems where the second physical read is expensive;
> it can return behind a measured win for that regime.)


### String prefilter on the parallel path (Epic #30 / #35)

The literal-substring prefilter (C4(i)) originally engaged only on the *serial*
reader: the byte-range parallel workers hard-coded an empty `str_prefilter`, so
the default `--memory auto` path (which parallelizes files ≥8 MiB — i.e. exactly
the large-file regime the prefilter targets) never applied it. #35 threads
`str_prefilter` through `plan_parallel` → `for_range` so every worker runs the
same raw-line pre-scan, and the per-worker skip counts surface as A1 telemetry
(one `prefilter skipped N row(s)` Info per worker, summing to total − matching).

Correctness is covered by `string_prefilter_engages_on_parallel_path`: a forced
streaming-parallel run is **byte-identical** to a forced-serial run of the same
program, and the workers' skip telemetry sums to the independently-derived
(total − matching) count.

Honest performance note (171 MiB, 5 M rows, 4 cpus, warm cache, a 0%-selectivity
`contains(name,"Zzqx")` so parse — not output — dominates; best of 5):

| | prefilter on | prefilter off (`--no-opt`) |
|---|---:|---:|
| **parallel** (default) | 0.246 s | 0.246 s |
| **serial** (`RIVUS_NO_PARALLEL=1`) | 2.218 s | 2.218 s |

Two honest takeaways: (1) the **parallel reader itself is the ~9× win** here
(0.25 s vs 2.22 s); (2) at this query shape the string-prefilter shows **no
measurable end-to-end gain on/off**, because the two-pass reader's *pass-1 global
type inference* scans and splits every row regardless — the prefilter only avoids
*pass-2 column building*, which is already near-zero at 0% selectivity. The
prefilter still earns its keep on shapes where pass-2 building dominates (the
earlier `contains(country,"JP")` serial measurement), and #35's value is making
it **engage on the default parallel path at all** (previously a silent no-op
there) with exact, surfaced accounting. Pushing the pre-scan into *pass-1
inference* — so it can skip the dominant scan too — is a tracked follow-up.

### SIMD predicate kernel — branch-free mask refactor + measured AVX2 negative result (Epic #38 / #39)

Lever 1 of the "aggressive structural bets" Epic (#38) is the vectorized
predicate kernel. Two things were done, both measured:

**1. Branch-free byte-mask refactor (landed).** The kernel
(`crates/rivus-runtime/src/kernel.rs`) used to build surviving-index `Vec`s and
narrow them predicate-by-predicate. It now writes a **byte mask** (`(v <cmp>
rhs) as u8`, no branch, no `push`) over each contiguous `&[i64]`/`&[f64]` lane,
ANDs masks for a conjunction, and collects indices in one final pass. This is
what LLVM auto-vectorizes into packed SIMD compares — **zero `unsafe`, zero
deps**. Measured on the 5 M-row / 179 MiB clean set, serial, `--no-opt` (so all
rows reach `FilterProject`), `age >= 30 and score < 50.0` (the `filter` node's
own `busy_ms` from `--json`, best of 5):

| kernel | filter node busy_ms |
|---|---:|
| previous (index-narrowing) | ~81 ms |
| **branch-free mask (this PR)** | **~77 ms** |

A small (~5%) but real win, and a cleaner base for the columnar gather (#40).

**2. Hand-written AVX2 `f64` kernel (prototyped, measured, NOT landed).** An
explicit `core::arch::x86_64` AVX2 compare (`_mm256_cmp_pd` + movemask, runtime
`is_x86_feature_detected!`, scalar fallback, byte-identical incl. NaN via the
ordered `_OQ` predicates) was implemented and benchmarked against the
auto-vectorized scalar form on a 5 M-element `f64` column (30 iters, release):

| stage | scalar | AVX2 |
|---|---:|---:|
| mask production only (compare) | 5.5 ms | 5.8 ms |
| full `run()` hi-sel (70%) | 44.1 ms | 42.1 ms |
| full `run()` lo-sel (2%) | 9.5 ms | 9.8 ms |

The compare is **memory-bandwidth-bound** (~40 MB read for 5 M `f64`), so
explicit AVX2 ties or slightly *loses* to LLVM's auto-vectorization — and the
full-run cost is dominated by **index collection** (the `keep.push(i)` gather),
not the compare (5.5 ms of a 44 ms run). Per the project law — *faster is never
asserted without a measured number* — the `unsafe` intrinsic path was **dropped**
rather than shipped for no measured gain. The real lever is the gather itself: a
columnar selection-vector / late-materialization design, which is Epic #38 lever
2 (#40). On CSV today the whole filter node is only ~3% of wall (parse dominates
at ~2.1 s of 2.7 s), so this kernel work matters for the *columnar* core to come,
not for end-to-end CSV wall time yet.

### Where the time goes on 1 GB (profiling for the DuckDB gap)

統括 measured a 1 GB / 30 M-row CSV wrangle at ~22 s where DuckDB does ~10 s.
Profiling the node `busy_ms` (`--json`) on a 1.13 GB / 30 M-row file
(`open … |? age>=30 |> id age score save out.csv`, 4 cpus):

| | serial busy_ms | note |
|---|---:|---|
| **open (CSV parse)** | **12 591** | dominates — line split + per-field parse of 30M×6 |
| save (CSV write) | 6 897 | second cost — formatting + write of ~20M rows |
| filter | 429 | the predicate kernel is already cheap (#39) |
| project | 24 | negligible |
| serial wall | ~16.9 s | |
| **default parallel wall** | **~6.8 s** | byte-range parse parallelizes |

Two honest findings:

1. **Declared types barely help**: forcing `(id:int age:int score:f64 …)` to skip
   schema inference left `open` at 12.5 s (vs 12.6 s inferred). So the two-pass
   *inference* is **not** the bottleneck — the **pass-2 build (split + parse the
   30M×6 fields)** is. The next lever is faster field scanning, not fewer passes.
   **First step landed**: a dependency-free **SWAR delimiter scan** in
   `split_offsets` (8 bytes/step, carry-free exact mask) — ~10–17 % off the `open`
   node, byte-identical (see "SWAR delimiter scan — landed" above). A widening to
   `core::arch` SIMD and faster int/float parse remain open (parse measured cheap
   here; pursue only behind a profile). Output writing (save) is the second target
   (buffered formatting).
2. The default parallel path already turns 16.9 s → 6.8 s; the remaining gap to
   DuckDB is parse+write throughput per core, which the columnar core (#40) and a
   SIMD scanner target. **Measurement required before claiming any win.**

(Also tracked: UTF-8 **BOM** at the start of a file is not yet stripped — the
first header cell keeps the `﻿`; see ROADMAP "Ingestion".)

---

## SIMD-native structural scan — AVX2 delimiter/quote scan (#71, landed)

First step of the SIMD-native parse bet (#71): an **AVX2** structural-character
scan (`PCMPEQB` + `movemask`, 32 bytes/step) for `split_offsets`, runtime-
dispatched (`is_x86_feature_detected!("avx2")`) with the **SWAR** scan (8 B/step)
as the std-only fallback on non-AVX2 / non-x86 hosts. `core::arch`, `unsafe`
only under the feature-detection guard; **dependency-zero** preserved.

**Micro-bench** (`bench_split_scan`, release, 64-byte 12-field line, 2 M iters):

| scan | time | throughput | vs SWAR |
|---|---:|---:|---:|
| SWAR (8 B/step) | 59.6 ms | 2 148 MB/s | 1.00× |
| **AVX2 (32 B/step)** | **34.6 ms** | **3 699 MB/s** | **1.72×** |

Byte-identical to the scalar reference (and to SWAR) across every length that
crosses the 8/32/64-byte boundaries, with delimiters/quotes at every offset and
multibyte UTF-8 (`simd_split_backends_match_scalar`); the quote-bail decision is
identical (return value depends only on whether the line contains a `"`).

**Honest scope**: this accelerates *field splitting* only — one of the three
parse costs. Per the profile above, `open` is split **plus** per-field numeric
parse **plus** column build; the latter two are still scalar. The next #71
sub-PRs (vectorized integer/decimal/epoch parse → fused scan→build into the SoA
layout #40) are where the remaining parse throughput is expected. End-to-end
`open` improvement from this step alone is bounded by the scan's share of parse;
measured separately as those land.

---

## SIMD-native parse — SWAR integer parse (#71 step 2, landed)

Second step of #71: a **vectorized-within-register (SWAR)** integer parser
replacing `str::parse::<i64>()` on the two hot lanes — the pass-2 `I64` column
build and the pass-1 `all_int` inference. 8 ASCII digits are converted per step
via pairwise horizontal sums (Lemire), gated by a branch-free 8-digit range
check; the common ≤18-digit case skips std's per-digit `checked_mul`/
`checked_add`. **Exact i64, no f64. Dependency-zero** (pure `u64` arithmetic, no
`core::arch`, no `unsafe`).

**Byte-identical by construction**: `parse_i64_fast` returns `Some(v)` *only*
when `v` is provably what `i64::from_str` yields, and `None` (defer to std) for
every edge — empty, lone sign, any non-digit byte, or ≥19 digits (possible
overflow). Proven against std across exhaustive small ranges, every 1–20-digit
boundary length, signs, overflow at `i64::MIN/MAX±1`, and non-numeric/UTF-8
inputs (`swar_int_parse_matches_std`); the inference `is_ok` decision is
unchanged.

**Micro-bench** (`bench_int_parse`, release, 1024 samples × 4000 reps):

| regime | std `from_str` | SWAR fast | speedup |
|---|---:|---:|---:|
| short / mixed (1–7 digit ids) | 1009 MB/s | 1121 MB/s | **1.11×** |
| wide (16-digit ids) | 933 MB/s | 2013 MB/s | **2.16×** |

**Honest finding**: the win scales with digit width. Typical short CSV ints gain
only ~1.11× (std `from_str` is already tight there, matching the earlier "int
parse measured cheap" note); wide integer keys/epoch-as-int gain 2.16×. No
regression on the common case, real win on the wide one. The remaining #71 lever
is the fused scan→build into the SoA layout (#40), where the parse result is
written contiguously without the per-cell `trim`/dispatch.

---

## SIMD-native parse — SWAR decimal parse (#71 step 3, landed)

Third step of #71, completing the numeric-parse stage (integer → decimal). The
exact decimal lane's magnitude build (`Decimal::parse_scaled`, the per-digit
`checked_mul`/`checked_add` over `i128`) now takes the same **SWAR** 8-digit
fast path for the ≤18-digit case, skipping the per-digit checks (which can never
overflow in that range). The SWAR digit primitives (`is_eight_digits`,
`parse_8_digits`, `accumulate_digits_u64`) moved to a shared
`rivus_core::numparse` module, **deduplicated** with the runtime's `i64` parser.
**Exact i128, no f64. Dependency-zero** (pure `u64` arithmetic, no `core::arch`,
no `unsafe`).

**Byte-identical**: the fast path runs only for ≤18 total digits, where the
magnitude fits `u64` and the scalar checked loop also never overflows — same
unscaled value, same half-even `rescale`. Proven against an independent scalar
checked-loop reference across signs, every int/frac width 0–20 around the
8/18-digit boundaries, dot positions, malformed inputs, and target scales
0/1/2/6/18 (`swar_decimal_parse_matches_scalar`, written first).

**Micro-bench** (`bench_decimal_parse`, release, 1024 samples × 4000 reps):

| regime | scalar checked | SWAR fast | speedup |
|---|---:|---:|---:|
| short (~8-digit) | 215 MB/s | 319 MB/s | **1.49×** |
| wide (16-digit) | 286 MB/s | 562 MB/s | **1.97×** |

**Honest scope**: the decimal lane is opt-in (`--exact` / `:decimal`), so this
helps only those runs; the per-cell `i128` `rescale` (division) still bounds the
gain below the raw digit-loop speedup. Same width gradient as the integer lane
(step 2). With int + decimal done, the remaining #71 lever is the fused
scan→build into the SoA layout — tracked under the larger columnar bet (#40).

---

## Columnar core — branch-free selection-vector build (#40, landed)

First landed step of the columnar bet (#40), aimed squarely at the **measured**
bottleneck of the predicate kernel: the AVX2 compare experiment (#39) found the
compare is memory-bandwidth-bound and the real cost is the **index collection**
(mask → surviving row indices), not the compare. That collection was a branchy
`mask.iter().filter(|m| *m != 0).collect()` — at ~50 % selectivity it mispredicts
roughly every other row.

`kernel::compact_mask` now builds the selection vector **branch-free**: write the
current index unconditionally, advance the write cursor by the mask bit
(`w += (m != 0) as usize`). No data-dependent branch → no mispredictions, so the
cost is flat across selectivity.

**Micro-bench** (`bench_compact_mask`, release, n = 1 000 000, 300 reps):

| selectivity | branchy `filter().collect()` | branch-free | speedup |
|---:|---:|---:|---:|
| 1 % | 220.8 ms | 184.5 ms | 1.20× |
| 25 % | 851.5 ms | 184.4 ms | 4.62× |
| **50 %** | **1.37 s** | **188.1 ms** | **7.31×** |
| 75 % | 901.7 ms | 184.5 ms | 4.89× |
| 99 % | 354.6 ms | 195.1 ms | 1.82× |

The branchy path peaks at 50 % (worst-case misprediction); the branch-free path
is ~185 ms regardless. **Byte-identical**: same surviving indices in the same
ascending order as the branchy reference, across every selectivity and length
incl. loop tails (`compact_mask_matches_branchy`), and the kernel's existing
oracle / `optimizer_equiv` / `stress` suites stay green. Dependency-zero; one
contained `unsafe` (pre-sized write cursor, `w ≤ i < n` invariant documented).

**Scope/honesty**: this speeds the selection-vector *build*, the measured hot
part. The subsequent `Column::gather` (materializing survivors) is the next #40
lever; whether a SIMD/branch-free gather pays is to be measured on the
SIMD-native path (post-#71) before claiming a win.

---

## End-to-end `open` baseline — post-#71, pre-fused-build (#40 next)

The "before" for the #40 fused scan→build, measured **after** the SIMD-native
parse (#71 ×3) landed. Criterion `huge` group, 2 000 000 clean rows ×
6 columns (`id,name,age,score,country,active` → int/str/int/f64/str/bool),
release build, AVX2 host:

| bench | median | throughput |
|---|---:|---:|
| `huge/open_only_2M` (pure parse → SoA build) | **829 ms** | **2.41 Melem/s** |
| `huge/filter_only_2M` (open + `age>=45`) | 865 ms | 2.31 Melem/s |

The filter adds only ~4 % — `open` (line scan + per-field parse + column build)
is the cost, as the 1 GB profile predicted. `open_only_2M` is the clean target
for the fused scan→build: today the reader is **row-major** (per row, split then
push each cell through the `ColBuilder` enum), so the column writes interleave.
The next #40 step buffers a chunk's offsets and builds **column-major** (one
contiguous SoA lane at a time, enum dispatch hoisted out of the inner loop),
measured against this baseline — byte-identical via the `stress` chunk-size
sweep + `optimizer_equiv`.

---

## #40 column-major fused build — measured negative, reverted

Tried the column-major build the forward note proposed: phase 1 buffers each
accepted row's kept-field bytes into one reused `cell_bytes` buffer + a flat span
array; phase 2 fills one SoA lane at a time with the `ColBuilder` enum dispatch
hoisted out of the inner loop. Byte-identical (stress chunk-size sweep,
`optimizer_equiv`, and a `push` vs `extend_cells` pin test all green).

**Result: ~7.8 % slower**, so it was **reverted** (faster is never asserted, and
never *shipped*, without a measured win).

| `huge/open_only_2M` | median | vs baseline |
|---|---:|---:|
| baseline (row-major) | 829 ms | — |
| column-major (this attempt) | 894 ms | **+7.8 % (regress)** |

**Why**: this regime is **parse-bound**, not dispatch- or gather-bound. The
per-cell int/f64/str parse dominates; hoisting the enum dispatch saved nothing
(LLVM already predicts the row-major dispatch well), while buffering the chunk
added a second `memcpy` per line (file→`line`→`cell_bytes`) the streaming
row-major path avoids. This matches #40's original finding and the kernel.rs
note (the AVX2 compare gave no win for the same bandwidth/parse-bound reason).

**Untried variant** (next, if revisited): read each line **directly** into the
chunk buffer (`read_line` appending into `cell_bytes`, truncate-on-reject) to
drop the second copy — column-major at *one* copy. Expected marginal at best
since parsing, not copying/dispatch, is the cost; measure before any further
work. The real remaining lever stays the **selection-vector gather** on a
genuinely gather-bound workload (multi-stage heavy predicates on cached input),
not the parse-bound `open`.

## #81 null model (STEP 2-①) — all-valid `open` regression check

The null model wraps every `Column` as `{ data: ColumnData, validity: Validity }`
with a per-column null bitmap. The promise (design 26 §26.1) is that an
**all-valid** column — the common case — costs *nothing*: `validity = None`, the
dense lane is the former representation byte-for-byte, and (after the lazy-
tracking fix) the reader does no per-cell validity work until a null appears.
This measures that promise on the parse-bound `open` path.

- **Workload**: `gen clean --rows 5_000_000 --seed 7` (171 MB, 6 columns, **no
  nulls**), declared schema `(id:int name:str age:int score:int country:bool…)`,
  flow `open → |? age>=0 → |> id age score → save`. Serial reader
  (`RIVUS_NO_PARALLEL=1`); `open` node `busy_ms` via `--json`. 8 runs each,
  4 vCPU.
- **Before** = `bd9143c` (the move-only test split; old `enum Column`, no null
  model). **After** = the null model + lazy ColBuilder validity.

| `open` busy_ms (5 M rows, all-valid) | min | median | max |
|---|---:|---:|---:|
| before (pre-null-model) | 1098 | 1114 | 1226 |
| after (null model, all-valid path) | 1109 | 1140 | 1204 |

**Result: no measurable regression.** The two ranges overlap heavily (the
before *max* 1226 exceeds the after *max* 1204); on the least-noisy **min** the
delta is **+11 ms (+1.0 %)** — within run-to-run jitter (the bands are ~110 ms
wide). The dense parse loop is unchanged machine code (`match col.data() { … }`
reads the same `&[T]`), and lazy validity tracking means an all-valid column
never allocates or fills a bitmap. Throughput ≈ **155 MB/s** serial, unchanged.

Null-bearing data pays only where it must: a column that actually carries a null
allocates a 1 bit/row bitmap (and the reader back-fills once, at the first null).
That cost is gated behind `has_nulls()` and never touches all-valid columns. The
`Validity::gather`/`append` helpers currently materialize through a `Vec<bool>`
(correctness-first); they are gated by `has_nulls()`, so all-valid data skips
them entirely, and they are a candidate for bit-twiddling once a null-heavy
workload proves the win.

### `sort` comparator hoist (PERF-G) — lane match + null check out of the inner loop

`Sort::finish` compared rows with a `cmp_rows(col, a, b)` that did a `has_nulls()`
check **and** a `match col.data()` lane dispatch on **every** comparison
(~`n·log n` ≈ 20 M times for 1 M rows). PERF-G resolves each sort key's lane and
null state **once** into a monotyped comparator closure (`make_cmp`); the
`idx.sort_by` inner loop then does only the typed compare (and a null branch only
when the column actually has nulls). **Byte-identical** to the old path — same
lane order, NaN→Equal, nulls-last/§26.2b, uri order for resources, and stable
tie-breaking (the existing sort stress/transform tests stay green).

Measured (1 M rows, 23 MB CSV, release, best of 3; **sort-only** = wall − the
0.151 s read+save baseline):

| sort key | before | after | Δ |
|---|---:|---:|---:|
| `id` (int)   | 0.514 s | 0.483 s | −6.0 % |
| `score` (f64)| 0.676 s | 0.632 s | −6.5 % |
| `name` (str) | 0.711 s | 0.650 s | −8.6 % |

The remaining cost is dominated by **cache misses on random row access**
(`v[a]`/`v[b]` into the full column), which the hoist does not change. The next
lever — extracting each key into contiguous `(key, idx)` pairs and sorting those
(cache-coherent, monomorphic, no dyn call) — landed as the decorate-sort below.

### `sort` decorate-sort (PERF-G follow-up) — sort contiguous `(key, idx)` pairs

The hoist removed the per-compare dispatch but the comparator still chased random
rows — `v[idx[a]]`/`v[idx[b]]` into the **full** column on every one of the
~`n·log n` comparisons — so the dominant cost stayed cache misses. The follow-up
**decorates**: for a single key (the common case, and every sort benchmark) it
extracts the key into a contiguous `Vec<(key, idx)>` and sorts *that*. The keys
travel **with** their indices, so the sort reads dense, cache-local key bytes
(and the closure is monomorphic in the lane type — no dyn call). The multi-key
path keeps the hoisted comparator unchanged (a composite decorated key needs a
memcomparable encoding per lane, deferred so byte-identity stays certain there).

**Byte-identical** — same `slice::sort_by` (stable), the same comparator return
values for the same key values, and the same initial `0..n` order, so the
algorithm makes the identical decisions and yields the identical permutation.
Verified by diffing full 1M-row outputs of the pre-/post-follow-up binaries
across every lane (int / f64 / str), the error-heavy (quarantine) and mixed-type
regimes, **and** the unchanged multi-key path — all byte-identical, including the
NaN→Equal inconsistent-order artifact.

Measured (1 M rows, release, **best of 7 interleaved**; `before` = the PERF-G
hoist above; **sort-only** = wall − the read+save baseline, which is identical in
both binaries so the wall delta is purely the sort):

| regime | key | before | after | Δ sort-only |
|---|---|---:|---:|---:|
| large (`clean`)      | `id` (int, pre-sorted) | 0.56 s | 0.55 s | ≈ flat |
| large (`clean`)      | `age` (int, random)    | 0.71 s | 0.66 s | **−7 %** |
| large (`clean`)      | `score` (f64, random)  | 0.91 s | 0.76 s | **−17 %** |
| large (`clean`)      | `name` (str)           | 0.79 s | 0.72 s | **−8 %** |
| error-heavy (0.3)    | `score` (f64)          | 0.60 s | 0.51 s | **−14 %** |
| error-heavy (0.3)    | `name` (str)           | 0.52 s | 0.51 s | −3 % |
| mixed-type (0.2)     | `value` (str fallback) | 0.41 s | 0.38 s | **−8 %** |
| mixed-type (0.2)     | `id` (int, pre-sorted) | 0.24 s | 0.25 s | ≈ flat |

The win tracks the comparison/cache cost: biggest on the random **f64** key
(−14…−17 %), solid on **str** and random **int** (−7…−8 %). The only non-win is a
**pre-sorted integer** column (`id` = `0,1,2,…`): the sort detects the existing
run and does almost no work, so there is nothing for the extraction to amortise —
it lands within noise (±3 %). On this shared container the absolute numbers carry
a few-percent run-to-run jitter; the interleaved before/after cancels the drift.

## Live observation — time-based snapshot sampling (PERF-H)

A live hook (`--tui` / `--serve`) published a `RuntimeSnapshot` every **8 source
chunks** on the serial path, so the snapshot build (`O(nodes)`) + JSON encode +
`Hub` publish rode the hot path at a rate set by **chunk count / throughput** —
unbounded as chunks get smaller or the source gets faster. PERF-H makes the
serial path **time-based** (publish at most every `SNAPSHOT_INTERVAL = 100 ms`,
matching the parallel coordinator's already-time-based `PAR_SAMPLE`), so the
overhead is bounded by wall-clock (≈ `run_secs × 10` snapshots) regardless of
chunk count. (The parallel path already sampled at 100 ms via `ParProgress`, and
a hook never forces the serial path — Observable First.)

Measured live-observation overhead = `--tui` wall − no-hook wall (serial,
`--memory low`, 1 M rows, best of 5; `--tui` isolates the engine cost without the
`--serve` server's ~2 s grace). Amplified with `--chunk-size 64` (~15 625 chunks
→ ~1 953 snapshots before, ~4 after):

| regime (chunk-size 64) | before | after |
|---|---:|---:|
| large (`clean`)      | 12.6 ms | 2.4 ms |
| error-heavy (0.3)    | 4.9 ms  | 1.0 ms |
| mixed-type (0.2)     | 10.0 ms | ≈ 0 ms |
| fan-out (2 sinks)    | ≈ 0 ms  | ≈ 0 ms |

At a normal `--chunk-size 4096` (~244 chunks → ~30 snapshots before) the overhead
is already a few ms and the difference is within noise — **no regression**, the
fix only removes the unbounded tail. byte-identity is unchanged (output is
identical with or without a hook; serial == parallel == chunk-size). The absolute
numbers are small here (a 4-node graph, ~0.36 s run); the cost — and so the
saving — grows with node count (`build_snapshot`/JSON are `O(nodes)`) and with
snapshot frequency, which is exactly what the cap bounds.

## datetime auto-parse — move-to-front AUTO_FORMATS trial (#135)

Real-world datetime is predominantly **non-ISO** (compact `yyMMddHHmmss`,
`yyyyMMdd`, log forms), but `DateTime::AUTO_FORMATS` lists the ISO forms first
and tries them in order (first match wins). So every cell of a non-ISO column
re-paid the failed ISO trials — a constant cost on *every* datetime flow, not a
narrow case. `parse_auto_sticky` remembers the format that matched the previous
cell of the column and tries it first (move-to-front); on a miss it still scans
every format (full fallback). A uniform column parses each cell after the first
in one attempt. The hint lives per-column / per-worker (never shared), so
serial == parallel is preserved.

**Byte-identical.** `AUTO_FORMATS` is mutually disjoint (separators +
full-consumption digit counts → at most one entry matches any input), so
reordering the trial cannot change which format wins. Verified two ways:
the `auto_formats_disjoint` / `parse_auto_sticky_byte_identical` unit pins, and
a before-vs-after `cmp` of the full 1 M-row output on every dataset below —
**all IDENTICAL**.

Full-flow wall (read + parse + save, 1 M rows, best-of-15 interleaved on a shared
container; the interleave cancels drift):

| dataset (1 M rows) | reader `:datetime` | expr `cast` |
|---|---:|---:|
| uniform ISO (`yyyy-MM-ddTHH:mm:ss`)    | ≈ flat (−1 %) | ≈ flat (+0.5 %) |
| **uniform non-ISO (`yyMMddHHmmss`)**   | **−22 %**     | **−16 %**       |
| realistic mixed (non-ISO runs, ~1 % ISO) | −16 %       | −12 %           |
| synthetic 50/50 alternation (worst case) | +5 %        | +9 %            |

The win is broad: any column with format **locality** — uniform (every real
column from a single source) or mostly-uniform with a sparse minority — gets it.
Uniform **ISO** is flat because the baseline already matched on the first trial
(sticky is a no-op there). The only regression is a **synthetic** column that
strictly alternates two datetime formats every row: move-to-front mispredicts on
every cell and pays one extra trial — that is the inherent move-to-front
trade-off, and it is not a shape real datetime columns take (a column comes from
one producer with one format). byte-identity holds in every case.

## partitioned `save` route — buffered → bounded-memory streaming (#143 ③)

The serial partitioned `save` first buffered the **whole** stream and wrote
every partition on `finish`, so peak memory grew with the *data*, not with the
open-file budget — a high-cardinality route blew up RSS. `SinkRoute` now
streams each chunk's rows to their partition files as they arrive through an
LRU pool of open handles (`RIVUS_ROUTE_FD_BUDGET`, default 512), evicting +
reopening (append) under the budget. The bytes per file are unchanged (shared
row formatters + within-partition stream order).

Peak RSS (`VmHWM`), **1 M rows × 20,000 partitions**, CSV template
`save "{k}.csv"`, `--memory low`, debug build:

| | peak RSS | vs buffered |
|---|---:|---:|
| `main` 1acb14c (buffered, finish-write) | 4,638,844 KB | — |
| this PR 298dce5 (streaming, LRU 512)    | **85,572 KB** | **≈ 1/54** |

**Byte-identical**: all 20,000 output files md5-match between the two builds,
and the eviction stress (`RIVUS_ROUTE_FD_BUDGET=1`, `chunk_size=1`, csv/jsonl/
json) pins each file equal to the default large-budget run. Peak memory is now
bounded by `min(distinct partitions, budget)` open writers + one input chunk,
not the stream length. (The parallel-merge path still buffers its already-merged
chunks; streaming it + spill is the remaining engineering, HANDOVER.)

## partitioned `save` route — parallel merge streams through the same writer (#143 ③, part 2)

The **parallel-merge** route write (`write_sink`, all three callers: the
chunk-partition merge, the single-partition flush, the group-by finalize) still
used the buffered one-shot form: `group_by_path` over the *whole* merged stream
gathered a second full copy of the output — for sequential keys that copy is
~one single-row sub-chunk *per row* (meta + schema arc + column headers each),
so peak RSS exploded exactly like the pre-streaming serial sink. It now streams
the merged chunks **chunk-wise through the same `RouteWriter`** the serial
operator uses (shared formatters, same within-partition stream order — bytes
unchanged by construction; the buffered form is kept as the `#[cfg(test)]`
oracle and the unit pins streamed ≡ buffered per codec, eviction included).

Peak RSS (`VmHWM`), 1 M rows via `ls | read` (the in-memory collector path =
the parallel merge), `save "{k}.…"` template, debug build:

| scenario | buffered merge (before) | streamed merge (after) | files |
|---|---:|---:|---:|
| CSV, 20 k partitions  | 4,561,952 KB / 46.1 s | **148,884 KB (≈ 1/31)** / 47.7 s (≈ flat) | md5 ≡ |
| JSON, 20 k partitions | 4,561,320 KB / 18.4 s | **148,048 KB (≈ 1/31)** / 53.9 s (× 2.9)  | md5 ≡ |
| JSON, 3 k partitions (budget 512)    | 3,321,520 KB / 11.8 s | **117,412 KB (≈ 1/28)** / 35.3 s (× 3.0) | md5 ≡ |
| JSON, 3 k partitions (budget 3,500)  | — | **99,536 KB (≈ 1/33)** / 13.2 s (+ 12 %) | md5 ≡ |

Every scenario's output files md5-match before vs after. The **wall trade is
the bounded-fd streaming trade** (#143 ③, same as the serial sink): with
distinct partitions ≫ `RIVUS_ROUTE_FD_BUDGET` and cyclic keys (LRU's worst
case) every write pays an evict + reopen, which the buffered one-open-per-
partition JSON write never did — ×3 wall on that synthetic worst case. Raising
the budget to ≥ the cardinality (within the fd ulimit; 3 k partitions, budget
3,500 above) removes the churn: wall lands within ~12 % of buffered while
keeping the ~1/30 RSS. CSV is ≈ flat even under worst-case churn (its buffered
write paid comparable per-partition costs). Robustness pin: a budget *over*
the ulimit (20 k partitions, budget 25 k, ulimit 4,096) fails per partition
with EMFILE, **aggregated into one surfaced critical event (one entry per
partition, never silent)** while the in-budget partitions still write —
continue-first holds.

Remaining engineering (HANDOVER): the merge path still *holds* the collected
worker outputs themselves; spilling those to disk (or per-worker part files for
routes) is the next, separate step.

## computed-projection pushdown — bare Filter / ProjectExpr consumers (#189, landed)

Before #220, a single computed column in the projection (`|> a (b*2) as d` →
`ProjectExpr`) disabled **every** pushdown: no `FilterProject` fusion arm →
all three source pushdowns (numeric/string prefilter, discovery-name
projection) recognized only the `FilterProject` consumer → `rivus explain`
showed `(no transformations applied)` on virtually every real ETL flow. #220
generalizes the consumers (bare `Filter` is additive so always safe;
`ProjectExpr` emits only its items so its referenced-column set is the live
set; `$_[i]` positional references keep pruning off as before).

Measured (research session, #220; release build, Linux container — relative
comparison, not cross-machine absolute): 1 M rows × 6 columns,
`|? age>=58 |> name (age*2) as d` with a file `save`, min of 5 runs:

| scenario | wall |
|---|---:|
| before (computed projection = no optimization; verified ≡ `--no-opt`) | 0.148 s |
| **after (prefilter + projection pushdown both fire)** | **0.094 s = 1.57×** |
| reference: pure projection (already optimized before #220) | 0.092 s |

The computed-column penalty is gone (within noise of the pure-projection
form). Correctness gate: `opt == --no-opt` output is **byte-identical**
(355,570-row diff match), pinned by
`optimizer_equiv::computed_projection_pushdown_is_equivalent` across three
shapes plus the `$_[i]` guard test.

Remaining (recorded in #189): pushdown through `rename`/`drop` (needs a
column-name reverse map) and a `(Filter, ProjectExpr)` fusion arm.

## sink numeric formatting — LUT itoa + exact short-decimal f64 fast path (numfmt)

The 1 GB profile puts `save` second after parse (6,897 busy_ms vs 12,591), and
on the default *parallel* path it is the Amdahl dominator: `open` splits across
workers (~1.0 s on 5 M rows) while the sink formats serially (~1.6 s). Lane
isolation showed `std::fmt::Display` for **f64 as the single hottest lane** —
one f64 column cost more than two i64 columns (grisu shortest-repr + `fmt`
machinery per cell):

| 5 M-row `save` (release, min/avg of 3) | before | after | speedup |
|---|---:|---:|---:|
| f64 column ×1 (`\|> score`) | 625 ms | **296 ms** | **2.11×** |
| i64 columns ×2 (`\|> id age`) | 405 ms | **234 ms** | **1.73×** |
| full 6-column row | 1,611 ms | **893 ms** | **1.80×** |

What landed (`rivus_core::numfmt`, std-only, dep-zero; wired into the CSV and
JSONL cell writers plus a `writeln!`→`write_all` row emit):

- **i64/u64**: two-digits-per-step LUT (itoa-style) — trivially byte-identical.
- **f64**: an *exact short fixed-decimal* fast path. Rust's `Display` prints
  the shortest round-trip decimal positionally (never e-notation — probed and
  pinned in tests). For |v| ≤ 2^53 we search the smallest fraction width `k`
  whose nearest candidate `m = round(v·10^k)` round-trips; because `m` and
  `10^k` are exactly representable, `(m as f64)/(10^k as f64) == v` is an
  **exact** decimal→binary round-trip test (correctly-rounded division), not a
  heuristic. Ambiguity (a same-width neighbor also round-trips, a trailing
  zero from a misrounded product, |v| > 2^53, non-finite) → **refuse and fall
  back to `std::fmt`** — the fast path never guesses, so byte-identity is
  constructive.

**Byte-identity, measured directly**: main binary vs this branch on the same
inputs — 5 M-row clean CSV out (179,246,534 B) `cmp`-identical; JSONL out
(444,246,501 B) identical; a 300 k **random-bit-pattern double** torture file
(long mantissas, ±0, 2^53±1, subnormals, 1e±308) identical. Property tests pin
fast==std over structured grids and 2 M random bit patterns, and assert the
short-decimal acceptance rate stays >98 % (the fast path must actually cover
the data-file shape, not silently degrade to the fallback).

Remaining recorded ideas: a full shortest-repr formatter (Ryū/Dragonbox port)
for the long-mantissa tail (random doubles fall back to std today), and the
same LUT for `Decimal`/date/time digit emission.

### Phase 2 — temporal/decimal lanes (the post-f64 heaviest cells)

Lane isolation after phase 1 showed the **datetime lane as the new heaviest
cell by far** — the default `Display` went through the `format("yyyy-MM-ddTHH:
mm:ss")` *template interpreter* (a String alloc + pattern scan per cell):

| 5 M-row `save`, one column (release, min of 3) | before | after | speedup |
|---|---:|---:|---:|
| datetime (`yyyy-MM-ddTHH:mm:ss`) | 1,045 ms | **292 ms** | **3.58×** |
| date (`yyyy-MM-dd`) | 435 ms | **152 ms** | **2.86×** |
| decimal(2) | 328 ms | **190 ms** | **1.73×** |
| i64 (phase-1 reference) | 113 ms | 131 ms | (noise) |

What landed: `numfmt` gained pair-LUT component writers (`push_date_ymd`,
`push_hms`, `push_frac`, `push_decimal`, `push_u128`); `DateTime`/`Date`/
`TimeOfDay`/`Decimal` **`Display` now delegates to them** (one implementation —
group keys and `to_string()` callers get the same speedup), and the CSV/JSONL
cell writers push digits directly, skipping `fmt` entirely on the hot path.
Out-of-common-era years (outside `0..=9999`) and oversized components refuse
and fall back to the canonical `{y:04}`-style rendering. The JSONL datetime/
date/time fast path quotes directly (the rendering is digits + `-T:.`, none of
which JSON escapes); the fallback keeps the escaping `json_string`.

**Byte-identity, measured directly**: phase-1 binary (old `Display` forms) vs
this one — 5 M-row datetime/date/decimal CSV (228,338,905 B) `cmp`-identical;
a 200 k-row torture file (years 1600–9999 incl. pre-1970 negative ticks,
negative decimals, `-0.001`) identical as CSV **and** JSONL. Property tests
re-implement the pre-LUT `write!` forms as oracles (200 k random
`i128×scale` decimals, boundary dates/times) and pin the refusal cases.

### Buffering-operator performance — the O(n²) validity bug + per-row key/alloc pathologies

Following the `ls…read` external comparison (above), a fully **contract-pinned**
end-to-end task (3M rows, 3 dirty CSVs — a malformed-row file + a file missing a
column — union-by-name, a dimension left join, coalesce, group, save; integer
cents so sums are bit-exact across engines) measured Rivus **23× slower than
DuckDB**. Profiling (per-node `--json` telemetry) localized it to specific
**implementation defects**, not "interpreter overhead." All fixes are
byte-identical (default 466/0, all-features 499/0; the pinned task's 16-row
output is unchanged bit-for-bit at every step).

**The headline bug — `Validity::append` was O(n²).** Every buffering operator
(join / sort / group / unbounded-merge) concatenates its buffered chunks. When
any column carried a null, `Validity::append` materialized the *entire*
accumulated bitmap into a `Vec<bool>` and repacked it **on every append** — so
concatenating N chunks was O(N²). A 735-chunk left join spent **5.3s** in
`concat` alone. Fixed to a word-granular in-place append: **5294ms → 117ms
(45×)**. This one fix helps every buffering op, not just join.

**Per-row allocation pathologies** (same shape in three places): a composite key
built as a fresh heap `String` (+ boxed `Value`) per row.
- **join probe**: `String` per left row (3M allocations for a 20-row dimension).
  Reused key buffer + borrowed `&str` key parts.
- **group**: `String` key *and* a throwaway `Vec<String>` of rendered parts per
  row (only the 16 *new* groups need the parts). Reused buffer + parts-on-insert:
  **1154ms → 240ms**.
- **coalesce** (`|>` project): per-cell `Value`/`String` round-trip. Columnar
  all-`Str` fast path borrowing the winning column's `&str`: **1057ms → 103ms**.
- **`cast_column`**: no identity fast-path — a same-lane cast rebuilt every cell
  (union-by-name reconciliation). `if col.dtype() == ty { return col }`.

**Result (best-of-5, 3M rows, same pinned contract, byte-identical output):**

| stage | rivus wall |
|---|---:|
| baseline | 8067 ms |
| + `Validity::append` O(n²) fix | 5294 ms |
| + group key reuse / parts-on-insert | 3769 ms |
| + `cast_column` identity fast-path | 3145 ms |
| + coalesce columnar fast-path | 2192 ms |
| + `read` typed single decode pass (parallel-inference plan) | **1501 ms** |
| **DuckDB 1.5 / Polars 1.42** | **345 / 532 ms** |

Rivus went from **23× → 4.4×** DuckDB (2.8× Polars) on this workload with no loss
of the continue-first / never-silent / union-by-name contract (which Polars could
not meet natively — it kept the ragged rows).

**`read` typed single decode pass.** The remaining peak was `read` (1090ms): the
old path (`CsvChunker::open`) paid a full serial inference scan THEN a full
decode scan per file. `read` now reuses the parallel-source machinery —
`plan_parallel` infers over newline-aligned byte ranges in parallel and
`for_range` decodes each range in file order with types known (one typed pass).
The inferred schema is pinned byte-identical to the serial reader's by the
engine's serial==parallel invariant; ranges are contiguous, so row order — and
the output, including the malformed-row count — is unchanged. `read`
1090 → 631ms. Unseekable inputs fall back to the buffered serial reader.

Remaining levers: the execution pipeline itself is still single-threaded
(parallelism is safe here — integer sum is associative — a further multiplier),
and join/group still render composite keys as text rather than hashing typed
lanes directly.

### The 10M × ⌈2.2·cores⌉-file standard fixture (統括指示) — and what it exposed

New mandatory perf fixture (see CLAUDE.md): **10M rows CSV and 10M JSON objects
JSONL, split across ⌈2.2 × physical cores⌉ files** (this box: 4 physical cores →
9 files; 181MB CSV / 606MB JSONL), dirty data in the mix (5 ragged/truncated
lines + 5 non-numeric amounts in file 1; file 9 missing the `category` column;
unknown regions in file 5). Same pinned contract as above; **rivus CSV ==
rivus JSONL == DuckDB CSV == DuckDB JSONL, all row-identical** (cross-format AND
cross-engine). Polars: CSV differs by exactly the 5 ragged rows it cannot
exclude; JSONL is a hard **DNF** (`read_ndjson(ignore_errors=true)` still aborts
on a truncated JSON line — the ragged-CSV story again).

A single-file test had hidden two structural problems the fan-out exposes:

1. **Peak RSS ~1.4GB on a 181MB dataset** — `read` buffers every decoded file
   and the blocking join buffers both sides again; DuckDB streams the same job
   in 258MB. The "bounded memory" claim currently holds for straight-through
   flows, not for `read`+blocking pipelines. Structural fix (streaming read /
   stream-probe join) tracked as the memory lever.
2. **JSONL `read` was 3× the CSV path** (16.9s vs 5.6s): `JsonlChunker::open`
   still paid serial full-file inference + a second decode parse.

**Landed now — parallel range decode inside `read` (both formats).** The range
plans (`csv::plan_parallel` / `jsonl::plan_parallel`) already exist for the
parallel source; `read` now decodes those newline-aligned ranges on scoped
threads and splices in range order (contiguous ranges ⇒ row order unchanged ⇒
byte-identical; verified: 10M CSV, 10M JSONL, and the 3M fixture all
bit-identical, error counts included).

| 10M × 9 files, warm best-of-3 | wall | peak RSS |
|---|---:|---:|
| rivus CSV (before → after) | 5559 → **4770 ms** | 1459 → 1169 MB |
| rivus JSONL (before → after) | 16901 → **11223 ms** | 1302 → 1155 MB |
| DuckDB CSV / JSONL | 920 / 1461 ms | 240 / 404 MB |
| Polars CSV | 1501 ms | 1974 MB |
| Polars JSONL | DNF (malformed line) | — |

Per-node at 10M (CSV): read 1285 / join 1373 / group 760 / cast+filter+project
~1180 — no single villain left; the gap is now the **serial sum of nodes**
(DuckDB runs the whole pipeline on 4 cores). JSONL: read 7926ms — the serial
`infer_global` (a full JSON parse of every line) plus the decode re-parse
dominated.

**Landed next — parallel JSONL inference.** `jsonl::plan_parallel` now infers
each newline-aligned range on its own thread and folds the results in range
order. The fold reproduces the sequential scan exactly: global column order =
the earliest started range's first valid object (= the file's first valid
object); each key's `Infer` merges commutatively (scalar flags AND/OR; a
struct's child order adopts the earliest range's first object; lists merge
recursively); keys outside the global set are discarded; malformed counts sum.
Verified: 10M JSONL output row-identical to DuckDB, malformed counts intact.
JSONL `read` 7926 → **4257ms**; JSONL pipeline 11223 → **7684ms** (from 16.9s at
the start of the catch-up = 2.2×).

The engine-level levers stay: pipeline parallelism (integer sums here are
associative ⇒ safe) and the buffering-memory ceiling.
