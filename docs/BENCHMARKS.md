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
