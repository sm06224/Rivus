# Rivus Revision Plan (改修計画)

A living, prioritized backlog. Each item has a **status** — ✅ done · 🚧 in
progress · 📋 planned — and a short design note so work can be picked up
incrementally. Driven by the project philosophy: *Stream correctness >
Zero-copy > Backpressure > Composability > Optimization visibility > Raw speed*,
and a **zero-dependency default build** — heavy/standard formats (compression,
Parquet, pickle) are allowed as **vetted, feature-gated, opt-in** adapters per
[`SUPPLY-CHAIN.md`](SUPPLY-CHAIN.md), so the core stays dependency-free.

The headline target is to **beat DuckDB for everyday data wrangling** — already
true for streaming filter/project ETL (Rivus ~1.45× faster at ~40× less memory,
see [`BENCHMARKS.md`](BENCHMARKS.md)) — and to keep extending the language and
formats until reaching for DuckDB/pandas is unnecessary.

---

## A. Ingestion & formats

| | item | note |
|---|---|---|
| ✅ | Streaming CSV (bounded memory) | `CsvChunker`, two-pass global inference |
| ✅ | Streaming + parallel CSV | byte-range workers, ordered part-file concat |
| ✅ | JSON / JSON Lines / NDJSON, fixed-width binary | |
| ✅ | **Header-less CSV** | `open f.csv noheader` → columns `c0,c1,…`; first line is data |
| ✅ | **Typed / named columns at `open`** | `open f.csv (id:int, name:str, age:int)` — give a schema instead of inferring; also names a header-less file |
| 🚧 | **Compressed inputs** | **`.gz` ✅** (feature `gzip`, `flate2`/`miniz_oxide`) and **`.zst` ✅** (feature `zstd`, pure-Rust `ruzstd` decoder) done — serial single-pass with sample inference (compressed streams can't seek → no byte-range parallel); default build stays dep-free. Next: `.zip`/tar. Vetting log in `SUPPLY-CHAIN.md`. |
| ✅ | **TSV / custom delimiter** (real) | `delim: u8` threaded through `OpenCsv`/`SinkCsv` (std-only). `.tsv`/`.tab` paths split on a tab automatically; `as tsv`/`as csv` overrides the extension. Reader, parallel reader, and sinks all honor it; `to_source` stays faithful. |
| 📋 | **Parquet / Arrow** | feature `parquet` via apache **`arrow`/`parquet`** (isolated behind the source/sink trait) |
| 📋 | **Python pickle**, YAML/TOML/INI/XML/HTML | `pickle` via `serde-pickle`; text formats likely std-only or a small vetted dep |
| 📋 | Transports: socket / HTTP / subscribe / scheduled-get | `docs/design/18` |

## B. Pipe / CLI ergonomics

| | item | note |
|---|---|---|
| ✅ | Inline `-c`, stdin heredoc, `open stdin` / `save stdout` | |
| ✅ | stdout = clean data, stderr = visualization | pipe-friendly today |
| ✅ | **First-class stdin→process→stdout** | make `cat x.csv \| rivus '<transforms>'` ergonomic: a default source (stdin) and sink (stdout) so a bare transform chain works as a Unix filter |
| ✅ | `-` sentinel for `open`/`save` | `open -` / `save -` map to stdin/stdout (alongside `stdin`/`stdout`) |
| ✅ | **`describe`** | `rivus describe <source>` / a `describe` verb: per-column type, count, nulls, min/max/mean — a streaming one-pass summary (pandas `.describe()` / SQL `DESCRIBE`) |

## C. Language: a more readable, typed flow syntax

This is a coordinated design (it touches the lexer, parser, IR and eval); land
it in small, gated steps.

| | item | note |
|---|---|---|
| ✅ | Computed columns `\|> (age*12) as months` (add-property style) | arithmetic `+ - * / %`, `as` alias |
| ✅ | **Readable filter** | `\|?` is terse; add a comma-separated form where `,` means AND, e.g. `where age >= 20, country == "JP"`. Keep `\|?` as an alias. |
| ✅ | **Inline type casts** | `age:int`, `price:f64`, `flag:bool`, `id:str` usable in predicates and projections, e.g. `where age:int >= 20` and `\|> (amount:f64 * 1.1) as gross` |
| 📋 | **Three ways to give types** (write them distinctly): | |
| ✅ | • at the source | `open f.csv (id:int name:str)` — declared schema (done) |
| ✅ | • mid-flow cast | `\|> (age:int) as age` — cast via a computed column (a `cast` verb is sugar, 1.x) |
| ✅ | • derive/add property | `\|> (expr) as name` computed columns (done) |
| ✅ | String functions, `case when … then … else` | `upper/lower/trim/len/substr/contains` ✅ and `case when … then … [else …] end` ✅ done |

## D. Relational & cleaning operators

| | item | note |
|---|---|---|
| ✅ | filter · project · group(sum/avg/min/max/count) · sort · distinct · take | |
| ✅ | **Joins (hash join)** | `A & B on k` **inner**, `A &left B`, `A &right B`, `A &full B` all done (outer joins pad the missing side with type defaults and preserve the join key; build side buffered, a pipeline-breaker like sort). |
| ✅ | **Missing-value imputation** (欠測補完) | `dropna [cols]` ✅, `fill col VALUE` ✅, `fill col ffill\|bfill` ✅ (directional carry across chunks), **`fill col mean\|median`** ✅ (whole-column statistic over the non-empty numeric cells). All chunk-size independent; bfill/mean/median are pipeline-breakers. Declare a column `:str` so its blanks survive parsing (a numeric column's blank becomes 0 at parse time). |
| ✅ | More aggregates | `std` (sample), `count_distinct`/`nunique`, `first`, `last`, `median`/`pNN` percentiles (linear interp) all done |
| 🚧 | `rename`, `drop`, `reorder` columns | `rename OLD NEW …` ✅ and `drop COL …` ✅ done (stateless, parallel-safe, reversible); `reorder` via `\|>` today |

## E. Performance — keep beating DuckDB

The wall (see [`BENCHMARKS.md`](BENCHMARKS.md) "high wall"): on stdout queries
over 5 M rows DuckDB lands ~0.33 s on *every* shape (regex, IN-set, numeric)
while Rivus is 2–3 s. The gap is the **CSV read path** (serial, two-pass
streaming inference), not the predicate engine. So the top perf levers now are
read-throughput, in priority order:

| | item | note |
|---|---|---|
| ✅ | Optimizer: dedup · fuse · projection pushdown · **filter pushdown** | |
| ✅ | Allocation-free field split, 256 KiB IO buffers | |
| ✅ | **Parallel reads incl. stdout sinks** | `save -` now assembles ordered parts to stdout; 363 MiB filter 5.2 s → 1.8 s (2.8×). Env knobs `RIVUS_PARALLEL_MIN_BYTES` / `RIVUS_NO_PARALLEL` |
| 📋 | **Auto-tune / lower the parallel threshold** | 256 MiB is conservative; mid-size files (e.g. 171 MiB) still run serial — measure the crossover and lower it |
| 📋 | **Single-pass inference** (sample + adaptive widen) | drop the second full scan that streaming type-inference costs |
| 📋 | **SIMD CSV scan** (`std::arch`, no deps) | find `,`/`\n` with SSE2/AVX2; bench-gated (SWAR tried, no win at current bottleneck — revisit after the above) |
| 📋 | **Vectorized / SIMD predicate kernels** for more shapes | extend `kernel.rs` beyond numeric conjunctions |
| 📋 | Push computed-column / string predicates into the reader | extend prefilter |
| 📋 | mmap the source; overlap decode with IO | |
| 📋 | Re-use buffers across chunks; arena-per-chunk recycling | |
| 📋 | JIT (Cranelift) for hot predicates/projections | design doc 09; needs a vetted dep |

## F. Observability & UX

| | item | note |
|---|---|---|
| ✅ | Live progress, execution-graph viz, error stream | |
| 📋 | Structured telemetry stream (JSONL on stderr/socket) | design doc 19 — base for editor/GUI |
| 📋 | `\| view` interactive grid (Out-GridView), live analytics GUI | design doc 19; streaming, never full-materialize |
| 📋 | Shell completion from IR/schema; nushell value interop | design doc 19 |

---

## Near-term order (how we eat the elephant)

1. ~~Header-less CSV (A)~~ ✅ done — `open f.csv noheader`.
2. ~~`describe` (B)~~ ✅ done — `open f.csv describe`.
3. ~~Typed/named columns at `open`~~ ✅ done — `open f.csv (id:int name:str)`.
4. ~~stdin→stdout filter ergonomics~~ ✅ done — `cat x | rivus '|? …'`.
5. ~~Inline type casts + comma filter~~ ✅ done (`age:int`, `where a, b`).
6. ~~Joins~~ ✅ inner + left hash join done; ~~imputation~~ ✅ `dropna`/`fill
   VALUE|ffill|bfill` done (D).
7. ~~Compressed inputs `.gz` / `.zst`~~ ✅ done — features `gzip` (`flate2`) and
   `zstd` (pure-Rust `ruzstd`), serial single-pass; default build stays dep-free.
8. **SIMD CSV scan** (E) — the next big speed lever vs DuckDB.

Each lands as a small commit on the single PR, gated locally (fmt · clippy ·
test · gitleaks · cargo-deny) and, for optimizations, with a before/after number
in `BENCHMARKS.md` and the equivalence oracle kept green.
