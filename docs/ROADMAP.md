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
| 📋 | **Typed / named columns at `open`** | `open f.csv (id:int, name:str, age:int)` — give a schema instead of inferring; also names a header-less file |
| 📋 | **Compressed inputs** (`.gz` first) | feature `gzip` via **`flate2`** (pure-Rust backend), serial single-pass (compressed streams can't seek → no byte-range parallel); then `.zst` (`ruzstd`), `.zip`/tar. Vetting log in `SUPPLY-CHAIN.md`. |
| 📋 | TSV / custom delimiter (real) | `as tsv` currently aliases CSV; add a `delim` to `OpenCsv`/`SinkCsv` (std-only) |
| 📋 | **Parquet / Arrow** | feature `parquet` via apache **`arrow`/`parquet`** (isolated behind the source/sink trait) |
| 📋 | **Python pickle**, YAML/TOML/INI/XML/HTML | `pickle` via `serde-pickle`; text formats likely std-only or a small vetted dep |
| 📋 | Transports: socket / HTTP / subscribe / scheduled-get | `docs/design/18` |

## B. Pipe / CLI ergonomics

| | item | note |
|---|---|---|
| ✅ | Inline `-c`, stdin heredoc, `open stdin` / `save stdout` | |
| ✅ | stdout = clean data, stderr = visualization | pipe-friendly today |
| 🚧 | **First-class stdin→process→stdout** | make `cat x.csv \| rivus '<transforms>'` ergonomic: a default source (stdin) and sink (stdout) so a bare transform chain works as a Unix filter |
| 📋 | `-` sentinel for `open`/`save` | the bare dash isn't lexable yet (only `stdin`/`stdout`) |
| 📋 | **`describe`** | `rivus describe <source>` / a `describe` verb: per-column type, count, nulls, min/max/mean — a streaming one-pass summary (pandas `.describe()` / SQL `DESCRIBE`) |

## C. Language: a more readable, typed flow syntax

This is a coordinated design (it touches the lexer, parser, IR and eval); land
it in small, gated steps.

| | item | note |
|---|---|---|
| ✅ | Computed columns `\|> (age*12) as months` (add-property style) | arithmetic `+ - * / %`, `as` alias |
| 📋 | **Readable filter** | `\|?` is terse; add a comma-separated form where `,` means AND, e.g. `where age >= 20, country == "JP"`. Keep `\|?` as an alias. |
| 📋 | **Inline type casts** | `age:int`, `price:f64`, `flag:bool`, `id:str` usable in predicates and projections, e.g. `where age:int >= 20` and `\|> (amount:f64 * 1.1) as gross` |
| 📋 | **Three ways to give types** (write them distinctly): | |
| | • at the source | `open f.csv (id:int, name:str)` — declared schema (§A) |
| | • mid-flow cast | `cast age:int score:f64` — change a column's lane |
| | • derive/add property | `\|> (age:int) as age2` or a `let age2 = …` form |
| 📋 | String functions, `case when … then … else` | `upper/lower/len/substr/contains` (design doc 20 “その後”) |

## D. Relational & cleaning operators

| | item | note |
|---|---|---|
| ✅ | filter · project · group(sum/avg/min/max/count) · sort · distinct · take | |
| 📋 | **Joins (real execution)** | hash join: buffer the build side, probe the stream. `A & B on k`. Inner first, then left/right/outer. Memory: build side bounded by its cardinality (document it as a pipeline-breaker like sort). |
| 📋 | **Missing-value imputation** (欠測補完) | `fill col with <value>` · `fill col mean\|median\|ffill\|bfill` · `dropna [cols]`. Mean/median need a stat pass (or streaming approx); ffill/bfill are streaming with carried state. |
| 📋 | More aggregates | `count_distinct`, `std`, `p50/p90`, first/last |
| 📋 | `rename`, `drop`, `reorder` columns | sugar over project |

## E. Performance — keep beating DuckDB

| | item | note |
|---|---|---|
| ✅ | Optimizer: dedup · fuse · projection pushdown · **filter pushdown** | |
| ✅ | Allocation-free field split, 256 KiB IO buffers | |
| 📋 | **SIMD CSV scan** (`std::arch`, no deps) | find `,`/`\n` with SSE2/AVX2; bench-gated |
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
2. **`describe`** (B) — high-value exploration, one streaming pass.
3. **Typed/named columns at `open`** (A/C) — declared schema; foundation for casts.
4. **stdin→stdout filter ergonomics** (B).
5. **Inline type casts + comma filter** (C) — readable, typed flow.
6. **Joins** (D), then **imputation** (D).
7. **SIMD CSV scan** (E) — the next big speed lever vs DuckDB.
8. **Compressed inputs** (A) — after the supply-chain decision.

Each lands as a small commit on the single PR, gated locally (fmt · clippy ·
test · gitleaks · cargo-deny) and, for optimizations, with a before/after number
in `BENCHMARKS.md` and the equivalence oracle kept green.
