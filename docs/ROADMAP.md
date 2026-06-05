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
| 📋 | **BOM / encoding handling** | strip a leading UTF-8 BOM (`EF BB BF`) so the first header cell isn't `﻿id`; detect UTF-16 LE/BE BOM and decode (or warn + continue). Today a BOM leaks into the first column name. std-only. Connects to design doc 06 §6.4 "text is stream" (encoding-aware decode) |
| 📋 | **Exact decimal lane at the reader** (design doc 21) | `open f.csv (price:decimal[(n)])` / `--exact[=auto\|N]`: parse into `Column::Dec` (i128 scaled int, **landed in core**). Scale auto-inferred (max fractional digits, 2-pass) or explicit. Unblocks byte-identical parallel decimal aggregation (#41) and exact money math |
| 📋 | **Datetime lane at the reader** (design doc 23) | `open f.csv (ts:datetime["yyMMddhhmmss"])` / `--dates`: epoch-integer parse, std-only strptime; bad values warn + continue |
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
| ✅ | **Three ways to give types** (written distinctly): | all done |
| ✅ | • at the source | `open f.csv (id:int name:str)` — declared schema |
| ✅ | • mid-flow cast | `\|> (age:int) as age` (computed column) **and** the `cast age:int price:f64` verb (re-types columns in place) |
| ✅ | • derive/add property | `\|> (expr) as name` computed columns (done) |
| ✅ | String / numeric functions, `case when … then … else` | `upper/lower/trim/len/substr/contains/replace/split_part/concat`, `starts_with/ends_with/like/glob/regexp`, numeric `abs/round/floor/ceil`, null-coalesce `coalesce`, and `case when … then … [else …] end` all done |
| 📋 | **Optional leading pipe before any stage** | allow (don't require) a `\|` before stages that today have none — `\| sort score`, `\| save out.csv`, `\| group …`. Makes every stage read as a pipe step; bare form still valid. Lexer/parser: treat a stage-leading `\|` as optional whitespace. (back-compat not required per 統括) |
| 📋 | **Flow prefix for label references** | a sigil so a stage that consumes a named upstream flow is syntactically obvious (today a bare `Adults` could be a label or a column). Proposed `@Label` (or `->Label`) for "inherit/continue this flow", e.g. `Merged: @Adults + @Minors`. Touches lexer/parser/`to_source`; reversible. (back-compat not required) |
| 📋 | **Combine derive + cast + rename in one block** | let a single projection stage create columns, cast types, and rename together, e.g. `\|> (price:f64 * qty) as total, age:int as years, name`. Today these split across `\|>` (computed), `cast` (re-type in place) and `rename` (separate verb) — unify them in one `\|>`/`select`-style block so a wrangle reads as one step. Touches parser (mixed projection items: derive\|cast\|rename\|passthrough) + `to_source`; reversible. |
| 📋 | **`is null` / `is not null` predicate + `null` literal** (explicit selection of missing rows) | §25 syntax v2 (design doc 25/§26.0). After the #81 null model lands. The null model #81 already lets you **drop / exclude / impute / detect** missing values (`dropna`, comparisons, `fill`, `coalesce`); this adds the missing piece — *selecting* missing rows explicitly, e.g. `\|? x is null`, `\|? x is not null`, plus a `null` literal. **Design it in Rivus's flow vocabulary** (consistent with existing predicates and `dropna`), not a bare SQL `WHERE x IS NULL` transcription — Rivus's strength is "SQL-equivalent **and** flow-native". Touches lexer/parser/`to_source` (reversible) + eval (validity-aware predicate). |

## D. Relational & cleaning operators

| | item | note |
|---|---|---|
| ✅ | filter · project · group(sum/avg/min/max/count, **multi-key**) · **multi-key sort** · distinct · take | `\|# country region sum:score`; `sort team score desc` (per-key direction) |
| ✅ | **Joins (hash join)** | `A & B on k` **inner**, `A &left B`, `A &right B`, `A &full B`, plus **composite keys** `on k1 k2 …` (join on the column tuple) all done (outer joins pad the missing side with type defaults and preserve the join keys; build side buffered, a pipeline-breaker like sort). |
| 📋 | **Join null-key semantics (§26.2a)** | a `null` join key must **not match** anything (SQL `NULL`-join semantics): an unmatched-by-null row drops on inner join, pads with null on left/right/full. Today the hash key uses the rendered cell, so null keys coalesce and **match** — the inverse of §26.2a (known gap, out of the STEP 2-② operator scope). Fix: make `join_key_at` yield 'no match' for any null key part (skip the build-side insert, never probe-match). Tracked from null model #81. |
| ✅ | **Missing-value imputation** (欠測補完) | `dropna [cols]` ✅, `fill col VALUE` ✅, `fill col ffill\|bfill` ✅ (directional carry across chunks), **`fill col mean\|median`** ✅ (whole-column statistic over the non-empty numeric cells). All chunk-size independent; bfill/mean/median are pipeline-breakers. **Null model (#81): STEP 2 complete (2-①〜⑤).** The reader reads a blank/unparseable cell — **numeric lanes included** — as a first-class `null` (no longer `0`); arithmetic propagates null; aggregations skip it (incl. COUNT(\*) vs `count:col`, non-null first/last/distinct); filter/`dropna`/`fill`/`cast`/`sort` are null-aware (BUG-A fixed — `dropna_drops_blank_numeric_rows_bug_a` green); group-by/distinct fold null keys; sinks round-trip `null`/`""`/`0` distinctly (§26.5); and serial == parallel == chunk-size holds on null-bearing data through the merge path (§26.4). Remaining as separate items: join null-key non-match (§26.2a, tracked below) and the `is null`/`is not null` predicate (§25 syntax v2). |
| ✅ | More aggregates | `std` (sample), `count_distinct`/`nunique`, `first`, `last`, `median`/`pNN` percentiles (linear interp) all done |
| ✅ | `rename`, `drop`, `reorder` columns | `rename OLD NEW …`, `drop COL …`, and `reorder COL …` (move named columns to the front, rest follow in order) all done — stateless, parallel-safe, reversible |
| 📋 | **Datetime lane** (`yyMMddhhmmss` etc.) | design doc 23. `(ts:datetime["fmt"])` / `--dates`; epoch-integer (scaled, like decimal) → exact compare/diff, associative → parallel-safe. `trunc(ts,"day")`/`year`/`hour`/`diff`/`format` for time-series group-by. Bad values → warning + continue |
| 📋 | **List/array aggregation** | design doc 23. `list:col` (array_agg), `set:col` (distinct), `join:col` (group_concat). New `Column::List` (offsets+values, Arrow-like). Parallel-safe (worker-order concat = byte-identical). Building block for pivot; JSON output emits real arrays |
| 📋 | **Pivot / unpivot (reshape)** | design doc 23. `pivot rows:… cols:… values:agg:col` (long→wide, dynamic schema, high-cardinality guard) + `unpivot` (wide→long). Pipeline-breaker like sort/group; deterministic column/row order; parallel when the inner group-by is parallel-safe (decimal/int/order-independent aggs) |

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
| ✅ | **Lower the parallel threshold (8 MiB)** | was 256 MiB (mid-size files ran serial); measured crossover and wired `parallel_min_bytes()` into the engine. 171 MiB filter: serial 1.6 s → parallel 0.4–0.7 s. `RIVUS_PARALLEL_MIN_BYTES`-overridable |
| ❌ | ~~**Single-pass retain-buffer reader**~~ (evaluated, dropped) | prototyped to drop the second scan; **measured *slower*** than two-pass on warm cache (4.0 s vs 3.4 s on 288 MB) — holding all lines in memory costs more than the page-cached re-read saves. Not shipped (faster needs a measured number). May return for cold-cache/network FS. See `BENCHMARKS.md` |
| ✅ | **Adaptive execution strategy** (Epic #30 / Pillar C, #33) | std-only host probe (`Analytics`: cpus + `/proc/meminfo`) → autotuner picks **serial vs parallel** and surfaces the decision (`RunResult.strategy`, `--json` `"strategy"`). `--memory low\|auto\|fast`; default `auto` parallelizes ≥8 MiB on multicore. 288 MB filter: serial 3.53 s → parallel **1.13 s** (3.1×), byte-identical |
| 📋 | **SIMD CSV scan** (`std::arch`, no deps) | find `,`/`\n` with SSE2/AVX2; bench-gated (SWAR tried, no win at current bottleneck — revisit after the above) |
| 🚧 | **Vectorized / SIMD predicate kernels** (Epic #38 lever 1 / #39) | kernel refactored to a **branch-free byte-mask** form (auto-vectorized, zero `unsafe`/deps; ~5% on multi-pred filters). Hand-written AVX2 **measured → no win** (compare is memory-bandwidth-bound; the *gather* dominates) so it was dropped — see `docs/BENCHMARKS.md`. Real lever = columnar selection vector (#40). String compares beyond numeric still planned |
| 🚧 | Push computed-column / string predicates into the reader | **string literal-substring prefilter ✅** (`contains`/`starts_with`/`ends_with`/`==`/`like`-literal → ripgrep-style raw-line pre-scan, result-invariant superset; Epic #30 C4(i)), now also on the **parallel byte-range path ✅** (#35, with per-worker skip telemetry; quote/newline needles declined for safety, #37). Computed-column predicates + pushing the pre-scan into pass-1 inference still planned |
| 📋 | mmap the source; overlap decode with IO | |
| 📋 | Re-use buffers across chunks; arena-per-chunk recycling | |
| 📋 | JIT (Cranelift) for hot predicates/projections | design doc 09; needs a vetted dep |
| 📋 | **GPU backend** (feature-gated, CPU fallback) | design doc 22; `--accel gpu\|auto\|cpu`; default build stays GPU-free / zero-dep. Beats the memory-bandwidth wall #39 hit — **must measure transfer-inclusive** before adopting |

## G. Correctness as an opt-in lane

| | item | note |
|---|---|---|
| 📋 | **Exact decimal lane** (COBOL-style scaled integer) | design doc 21. `--exact[=auto\|N]` / `open f.csv (price:decimal[(n)])`. i128 scaled-integer → addition is associative & exact → **parallel group-by becomes byte-identical** (#41), and money math is exact. Default stays f64 (fastest). Scale auto-inferred or explicit; avg/std divide-then-round deterministically |
| 📋 | **Parallel group-by / join** (#41) | blocked on byte-identity for f64 sum/avg/std (measured ULP drift from non-associativity). Lands cleanly for decimal & integer columns + order-independent aggs (min/max/count/first/last/pct); f64 sum/avg/std stay serial unless `--exact` |

## F. Observability & UX

| | item | note |
|---|---|---|
| ✅ | Live progress, execution-graph viz, error stream | |
| ✅ | Structured telemetry stream (JSONL on stderr/socket) | **done** — `rivus run … --json` emits one JSON object per node (counters: chunks/rows in·out, busy_ms, rows/s, selectivity, mode) + per error event + a summary; stdout stays clean. `--telemetry-addr HOST:PORT` streams the same JSONL to a TCP socket (a live feed for an external viewer), falling back to stderr on a connection error. std-only (no serde, `std::net`). |
| ✅ | Live dashboard (TUI + browser) | **done** (Epic #30 Pillar B) — `rivus run … --tui` repaints an ANSI dashboard on stderr; `--serve [ADDR]` runs a std-only HTTP/1.1 + SSE server (embedded HTML/JS/SVG at `GET /`, `GET /snapshot`, live `GET /events`). Browser does the drawing; Rust ships JSON snapshots from `RuntimeSnapshot`. Zero new deps. **#36**: `--tui`/`--serve` now honor `--memory` (live observation still runs serial for a coherent stream, and the surfaced strategy says so — `…→ parallel; live observation → serial`); per-worker breakdown (A2) exposed in the `--json` summary as `worker_breakdown`; serve hardened with a read timeout + connection cap. |
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
