# Changelog

All notable changes to Rivus. Format loosely follows
[Keep a Changelog](https://keepachangelog.com); versions follow
[SemVer](https://semver.org).

## [Unreleased]

### Changed
- **Parallel reads now cover `stdout` sinks too.** The byte-range parallel CSV
  reader previously bailed to serial whenever the sink was `save -` (stdout);
  it now assembles the ordered part files to stdout, so the Unix-filter form
  (`… | rivus '… save -'` / `rivus run … > out`) is parallel as well. On a
  363 MiB file `… |? age>=50 |> id name age save -` drops **5.2 s → 1.8 s**
  (2.8×), closing the gap to DuckDB from ~5× to ~1.8×. Output is byte-identical
  and order-preserving vs the serial path (CLI-tested). The streaming-parallel
  reader engages for any single-CSV file source at/above
  `RIVUS_PARALLEL_MIN_BYTES` (default **8 MiB**); `RIVUS_NO_PARALLEL` forces the
  serial path (a true single-thread baseline).
- **Actually lower the parallel threshold to 8 MiB (was a docs-only change).**
  A prior commit lowered the documented threshold from 256 MiB to 8 MiB but the
  engine const stayed at 256 MiB, so files between 8 and 256 MiB silently fell to
  the *in-memory* chunk-partition path — which materializes the whole file and
  measured **slower than serial** (171 MiB numeric filter: serial 1.5 s vs
  in-memory 1.7 s). The threshold is now read from `parallel_min_bytes()`
  (default 8 MiB, `RIVUS_PARALLEL_MIN_BYTES`-overridable), so mid-size files use
  the byte-range streaming reader. Measured win where it now engages: a 380 MiB
  numeric filter to stdout drops **3.33 s → 0.91 s (3.7×)**; output stays
  byte-identical to serial.

### Added
- **JSON array output: `save out.json` / `save - as json` (std-only).** A
  `.json` path (or `as json`) now writes a single JSON array (`[{…},{…}]`)
  instead of NDJSON; `.jsonl` / `.ndjson` / `as jsonl` stay one-object-per-line,
  and `writejson` is unchanged (NDJSON). The array sink streams incrementally
  (open `[`, comma-separate rows across chunks, close `]`) so it stays
  bounded-memory; an empty result is `[]`. Output is valid JSON (round-trips
  back through `open`), and byte-identical on the serial and parallel paths.
  No new dependencies.
- **`cast COL:type [COL:type …]` verb (std-only).** Re-types named columns in
  place (position and name kept; values re-coerced through the same cast lane as
  an inline `(col:type)` projection) — e.g. `cast age:int price:f64`. The
  readable form of the "mid-flow cast" (sugar over a computed projection that
  keeps the rest). Unknown columns warn and are skipped; round-trips through
  `to_source` (type names render canonically, `int` → `i64`). Oracle-tested
  (re-type + dtype check, chunk-size independent). No new dependencies.
- **Numeric functions `abs` / `round` / `floor` / `ceil` and null-coalesce
  `coalesce` (std-only).** Usable anywhere an expression is. `abs/round/floor/
  ceil` coerce a numeric string (e.g. a `:str`-declared column) by parsing it,
  return an integer when the result is whole (else a float), and a non-numeric
  value yields null (continue-first); `round` rounds ties away from zero.
  `coalesce(a, b, …)` returns the first argument whose text is non-empty (empty
  string if all are), preserving its lane. All lower to `Expr::Func`, round-trip
  through `to_source`, and are chunk-size independent (oracle-tested). No new
  dependencies.
- **Multi-key sort: `sort k1 [asc|desc] k2 [asc|desc] …` (std-only).** `sort`
  now accepts more than one key, each with its own direction (default ascending),
  comparing by each key in turn — e.g. `sort team score desc` orders by team
  ascending, then by score descending within a team. Still a stable sort (ties
  keep source order) and chunk-size independent (oracle-tested). Single-key
  `sort age [desc]` is unchanged; round-trips through `to_source`. No new deps.
- **Composite-key joins: `A & B on k1 k2 …` (std-only).** Every join kind
  (`&`/`&left`/`&right`/`&full`) now joins on one *or more* key columns — e.g.
  `A & B on country region` matches rows agreeing on the (country, region)
  tuple. Each key may be `lk:rk` when the sides name it differently (`on a x:y`),
  and the forms mix (`on a x:y`). Rows are keyed on the key values joined by the
  ASCII unit separator (`0x1F`), so tuples never collide; outer joins drop the
  right key columns and preserve every left key value (right/full carry the right
  key into the output). Round-trips through `to_source`; oracle-tested (a
  same-country / different-region pair must *not* match), chunk-size independent.
  No new dependencies.
- **Multi-key grouping: `|# key1 key2 … [func:col …]` (std-only).** `|#` now
  accepts more than one group key — e.g. `|# country region sum:score` groups by
  the (country, region) tuple. Each key becomes its own output column (in key
  order, before `count`), then the aggregate columns. Groups are keyed on the
  key values joined by the ASCII unit separator (`0x1F`, which can't appear in a
  parsed field), so distinct tuples never collide. Single-key `|#` is unchanged.
  Round-trips through `to_source`; oracle-tested (count + sum per tuple,
  chunk-size independent). No new dependencies.
- **`reorder COL [COL ...]` column reordering (std-only).** Moves the named
  columns to the front in the given order; every other column follows in its
  original order. Unknown names are ignored and a repeated name is deduped. A
  pure permutation — types and values are untouched, stateless and streaming
  (works on the parallel path), and round-trips through `to_source`. Completes
  the `rename` / `drop` / `reorder` trio. Oracle-tested (schema + values
  chunk-size independent). No new dependencies.
- **String functions `replace` / `split_part` / `concat` (std-only).** Usable
  anywhere an expression is (computed columns, filters). `replace(s, from, to)`
  swaps every literal occurrence (an empty `from` is a no-op); `split_part(s,
  sep, n)` returns the `n`-th field (1-based, DuckDB/awk convention) after
  splitting on a literal separator, or empty when out of range; `concat(a, b,
  …)` joins any number of arguments as text. All lower to `Expr::Func`,
  round-trip through `to_source`, and are chunk-size independent (oracle-tested).
  No new dependencies.
- **Structured telemetry: `rivus run … --json` (std-only).** Emits the run as
  **JSON Lines** on stderr — one `{"event":"node",…}` per flow node (counters:
  `chunks_in/out`, `rows_in/out`, `errors`, `busy_ms`, `rows_per_sec`,
  `selectivity`, `mode`, `finished`), one `{"event":"error",…}` per error-stream
  event (severity, scope, message, node, chunk_id), and a final
  `{"event":"summary",…}`. stdout stays clean data, so a `save -` sink still
  pipes downstream while a tool reads telemetry from stderr (the base for an
  editor/GUI integration, Observability spec §19). `--telemetry json` is an
  alias; `--telemetry-addr HOST:PORT` streams the same JSONL to a **TCP socket**
  (a live feed for an external viewer; falls back to stderr on a connection
  error). In JSON mode the ASCII banner, optimizer report and live progress are
  suppressed. A tiny hand-rolled JSON writer + `std::net` — no serde, no deps.
- **zstd input: `open data.csv.zst` (opt-in `--features zstd`).** Reads
  zstd-compressed CSV/TSV (`.zst` / `.zstd`) through the **pure-Rust `ruzstd`
  decoder** (no C toolchain). Same serial single-pass, sample-inference path as
  gzip (the compressed reader is now format-agnostic over `.gz`/`.zst`), bounded
  memory, forced serial (no byte-range parallel). **The default build stays
  zero-dependency**: a default binary opening a `.zst` raises an actionable error
  (`rebuild with --features zstd`). The runtime decode tree is all pure-Rust
  (`ruzstd`→`twox-hash`); the `.zst` test fixtures are written with the `zstd`
  crate as an **encode-only `[dev-dependency]`** that never ships. Oracle-tested
  across chunk sizes. The `.zst`/`.zstd` suffix is stripped before the delimiter
  is chosen, so `.tsv.zst` stays tab-delimited.
- **Right & full outer joins: `A &right B` / `A &full B` (std-only).** Complete
  the join family alongside `&` (inner) and `&left`. `&right` keeps every right
  row (left columns padded with type defaults); `&full` keeps every row from
  both sides. Outer joins **preserve the join key**: an unmatched right row
  carries its key into the output key column (so a right/full join never drops
  it). Same buffered hash-join machinery and blocking/serial semantics. Lowers
  to `Op::Join { kind: Right|Full }`, round-trips through `to_source`.
  Oracle-tested (right rows = matched + orphan-right; full = matched +
  unmatched-left + orphan-right; key never empty), chunk-size independent. No
  new dependencies.
- **gzip input: `open data.csv.gz` (opt-in `--features gzip`).** Reads
  gzip-compressed CSV/TSV (`.csv.gz` / `.tsv.gz`) through `flate2`'s pure-Rust
  `miniz_oxide` backend (no C toolchain). A compressed stream can't be seeked,
  so this uses a **serial, single-pass** reader with *sample inference* (buffer
  the first chunk of rows, infer the schema, then stream the rest) — bounded
  memory, no byte-range parallelism (the engine forces `.gz` sources serial).
  **The default build stays zero-dependency**: the dependency is optional and
  feature-gated, and a default binary reading a `.gz` raises an actionable error
  (`rebuild with --features gzip`). The `.gz` suffix is stripped before the
  delimiter is chosen, so `.tsv.gz` is still tab-delimited. Vetted per
  `docs/SUPPLY-CHAIN.md` (flate2 + its pure-Rust tree; `cargo deny check
  --all-features` green). Oracle-tested across chunk sizes. Sample inference is
  the documented trade-off: a type that only widens deep past the sample can
  mis-infer (unlike the seekable two-pass reader).
- **Left outer join: `A &left B on key` (std-only).** Alongside the inner join
  (`A & B`), `&left` keeps every left row; an unmatched left row is emitted once
  with the right columns padded to type defaults (`0` / `0.0` / `false` / empty
  string). Same hash-join machinery (build the right side, probe the left) and
  same blocking/serial semantics as the inner join; row order is the left order,
  and the result is chunk-size independent (oracle-tested: the left-join
  `sum(amount)` equals the inner-join sum, with one padded row per never-matched
  left key). Lowers to `Op::Join { kind: Left }`, round-trips through
  `to_source`. No new dependencies. Right/full outer joins remain on the roadmap.
- **Statistical missing-value fill: `fill col mean|median` (std-only).**
  Replaces a text column's blank cells with a whole-column statistic of its
  non-empty numeric cells: `mean` (arithmetic average) or `median` (p50,
  linear-interpolated, matching the `|# median:` aggregate). Buffers the entire
  stream (a pipeline-breaker like `sort`, and it forces the serial path), since
  the statistic needs every value; non-numeric cells are ignored when computing
  it but kept in the output, and an integral result is formatted without a
  trailing `.0`. Declare the column `:str` so its blanks survive parsing.
  Round-trips through `to_source`. Oracle-tested (the filled-column sum equals
  `sum(present) + blanks × statistic`, chunk-size independent). This completes
  the imputation roadmap item (D). No new dependencies.
- **Directional missing-value fill: `fill col ffill|bfill` (std-only).**
  Alongside the existing constant `fill col VALUE`, `ffill` carries the last
  non-empty value forward over blank cells and `bfill` the next value back —
  both across chunk boundaries, so the result is chunk-size independent
  (oracle-tested). `ffill` is fully streaming; `bfill` buffers the stream and
  emits on finish (a pipeline-breaker like `sort`, and it forces the serial
  path). A leading blank (`ffill`) / trailing blank (`bfill`) has no neighbour
  and stays empty. Operates on text columns (declare `:str` to detect a numeric
  column's blanks). Round-trips through `to_source`. No new dependencies.
  `fill col mean|median` remains planned — it needs a null-bitmap, since a blank
  numeric cell currently parses to `0` (missingness is lost at parse time).
- **`like` / `glob` pattern matching (std-only, no regex dependency).**
  `like(s, "JP-%")` is SQL `LIKE` (`%` any run, `_` any single char);
  `glob(s, "[JD]*-00??")` is shell glob (`*`, `?`, `[abc]`/`[a-z]`/`[!..]`
  classes). `like` uses a two-pointer matcher (no catastrophic backtracking).
  Covers DuckDB `LIKE`/`GLOB`-class patterns; true regex (`regexp_matches`)
  would need a vetted, feature-gated crate (deferred, needs sign-off).
- **`starts_with` / `ends_with` string functions (std-only).** Prefix/suffix
  predicates (`|? starts_with(code, "JP")`, `|? ends_with(name, "e")`) — the
  typed equivalent of grep `^…` / `…$`. Emit a boolean column. No new deps.
- **`rivus gen` — self-hosted data generation (dogfooding).** A new CLI
  subcommand emits deterministic, seeded benchmark/demo data to stdout, so
  benches and docs need no external awk/python: `rivus gen clean --rows N
  [--seed S]`, plus `error-heavy` / `mixed` (`--ratio R`) and `jsonl` shapes.
  Same seed → byte-identical output. Wraps the existing `gendata` generators.
- **Percentile group aggregates (std-only).** `|#` gains `median` and `pNN`
  (`p50`, `p90`, `p99`, …) — linear-interpolated percentiles (numpy/pandas
  default). They buffer each group's numeric values (bounded by group
  cardinality, a pipeline-breaker like `sort`), emit an `F64` column, and are
  chunk-size independent. `median` round-trips as `median`; others as `pNN`.
- **`-` sentinel for `open`/`save`.** `open -` reads stdin and `save -` writes
  stdout, alongside the existing `stdin`/`stdout` keywords — so a Rivus flow
  drops into a Unix pipe the conventional way (`… | rivus -c '… open - … save -'`).
- **`case when … then … [else …] end` expression (std-only).** A row-wise
  conditional usable anywhere an expression is (computed columns, filters).
  The first truthy `when` branch yields its value; with no match the `else`
  value (or an empty string) is used. Lowers to `Expr::Case`, round-trips
  through `to_source`, and is chunk-size independent. No new dependencies.
- **Column `rename` / `drop` (std-only).** `rename OLD NEW [OLD NEW ...]`
  renames columns in place (position, type and values untouched); `drop COL
  [COL ...]` removes columns, keeping the rest in order. Both are stateless,
  streaming, work on the parallel path, and round-trip through `to_source`.
  Unknown columns warn (rename) or are ignored (drop). No new dependencies.
- **More group aggregates (std-only).** `|#` gains `std` (sample standard
  deviation, ddof=1), `count_distinct` (alias `nunique`, emitted as an integer),
  and `first` / `last` (first/last non-empty value in source order, emitted as
  text) alongside the existing `sum`/`avg`/`min`/`max`. Each aggregate's
  accumulator tracks only the state its function needs; results stay chunk-size
  independent (oracle-tested).
- **TSV / custom delimiter (std-only).** `OpenCsv`/`SinkCsv` now carry a
  `delim: u8`. `.tsv` and `.tab` files are read and written tab-delimited
  automatically; `as tsv` / `as csv` overrides the extension either way. The
  delimiter flows through the streaming reader, the byte-range parallel reader,
  and both serial and parallel sinks. `to_source` only emits an `as …` modifier
  when the delimiter disagrees with the path extension, so round-trips stay
  clean and faithful. No new dependencies.

## [1.0.0] — 2026-05-30

First stable release. Rivus is a flow-oriented, DAG-native, continue-first,
streaming data runtime — and a credible, faster, far lighter alternative to
DuckDB/awk/Python for everyday data wrangling.

### Performance (measured)

- **Beats DuckDB on streaming ETL.** A 1.1 GB / 48 M-row CSV through
  `open |? age>=50 |> name age save out.csv` runs in **3.0 s at ~10 MiB peak
  RSS** — **~1.45× faster than DuckDB at ~40× less memory** (DuckDB: 4.4 s,
  407 MiB), **3.8× faster than awk**, **~10× faster than Python**. See
  [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).
- **Bounded memory at any file size.** Streaming CSV source and sinks; a sink-
  less `open big.csv` previews instantly in ~10 MiB.
- **Parallel streaming** (files > 256 MiB with a sink): newline-aligned byte-
  range workers writing ordered part files — parallel *and* still ~10 MiB.
- Optimizer: source dedup · filter+project fusion · projection pushdown ·
  **filter pushdown** into the reader (skips building dropped rows). Every rule
  is shown by `rivus explain` and gated byte-identical by `optimizer_equiv`.

### Language & operators

- Sources: CSV (quoted fields, **header-less** `noheader`, **declared schema**
  `open f.csv (id:int name:str)`), JSON / JSON Lines / NDJSON, fixed-width
  binary (`readbin`), and `open stdin`. Format from extension, `as FMT`
  override, or `readcsv`/`readjson` verbs.
- Transforms: `|?` filter (with `where` alias and **comma = AND**), `|>`
  project / **computed columns** (arithmetic `+ - * / %`, `as` alias),
  **inline type casts** `expr:type`, `|#` group (sum/avg/min/max/count),
  `take`/`limit`/`head`, `sort`, `distinct`, `describe`.
- DAG: `->` branch (tee), `+` merge, `&` **inner hash join** (`on key` /
  `on lk:rk`).
- Sinks: `save PATH [as FMT]`, `writecsv`/`writejson`, `print`, `save stdout`.
- Continue-first error stream + `on error … transition <mode>` lifecycle hooks.
- Three ways to type a column: at the source, mid-flow cast, computed column.

### CLI & UX

- `rivus run | explain | check`; programs as a file, inline `-c`, or stdin
  heredoc.
- **Unix-filter shorthand**: `cat data.csv | rivus '|? age >= 20 |> name age'`
  (a transform-only program reads CSV from stdin, writes stdout).
- Live progress on a TTY; execution-graph + error-stream visualization on
  stderr, clean data on stdout.

### Engineering

- **Zero third-party dependencies in the default build** (core/ir/parser/
  optimizer/runtime/cli are std-only). Heavy formats (compression, Parquet,
  pickle) are reserved as vetted, feature-gated, opt-in adapters — see
  [`docs/SUPPLY-CHAIN.md`](docs/SUPPLY-CHAIN.md).
- Correctness gate: oracle stress tests assert results are independent of
  `chunk_size`; the optimizer equivalence test asserts optimized == unoptimized
  byte-for-byte.
- Distribution: tag-driven release workflow builds macOS (Apple Silicon) and
  Windows 11+ x64 binaries (portable + CPU-tuned). See
  [`dist/`](dist/README.md).
- Docs: [`docs/GUIDE.md`](docs/GUIDE.md) (full syntax + one-liner cookbook),
  the 20-section design set, `ROADMAP`, `BENCHMARKS`, `SUPPLY-CHAIN`.

### Known limitations / on the roadmap (1.x)

Compressed/Parquet/pickle inputs (pending vetted deps), SIMD CSV scan,
left/right/outer & streaming joins, missing-value imputation, real TSV/custom
delimiters, structured-telemetry stream and interactive viewer. Tracked in
[`docs/ROADMAP.md`](docs/ROADMAP.md).
