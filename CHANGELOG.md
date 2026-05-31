# Changelog

All notable changes to Rivus. Format loosely follows
[Keep a Changelog](https://keepachangelog.com); versions follow
[SemVer](https://semver.org).

## [Unreleased]

### Added
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
