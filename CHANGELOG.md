# Changelog

All notable changes to Rivus. Format loosely follows
[Keep a Changelog](https://keepachangelog.com); versions follow
[SemVer](https://semver.org).

## [Unreleased]

### Added
- **Protected-channel distributed execution (design §33 / §17, Pillar 3).** Run a
  flow across the network by shipping its **IR as the deployment artifact**
  (§28.12.5-4) to a remote worker that executes it on the same chunk engine and
  streams the result back — **byte-identical to a local run** (interpret==
  distribute, §0.5). `rivus serve [--bind ADDR]` is the worker; `rivus run
  flow.riv --on rivus://host:port` is the coordinator. The channel is **never a
  raw listener** (§28.12.5-1):
  - **Primary = ride kernel WireGuard, embed no crypto (§28.12.5-2).** std-only,
    **zero new dependencies**; Rivus only *enforces* binding to the trusted
    interface (`RIVUS_CAP_NET_IFACE`) and an **allowlist of peer identities**
    (`RIVUS_CAP_NET_PEERS`, the static-public-key ↔ wg-IP boundary). Loopback is
    the one exception. Control+data are multiplexed with **credit-based bounded
    pull** (§28.12.2 ④). Fully working and tested (`tests/net.rs::distributed_*`).
  - **Alternative = QUIC (feature `quic`, §28.12.5-3), now complete:** `quinn` +
    `rustls`/`ring` + `rcgen`, identity = the cert's public-key **fingerprint**,
    allowlist pins peer fingerprints (`RIVUS_CAP_NET_PEER_KEYS`). Mutual-auth
    handshake, static-key pinning, **and the byte-identical credit-streamed result
    round-trip** all work and are tested (`tests/quic.rs`); `rivus serve --quic` /
    `rivus run --on quic://…` demo it end-to-end. (Off by default; not yet in
    `full` pending a `cargo deny` pass on its tree.) The streaming stall during
    development was a `QuicConfig` `#[derive(Default)]` giving `window = 0` (the
    client granted zero credit → the worker blocked forever) — fixed with a manual
    `Default` (window 8) and a defensive `window.max(1)`.
  - Capability denials name only the target, never the allowlist (§28.12.4);
    credentials never ride the IR / telemetry / error stream.
- **Transport architecture: logical channel separation + event-centric
  observability (design §34, the maintainer's transport memo).** The distributed
  wire protocol now tags every frame with a logical **channel** — Control
  (lifecycle/credit), Data (result chunks), Telemetry (events) — multiplexed on
  one connection (the QUIC stream-separation lesson, without N sockets). The
  worker narrates structured events on the telemetry channel (`flow.started` /
  `flow.completed result_bytes=… ms=…` / `transfer.done frames=… bytes=…`)
  instead of the client packet-sniffing; `run_remote_observed` / `rivus run --on`
  surface them on stderr while the result flows on the data channel (clean
  stdout). Staged next (design only): explicit CPU-budget/affinity (`cpubudget`)
  and DPU/SmartNIC offload.
- **Host Transport Service over a Unix-domain socket — §34.4 slices 1+2
  (pre-implementation, `feature net`, unix-only).** A worker fronts a UDS that
  co-located Rivus processes use instead of each owning a network stack (the PMCN
  "consolidate comms responsibility" idea). `rivus serve --uds PATH
  [--upstream rivus://host:port]` / `rivus run --on uds://PATH`;
  `distributed::{serve_uds, run_remote_uds, forwarding_handler}`.
  - **s1 (UDS front):** the worker/client protocol cores were extracted
    **transport-agnostic** (`serve_protocol` / `client_protocol`), so the **same
    channel-tagged frames** (Control/Data/Telemetry + credit + `flow.*` events)
    run over UDS and yield a byte-identical round-trip — proving §34.1's transport
    orthogonality. UDS is local + filesystem-permission-gated (no IP allowlist).
  - **s2 (forwarding gateway):** with `--upstream`, the service's handler is
    `forwarding_handler` (relay the IR to a remote worker and return its bytes),
    so co-located Rivus reach the upstream through **one** local service that owns
    the network egress — the PMCN consolidation, demonstrated end-to-end
    (UDS client → UDS service → TCP worker → back, byte-identical).
  - **s2' (persistent sessions):** the protocol now carries **many jobs per
    connection** (the worker's `serve_protocol` is a job loop; stray credit between
    jobs is skipped). A client `Session` (`connect` HELLOs once, `run` ships each
    job) amortizes connect/handshake, and the gateway's
    `forwarding_session_handler` shares **one** persistent upstream connection
    across all co-located Rivus (`Mutex<Session>`) — true session sharing. Reuse
    is **1.4× on std** (per-call 0.633 → session 0.441 ms/job) and is the lever
    for QUIC's 8.6 ms per-call cost (#176). A bonus: the job loop's read-until-EOF
    structurally subsumes the old single-job graceful drain (no large-result RST).
  Remaining (routing, core pinning, QUIC session reuse) stay design-gated.
- **Networking transport (design §33, feature `net`, std-only / zero new deps).**
  Two client-side network transports behind the off-by-default `net` feature —
  the default build stays zero-dependency, parsing / `rivus explain` are always
  std, and *running* a network flow without the feature is refused up front with
  rebuild guidance (never silent):
  - **`open "http://host[:port]/path"`** — a bounded HTTP/1.1 GET (a minimal
    client over `std::net`): fetch a remote **CSV or JSON** and wrangle it like a
    local file (filter pushdown and all). Body framing handles `Content-Length`,
    `Transfer-Encoding: chunked` and connection-close; up to 5 redirects are
    followed; the body decodes single-pass in bounded memory and is chunk-size
    independent. `https://` is rejected with guidance (TLS is out of scope).
  - **`subscribe "tcp://host:port"`** — an unbounded TCP client feed (CSV or
    `as json`): newline-delimited records streamed with lossless backpressure.
    Like `watch` it is outside the determinism contract (§0.14) — the optimizer
    and the parallel executor leave it alone and a whole-stream aggregate
    downstream is refused (needs a window); it ends on peer close or `take N`.
  - **Capability boundary** (§28.12.4/5): loopback is always reachable; any other
    host must be in `RIVUS_CAP_NET_HOSTS` (else rejected, naming only the target).
    Read timeout via `RIVUS_NET_TIMEOUT_MS` (default 30 s). No listener is ever
    bound (§28.12.5). Runnable demo: `examples/networking-demo.sh`.

### Changed
- **Expression `cast` to a temporal lane now parses a string source (BUG-D) —
  behavior change.** `cast ts:datetime` / `(ts:datetime) as t` (and `date`/`time`)
  previously reinterpreted a string as raw epoch ticks (`"2026-06-01"` → epoch-0,
  `"260601120000"` → year 10228). It now *parses* the string with the auto
  formats — the same meaning as the reader's exact path, so `cast ts:datetime` is
  byte-identical to declaring `(ts:datetime)` at `open` (only the path/speed
  differs). A non-null cell that won't parse becomes `null` (continue-first) and
  the count is surfaced once on finish (never-silent; in parallel the per-worker
  counts sum to the serial total). This holds on **every cast path** — the `cast`
  verb and computed columns (`|>`), and now also a cast inside a `|?` **predicate**
  or a **function argument** (the scalar interpreter carries the same failure
  accumulator). A **parse format stays schema-only**: `cast ts:datetime("fmt")` is
  now a never-silent parse error ("declare the format in the schema"); the reader
  form `(ts:datetime("fmt"))` is unchanged. `Expr::Cast` is structurally unchanged,
  so `to_source` round-trips as before. (`docs/design/23-datetime-and-reshape.md`
  §23.6)
- **`substr` is now 1-based (SQL/DuckDB convention) — breaking.** `substr(s, 1)`
  is the first char (was 0-based, which was misleading). The mapping is lenient
  — `start <= 1` clamps to the beginning — so an old `substr(s, 0, n)` call still
  returns the same prefix; only calls with an explicit start `≥ 2` shift by one.
  (#bugreport ③)
- **CSV parse failures are surfaced on the error stream (continue-first
  observability).** A non-empty cell that can't be parsed into its column's lane
  is still defaulted to `0` (continue-first), but the loss is no longer silent:
  the reader counts the failures per column and the source raises one
  `Recoverable` summary on exhaustion — e.g. `2 value(s) in column 'amount' (as
  decimal(2)) could not be parsed; kept as default 0`. This includes **decimal
  `i128` overflow** (#bugreport ②④) and the **datetime and duration lanes**
  (#80) — every typed lane now reports a non-empty unparseable cell, so no lane
  defaults silently. Empty cells are "missing", not failures, so
  they never count (no false positives on clean data); the count is exact and
  chunk-size independent. Covers the serial and byte-range-parallel streaming
  readers; the result is unchanged. (The compressed and in-memory build paths
  don't count yet — follow-up. Distinguishing null/empty/0 and making `dropna`
  see a defaulted numeric blank — #bugreport ①⑤ — needs the nullable-column model,
  tracked separately.)
- **Live observation no longer throttles processing (Observable First).** A live
  progress hook (`--tui` / `--serve`) previously forced the **serial** path so
  the dashboard saw one coherent stream — i.e. *observing* a run downgraded it to
  one core (the view changing the computation). Now observation keeps the run
  **fully parallel**: each worker mirrors its partition's per-node counters into
  per-worker atomic slots, and the coordinator thread samples them (~every
  100 ms) into one aggregate `RuntimeSnapshot` for the hook. The live view is
  coarser (node totals summed across workers, not a per-worker breakdown) — the
  acceptable *display* limitation — while the processing stays parallel. Serial
  now happens only when genuinely chosen (small input / `--memory low`) or
  non-partitionable (preview, multi-source). Byte-identical (observation only;
  stress sweep + `optimizer_equiv` green). Zero new dependencies.
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
- **`:` definition chain in `|>` projections (design §29.2, s1, std-only).**
  `col :alias :type` stacks definitions left→right, light→heavy: `:identifier`
  renames, `:type` casts (`|> amount :amt :decimal(2)`), at most one of each in
  that order. Pure parser sugar over the existing `Op::ProjectExpr` items —
  IR, runtime, optimizer and output bytes are identical to the parenthesized
  `(amount:decimal(2)) as amt` spelling (locked by an optimizer-equiv test).
  The chain is the canonical `to_source` form; older spellings still parse and
  `rivus fmt` rewrites them to it. After `:` a type word always means a cast
  (the disjointness rule that keeps round-trips exact); renaming *to* a
  type-word name uses the parenthesized escape hatch `(col) as int`. The
  `rename`/`cast` **verbs are unchanged** — they fix columns in place keeping
  the whole row, which a projection cannot express, so they are different
  operations, not aliases (§29.2).
- **`|!` validate — declare a row contract (Epic #82 / #83, §24, std-only).** A
  validator is not a filter: `|! <pred> warn|reject|halt` declares a contract and
  disposes of a non-conforming row **explicitly and always reports it** on the
  error stream (never silent). The disposition is **required** (no implicit
  default → no silent policy): `warn` keeps the row, `reject` drops it, `halt`
  raises a `Fatal` (the run stops). `warn`/`reject` emit one summary on
  completion (count + rule + a sample offending row; the count is chunk-size
  independent), and `reject` is byte-identical serial vs parallel. `Op::Validate`
  round-trips through `to_source`; the optimizer treats it as a barrier
  (optimized == `--no-opt`). The CSV reader's parse-failure reporting is the same
  idea at ingest. Declarative rules / `quarantine(sink)` / inter-row checks are
  on the roadmap (§24).
- **Time-series subtypes `date` & `time` (Epic #56 / #58, std-only).** Two exact,
  dependency-zero temporal lanes built on the Hinnant civil↔days math: `date`
  (i32 epoch-day, ISO `yyyy-MM-dd`) and `time` (i64 ticks-since-midnight,
  `HH:mm:ss`; second resolution). Declare them in a schema (`(d:date t:time)`) or
  `cast`; reads count parse failures and surface them like the other lanes (an
  impossible date `2024-02-30` / bad time `25:00:00` is reported, empty cells are
  "missing", never-silent). Both round-trip through `to_source` and are exact +
  associative, so `min`/`max`/`count` and group-by keep the type and are
  **byte-identical in parallel**. New extractors usable anywhere an expression
  is: `date(x)`, `time(x)`, `weekday(x)` (`0=Mon…6=Sun`), `is_weekend(x)`.
- **Animated SVG flow dashboard + `--open` (Epic #30 / Pillar B, std-only).**
  `--serve`'s dashboard was a near-static table (hard to tell from `--tui`); it
  now renders the flow as a left→right layered **SVG DAG and animates the live
  run**: particles stream along each data edge at the node's throughput, nodes
  pulse blue while active, turn green when finished and red on errors, and the
  continue-first error side-channel edges are drawn dashed and flow red. A new
  `GET /graph` endpoint (`render_graph_json`) ships the static topology
  (nodes + edges) once; the browser lays out the DAG (longest-path layering) and
  animates from the existing `/events` SSE row counts (per-tick snapshot
  unchanged). New `--open` flag launches the dashboard URL in the system browser
  (`xdg-open` / `open` / `cmd start`; shell-free arg passing, detached,
  non-fatal, opt-in). Dependency-free inline HTML/JS/SVG; heavy rendering stays
  in the browser.
- **SIMD-native CSV parse (Epic #38 / #71, dependency-zero).** The parse path —
  measured as the dominant cost of `open` — is rebuilt around SIMD-within-a-
  register and `core::arch` kernels, each **byte-identical** to the scalar/std
  reference and gated by equivalence tests written first:
  - **AVX2 structural scan** for field splitting (`PCMPEQB` + `movemask`,
    32 bytes/step), runtime-dispatched via `is_x86_feature_detected!`, with the
    SWAR scan (8 bytes/step) as the std-only fallback on non-AVX2 / non-x86
    hosts. Measured **1.72×** over SWAR.
  - **SWAR integer parse** on the hot `i64` build + inference lanes (8 ASCII
    digits/step via Lemire horizontal sums; exact `i64`, no `f64`). Measured
    **1.11×** short ids / **2.16×** wide 16-digit ids.
  - **SWAR decimal parse** for the exact `i128` lane (`Decimal::parse_scaled`),
    sharing the digit primitives via a new `rivus_core::numparse`. Measured
    **1.49×** short / **1.97×** wide.
- **Columnar core: branch-free selection-vector build (Epic #38 / #40).** The
  predicate kernel's measured bottleneck — collecting surviving row indices from
  the mask — is now branch-free (`w += (m != 0) as usize`), so a random
  ~50 %-selectivity mask pays no branch mispredictions. Measured **7.31×** at
  50 % selectivity (flat across selectivity); byte-identical to the branchy
  reference.
- **Live observability: `rivus run … --tui` and `--serve [ADDR]` (Epic #30 /
  Pillar B — issue #32, std-only).** Built on Pillar A's `RuntimeSnapshot`.
  `--tui` repaints an ANSI dashboard on stderr each tick (rows/s, per-node bars,
  state). `--serve` launches a **std-only HTTP/1.1 + SSE** server (a
  `TcpListener`, hand-written request parsing, no third-party crates): `GET /`
  serves an embedded HTML/JS/SVG dashboard, `GET /snapshot` the latest snapshot
  JSON, `GET /events` a live `text/event-stream`. Heavy rendering lives in the
  browser; Rust ships only JSON snapshots. The flow runs on a worker thread that
  publishes to a shared `Hub`; stdout stays clean (a `save -` sink still pipes).
  A bind failure falls back to a normal run. **Zero new dependencies**
  (`cargo deny --all-features` green). CLI-tested over loopback (HTML + SSE
  frame + clean stdout).
- **Inference-decision telemetry (Epic #30 / Pillar A — issue #31, A4).** A CSV
  source now records its per-column inference outcome `(name, type, widened)` in
  `RunResult.inference`; the `--json` summary lists `widened_columns` (columns an
  int candidate was knocked down to float). Surfaced **off the error stream**
  (summary-only), so the JSONL `node`/`error` contract and "clean data → no
  errors" semantics are untouched. Pure accounting, result-invariant. Completes
  Pillar A's measurement core. No new dependencies.
- **String prefilter pushdown (Epic #30 / Pillar C, C4(i)).** `filter_pushdown`
  now also lifts **literal-substring** predicates (`contains` / `starts_with` /
  `ends_with` / `==` / the literal run of a `like` pattern) into the CSV reader
  as a ripgrep-style raw-line byte pre-scan: a line lacking the needle is skipped
  *before* it's split into fields. It's a **superset** filter — the downstream
  `FilterProject` re-checks every survivor, so the result is byte-identical (a
  substring landing in the wrong column is still rejected) — and it costs no
  extra memory. Measured **~2.0×** on `contains(country,"JP")` over 171 MiB
  serial (3.45 s → 1.70 s); the skipped-row count shows up as A1 telemetry.
  Equivalence-gated (`optimizer_equiv`), result unchanged. No new dependencies.
  (The byte-range parallel reader doesn't apply it yet — tracked for later.)
- **Streaming runtime snapshots (Epic #30 / Pillar A — issue #31, A5).** New
  `RuntimeSnapshot` / `NodeSnapshot` (a cheap point-in-time view: elapsed,
  rows_seen, mode, per-node counters) and `run_with_progress(graph, opts, hook)`
  — the engine calls an optional `ProgressHook` (`&mut dyn FnMut(&RuntimeSnapshot)`)
  every few source chunks and once at the end. The base for live TUI / HTTP
  dashboards (Pillar B / §14.4 `RuntimeHandle::subscribe`). `run` is unchanged
  (calls it with no hook); with no subscriber nothing is built, so the cost is
  ~0. A subscriber **no longer forces the serial path** — parallel runs feed it
  an aggregate cross-worker snapshot (see "Live observation no longer throttles
  processing" above); results are identical. Oracle-tested (≥1 snapshot,
  monotonic rows_seen, final snapshot sees every row, result invariant). No new
  dependencies.
- **First-row latency & parse phase in `--json` summary (Epic #30 / Pillar A —
  issue #31, A3).** `RunResult` gains `first_row_latency: Option<Duration>` (wall
  to the first produced chunk; min across workers in parallel), and the JSONL
  `summary` line gains `first_row_latency_ms` and `parse_busy_ms` (source busy)
  — surfacing the previously-collected-but-unrendered `Chunk.meta.created_at`
  signal. **Summary-only**: the `node` / `error` line keys are byte-stable
  (existing JSONL contract unchanged). Pure accounting, result-invariant. No new
  dependencies.
- **Per-worker telemetry (Epic #30 / Pillar A — issue #31, A2).** A parallel
  (byte-range) run now records a `WorkerTelemetry { worker, rows_out, busy }` per
  worker in `RunResult.workers`, so parallel skew (uneven rows / busy time across
  workers) is observable instead of being collapsed into the node aggregate. The
  serial path leaves it empty — purely additive, results and the existing
  node-aggregate `telemetry` unchanged. Oracle-tested (workers indexed 0..n,
  `rows_out` sums to the result). No new dependencies.
- **Prefilter-skip telemetry (Epic #30 / Pillar A — issue #31, A1).** The CSV
  reader now counts the rows its pushed-down prefilter skips *building* and the
  source reports it once on exhaustion: `prefilter skipped N row(s) at the
  reader` (an `Info` event, visible in `explain`/`--json`). Pure accounting —
  the result is byte-identical (those rows would be dropped by the downstream
  `FilterProject` anyway), and the count is chunk-size independent. First step of
  making "what the optimizer skipped" measurable. New
  `tests/observability.rs`. No new dependencies.
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
