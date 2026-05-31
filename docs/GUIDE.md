# Rivus — Syntax & Usage Guide

Rivus is a flow-oriented, DAG-native, streaming data runtime. You describe a
**flow** — sources → transforms → sinks — and Rivus executes it chunk by chunk,
in bounded memory, with the optimizer and live telemetry built in.

This guide is the practical reference: the full syntax, every operator, and a
gallery of copy-pasteable one-liners. For the design rationale see
[`docs/design/`](design/README.md); for install see the [README](../README.md).

> 🇯🇵 日本語版は [**`docs/GUIDE.ja.md`**](GUIDE.ja.md) を参照してください。

---

## 1. The 10-second mental model

```
Scope:                 # a named node in the execution graph
    open data.csv      # a source (head of the flow)
    |? age >= 20       # a transform (filter)
    |> name age        # a transform (project)
    save out.csv       # a sink
;                      # end of scope
```

- A program is a set of **scopes**. `Name: … ;` defines one.
- The first line of a scope is its **source** (`open …`); the rest are
  **transforms** and **sinks** applied left-to-right.
- `|?` `|>` `|#` are pipe operators; `->` `+` `&` build the DAG (branch / merge /
  join). Scopes can reference each other by name.
- Whitespace and newlines are insignificant — a scope can be one line or many.
  `#` starts a line comment (but `|#` is the group operator).

---

## 2. Running a flow

```sh
rivus run     <program>     # execute + visualize (graph, errors, output preview)
rivus explain <program>     # show the DAG IR, optimizer report, regenerated source
rivus check   <program>     # parse only (report syntax errors)
```

`<program>` is **one of**:

| form | example |
|---|---|
| a file | `rivus run flow.riv` |
| inline string (`-c`) | `rivus run -c 'U: open users.csv \|? age >= 20 ;'` |
| stdin (`-`, heredoc) | `rivus run - <<'RIV' … RIV` |

Flags: `--chunk-size N` (rows per chunk, default 4096), `--no-opt` (disable the
optimizer), `--json` (emit machine-readable **JSONL telemetry** to stderr
instead of the ASCII view — one object per node + per error + a summary; stdout
stays clean data, so `rivus run flow.riv --json 2>telemetry.jsonl >out.csv`
splits data and metrics cleanly), `--telemetry-addr HOST:PORT` (stream that same
JSONL to a TCP socket for a live external viewer; falls back to stderr if the
connection fails).

**stdout vs stderr.** The execution graph, telemetry and error stream go to
**stderr**; a `save stdout` sink writes clean data to **stdout**. So Rivus drops
straight into a shell pipeline:

```sh
rivus run -c 'U: open users.csv |? age >= 20 |> name age save stdout as csv ;' | sort
```

---

## 3. Sources (the head of a flow)

| syntax | reads |
|---|---|
| `open PATH` | format from the extension (`.csv` → CSV, `.jsonl`/`.ndjson`/`.json` → JSON) |
| `open PATH as FMT` | force the format (`FMT` = `csv` \| `tsv` \| `json` \| `jsonl` \| `ndjson`) |
| `open PATH` (`.tsv`/`.tab`) | **TSV** — tab-delimited, picked up from the extension (std-only). `as tsv` forces it on any path; `as csv` forces commas back |
| `open PATH.gz` / `PATH.zst` | **compressed** CSV/TSV — gzip (`.gz`, opt-in `--features gzip`) or zstd (`.zst`/`.zstd`, `--features zstd`). Serial single-pass, bounded memory. The default (zero-dependency) build errors with `rebuild with --features gzip`/`zstd` |
| `open PATH noheader` | CSV with **no header row** — every line is data, columns are named `c0, c1, c2, …` |
| `open PATH (col[:type] …)` | **declare a schema**: name columns positionally (overrides the header / `c0…`) and optionally fix a column's type — `int`/`i64`, `float`/`f64`, `str`/`string`, `bool`. e.g. `open f.csv (id:int zip:str age)` keeps `zip`'s leading zeros |
| `readcsv PATH` | CSV, explicitly |
| `readjson PATH` | JSON / JSON Lines, explicitly |
| `readbin PATH [le\|be] [packed\|aligned] (name:type …)` | fixed-width binary records (a C-struct dump) |
| `open stdin` / `open -` | read CSV (or `as FMT`) from standard input |
| `stream NAME` | replay a named flow (MVP: reference) |

Format detection deliberately **does not over-trust the extension**: use
`open data.dat as json` when the extension lies, or the `readcsv`/`readjson`
verbs when you want it obvious at a glance.

**Supported formats today:** CSV (with quoted-field handling), JSON Lines
(one object per line) and JSON arrays (`[ {...}, {...} ]`), and fixed-width
binary. JSON/JSONL/NDJSON all go through the same reader.

**Binary example** — decode `(i32 id, i32 age, f64 score, u8 active)` records:

```
B: readbin dump.bin (id:i32 age:i32 score:f64 active:u8) |? age >= 18 ;
```

`le`/`be` choose byte order (default little-endian); `packed` (default) vs
`aligned` choose C `repr(C)` natural-alignment padding. Field types:
`i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 bool`.

---

## 4. Transforms

Applied left to right; each consumes the stream and produces a new one.

### `|?` — filter

Keep rows where the predicate is true.

You can use `where` as a readable alias, and **commas mean AND**:

```
|? age >= 20
where age >= 20, country == "JP"      # comma = AND (same as `and`)
|? country == "JP" and active == true
|? score > 90 or age < 18
|? (score / age) > 3          # arithmetic in parens (see §6)
```

### `|>` — project / compute columns

Select columns, rename them, or compute new ones. Each item is one of:

| item | meaning |
|---|---|
| `name` | keep column `name` |
| `name as alias` | keep + rename |
| `(expr) as alias` | a **computed column** (arithmetic in parens) |

```
|> name age                                   # select
|> name age as years                          # rename
|> name (age * 12) as months (score / 100) as pct
```

### `|#` — group by

Partition by one or more key columns and aggregate. A `count` column is always
emitted; each `func:col` adds one aggregate. Functions:

- numeric: `sum avg min max std` (std is sample, ddof=1)
- percentiles: `median` and `pNN` (`p50 p90 p99 …`, linear interpolation)
- distinct count: `count_distinct` (alias `nunique`)
- positional: `first last` (first/last non-empty value in source order)

```
|# country                          # → country, count
|# country region sum:score         # multi-key: → country, region, count, sum_score
|# country sum:score avg:age        # → country, count, sum_score, avg_age
|# country median:score p90:score   # → country, count, median_score, p90_score
|# country count_distinct:city      # → country, count, count_distinct_city
```

Multiple keys partition by the column *tuple* (each key becomes its own output
column, before `count`). Output columns are named `count` and `<func>_<col>` (e.g. `sum_score`,
`p90_score`). `std`/percentiles buffer each group's values (a pipeline-breaker
like `sort`); the rest stream in O(1) memory per group.

### `take` / `limit` / `head` — cap rows

```
take 100        # keep the first 100 rows, then stop
limit 100       # alias
head 100        # alias
```

### `sort` — order by one or more keys

A stable sort over the whole stream (a blocking step). Ties keep source order.
Multiple keys sort by each in turn, each with its own direction.

```
sort age              # ascending (default)
sort age asc
sort score desc
sort team score desc  # team ascending, then score descending within a team
```

### `distinct` — drop duplicates

Keep the first occurrence. With no keys the whole row is the dedup key;
otherwise only the named columns.

```
distinct                # unique rows
distinct user_id        # first row per user_id
distinct country region # first row per (country, region)
```

### `dropna` / `fill` — missing values

```
dropna                 # drop rows blank in ANY column
dropna city region     # drop rows blank in these columns
fill city "UNKNOWN"    # replace blank cells of `city` with a constant
fill price ffill       # forward-fill: carry the last non-empty value down
fill price bfill       # backward-fill: carry the next non-empty value up
fill score mean        # fill blanks with the column mean (numeric cells)
fill score median      # fill blanks with the column median
```

A "missing" cell is an empty string. Numeric columns can't hold a blank (it
parses to 0), so declare a column `:str` if you need to detect/clean its blanks.
`ffill`/`bfill` carry the nearest neighbour across chunk boundaries (a leading
blank has nothing to forward-fill from, a trailing blank nothing to back-fill);
`bfill` buffers the stream to finish (a pipeline-breaker like `sort`), `ffill`
is fully streaming. `mean`/`median` compute a whole-column statistic over the
non-empty numeric cells and substitute it for the blanks (also pipeline-breakers,
since the statistic needs every value); an integral result is written without a
trailing `.0`. All `fill` methods leave non-blank cells untouched.

### `describe` — one-pass column summary

Replace the stream with a per-column summary (like pandas `.describe()` / SQL
`DESCRIBE`): `column`, `type`, `count`, and — for numeric columns — `min`,
`max`, `mean`. Streaming, single pass.

```
open data.csv describe save stdout as csv
# column,type,count,min,max,mean
# id,i64,1000,1,1000,500.5
# name,str,1000,,,
```

### `rename` / `drop` / `reorder` — column shape

Stateless, streaming column operations (no `|>` needed):

```
rename age years city loc   # rename in place: age→years, city→loc
drop zip notes              # remove columns, keep the rest in order
reorder name id             # move name,id to the front; rest follow in order
```

`rename` keeps each column's position, type and values (unknown names warn);
`drop` removes the named columns (unknown names are ignored); `reorder` is a
pure permutation that floats the named columns to the front (unknown names
ignored, duplicates deduped). All three round-trip through `to_source`.

### Composing them

Transforms chain in any order:

```
open events.csv
  |? status == "ok"
  distinct session_id
  |> user (bytes / 1024) as kib
  sort kib desc
  take 20
```

---

## 5. DAG: branch, merge, join

A "linear" pipe is just a degenerate DAG. To fan out and back in:

```
# branch.riv — tee one source into two filtered flows, then merge
Users:
    open users.csv
    -> Adults: |? age >= 20 ;     # a child scope continuing from Users
    -> Minors: |? age <  20 ;
;
Merged:
    Adults + Minors               # merge (union) of two named scopes
;
```

- `-> Child: body ;` — **branch (tee)**: every chunk is forwarded to the child.
- `A + B [+ C …]` — **merge**: union of the named streams.
- `A & B on key` — **inner join** on a key (use `on lkey:rkey` when the two
  sides name the key differently). Output = left columns + right columns (minus
  the join key; a name clashing with a left column is suffixed `_r`).
- **Composite keys:** `on k1 k2 …` joins on the column *tuple* — e.g.
  `A & B on country region` matches rows agreeing on both. Each key may be
  `lk:rk` for differing names (`on a x:y`). Works for every join kind below.
- `A &left B on key` — **left outer join**: every left row is kept; when no
  right row matches, the right columns are padded with type defaults (`0` /
  `0.0` / `false` / empty string).
- `A &right B on key` — **right outer join**: every right row is kept (the left
  columns padded with defaults). The join-key column keeps the right key, so an
  orphan right row never loses its key.
- `A &full B on key` — **full outer join**: every row from both sides; unmatched
  rows are padded on the missing side.

```
# inner join two CSVs on `id`
Users:  open users.csv ;
Orders: open orders.csv ;
Joined: Users & Orders on id  |> name amount  save out.csv ;

# left join: keep every user, even those with no order (amount → 0)
AllUsers: Users &left Orders on id  |> name amount  save out.csv ;
```

Reference scopes by the names you gave them. The CLI prints the whole graph.
Join is a blocking step (it buffers both inputs), like `sort`/`|#`.

---

## 6. Expressions

Used in `|?` predicates and `(…)` computed columns.

**Values**

| kind | examples |
|---|---|
| integer / float | `42`, `3.14` |
| string | `"JP"` (escapes: `\n \t \" \\`) |
| boolean | `true`, `false` |
| field of the current row | `age` (bare), `$_.age` (explicit) |
| deep / dynamic field | `$_..age` (recursive), `item("age")` (dynamic) |
| parent scope field | `$_:1.country` (`$_:0` = current, `$_:1` = parent …) |

**Functions**

- *string* — `upper(s)`, `lower(s)`, `trim(s)`, `len(s)` → int,
  `substr(s, start, len)`, `replace(s, from, to)`, `split_part(s, sep, n)`
  (1-based field), `concat(a, b, …)`.
- *predicates* (→ bool) — `contains(s, sub)`, `starts_with(s, p)`,
  `ends_with(s, p)`, `like(s, pat)`, `glob(s, pat)`, and (with `--features
  regex`) `regexp(s, re)`.
- *numeric* — `abs(x)`, `round(x)` (ties away from zero), `floor(x)`, `ceil(x)`;
  each returns an integer when the result is whole, else a float.
- *null-coalesce* — `coalesce(a, b, …)`: the first argument whose text is
  non-empty (the SQL/pandas null-coalesce).

```
|? contains(email, "@gmail")
|> (upper(name)) as NAME (len(name)) as nlen (substr(zip, 0, 3)) as area
|> (round(price * 1.1)) as gross (coalesce(nick, name)) as display
```

**Comparison** — `==  !=  <  <=  >  >=`
**Logic** — `and`, `or`
**Arithmetic** (inside parentheses) — `+  -  *  /  %`, with `* / %` binding
tighter than `+ -`; nest with parens.

```
|? country == "JP" and (score / age) >= 2.5
|> name (qty * price) as total (qty * price * 0.1) as tax
```

> Arithmetic operators are only tokenized **inside parentheses**, so paths like
> `open /tmp/a-b.csv` keep lexing as a single word outside parens. Wrap any
> computed expression in `( … )`.

**Type casts** — `expr:type` reinterprets a value's lane (`int`/`i64`,
`float`/`f64`, `str`/`string`, `bool`), binding tightest:

```
|? age:int >= 20            # compare a *string* column numerically
|> id (price:f64 * 1.1) as gross
|> (age:str) as age_text    # the add-property cast (3rd way to type a column)
cast age:int price:f64      # the `cast` verb: re-type columns in place
```

The **`cast COL:type [COL:type …]`** verb is sugar for re-typing named columns
in place (position and name kept), e.g. `cast age:int price:f64`. Unknown
columns warn and are skipped; it round-trips through `to_source` (type names
render canonically, `int` → `i64`).

Numeric arithmetic stays integer when both sides are integers (except `/`,
which is always float, like SQL/pandas). Strings are parsed best-effort to a
number where arithmetic needs one; division/modulo by zero yields NaN/0 rather
than crashing (continue-first).

---

## 7. Sinks (the tail of a flow)

| syntax | writes |
|---|---|
| `save PATH` | format from the extension (mirrors the sources; `.tsv`/`.tab` → tab-delimited; `.json` → JSON array; `.jsonl`/`.ndjson` → NDJSON) |
| `save PATH as FMT` | force the format (`csv` \| `tsv` \| `json` \| `jsonl` \| `ndjson`) |
| `writecsv PATH` / `writejson PATH` | explicit verbs (`writejson` = NDJSON) |
| `save stdout` / `save -` | write to standard output |
| `print` | capture for the on-screen preview |

```
… save out.csv
… save out.json              # a single JSON array: [{…},{…}]
… save out.jsonl             # NDJSON: one object per line
… save - as json             # JSON array to stdout (pipe-friendly)
… save out.tsv               # tab-delimited
```

A flow can read and write the same format ("write what you can read"): CSV/TSV,
JSON array and JSON Lines are all symmetric. **`as json` is a single bracketed
array**; **`as jsonl`/`.jsonl`** is one object per line (and what `writejson`
emits). Both stream in bounded memory; an empty result is `[]` (json) or no
lines (jsonl).

---

## 8. Lifecycle hooks (continue-first)

Rivus never crashes on bad input — malformed rows become events on a side
**error stream**, and the flow keeps running. You can react to that stream:

```
Import:
    open messy.csv
    |? age >= 20
    on error severity >= warning:
        transition degraded        # escalate the runtime mode
    ;
;
```

Hook form: `on EVENT [severity >= SEV] : ACTION ;` where `ACTION` is
`transition <mode>` | `log "message"` | `route <Label>`. Modes:
`normal degraded recovery isolation emergency`. Only `Fatal`-severity errors
halt the flow; everything else flows on.

---

## 9. One-liner cookbook

Rivus is built to be used like `awk`/`jq` — inline, in a pipe, or as a heredoc.

**Inline (`-c`)** — visualization to stderr, data to stdout:

```sh
# filter + project a CSV to stdout
rivus run -c 'U: open users.csv |? age >= 20 |> name age save stdout as csv ;'

# CSV → JSONL conversion (one object per line)
rivus run -c 'U: open users.csv save stdout as jsonl ;' > users.jsonl

# CSV → JSON array (a single [{…},{…}], pipe straight into jq)
rivus run -c 'U: open users.csv |? age >= 20 save - as json ;' | jq '.[].name'

# top-5 by a computed column
rivus run -c 'S: open sales.csv |> product (qty * price) as total sort total desc take 5 save stdout as csv ;'

# group + aggregate
rivus run -c 'G: open sales.csv |# region sum:amount avg:amount save stdout as csv ;'

# dedup then count distinct via group
rivus run -c 'U: open log.csv distinct user_id |# day save stdout as csv ;'
```

**Unix-filter shorthand.** A *transform-only* program (one that starts with a
pipe `|…` or a transform verb) is automatically wrapped to read CSV from stdin
and write CSV to stdout — so Rivus drops in like `awk`/`jq`, no scope needed:

```sh
cat data.csv | rivus '|? age >= 20 |> name age'   # filter + project
cat data.csv | rivus 'sort age desc'              # sort
cat data.csv | rivus 'describe'                    # summary
cat data.csv | rivus '|# country sum:amount'       # group + aggregate
```

(For non-CSV stdin or other sinks, write the full `open stdin as … / save …` form.)

**Pipe into other tools** (stdout stays clean):

```sh
rivus run -c 'U: open users.csv |? age >= 20 |> name age save stdout as jsonl ;' | jq .
cat users.csv | rivus run -c 'U: open stdin |? age >= 20 save stdout as csv ;'
```

**Heredoc** for a multi-line flow without a file:

```sh
rivus run - <<'RIV'
Report:
    open events.csv
    |? status == "ok"
    |> user (bytes / 1048576) as mib
    sort mib desc
    take 10
    save stdout as csv
;
RIV
```

**Peek at a huge file instantly** — a sink-less run is a *preview*: Rivus
samples the schema and shows the first rows in flat memory, even for a 15 GB
file (add a `save` to process every row):

```sh
rivus run -c 'B: open big.csv ;'        # instant head, ~10 MiB RAM
```

---

## 9b. Worked examples (the harder stuff)

Real pipelines that exercise the DAG, joins, grouping and cleaning together.
Each is a complete program — save it to a `.riv` file or pass it with `-c`.

**Enrich orders with customers, then revenue per (country, tier).** A composite
join feeding a multi-key group with several aggregates and a percentile:

```
Customers: open customers.csv ;        # id, country, tier
Orders:    open orders.csv ;           # cust_id, amount, status

Revenue:
    Orders &left Customers on cust_id:id   # keep every order; fill missing cust
    |? status == "paid"
    |> country tier (amount:f64) as amount
    |# country tier sum:amount avg:amount p90:amount count_distinct:cust_id
    sort sum_amount desc
    save revenue.csv
;
```

**Clean a messy export, then bucket and summarize.** Declared types, imputation,
a `case` bucket and a group — the kind of thing you'd reach to pandas for:

```
Clean:
    open raw.csv (id age:str score:str region:str)
    cast age:int score:f64                 # re-type the string columns
    fill region ffill                      # carry the last region over blanks
    fill score mean                        # impute missing scores with the mean
    |> id age region score
       (case when age >= 65 then "senior"
             when age >= 18 then "adult"
             else "minor" end) as band
    |# region band avg:score median:score std:score
    save out.json                          # a single JSON array
;
```

**Sessionize a log and rank within each user.** Branch a source, compute on each
side, and emit JSON for a dashboard — with live telemetry to a socket:

```
Events:
    open events.csv.gz                     # gzip input (needs --features gzip)
    |? status == "ok"
    |> user ts (bytes / 1048576.0) as mib
    sort user mib desc                      # user asc, mib desc within user
    |> user (round(mib)) as mib (concat(user, "@", ts)) as event_id
    save - as json
;
```
```sh
rivus run sessions.riv --telemetry-addr 127.0.0.1:9000   # stream metrics live
```

**Find IDs that match a pattern and normalize them.** `regexp` (feature-gated),
`replace`, `split_part`, `coalesce`:

```
Ids:
    open access.csv
    |? regexp(path, "^/api/v[0-9]+/")       # only versioned API routes
    |> (split_part(path, "/", 3)) as version
       (replace(path, "//", "/")) as norm_path
       (coalesce(user, "anon")) as who
    |# version who
    save stdout as csv
;
```

---

## 10. Performance notes

- **Streaming, bounded memory.** CSV sources and sinks stream; a 1.1 GB / 48 M-row
  file through `open |? age>=50 |> name age save out.csv` runs in **~10 MiB** of
  RAM (it does not load the file) at **~1.45× faster than DuckDB and ~40× less
  memory** (3.0 s vs 4.4 s / 407 MiB), ~3.8× faster than awk, ~10× faster than
  Python — see [`docs/BENCHMARKS.md`](BENCHMARKS.md).
- **Parallel by default.** A single-CSV file ≥ **8 MiB** with a `save` sink (incl.
  `save -`) is streamed across CPU cores automatically (newline-aligned
  byte-range workers → ordered output), still in bounded memory. On a 171 MiB
  filter that's ~1.6 s serial → **~0.4 s** parallel. Tune with
  `RIVUS_PARALLEL_MIN_BYTES` (bytes; `0` = always) or force serial with
  `RIVUS_NO_PARALLEL=1`. Compressed (`.gz`/`.zst`) sources can't be seeked, so
  they read serially.
- **Live progress.** An interactive `rivus run` prints a `… N rows  T s  R
  rows/s` line on stderr while a long job streams.
- **Machine-readable telemetry.** `rivus run … --json` emits per-node JSONL
  (rows in/out, busy_ms, rows/s, selectivity, mode) + errors + a summary to
  stderr (stdout stays clean data); `--telemetry-addr HOST:PORT` streams it to a
  TCP socket for a live viewer.
- **Live dashboard.** `rivus run … --tui` repaints an ANSI dashboard on stderr
  (per-node bars, rows/s, state) as the run streams. `rivus run … --serve [ADDR]`
  launches a tiny std-only HTTP server (default an ephemeral loopback port):
  open the printed URL for a live browser dashboard (`GET /`), poll `GET
  /snapshot`, or subscribe to `GET /events` (Server-Sent Events). Heavy drawing
  is in the browser; Rust ships only JSON snapshots — no extra dependencies.
- **The optimizer runs by default** (dedup sources, fuse filter+project,
  projection pushdown, filter pushdown into the reader). `rivus explain` shows
  exactly what it did and regenerates the source from the optimized IR.
  `--no-opt` turns it off; correctness is gated byte-for-byte by the
  `optimizer_equiv` tests.

---

## 11. Full CLI reference

```
rivus run     <program> [--chunk-size N] [--no-opt] [--json]  run a flow
rivus explain <program> [--no-opt]                    show DAG IR + optimizer report
rivus check   <program>                               parse only
rivus gen     <shape>   [--rows N --seed S --ratio R] write seeded data to stdout

PROGRAM:
  <file.riv>                 read the program from a file
  -c, --command <STRING>     pass the program inline as a string
  - | stdin                  read the program from stdin (heredoc)

GEN SHAPES (deterministic, seeded — for benches/demos, no awk needed):
  clean         well-formed id,name,age,score,country,active CSV
  error-heavy   ~ratio malformed rows (default 0.1) — continue-first stress
  mixed         ~ratio type-mixed cells (default 0.1)
  jsonl         one flat JSON object per line
```

```
# self-hosted bench: generate, then filter — no external tools
rivus gen clean --rows 1000000 | rivus '|? age >= 50 |> name age'
```

---

## 12. Quick grammar reference

```
program    = scope* ;
scope      = IDENT ':' body ';'  |  ':' body ';' IDENT? ;     (named / anonymous)
body       = source transform* ;

source     = 'open' PATH ('as' FMT)? 'noheader'? ('(' (IDENT (':' TYPE)?)+ ')')?
           | 'readcsv' PATH | 'readjson' PATH
           | 'readbin' PATH ('le'|'be')? ('packed'|'aligned')? '(' (IDENT ':' BINTYPE)+ ')'
           | 'stream' IDENT
           | IDENT (('+' IDENT)+ | ('&'('left'|'right'|'full')? IDENT 'on' KEY+))? ;  (merge / join)

transform  = ('|?' | 'where') expr (',' expr)*                                        (filter)
           | '|>' proj+                                       (project / compute)
           | '|#' IDENT+ ((AGG) ':' IDENT)*                    (group, 1+ keys)
           | ('take'|'limit'|'head') INT
           | 'sort' (IDENT ('asc'|'desc')?)+
           | 'distinct' IDENT*
           | 'describe'
           | 'dropna' IDENT* | 'fill' IDENT (VALUE | 'ffill' | 'bfill' | 'mean' | 'median')
           | 'rename' (IDENT IDENT)+ | 'drop' IDENT+ | 'reorder' IDENT+
           | 'cast' (IDENT ':' TYPE)+
           | '->' IDENT ':' body ';'                          (branch)
           | ('save' PATH ('as' FMT)? | 'writecsv' PATH | 'writejson' PATH | 'print')
           | 'on' EVENT ('severity' '>=' SEV)? ':' action ';' (hook)

proj       = IDENT ('as' IDENT)? | '(' expr ')' 'as' IDENT ;
expr       = or ; or = and ('or' and)* ; and = cmp ('and' cmp)* ;
cmp        = add (CMP add)? ; add = mul (('+'|'-') mul)* ; mul = primary (('*'|'/'|'%') primary)* ;
primary    = INT | FLOAT | STRING | 'true' | 'false' | '(' expr ')'
           | IDENT | '$_' field-tail | '$_:'N field-tail | 'item' '(' STRING ')'
           | FUNC '(' expr (',' expr)* ')'
           | 'case' ('when' expr 'then' expr)+ ('else' expr)? 'end' ;
FMT        = 'csv' | 'tsv' | 'json' | 'jsonl' | 'ndjson' ;
AGG        = 'sum' | 'avg' | 'min' | 'max' | 'std'
           | 'count_distinct' | 'nunique' | 'first' | 'last'
           | 'median' | 'p' DIGITS ;   (percentile, 0..=100)
CMP        = '==' | '!=' | '<' | '<=' | '>' | '>=' ;
```

That is the whole language as implemented today. Start from a one-liner in §9
and grow it.
