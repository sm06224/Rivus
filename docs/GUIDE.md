# Rivus — Syntax & Usage Guide

Rivus is a flow-oriented, DAG-native, streaming data runtime. You describe a
**flow** — sources → transforms → sinks — and Rivus executes it chunk by chunk,
in bounded memory, with the optimizer and live telemetry built in.

This guide is the practical reference: the full syntax, every operator, and a
gallery of copy-pasteable one-liners. For the design rationale see
[`docs/design/`](design/README.md); for install see the [README](../README.md).

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
optimizer).

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
| `open PATH as FMT` | force the format (`FMT` = `csv` \| `json` \| `jsonl` \| `ndjson`) |
| `open PATH noheader` | CSV with **no header row** — every line is data, columns are named `c0, c1, c2, …` |
| `open PATH (col[:type] …)` | **declare a schema**: name columns positionally (overrides the header / `c0…`) and optionally fix a column's type — `int`/`i64`, `float`/`f64`, `str`/`string`, `bool`. e.g. `open f.csv (id:int zip:str age)` keeps `zip`'s leading zeros |
| `readcsv PATH` | CSV, explicitly |
| `readjson PATH` | JSON / JSON Lines, explicitly |
| `readbin PATH [le\|be] [packed\|aligned] (name:type …)` | fixed-width binary records (a C-struct dump) |
| `open stdin` | read CSV (or `as FMT`) from standard input |
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

```
|? age >= 20
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

Partition by a key column and aggregate. A `count` column is always emitted;
each `func:col` adds one aggregate. Functions: `sum avg min max`.

```
|# country                        # → country, count
|# country sum:score avg:age      # → country, count, sum_score, avg_age
```

Output columns are named `count` and `<func>_<col>` (e.g. `sum_score`).

### `take` / `limit` / `head` — cap rows

```
take 100        # keep the first 100 rows, then stop
limit 100       # alias
head 100        # alias
```

### `sort` — order by a key

A stable sort over the whole stream (a blocking step). Ties keep source order.

```
sort age            # ascending (default)
sort age asc
sort score desc
```

### `distinct` — drop duplicates

Keep the first occurrence. With no keys the whole row is the dedup key;
otherwise only the named columns.

```
distinct                # unique rows
distinct user_id        # first row per user_id
distinct country region # first row per (country, region)
```

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

```
# inner join two CSVs on `id`
Users:  open users.csv ;
Orders: open orders.csv ;
Joined: Users & Orders on id  |> name amount  save out.csv ;
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

Numeric arithmetic stays integer when both sides are integers (except `/`,
which is always float, like SQL/pandas). Strings are parsed best-effort to a
number where arithmetic needs one; division/modulo by zero yields NaN/0 rather
than crashing (continue-first).

---

## 7. Sinks (the tail of a flow)

| syntax | writes |
|---|---|
| `save PATH` | format from the extension (mirrors the sources) |
| `save PATH as FMT` | force the format (`csv` \| `json` \| `jsonl` \| `ndjson`) |
| `writecsv PATH` / `writejson PATH` | explicit verbs |
| `save stdout` | write to standard output |
| `print` | capture for the on-screen preview |

```
… save out.csv
… save out.jsonl as jsonl
… save stdout as csv          # pipe-friendly
```

A flow can read and write the same format ("write what you can read"): CSV and
JSON Lines are symmetric.

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

# CSV → JSONL conversion
rivus run -c 'U: open users.csv save stdout as jsonl ;' > users.jsonl

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

## 10. Performance notes

- **Streaming, bounded memory.** CSV sources and sinks stream; a 1.1 GB file
  through `open |? … |> … save out.csv` runs in **~10 MiB** of RAM (it does not
  load the file). On a real ETL this matches DuckDB's wall time at ~40× less
  memory — see [`docs/BENCHMARKS.md`](BENCHMARKS.md).
- **Parallel.** Large files (> 256 MiB) with a `save` sink are streamed across
  CPU cores automatically (byte-range workers → ordered output), still in
  bounded memory.
- **Live progress.** An interactive `rivus run` prints a `… N rows  T s  R
  rows/s` line on stderr while a long job streams.
- **The optimizer runs by default** (dedup sources, fuse filter+project,
  projection pushdown). `rivus explain` shows exactly what it did and
  regenerates the source from the optimized IR. `--no-opt` turns it off.

---

## 11. Full CLI reference

```
rivus run     <program> [--chunk-size N] [--no-opt]   run and visualize a flow
rivus explain <program> [--no-opt]                    show DAG IR + optimizer report
rivus check   <program>                               parse only

PROGRAM:
  <file.riv>                 read the program from a file
  -c, --command <STRING>     pass the program inline as a string
  - | stdin                  read the program from stdin (heredoc)
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
           | IDENT (('+' IDENT)+ | ('&' IDENT))? ;            (merge / join over scopes)

transform  = '|?' expr                                        (filter)
           | '|>' proj+                                       (project / compute)
           | '|#' IDENT (('sum'|'avg'|'min'|'max') ':' IDENT)*  (group)
           | ('take'|'limit'|'head') INT
           | 'sort' IDENT ('asc'|'desc')?
           | 'distinct' IDENT*
           | 'describe'
           | '->' IDENT ':' body ';'                          (branch)
           | ('save' PATH ('as' FMT)? | 'writecsv' PATH | 'writejson' PATH | 'print')
           | 'on' EVENT ('severity' '>=' SEV)? ':' action ';' (hook)

proj       = IDENT ('as' IDENT)? | '(' expr ')' 'as' IDENT ;
expr       = or ; or = and ('or' and)* ; and = cmp ('and' cmp)* ;
cmp        = add (CMP add)? ; add = mul (('+'|'-') mul)* ; mul = primary (('*'|'/'|'%') primary)* ;
primary    = INT | FLOAT | STRING | 'true' | 'false' | '(' expr ')'
           | IDENT | '$_' field-tail | '$_:'N field-tail | 'item' '(' STRING ')' ;
FMT        = 'csv' | 'json' | 'jsonl' | 'ndjson' ;
CMP        = '==' | '!=' | '<' | '<=' | '>' | '>=' ;
```

That is the whole language as implemented today. Start from a one-liner in §9
and grow it.
