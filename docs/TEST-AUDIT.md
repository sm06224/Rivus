# Test-design audit & fix plan (2026-06-03)

Standing-request audit: confirm the test design across the GUIDE feature set, add
the missing tests, and **plan** (not implement) the fixes for the confirmed bugs.
Maintainer report: bad datetime / `dropna` behaviour. Reproduced below.

## 1. Confirmed bugs (reproduced) — fixes are PLANNED only

### BUG-A · `dropna` is blind to a blank in an inferred-numeric column
**Repro** (`id,age,name` with blank `age` on rows 2,4):
```
open dn.csv               dropna age      # age inferred i64  → keeps ALL rows (2,4 show age=0)
open dn.csv (age:str)     dropna age      # → correctly drops rows 2,4
```
**Root cause.** A blank numeric cell is parsed to `0` at read time (no null
representation), so by the time `dropna` runs the "missing" is indistinguishable
from a real `0`. `dropna` only works on `:str` columns, where blank stays `""`.
This is the #bugreport ①⑤ / §24 nullable-column gap.
**Fix plan (null model).** Introduce per-column missingness (a null bitmap on
`Column`, or a sentinel tracked at parse): the reader marks an empty numeric cell
*missing* instead of `0`; `dropna`/`Required` validators test missingness;
`null`/`empty`/`0` become distinct (`Value::Null`). Large, cross-cutting (core
`Column`, reader, operators, aggregates, sinks, byte-identity). Belongs to the
validation-layer epic (§24); design doc + sign-off before code. Until then the
GUIDE must state `dropna` only sees blanks in **text** columns (see §3).

### BUG-B (RESOLVED #92) · datetime / date / time are never auto-inferred
**Repro.** `open f.csv` over an ISO-8601 `ts` column → the column stays `Str`
(works in a filter only by lexicographic luck); only an explicit
`(ts:datetime)` rides the datetime lane. `Flags::resolve` (`csv.rs`) yields only
`I64`/`F64`/`Bool`/`Str`.
**Fix plan.** Extend the inference `Flags` to also probe the temporal lanes:
track "all cells parse as datetime (AUTO_FORMATS) / date (`yyyy-MM-dd`) / time
(`HH:mm:ss`)" and have `resolve()` pick the temporal type when every non-empty
cell matches and at least one is unambiguously temporal (avoid mis-inferring a
plain integer column as a date). Must stay sample-inference-safe and
byte-identical to a declared read. Add A4 widening telemetry for the new lanes.

### BUG-C (RESOLVED #93) · AUTO_FORMATS rejects fractional-second / timezone ISO datetimes
**Repro.** `(ts:datetime)` over `2024-06-03T14:30:00.5`, `…Z`, `…+09:00` → each is
reported as a parse failure and defaulted to epoch (the count *is* surfaced —
never-silent works — but the value is lost).
**Fix plan.** Add the common ISO-8601 variants to `DateTime::AUTO_FORMATS` /
the format matcher: fractional seconds (`.f`…`.fffffffff`, truncated to the
column unit) and a trailing `Z` / `±HH:mm` offset (normalised to UTC ticks). Keep
each new format equivalence-tested against an oracle. (Sub-second needs the lane
at a sub-second `unit`; today datetime is `Sec` MVP — pair with the unit work.)

## 2. Coverage map (GUIDE feature → tests → status)

| GUIDE area | tests | status |
|---|---|---|
| filter `\|?` (incl. comma=AND, kernel path) | stress, optimizer_equiv | ✅ |
| project / computed cols `\|>` | stress, parser | ✅ |
| group `\|#` (sum/avg/min/max/std/distinct/first/last/pct, multi-key) | stress | ✅ |
| **validate `\|!`** (warn/reject/halt, parallel count) | stress, parser | ✅ (new) |
| join (`&`/left/right/full, composite key) | stress | ✅ |
| sort (multi-key), distinct, take/head | stress | ✅ |
| rename/drop/reorder/cast | stress, parser | ✅ |
| string fns (upper/…/replace/split_part/concat/like/glob/starts/ends) | stress | ✅ |
| numeric fns (abs/round/floor/ceil/coalesce, case) | stress | ✅ |
| decimal lane | stress, value | ✅ |
| datetime lane (read/fn/trunc/format/groupby/parallel) | stress | ✅ declared; ✅ auto-infer (#92), ✅ frac/TZ (#93) |
| **date / time lanes** (#58) | stress, parser, core | ✅ (auto-infer #92) |
| **dropna** | stress (str only) | ⚠️ **numeric blind (BUG-A)** — only `:str` tested |
| fill (value/ffill/bfill/mean/median) | stress | ✅ |
| sinks (csv/tsv/json/jsonl, stdout) | stress, cli | ✅ |
| sources (csv/tsv/jsonl/binary/stdin, declared/noheader) | stress, cli | ✅ |
| compression (gzip/zstd) | stress (feature-gated) | ✅ |
| parse-failure surfacing (all lanes) | observability, stress | ✅ |
| observability (`--json`/`--tui`/`--serve`, telemetry) | observability, cli | ✅ |
| optimizer (dedup/fuse/pushdown) | optimizer_equiv, optimizer | ✅ |
| parallel byte-identity (serial==parallel==chunk-size) | stress | ✅ |

## 3. Added tests (this change)
- **Executable bug specs** (marked `#[ignore]` so the gate stays green; un-ignore
  with the fix): `dropna_drops_blank_numeric_rows_BUG_A`,
  `datetime_auto_inferred_without_declaration_BUG_B`,
  `datetime_parses_fractional_and_timezone_BUG_C`. Each asserts the *intended*
  behaviour and is the acceptance test for its fix.
- Coverage fills where the matrix was thin are tracked as follow-ups (the working
  areas above already have oracle + chunk-size + parallel tests).

## 4. GUIDE accuracy follow-up (doc-only, allowed now)
The GUIDE's `dropna` section must note it currently sees blanks only in **text**
columns (declare `:str`) until the null model lands — otherwise it reads as a bug.
Tracked here; applied alongside the BUG-A fix design.

## 5. Relationship to existing issues (for reviewer confirmation)
- **BUG-A** = **#81** (null-column model) almost verbatim — same `dropna`×cast
  repro, same root (parse-failure/blank → 0, not null). The fix is #81's null
  bitmap; `dropna`/`Required` ride it (and the `required` validator of #83/#82).
  #81 already lists interim mitigations (`--on-parse-error` strict, a
  dropna×cast **lint** in `explain`/`check`). → BUG-A needs **no new issue**;
  this audit just adds the executable acceptance test for it.
- **BUG-B** (datetime/date/time **not auto-inferred**) — **no existing issue**.
  #58 added the subtypes and #56 is the time-series epic, but neither covers
  *schema inference* of temporal columns from CSV. Candidate: a new #56
  sub-issue, or a #58 follow-up.
- **BUG-C** (AUTO_FORMATS lacks fractional-second / `Z` / `±offset`) — **no
  existing issue**. Closest is #54 (DateTime lane) / #58. Candidate: a small new
  issue (or #58/#54 follow-up). Note sub-second needs a sub-second datetime
  `unit` (today `Sec` MVP), so it couples to the unit work flagged in #58's
  Column::Time note.

**Questions for the reviewer** (this PR is docs+tests only — fix is plan-only):
1. Confirm BUG-A is owned by #81 (so this audit's spec attaches there, no new issue)?
2. BUG-B and BUG-C have no tracking issue — file them as new #56 sub-issues, or
   fold into #58/#54? Which, and what priority vs the #56 windowing roadmap?
3. Is the per-feature coverage matrix (§2) missing anything you'd want pinned
   before the #56/#82/#86 epics build on top?

