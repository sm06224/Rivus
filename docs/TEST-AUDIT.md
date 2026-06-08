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
**Fix plan (null model #81).** Per-column missingness via a null bitmap on
`Column` (design 26): the reader marks an empty numeric cell *missing* instead of
`0`; `null`/`empty`/`0` become distinct (`Value::Null`). Large, cross-cutting
(core `Column`, reader, operators, aggregates, sinks, byte-identity), staged.
**Status: RESOLVED.** STEP 2-① (#105) landed `Column { data, validity }` and the
reader reads a blank/unparseable numeric cell as `null`. STEP 2-② made `dropna`
validity-aware (drops null rows on every lane), so this test is **un-ignored and
green** — `dropna age` now drops the blank-numeric rows. (filter predicates,
`fill`, group-by/distinct keys and sort are null-aware too.)

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

### BUG-D · `datetime("fmt")` is ignored in a cast / computed-column (only the reader schema works)
**Repro (Linux).** `open f.csv (ts:datetime("yyMMddHHmmss"))` works; but
`cast ts:datetime("yyMMddHHmmss")` ignores the format and treats the field as raw
epoch ticks (`260601120000` → year 10228), and `(ts:datetime("yyMMddHHmmss"))` is
a parse error. **Root cause.** `DataType::DateTime { unit }` carries no format —
only the reader keeps `dt_formats` (a side table on the source op), so cast/eval
have no format to parse with. **Fix (DESIGN confirmed, maintainer-ratified
2026-06-08; NO new type system).** The format's sole owner is the **schema
declaration** (reader schema / `dt_formats`, unchanged — fastest, exact text
path). The expression `cast` is a **different use** (change type mid-computation,
**no format**, source-aware): it must parse `Str → DateTime/Date/Time` correctly
(the same *meaning* as the reader; only the *path*/speed differs — same result by
location is the byte-identity contract, and the current divergence IS BUG-D).
Slice A: (1) make expr `cast` source-aware in `eval.rs` (`cast_value`/
`cast_column`); (2) **never-silent** cast failures (null + surfaced on the error
stream, serial == parallel, extensible to other lanes); (3) an **explicit format
in expression position** (`cast x:datetime("fmt")`) becomes a **never-silent
parse error** ("declare the format in the schema"); (4) `Expr::Cast` structure
unchanged (no `format` field) — `to_source` round-trip unchanged. Full design in
`docs/design/23-datetime-and-reshape.md` §23.6 (option B / new-type approaches
rejected). **Status: RESOLVED (Slice A).** The expression `cast` is source-aware
(`cast_value`/`cast_column` parse a `Str` → `DateTime`/`Date`/`Time` via the auto
formats), so `cast str:datetime` now matches the reader's exact path byte-for-byte
(pinned by `datetime_cast_in_expression_is_source_aware_BUG_D`). A non-null cell
that won't parse → `null` (continue-first) and is **surfaced** once per column on
finish (never-silent); in parallel the per-worker counts sum to the serial total
(`cast_datetime_failures_sum_serial_eq_parallel`). An explicit format in
expression position (`cast x:datetime("fmt")`) is a **never-silent parse error**
pointing at the schema (`datetime_format_in_expr_cast_is_rejected_BUG_D`); the
reader schema `(ts:datetime("fmt"))` is unchanged. `Expr::Cast` is structurally
unchanged (no `format` field) → `to_source` round-trip unchanged. Tracked
follow-ups: a source-adjacent cast → codec-schema pushdown (and named-schema
reuse). **Slice A-2 (done):** the scalar path now surfaces too — `eval`/
`eval_predicate` carry the `fails` accumulator (`eval_acc`/`eval_predicate_acc`,
threaded through `call_func`/`arith_value`/`compare_fast`), so a cast failure in a
`|?` predicate or a function argument is surfaced by `Filter`/`Validate`/
`FilterProject`/`ProjectExpr` (continue-first null; per-worker partials sum to the
serial total). Pinned by `scalar_cast_failures_surface_BUG_D_a2` and
`pred_cast_failures_sum_serial_eq_parallel`.

### BUG-E (RESOLVED) · a leading UTF-8 BOM on the flow *script* breaks parsing
**Repro.** A `.rivus` saved with a BOM → `unexpected character 'ï'` at line 1
(data CSV BOM is stripped, but the script wasn't). **Fix.** `rivus_parser::parse`
strips a leading `\u{FEFF}` before lexing (covers file / stdin / `-c` uniformly).
Test `leading_bom_on_flow_script_is_stripped` (green). Guide §2 updated.

### BUG-F (RESOLVED, fix (a) surface) · headerless + schema consumed the first data row silently
**Repro.** `open data.csv (id:int name:str)` (no `noheader`) over a file whose
first line is data → the first line is treated as a header and dropped (2 rows →
1). The drop was **silent** (never-silent violation). **Fix (a), maintainer-
ratified 2026-06-08.** The semantics are unchanged (a column-naming schema still
renames an existing header), but the consumption is now surfaced: when the
consumed first line **looks like data** under the declared schema — i.e. every
*typed* cell parses in its lane (`int`/`float`/`decimal`/`date`; a real header of
column-name strings would not) — a never-silent `Warn` is raised on the error
stream pointing at `noheader` as the remedy. Conservative both ways (an
all-text/untyped rename never false-warns; a real header is never flagged), and
fired identically on the serial and parallel readers (byte-identity of the error
stream). Spec `headerless_schema_surfaces_consumed_data_row_BUG_F` (green,
includes the no-false-positive real-header case). Guide §3 (en+ja) updated.
**Status: RESOLVED.**

### PERF-G (RESOLVED, hoist) · `sort` per-compare type dispatch
**Repro (1M rows, release).** sort id(int) 0.72 s / score(f64) 0.91 s / name(str)
1.17 s. **Root cause.** `cmp_rows` (`operators/transform.rs`) did a `has_nulls()`
check + `match col.data()` lane dispatch + random access on **every** comparison
(~20 M). **Fix (landed).** `make_cmp` resolves each key's lane + null state once
into a monotyped comparator; the `sort_by` loop does only the typed compare (and
a null branch only when needed). **Byte-identity preserved** (same order,
nulls-last/§26.2b, desc-reverses-the-whole-order, stable) — pinned by
`sort_nulls_last_asc_first_desc_byte_identical` and the existing chunk-size
sort tests. Sort-only Δ ≈ −6…−9 % (`docs/BENCHMARKS.md`).
**Follow-up: DONE (decorate-sort).** The dominant cost was cache misses on random
row access; the single-key path now extracts the key into a contiguous
`Vec<(key, idx)>` and sorts *that* (monomorphic, cache-coherent, no dyn call).
Sort-only Δ on top of the hoist: **−17 % f64**, −7…−8 % random int / str, ≈ flat
only on a pre-sorted int key (`docs/BENCHMARKS.md`). Byte-identity verified by
diffing full 1M-row outputs of the pre-/post binaries across every lane, the
error-heavy + mixed regimes, and the (unchanged) multi-key path; pinned by
`sort_f64_lane_with_nulls_orders_ascending_nulls_last` and
`sort_f64_with_nan_is_chunk_size_independent` alongside the existing sort tests.
Multi-key keeps the hoisted comparator (a composite memcomparable key is a
further follow-up).

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
| **dropna** | stress, stress/null | ✅ **BUG-A fixed** (null model 2-②): drops null on every lane incl. inferred-numeric; `dropna_drops_blank_numeric_rows_bug_a` un-ignored & green |
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

