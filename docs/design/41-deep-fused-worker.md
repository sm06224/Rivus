# 41 — Deep-fused worker: shape-selected monolithic pipelines（統括指示 2026-07-11）

Directive: format-agnostic speed is not negotiable — columnar, row-oriented,
dump or stream. Optimizing the parse engine alone is no longer the ceiling:
**fusing the transport, the reader and the operator pipeline into one deep,
shape-selected monolithic implementation is authorized**, provided every
existing invariant (byte-identity, continue-first, never-silent, bounded
memory) survives with proof.

This document fixes the design before code lands, because the fusion crosses
layers that today isolate our correctness arguments.

## 0. What the measurements say (10M standard, per 20 MB file, 4-core box)

The generic worker pipeline materializes columns at every op boundary:

```
decode → [Chunk] → reconcile → [Chunk] → cast → [Chunk] → probe(gather ALL
columns for 10M output rows) → [Chunk] → filter → project(rebuild coalesce
columns) → [Chunk] → group(Value per row per agg)
```

| stage | cost/file | of which pure materialization |
|---|---|---|
| decode | ~110 ms | — (already block-fused, slice 13) |
| reconcile | ~33 ms | lane widening only when misaligned (slice 12/14) |
| probe | ~51 ms | `gather_opt` of every column ×10M rows |
| project | ~28 ms | full column rebuild for coalesce |
| group | ~65 ms | `Value` per row per agg + key rebuild |
| open (pass 1) | ~210 ms CSV / ~540 ms JSONL | a second full scan of every byte |

Two structural facts follow:

1. **Between probe and group, no chunk ever needs to exist.** The shape
   detector already proves the exact op sequence; the only consumers of the
   probe/project output are the group's key builder and accumulators.
2. **Pass 1 and pass 2 walk the same bytes twice.** For JSONL that is
   2 × 606 MB of scanning — the single largest remaining cost anywhere.

## 1. Fusion stage A — the monolithic worker loop (`FusedReadGroup`)

For the detected `read → cast? → (⋈ broadcast)* → filter? → project? → group`
shape with **flat scalar schemas and scalar-only expressions**, the worker
runs ONE hand-written row loop instead of the generic op chain:

```
for each decoded row (straight off the block walk / ColBuilder lanes):
    cast lanes resolved once per file (identity = no-op; real cast = lane fn)
    build join key into reused buf → Fx lookup → for each right match:
        eval filter preds row-wise (the interpreter path that is ALREADY
        pinned identical to the kernel path)
        build group key into reused buf (projection exprs evaluated row-wise
        into reused scratch — coalesce/str concat only in stage A)
        observe aggs from the typed lanes directly (no Value)
```

No intermediate `Chunk`, no `gather_opt`, no projected columns, no `Value`
per row. Eliminates the materialization column above (~120 ms/file CPU) and
the group `Value` round trip.

**Selection (選択式)**: `eligible_read_group_flow` gains a stricter
`fused_eligible` check — every expr in filter/project must be in the fused
interpreter's supported set (start: column refs, literals, coalesce, cmp,
arithmetic on i64/f64 — grow by measurement). Anything else falls back to
the generic worker unchanged. The choice is recorded in `RunResult::strategy`
and `explain` (Observable First — never a silent engine swap).

**Proof obligations (all existing machinery):**
- `cmp` bit-identity of every 10M fixture (4 formats × parallel/serial)
  against the generic path, per slice.
- A property test: random flat chunks → fused vs generic worker → identical
  partial groups (extends `optimizer_equiv.rs` style).
- Continue-first: malformed rows already died in pass 1; key-path fails and
  cast fails must reach the SAME error-stream messages (counted, surfaced at
  the same nodes).

## 2. Fusion stage B — transport windows (mmap, bounded)

The reader still pays one kernel→user copy per byte per pass (fill_buf).
A `MmapTransport` (behind the same `FileTransport` seam) hands the block
walk **windows of a private read-only mapping** instead:

- Zero copy, zero carry (the mapping is contiguous — a line never straddles
  a window), simpler hot loop.
- **Bounded residency**: `madvise(SEQUENTIAL)` at map time and
  `madvise(DONTNEED)` behind the cursor every N MB keeps peak RSS at the
  window budget — the 12–16 MB story must survive (it is part of the brand;
  a naive mmap balloons VmHWM to file size × workers).
- Supply chain: `memmap2` (+`libc`) via the SUPPLY-CHAIN.md checklist, or a
  40-line direct `libc::mmap` wrapper — decided by the checklist, feature
  visibility `default` (policy v2).
- Fallback: any mmap failure (network FS, 32-bit, huge file) silently uses
  the existing BufReader path — same bytes, same output.

Expected: kills ~1.2 GB of copies on the JSONL standard (~2 × 606 MB),
~360 MB on CSV; measured before/after decides if it stays.

## 3. Fusion stage C — one-pass speculative scan+decode (JSONL first)

Pass 1 (infer) and pass 2 (decode) walk identical bytes. Fusing them needs
the global type answer before columns exist — so speculate per file:

- The worker scans AND decodes in one pass **assuming the running local
  schema**; on a type upgrade mid-file (int→float→str), only the affected
  column re-decodes from the start of the file (bounded by ncols upgrades,
  in practice 0–2), not the whole file.
- The chunk stream feeds the fused loop (stage A) only after the file's
  local schema is final — for the group shape the partial-group state is
  tiny, so a late upgrade can also just **discard the file's partial and
  re-run that one file** (rare, measured, never wrong).
- Union reconciliation stays: local→union lane coercion happens in the
  fused loop's per-file resolved lane table (exactly today's reconcile
  semantics, without the column pass).

This deletes the `open` phase as a separate scan (CSV −210 ms, JSONL
−540 ms wall on the standard) at the cost of the re-decode tail risk.
Stage C lands only with the discard-and-rerun safety net and the same cmp
gates.

## 4. Order and gates

A (fused loop) → B (mmap windows) → C (one-pass) — each lands separately on
the same local gate + fixture-cmp discipline as slices 12–18. Projected
combined effect on the group standard: CSV 939 → ~600 ms class (vs DuckDB
881), JSONL 1905 → ~1100 ms class (vs 1418) — both sides of 1×, all formats.

Ratification: stages A and B are engine-internal (no syntax, no IR change)
and ride the standing perf mandate; stage C changes pass structure but not
observable output — flagged to 統括/指揮 in #237 before landing regardless.
