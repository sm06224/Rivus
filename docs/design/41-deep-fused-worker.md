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

> **判定（2026-07-17・実測で不採用）**: 本節の設計どおり実装・計測した結果、
> CSV group で **mmap が全 reclaim 設定（DONTNEED 無効含む）で ~8% 負け** —
> 敗因は madvise ではなく soft page fault 経路そのもの（4KiB 粒度、cgroup 箱）
> が 256KiB buffered copy（L2 常駐の再利用バッファ）より高いこと。Stage C で
> 全経路が 1 パスになった今、ページ再利用が無く zero-copy の勝ち筋が消えた。
> 詳細と再訪条件は BENCHMARKS.md「Negative result: mmap windows」。

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

## 5. Stage C refined（2026-07-18 の設計検証 — 実装前に固定）

素朴な投機（sample 推論→投機 decode→矛盾で file 再走）は**汚れデータ標準で
敗北する**: 矛盾ファイルの full スキーマが union を拡幅すると、他ファイルの
投機 partial は「狭い union」前提で無効化され全体再走 — 標準 fixture は
設計上汚れを含むため、常に「今日のコスト＋投機の無駄」になる。

### 生き残る形: 局所再走の等価条件

矛盾ファイル F だけを正準二パスで再走し、**他ファイルの投機 partial を保持**
できる条件（C-eq）:

union が列 c を W に拡幅したとき、非矛盾ファイルの partial が正準実行と
byte-identical であるのは、c が次のいずれかを満たす場合に限る:

1. **c は group キーとしてのみ消費**: キー符号化は Display 経由なので、
   狭レーン直接（I64 の桁）と W 経由（Str の同桁文字列）は**同一バイト**。
   key_parts も同様。
2. **c は集約前に明示 cast で正規化**: 狭レーンに適合したセルに限り、
   narrow-direct == widen-then-cast が成立（int 往復・f64 Display 往復の
   正確性）。#239 第14弾の「押し下げ不可」と矛盾しない — あちらは全セル、
   こちらは**狭レーン適合済みセルのみ**が対象なので切り捨てフォールバックの
   差分が発生しない。
3. 上記以外（cast 無しで集約に入る等）→ C-eq 不成立 → **全体を正準二パスへ
   フォールバック**（正しさは常に保持、速度だけ今日並み）。

### 検出器の完全性

投機の妥当性 = 「sample スキーマで decode して非空 parse 失敗ゼロ」。
証明: 無矛盾 ⇒ 全セルが sample レーンに適合 ⇒ full 推論も同型に解決
（格子の上限一致）。**例外は Bool レーン**（`t=="true"` は失敗を発しない
ため "maybe" を無音で偽に折る）— sample に Bool 列を含むファイルは投機
不適格（二パスへ）。Str はレーン頂点なので常に安全。

### 期待値（10M標準）

sample 開 ~2ms/file×9 ＋ 投機 decode（今日の decode と同額）＋ 汚れ2ファイル
の局所再走（~2×110ms CPU）で、open の全走査 210ms（CSV）/320ms（JSONL）
wall がほぼ消える。C-eq は標準 flow（amount は cast:int 済み・キーは
country/region/category）で成立する。

### 実装順

C-1: CSV group driver に C-eq 判定＋sample 開＋矛盾検出＋局所再走。
C-2: sink driver・JSONL。C-3: 統括へ実測報告（本節が事前提示を兼ねる —
統括の「最終段まで一気に」指示 2026-07-18 に基づき着手）。

### C-1 実装で確定した精密化（2026-07-17 着地）

- **数値レーン間の拡幅（i64→f64）は C-eq 不成立**: 2^53 超の i64 は f64
  経由で Display が変わるため、条件1（Display 同一）も条件2（narrow-fit
  cell の cast 等価）も破れる。再走時に union′ が **Str 以外**へ拡幅した
  列を検出したら並列 driver ごと放棄して正準直列へ（R3b で恒久固定）。
  →Str 拡幅のみ局所再走で partial 保持。
- **C-eq 静的ゲート `stage_c_eligible`**: fused shape 前提で、(a) cast 対象
  は I64/F64/Str のみ（temporal/decimal cast は生セルを消費するため不適格）、
  (b) filter 述語の列は**cast 済みのみ**（cast は述語より前）、(c) 集約入力
  は cast 済み or `count`（null 性はレーン非依存）、(d) キーは bare /
  coalesce(col,"lit") / StrLit。右起源列の静的判別は不可能なので全列を
  左起源とみなす（保守的・常に安全、外れても正準に落ちるだけ）。
- **arity 不正行の在流カウント**: sample 開は pass 1 を走らせないため
  `CsvChunker::count_stream_bad` が stream 中に計数（preview 窓の再デコード
  分はリセットして二重計数を防ぐ）。pass 1 と同一基準（空行スキップ後の
  列数不一致のみ）— 単体テストで pass 1 の計数と一致を固定。
- **Observable First**: 投機が 1 ファイルでも発動したら strategy が
  `parallel read group-by (per-file workers, speculative open)` になる。
  圧縮/JSONL/Bool-sample はファイル単位で正準へフォールバック（suffix 無し）。
- **実測（10M CSV group 標準・4 コア箱・同日 interleave）**: open 210ms→
  2-3ms、wall 945ms→**799ms**（旧バイナリ比 −146ms）、DuckDB 同条件 943ms
  → **0.85×**（初の明確な DuckDB 超え）。peak RSS 9.5MB（従来 11-16MB より
  低下 — pass 1 バッファ消滅）。全 4 経路（plain/serial/gz/jsonl/ETL）
  byte-identical、汚れ標準の再走 0 件（arity 汚れは矛盾ではない）。
