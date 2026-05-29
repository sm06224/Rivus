# 07. Memory Model

## 7.1 哲学

GC 依存ではなく、bounded heap / arena・chunk allocation / ownership-aware transfer /
copy minimization / reusable buffers を重視する。**Chunk の再利用を第一級概念**にする。

## 7.2 所有権モデル（ownership graph）

chunk は edge を通って「move（所有権移転）」するのが基本。fan-out（branch）でのみ
clone が必要になる。

```
 #0 source ──move──▶ #1 filter ──move──▶ #2 project ──move──▶ leaf 捕捉
                          (単一後続: そのまま move、複製ゼロ)

 #0 source ─┬─clone──▶ #1 filter   ← fan-out は後続数 -1 回 clone、
            └─move───▶ #2 filter      最後の1本だけ move（engine の distribute）
```

MVP の `distribute`（`engine.rs`）は「後続が1本なら move、複数なら最後以外を
clone」を実装済み。Arrow 化後は clone が **バッファ共有（Arc refcount++）** になり、
実コピーは消える。

## 7.3 arena / chunk allocation（Phase 1 設計）

per-chunk に小さな allocation を多数行うと断片化・GC 圧が出る。chunk 単位の
**arena** を使い、chunk が消えるとき arena ごと解放する。

```
ChunkArena
├─ columns 用の連続領域（lane ごと）
├─ string 用の bytes 領域（offset 参照）
└─ drop で一括解放（個別 free しない）
```

## 7.4 reusable buffers（buffer pool）

同形 chunk（同 schema・近い容量）の列バッファをプールし、`Drop` 時に返却・再利用
する。filter/map のような「入って出る」操作で alloc/free を往復させない。

```
                ┌──────── BufferPool（schema 形ごと）────────┐
 process 開始 ──┤ acquire(schema, cap) → 再利用 or 新規        │
 process 終了 ──┤ release(columns)     → ゼロ化せず容量保持で返却 │
                └────────────────────────────────────────────┘
```

bounded heap：プール上限と edge queue 上限（05 の credit）でヒープ総量を縛り、
implicit unbounded buffering（禁止）を物理的に防ぐ。

## 7.5 ownership-aware transfer の規則

- **consume**: sink（`SinkCsv`）は chunk を受け取り所有権を取り、外へは出さない
  （`process` が空 `Vec` を返す）。
- **forward**: merge/print は所有権をそのまま下流へ渡す。
- **transform**: filter/project は新 chunk を作る（将来は入力バッファを再利用）。
- **accumulate**: group/join は内部に貯め、`finish` でまとめて手放す
  （materialization 境界）。

この4分類が「どこで複製が起き、どこで再利用できるか」のオーナーシップグラフを
決める。

## 7.6 copy minimization の実測フック

telemetry に「複製回数 / 再利用ヒット率 / arena 使用量」を追加し（Phase 1）、
zero-copy propagation（優先順位2）が実際に効いているかを可視化する。観測できない
最適化はしない（アンチパターン回避）。

### 段階表

| | メモリモデル |
|---|---|
| MVP | move/clone 最小化（distribute）/ `Arc<Schema>` 共有 |
| 次 | chunk arena / buffer pool / bounded heap / copy 計測 telemetry |
| 将来 | off-heap / mmap / NUMA-local arena / 分散転送時の zero-copy serialize |
