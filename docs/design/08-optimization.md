# 08. Optimization Pipeline

## 8.1 原則：DAG 変換であり、意味は保存する

最適化は IR(PlanGraph) を入力にとり IR を返す **graph transformation** である
（原則3/5、Observability §18）。semantic preservation は必須（Master §15）。
そして optimizer は不透明であってはならない（アンチパターン: opaque optimizer）。
各 rule は「適用したことと理由」を記録し、`explain --opt` で可視化する。

```
PlanGraph ──▶ [rule0] ──▶ [rule1] ──▶ ... ──▶ PlanGraph'   (+ 適用ログ)
                 │            │
                 └─ 各 rule は局所書き換え（部分グラフ → 部分グラフ）
```

## 8.2 最適化カタログ

| 最適化 | 内容 | 段階 |
|---|---|---|
| **operator fusion** | `filter`→`project` 等を1カーネルに融合し中間 chunk を消す | Phase 1 |
| **predicate/projection pushdown** | filter/列選択を source へ押し下げ、読み込み量を削減 | Phase 1 |
| **branch pruning** | 到達不能 branch・未使用 scope を削除 | Phase 1 |
| **chunk merging / coalescing** | 細かい chunk を結合し粒度を最適化 | Phase 1 |
| **reorder** | 選択率の高い filter を前へ（コストモデル） | Phase 1 |
| **SIMD lowering** | predicate を列ベクトル演算へ | Phase 1→2 |
| **adaptive chunk sizing** | 実行時にレイテンシ/スループットで chunk 粒度調整 | Phase 1（runtime） |
| **allocation reduction** | buffer 再利用が効く形へ整形 | Phase 1 |
| **speculative specialization** | 観測型に賭けた特化 + guard | Phase 2 |
| **hot path JIT** | hot な fused operator を機械語化 | Phase 2 |
| **zero-copy propagation** | 不要な複製を消す書き換え | Phase 1 |

## 8.3 fusion の例

```
変換前:                          変換後（fused）:
 source ─▶ filter(age>=20) ─▶ project(name)        source ─▶ scan+filter+project
            └ 中間 chunk 生成 ┘                              └ 1 パスで列だけ出力 ┘
```

fusion 規則：隣接する **stateless・1入力1出力** operator（filter/project/map）を
1つの合成カーネルに畳む。group/join/branch/merge は境界（fusion を跨がない）。
MVP のエンジンはこの境界条件を operator の性質（source/stateless/accumulate/
fan）として既に区別しているため、fusion はその分類に沿って実装できる。

## 8.4 pushdown の例

```
変換前: open(users.csv) ─▶ filter(country=="JP") ─▶ project(name,age)
変換後: open(users.csv, pushed_filter=country=="JP", pushed_proj=[name,age])
        → CSV reader が読みながら列を捨て・行を捨てる（IO/メモリ削減）
```

`OpenCsv` に `pushed_filter` / `pushed_projection` フィールドを追加し、reader が
それを尊重する（source operator が pushdown を「能力」として宣言する）。

## 8.5 コストモデルと reorder

- 各ノードに `selectivity`（rows_out/rows_in、telemetry から学習）と推定コストを
  付与。`NodeTelemetry::selectivity()` は既に MVP にある。
- 可換な filter 群を「選択率の低い（よく落とす）ものを先に」並べ替える。
- 初回は静的ヒューリスティック、2回目以降は前回 run の telemetry を使う
  **adaptive**（Polars lazy / DataFusion 的）。

## 8.6 lazy + streaming hybrid（中核思想4）

完全 lazy（Polars）でも完全 streaming（Flink）でもない。基本 streaming で、必要時
にのみ lazy 最適化・bounded buffering・dynamic graph rewrite・runtime fusion を
行う。

```
通常:        streaming（chunk が即流れる）
最適化対象:  グラフ全体を IR として保持 → 実行前/実行中に rewrite
materialize: Users! や group/join 境界でのみ実体化（明示）
```

dynamic graph rewrite：実行中の telemetry（desync, selectivity 乖離）に応じて
runtime が部分グラフを差し替える（Phase 2）。これも graph transformation。

## 8.7 観測可能性（optimizer は透明であること）

```
$ rivus explain --opt examples/save.riv
 applied: pushdown(filter country=="JP") into open#0
 applied: fuse(filter#1, project#2) -> scan_fp#1
 cost:    rows_read 8 -> (pushed) ; intermediate chunks 2 -> 0
```

最適化前後の IR を両方 `to_source()` できる（可逆性）。「何が・なぜ変わったか」を
常に source 差分として見せる。

### 段階表

| | 最適化 |
|---|---|
| MVP | なし（interpreter 直実行）。telemetry に selectivity は計測済み |
| 次 | fusion / pushdown / prune / reorder / coalesce / 適用ログ・explain --opt |
| 将来 | コストベース最適化 + 実行時 adaptive rewrite + speculative + JIT 連携 |
