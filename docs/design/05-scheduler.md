# 05. Scheduler

## 5.1 要件（Observability §8）

スケジューラは chunk-aware / mode-aware / branch-aware / backpressure-aware で
あり、flow continuity / degradation tolerance / adaptive recovery を優先する。

## 5.2 MVP スケジューラ（single-thread round-driven）

`rivus-runtime/src/engine.rs`。トポロジカル順に全ノードを1巡し、各ノードが
「1単位の仕事（pull 1 / process 1 chunk / finish）」を行う round を、進捗が
なくなるまで繰り返す。

```
state:
  in_q[node]               入力チャンクキュー（(from, chunk)）
  done[node]               完了フラグ
  upstream_remaining[node] 未完了の上流数
  mode                     現在の runtime mode
  errors                   error stream（append-only）
  telemetry[node]          観測カウンタ

round:
  for nid in topo_order:
     source?  -> pull 1 か枯渇で finish
     queue?   -> process 1 chunk
     上流完了? -> finish（flush）
     else     -> skip（上流待ち）
```

- **chunk-aware**: 1 訪問につき 1 chunk。chunk が最小スケジュール単位。
- **branch-aware**: `distribute` が後続複数なら clone して全 edge へ（fan-out）。
- **mode-aware**: `distribute` が emit 時に現在 mode を chunk へ stamp。error hook が
  `transition` すれば以降の chunk は新 mode を帯びる。
- **finish 伝播**: ノード完了で後続の `upstream_remaining` を減らす。0 になった
  ノードは入力キューが空になり次第 flush できる（group/join の境界）。

### 完了とデッドロック回避

source は必ず有限回で `pull → None`（枯渇）するため、全ノードがいずれ
`upstream_remaining == 0` に到達し flush・完了する。round で誰も進捗しなければ
ループ終了。循環は `topo_order()` が `None` を返してビルド時に弾く。

## 5.3 mode-aware スケジューリング（Phase 1 設計）

mode は scheduler の挙動そのものを変える（Observability §5）。

| mode | scheduler の振る舞い | buffering | retry |
|---|---|---|---|
| normal | 均等・throughput 優先 | 標準 | なし |
| degraded | error-flow を優先・priority 制御 | buffer 増 | aggressive + checkpoint |
| recovery | 破損 chunk の再経路を優先 | bounded | 再投入 |
| isolation | 該当 branch を隔離し他を継続 | branch 単位 | branch 限定 |
| emergency | error-flow を最優先・force synchronize | 最小 | 抑制 |
| halted | 停止（fatal のみ） | — | — |

実装方針：round 内のノード訪問順を mode に応じた **priority queue** にする。
degraded/emergency では error path（`EdgeKind::Error`）の下流と recovery scope を
先に処理する。

## 5.4 backpressure（Phase 1 設計）

各 Stream edge に bounded queue（容量 = chunk 数）と credit を持たせる。

```
upstream ──(credit?)──▶ edge[cap=4] ──▶ downstream
   ▲                                        │
   └──────────── credit 返却 ◀──────────────┘  consume 後に credit を戻す
```

credit が 0 の edge を持つ upstream は `pull`/emit を停止（停止理由を telemetry に
記録 → 可視化）。これにより implicit unbounded buffering（禁止）を構造的に防ぐ。
synchronized join のような本質的にバッファを要する箇所は、推定バッファ量を
**明示警告**する（Observability §11）：

```
WARN: synchronized join detected — estimated buffering: 4 chunks
```

## 5.5 並列化（Phase 1 設計, runtime responsibility）

並列はユーザ責任ではなくランタイム責任（Concurrency Philosophy）。

```
                 ┌─ worker0 ─┐
 source → split ─┼─ worker1 ─┼─ ordered merge → downstream
                 ├─ worker2 ─┤
                 └─ worker3 ─┘
```

- **pipeline parallelism**: ノード単位で別スレッド（stage 並列）。
- **data parallelism**: 1 chunk を split し複数ワーカで map（chunk split）。
- **IO overlap**: source の async 読みと下流処理を重ねる（Tokio）。
- work-stealing キューで負荷分散。adaptive chunk sizing で chunk 粒度を実行時に
  調整（小さすぎ→オーバーヘッド、大きすぎ→レイテンシ/メモリ）。

順序保証は sub-chunk の sequence 番号で復元。stateful operator（group/join）は
partition 単位に並列化し、partition 内は逐次。

### 段階表

| | Scheduler |
|---|---|
| MVP | single-thread round-driven / fan-out clone / mode stamp / finish 伝播 |
| 次 | priority(mode-aware) / bounded queue+credit / 並列ワーカ / adaptive chunk |
| 将来 | 分散スケジューラ / speculative scheduling / NUMA-aware 配置 |
