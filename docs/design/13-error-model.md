# 13. Error Model（Continue-First）+ Mode System

## 13.1 原則

エラーは「停止原因」ではなく「観測可能なイベント」。throw-first / stack
unwinding / immediate termination を採らない（原則2）。エラーは side-channel の
**error stream** を流れ、main flow は継続する。

## 13.2 Severity ladder

`rivus-core/src/error.rs`：`Ord` 派生で `severity >= warning` を単純比較に。

```
Info < Warn < Recoverable < Critical < Fatal
                                         └─ これのみグラフを停止（mode=Halted）
```

| severity | 既定動作 | 例 |
|---|---|---|
| Info | 記録のみ | replay 履歴なし |
| Warn | 記録 + hook 評価 | projection で未知フィールド（passthrough） |
| Recoverable | 記録 + hook + recovery 経路候補 | 壊れた行 skip / decode 置換 |
| Critical | 記録 + isolation 候補 | sink 書き込み失敗 |
| Fatal | グラフ停止 | source が開けない |

## 13.3 エラーの粒度（Observability §4）

```rust
enum ErrorScope { Item, Chunk, Branch, Graph }
struct ErrorEvent { severity, scope, message, node: Option<String>, chunk_id: Option<u64> }
```

- **Item**: 1行（壊れた CSV 行）
- **Chunk**: chunk 単位（schema 不一致）
- **Branch**: 分岐単位（join 未確立）
- **Graph**: グラフ全体（source open 失敗）

非ブロッキング・遅延・観測可能を旨とする。

## 13.4 main flow と error flow の分離

```
 main flow :  ──c0──▶──c1──▶──c2──▶ ...（継続）
                       │ item error
 error flow:           └────▶ [ErrorEvent] ──▶ on error hook ──▶ Errors scope へ route
```

`on error: Errors` は error を別 flow（`Errors`）へ流す。error stream は
**graph-level**：あるスコープの `on error` hook はフロー全体の新規エラーに反応
できる（MVP 実装 `engine.rs::apply_error_hooks` は全ノードの hook を評価）。

## 13.5 Mode System（runtime 状態機械）

```
            error severity↑ / buffer pressure↑ / desync↑ / corruption↑
   normal ───────────────────────────────────────────▶ degraded
      ▲                                                    │
      │ 安定化                                              ▼
  (recover 完了)◀── recovery ◀── corruption ── degraded ──▶ isolation
                                                            │
                                                            ▼
                                                        emergency ──(fatal)──▶ halted
```

| mode | 役割 | scheduler 影響（05） |
|---|---|---|
| normal | 通常 | throughput 優先 |
| degraded | 劣化継続 | priority 制御 / buffer 増 / aggressive retry / checkpoint |
| recovery | 破損回復 | damaged-chunk reroute 優先 |
| isolation | branch 隔離 | 該当 branch を切り離し他を継続 |
| emergency | 危機 | error-flow 最優先 / force synchronize |
| halted | 停止 | fatal のみ |

mode は chunk meta に stamp され（観測可能）、`transition` hook で escalate する。

```
on error severity >= warning:
    transition degraded
;
```

MVP で動作確認済み（`recover.riv` → final mode: degraded）。

## 13.6 Control Plane（異常時の制御）

mode が normal を外れると Control Plane が介入（01 §1.3）：rerouting / isolation /
prioritization / checkpointing / synchronization 強制。MVP は mode 状態 + hook
transition に縮退。Phase 1 で `RuntimeHandle`（11）から制御 API を公開。

## 13.7 structured recovery

```
Import:
    open telemetry.bin
    on error severity >= recoverable:
        transition recovery
    ;
    on recovery:
        reroute damaged-chunks
    ;
;
```

recovery は runtime state。`on recovery` hook はその state に入ったとき発火し、
破損 chunk（`ChunkMeta::corrupt`）を別経路へ流す。MVP は hook を IR 保持（実行は
Phase 1）。

### 段階表

| | Error / Mode |
|---|---|
| MVP | error stream / severity / scope / `on error`→`transition`（graph-level）/ Halted |
| 次 | error→`Errors` routing 実体 / recovery・isolation の実行 / control plane API |
| 将来 | 分散時の部分障害隔離 / checkpoint からの replay 回復 / SLA 連動 escalation |
