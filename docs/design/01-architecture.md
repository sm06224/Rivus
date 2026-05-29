# 01. 全体アーキテクチャ

## 1.1 パイプライン全景

Rivus は「source を可逆 IR に変換し、最適化し、chunk として実行し、その実行を
観測・可視化し、再び source へ戻せる」一気通貫のシステムである。

```
Source (Unified Flow Syntax)
        │  rivus-parser (lexer + recursive descent)
        ▼
   AST  ────────────────────┐
        │  lower             │ (MVP は parse 中に直接 lower)
        ▼                    │
   DAG IR (PlanGraph)        │  ◄── すべての変換の単一の真実
        │  optimizer         │
        ▼                    │
   Optimized DAG IR          │
        │  scheduler         │
        ▼                    │
   Chunk Execution Engine    │
        │  telemetry tap     │
        ▼                    │
   Telemetry Layer ──────────┘
        │
        ├─▶ Visualization Layer (ASCII / TUI / SVG / `rivus live`)
        └─▶ Re-generated Source  (PlanGraph::to_source)
```

この図の各辺は「双方向」が原則である（原則5 IR Reversible）。最適化後の DAG から
source を再生成でき、リファクタリングは text rewrite ではなく **graph transformation**
として行われる（Observability §18）。

## 1.2 クレート構成

責務を最小カップリングで分割する。依存は一方向（下が上に依存しない）。

```
rivus-cli      ── shell / runner / 可視化のフロントエンド（bin: rivus）
   │ depends
   ▼
rivus-runtime  ── scheduler・operator・telemetry・mode・error stream
   │
   ▼
rivus-parser   ── source → DAG IR（rivus-runtime には依存しない）
   │
   ▼
rivus-ir       ── PlanGraph / Op / Expr / to_source（可逆）
   │
   ▼
rivus-core     ── Chunk / Column / Schema / Value / Mode / ErrorEvent / Severity
```

| crate | 主要型 | 役割 |
|---|---|---|
| `rivus-core` | `Chunk` `Column` `Schema` `Value` `Mode` `ErrorEvent` | 全層が共有するデータモデル |
| `rivus-ir` | `PlanGraph` `Node` `Edge` `Op` `Expr` `Hook` | DAG の表現と可逆 source |
| `rivus-parser` | `Lexer` `Parser` | 構文 → IR の lowering |
| `rivus-runtime` | `run()` `Operator` `OpCtx` `NodeTelemetry` `RunResult` | 実行エンジン |
| `rivus-cli` | `viz` | `run` / `explain` / `check` |

将来 `rivus-optimizer`（Phase 1）、`rivus-jit`（Phase 2）、`rivus-dist`
（Phase 3）を追加する。いずれも `rivus-ir` を入力にとり、`rivus-ir` を出力する
（IR-in / IR-out）か、`rivus-runtime` の `Operator` を実装する形にして、
中核に手を入れずに段階導入できるようにする。

## 1.3 データ平面と制御平面

ランタイムは二層で構成される（Observability §6/§7）。

```
        ┌─────────────────────────── Control Plane ───────────────────────────┐
        │  mode state machine / rerouting / isolation / checkpoint / sync 強制   │
        └───────────▲───────────────────────────────────────────┬─────────────┘
                    │ telemetry / error events                   │ 制御指示
        ┌───────────┴───────────────────────────────────────────▼─────────────┐
        │   Data Plane :  source → transform → branch/merge/join → sink         │
        │                 （chunk が流れる。通常時はここだけが動く）             │
        └───────────────────────────────────────────────────────────────────────┘
```

通常時は Data Plane だけが動く。error severity / buffer pressure / desync /
latency / corruption ratio が閾値を超えると Control Plane が介入し、mode を
escalate して scheduler・buffering・retry・resource alloc を変更する。
MVP では Control Plane は「mode 状態 + error hook による transition」に縮退して
実装されている（`rivus-runtime/src/engine.rs` の `apply_error_hooks`）。

## 1.4 想定技術スタックと段階導入

| 領域 | MVP（現状） | Phase 1+ |
|---|---|---|
| データ表現 | 自前 columnar `Vec` | **Apache Arrow** array（zero-copy / FFI） |
| 非同期 IO | 同期・全読み | **Tokio** + async stream（IO overlap） |
| クエリ最適化 | 自前 rule 群 | **DataFusion** の logical optimizer を IR 変換に流用 |
| JIT | なし（interpreter） | **Cranelift**（→必要なら LLVM/MLIR）で hot path 特化 |
| 並列 | single-thread | work-stealing scheduler（rayon 風 + chunk split） |

ポイントは「外部クレートは中核 trait の **裏側** に入る」こと。`Operator` /
`Chunk` の API を保ったまま、`Column` の実体を Arrow に差し替え、`run()` を
async 化できる。

## 1.5 設計のキー判断

- **IR を単一の真実にする**: AST を別途長く保持しない。最適化・実行・再生成は
  すべて `PlanGraph` に対して行う。
- **Operator 境界を薄く保つ**: `process(from, chunk, ctx) -> Vec<Chunk>` という
  最小 API に集約。これにより CSV→Arrow、interpreter→JIT を差し替え可能にする。
- **観測点を中核に置く**: telemetry はエンジン側で計測（operator に計測責務を
  持たせない）。operator はデータ変換に専念。

### 段階表

| | 内容 |
|---|---|
| MVP | 5 crate / 一方向依存 / interpreter / ASCII viz |
| 次 | optimizer・jit crate を IR-in/IR-out で追加、Arrow backing |
| 将来 | dist crate、control plane の本格分離、async IO |
