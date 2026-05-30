# Rivus 設計ドキュメント

> Rivus — flow-oriented / DAG-native / continue-first / observable-first な
> ストリームネイティブ実行基盤。

このディレクトリは、3つの仕様文書（Unified Flow Syntax v1 / Runtime・
Observability Requirements v0.2 / Master Implementation Prompt）を統合した
**実装可能な設計**である。抽象論で終わらせず、`crates/` 配下の MVP 実装に
直結する粒度で記述する。

## 絶対原則（物理法則）

設計判断ではなく「破ってはいけない制約」として全ドキュメントを貫く。

1. **Everything is Flow** — function / filter / scriptblock を分離せず、すべて Scope + Flow に統一する
2. **Continue First** — エラーは停止原因ではなくイベント。デフォルトは継続
3. **DAG Native** — line pipeline を禁止し、すべてをグラフとして実行する
4. **Observable First** — telemetry は後付けではなく中核。runtime は必ず可視化可能
5. **IR Reversible** — `source ⇄ DAG IR ⇄ optimized IR ⇄ source` で意味を保持
6. **Chunk Native** — item ではなく chunk を基本単位とし、SIMD を前提にする
7. **Execution-aware typing** — 型はメモリ表現ではなく実行経路（lane）である
8. **Text is stream** — string は完成物ではなくデコード継続ストリームである

## 優先順位（速度だけを優先しない）

1. Stream correctness
2. Zero-copy propagation
3. Backpressure safety
4. Pipeline composability
5. Optimization visibility
6. Raw benchmark speed

## アンチパターン（禁止）

hidden full materialization / implicit unbounded buffering / string-only pipeline /
hidden serialization / opaque optimizer / runtime magic without observability。

## ドキュメント一覧

| # | ドキュメント | 内容 |
|---|---|---|
| 01 | [architecture](01-architecture.md) | 全体アーキテクチャとクレート構成 |
| 02 | [execution-model](02-execution-model.md) | Flow 実行モデル（DAG + push schedule） |
| 03 | [stream-chunk-model](03-stream-chunk-model.md) | Stream / Chunk / Column とメモリレイアウト |
| 04 | [pipeline-ir](04-pipeline-ir.md) | DAG IR・AST・式・可逆 source |
| 05 | [scheduler](05-scheduler.md) | chunk/mode/branch/backpressure-aware スケジューラ |
| 06 | [type-system](06-type-system.md) | gradual + execution-lane typing |
| 07 | [memory-model](07-memory-model.md) | arena / chunk 再利用 / ownership transfer |
| 08 | [optimization](08-optimization.md) | DAG 変換・fusion・pushdown・semantic preservation |
| 09 | [jit](09-jit.md) | observed-type 特化と Cranelift JIT 戦略 |
| 10 | [shell-syntax](10-shell-syntax.md) | Unified Flow Syntax 文法 |
| 11 | [runtime-api](11-runtime-api.md) | Runtime / 埋め込み API・query API |
| 12 | [plugin-abi](12-plugin-abi.md) | プラグイン ABI（operator/source/sink） |
| 13 | [error-model](13-error-model.md) | continue-first error stream・mode system |
| 14 | [observability](14-observability.md) | telemetry・可視化・PKC Markdown |
| 15 | [benchmark](15-benchmark.md) | ベンチ戦略と回帰検知 |
| 16 | [mvp-scope](16-mvp-scope.md) | MVP の確定スコープと現状実装 |
| 17 | [distributed](17-distributed.md) | 将来の分散アーキテクチャ |
| 18 | [io-formats-and-transports](18-io-formats-and-transports.md) | 入出力フォーマット・トランスポートの拡張計画 |
| 19 | [interactive-and-shell](19-interactive-and-shell.md) | 対話ビューア（Out-GridView 相当）・実行アナリティクス GUI・シェル統合 |
| 20 | [computed-columns](20-computed-columns.md) | 計算列（算術式＋別名）と式モード字句解析 — 次の実装本丸 |

## 段階設計（MVP → 最適化 → JIT/分散）

```
Phase 0  MVP            : Parser → DAG IR → single-thread chunk runtime → telemetry → ASCII viz   ← 現状ここ
Phase 1  Optimization   : DAG rewrite (fusion/pushdown/branch-prune) + 並列スケジューラ + Arrow backing
Phase 2  JIT            : observed-type specialization → Cranelift で hot predicate/projection を JIT
Phase 3  Distributed    : graph partition → shuffle → 複数 worker・control plane の分散化
```

各ドキュメントの末尾に「**MVP / 次 / 将来**」の段階表を置く。

## 現状の実装（動く MVP）

```
crates/
  rivus-core     Chunk / Column / Schema / Value / Mode / ErrorEvent
  rivus-ir       PlanGraph(DAG) / Op / Expr / to_source()（可逆）
  rivus-parser   Unified Flow Syntax → DAG IR（lexer + recursive descent）
  rivus-runtime  単一スレッド chunk 実行エンジン / operators / telemetry
  rivus-cli      `rivus run | explain | check`（ASCII 可視化つき）
examples/        *.riv サンプル + users.csv
```

```sh
cargo test           # 11 tests
cargo run -p rivus-cli -- run     examples/branch.riv
cargo run -p rivus-cli -- explain examples/branch.riv   # IR + 再生成 source
```
