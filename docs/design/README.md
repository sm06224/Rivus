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

状態の凡例: **実装済** = 動作・テスト済 / **一部** = 中核は実装、残りは計画 /
**設計中** = 本文書をレビュー中（未着手） / **計画** = 設計のみ・未実装。
（**設計文書は archive しない** — 1 節 1 ファイルで残し、状態列で完了/置換を示す。）

| # | ドキュメント | 状態 | 内容 |
|---|---|---|---|
| 01 | [architecture](01-architecture.md) | 実装済 | 全体アーキテクチャとクレート構成 |
| 02 | [execution-model](02-execution-model.md) | 実装済 | Flow 実行モデル（DAG + push schedule） |
| 03 | [stream-chunk-model](03-stream-chunk-model.md) | 実装済 | Stream / Chunk / Column とメモリレイアウト |
| 04 | [pipeline-ir](04-pipeline-ir.md) | 実装済 | DAG IR・AST・式・可逆 source |
| 05 | [scheduler](05-scheduler.md) | 一部 | chunk/mode/branch/backpressure-aware スケジューラ |
| 06 | [type-system](06-type-system.md) | 実装済 | gradual + execution-lane typing |
| 07 | [memory-model](07-memory-model.md) | 一部 | arena / chunk 再利用 / ownership transfer |
| 08 | [optimization](08-optimization.md) | 実装済 | DAG 変換・fusion・pushdown・semantic preservation |
| 09 | [jit](09-jit.md) | 計画 | observed-type 特化と Cranelift JIT 戦略 |
| 10 | [shell-syntax](10-shell-syntax.md) | 実装済 | Unified Flow Syntax 文法 |
| 11 | [runtime-api](11-runtime-api.md) | 一部 | Runtime / 埋め込み API・query API |
| 12 | [plugin-abi](12-plugin-abi.md) | 計画 | プラグイン ABI（operator/source/sink） |
| 13 | [error-model](13-error-model.md) | 実装済 | continue-first error stream・mode system |
| 14 | [observability](14-observability.md) | 実装済 | telemetry・可視化・PKC Markdown |
| 15 | [benchmark](15-benchmark.md) | 実装済 | ベンチ戦略と回帰検知 |
| 16 | [mvp-scope](16-mvp-scope.md) | 実装済 | MVP の確定スコープと現状実装 |
| 17 | [distributed](17-distributed.md) | 計画 | 将来の分散アーキテクチャ |
| 18 | [io-formats-and-transports](18-io-formats-and-transports.md) | 一部 | 入出力フォーマット・トランスポートの拡張計画（csv/tsv/json/binary/gzip/zstd 実装済） |
| 19 | [interactive-and-shell](19-interactive-and-shell.md) | 一部 | 対話ビューア・実行アナリティクス GUI・シェル統合（`--tui`/`--serve` 実装済） |
| 20 | [computed-columns](20-computed-columns.md) | 実装済 | 計算列（算術式＋別名）と式モード字句解析 |
| 21 | [exact-decimal](21-exact-decimal.md) | 実装済 | 10進固定小数点レーン（COBOL的・厳密/並列安全）。`--exact`・`:decimal` でオプトイン |
| 22 | [gpu-backend](22-gpu-backend.md) | 計画 | GPU backend（feature-gate任意・CPU fallback・既定は依存ゼロ）。`--accel` |
| 23 | [datetime-and-reshape](23-datetime-and-reshape.md) | 一部 | 日時/日付/時刻レーン（実装済）・list/set/join 集計・pivot/unpivot（計画） |
| 24 | [validation](24-validation.md) | 一部 | バリデーション層（`\|!` warn/reject/halt 実装済・宣言ルール/quarantine 計画）。`#80`/`#81` を収斂 |
| 25 | [syntax-v2](25-syntax-v2.md) | 一部 | 構文 v2（fmt・コメント trivia・分岐 round-trip・`\| name` 再利用・`$x` 値ホール 実装済／signature・以降 計画）。Epic `#86`/`#87` |
| 26 | [null-model](26-null-model.md) | 一部 | null モデル（列ごと validity bitmap・null/empty/0 区別・述語/順序/伝播/集約セマンティクス・null 込み byte-identity・sink round-trip）。`#81`（BUG-A の本丸）。**STEP 2-①〜④ 実装済**：core validity・reader null 化・算術伝播・null 込み byte-identity（2-①）／filter null=false・dropna(BUG-A 解消)・fill・cast・sort nulls-last・group-by/distinct キー null 等価（2-②）／COUNT(*) vs COUNT(col)・first/last/distinct 非 null 整流＋operators.rs モジュール分割（2-③）。sink null round-trip（2-④）。並列マージの null 込み byte-identity（2-⑤）・join null キー非マッチ・`is null` 述語（§25.11）は計画 |

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
