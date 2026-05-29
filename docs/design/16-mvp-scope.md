# 16. MVP Scope

## 16.1 確定スコープ（Master §17）

```
Parser → DAG IR → Single-thread runtime → Chunk execution → Basic telemetry → Simple viz(ASCII)
```

最初のゴール：**「動く DAG フローとその可視化」**。これは達成済み。

## 16.2 実装済み（このリポジトリで動く）

| 領域 | 実装 | 場所 |
|---|---|---|
| データモデル | Chunk / Column / Schema / Value / Mode / ErrorEvent | `rivus-core` |
| DAG IR | PlanGraph / Op / Expr / Hook / `to_source()`（可逆） | `rivus-ir` |
| Parser | Unified Flow Syntax → IR（lexer + recursive descent） | `rivus-parser` |
| 実行エンジン | single-thread push scheduler / chunk 粒度 / continue-first | `rivus-runtime/engine.rs` |
| Operator | open(csv) / filter / project / group / merge / join(stub) / sink(print,csv) | `rivus-runtime/operators.rs` |
| 式評価 | row-wise interpreter（Tier0 基準実装） | `rivus-runtime/eval.rs` |
| Telemetry | per-node rows/chunks/errors/busy/mode | `rivus-runtime/telemetry.rs` |
| 可視化 | ASCII execution graph + table + error stream | `rivus-cli/viz.rs` |
| CLI | `run` / `explain` / `check` | `rivus-cli` |
| 例 | adults / branch / group / recover / save | `examples/` |
| テスト | 11 tests（core3 / ir2 / parser3 / runtime3） | 各 crate |

## 16.3 動作する中核思想の対応

| 原則 | MVP での現れ |
|---|---|
| Everything is Flow | scope=node、operator が単一 `Op` enum に統一 |
| Continue First | 壊れた行 skip + warning、fatal のみ停止、mode escalation |
| DAG Native | branch(tee)+merge を DAG として実行（線形でない） |
| Observable First | telemetry + ASCII viz + error stream 表示 |
| IR Reversible | `explain` が IR から source 再生成 |
| Chunk Native | columnar Chunk が最小実行単位、`--chunk-size` で粒度可変 |
| Execution-aware typing | DataType=lane（i64/f64 集約）、Access タグ |
| Text is stream | 不正行は warning + 継続（CSV 層） |

## 16.4 既知の MVP 限定（設計は済み、実装は次段階）

- **join 実行**: IR/source は完備、runtime は left 転送 + info（`operators.rs::Join`）。
- **group**: count のみ（sum/avg は Phase 1）。materialization 境界として実装。
- **map block / scope stack / deep / dynamic**: parse 済み、評価は flat 解決。
- **mode 定義・recovery/isolation 実行**: hook は IR 保持、transition のみ実行。
- **CSV source**: 全読み materialize（streaming reader は Phase 1）。引用は簡易対応。
- **並列・backpressure credit・JIT・最適化**: 未実装（02/05/08/09 に設計）。

## 16.5 次の一歩（Phase 1 の着手順）

```
1. optimizer crate: fusion + pushdown を IR-in/IR-out で（08）。explain --opt。
2. Arrow backing: Column → ArrayRef（03 §3.5）。zero-copy・SIMD kernel。
3. group 拡張 + join 実行（hash join, 05 の sync 警告つき）。
4. bounded queue + credit backpressure（05 §5.4）。
5. criterion bench + 差分テスト（15）。
```

各ステップは中核 trait（`Operator` / `Chunk` / `PlanGraph`）を保ったまま差し込め、
既存テストが回帰ガードになる。

## 16.6 受け入れ確認（再現コマンド）

```sh
cargo test                                   # 11 passed
cargo run -p rivus-cli -- run     examples/branch.riv
cargo run -p rivus-cli -- run     examples/recover.riv   # final mode: degraded
cargo run -p rivus-cli -- run     examples/group.riv     # |# country -> counts
cargo run -p rivus-cli -- explain examples/branch.riv    # IR + 再生成 source
cargo run -p rivus-cli -- run     examples/save.riv && cat examples/jp_users.csv
```
