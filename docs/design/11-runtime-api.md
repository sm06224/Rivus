# 11. Runtime API

## 11.1 埋め込み API（Rust）

ライブラリとして組み込む経路。MVP で既に動く：

```rust
use rivus_runtime::{run, RunOptions};

let graph = rivus_parser::parse(source)?;          // source -> DAG IR
let result = run(&graph, RunOptions { chunk_size: 4096 })?;

result.final_mode;     // 終了時の runtime mode
result.errors;         // error stream（Vec<ErrorEvent>）
result.telemetry;      // Vec<NodeTelemetry>（per-node 観測値）
result.outputs;        // Vec<Output>（leaf 捕捉した chunk 群 + label）
```

低レベル経路（IR を直接組む／最適化を挟む）も同じ `run` に流せる：

```rust
let mut g = PlanGraph::new();
let src = g.add_node(Op::OpenCsv { path: "users.csv".into() });
let flt = g.add_node(Op::Filter { pred });
g.add_edge(src, flt, EdgeKind::Stream);
g.label_node(flt, "Users");
let g = optimize(g);            // Phase 1: IR-in / IR-out
run(&g, RunOptions::default())?;
```

## 11.2 CLI（`rivus`）

```
rivus run     <file.riv> [--chunk-size N]   実行 + ASCII 可視化
rivus explain <file.riv>                    DAG IR + 再生成 source
rivus check   <file.riv>                    parse のみ
```

`run` は実行後に「execution graph（bar 付）/ error stream / outputs」を表示する
（14 参照）。`explain` は nodes/edges/topo + `to_source()` を表示（可逆性の確認）。

## 11.3 Runtime Query API（設計）

実行中/実行後のランタイム状態を取得する言語内コマンド（Observability §15）：

```
monitor Users;        # 単一 flow node の telemetry を購読
watch flow Import;     # flow 全体の live 監視
visualize Runtime;     # 実行グラフ全体を描画
```

これらは「runtime を問い合わせる flow」として実装する（runtime 自身が観測対象を
chunk stream で返す＝ everything is stream の自己適用）。MVP では parser が
directive として受理し no-op。Phase 1 で `RuntimeHandle` を導入：

```rust
struct RuntimeHandle {
    fn snapshot(&self) -> RuntimeSnapshot;        // 全ノードの NodeTelemetry + mode
    fn subscribe(&self, node: &str) -> Stream<TelemetryEvent>;
    fn graph(&self) -> &PlanGraph;                // 現行（最適化後）グラフ
    fn transition(&self, mode: Mode);             // 制御平面への明示介入
}
```

## 11.4 観測の公開フォーマット

`RuntimeSnapshot` は JSON / Arrow / SVG / ASCII にレンダリングできる中立表現に
する。これが TUI・ブラウザ UI・`rivus live` Markdown 埋め込み（14）の共通入力。

```
RuntimeSnapshot {
  mode: Mode,
  nodes: [{ id, label, kind, rows_in, rows_out, errors, busy_ns, mode, finished }],
  edges: [{ from, to, kind, queued_chunks }],
  errors: [ErrorEvent],
}
```

## 11.5 実行の制御（control plane API）

`transition(mode)` のほか、Phase 1 で `isolate(branch)` / `checkpoint(scope)` /
`reroute(from, to)` を公開し、外部オーケストレータ（または mode hook）から制御
平面を駆動できるようにする（01 §1.3 / 13）。

### 段階表

| | Runtime API |
|---|---|
| MVP | `run()` / `RunResult` / CLI `run・explain・check` |
| 次 | `RuntimeHandle`（snapshot/subscribe/transition）/ JSON・Arrow 公開 |
| 将来 | リモート制御 API（gRPC）/ 分散 runtime の集約ビュー |
