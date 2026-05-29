# 15. Benchmark Strategy

## 15.1 方針：正しさが先、速度は観測して語る

優先順位は Stream correctness → … → Raw benchmark speed（最後）。だが速度向上は
必ず計測で示す（「速くなったはず」は禁止）。ベンチは回帰検知の門にする。

## 15.2 三本柱

```
1. correctness benches  : Tier0(interpreter) と最適化/JIT 結果の差分ゼロを保証
2. micro benches        : filter / project / group / csv-parse の単体スループット
3. end-to-end benches   : 代表 .riv（adults/branch/group/save）の総時間・メモリ
```

差分テストが最重要：fusion・vectorize・JIT は Tier0 と **bit 一致**（または
定義された許容誤差内）でなければ採用しない（09 §9.2）。

## 15.3 指標

| 指標 | 取得元 |
|---|---|
| throughput（rows/s, chunks/s） | `NodeTelemetry`（既に計測） |
| latency（first-row, p50/p99） | chunk meta `created_at` |
| selectivity 実測 vs 推定 | `selectivity()`（optimizer 学習に使う） |
| メモリ peak / alloc 回数 / 複製回数 | arena・buffer pool 計測（07, Phase 1） |
| 並列スケール（1→N worker） | scheduler 計測（05, Phase 1） |

## 15.4 ハーネス

- **criterion** で micro/e2e を計測（統計的に有意な比較・回帰検知）。
- 生成データ（行数・列数・選択率・型分布をパラメタ化）で再現性を担保。
- CI に「閾値超の回帰で fail」を組み込む（Phase 1）。

```
benches/
  filter.rs       criterion: i64 述語の chunk スループット（Tier0 vs Tier1 vs JIT）
  pipeline.rs     criterion: open|?|>save の e2e
  scale.rs        worker 数を振った並列スケール
fixtures/
  gen.rs          パラメタ化データ生成（seed 固定）
```

## 15.5 比較対象（外部）

同等処理を **DuckDB / Polars / `awk`+pipe / DataFusion** と比較し、Rivus の
「streaming + observable」の代償と利得を定量化する。目的は勝つことより「どこで
速くどこで遅いか」を観測可能にすること。

## 15.6 ベンチで駆動する最適化

各最適化（08）と各 JIT ティア（09）は「導入前後の bench 差」を PR に添付する。
adaptive chunk sizing / reorder は bench の分布から既定値を決める。

### 段階表

| | Benchmark |
|---|---|
| MVP | `cargo test`（11 tests）で correctness。bench は未整備 |
| 次 | criterion micro/e2e + 差分テスト + CI 回帰ゲート + 外部比較 |
| 将来 | 連続ベンチ（時系列回帰）/ 分散スループット / コストモデル較正 |
