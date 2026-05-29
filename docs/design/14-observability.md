# 14. Observability

## 14.1 原則：観測は言語仕様の一部

実行は不可視であってはならない（Observability §1）。telemetry は外部ツールでは
なく言語仕様に含まれ、runtime は flow state / mode transition / chunk movement /
synchronization / degradation / recovery を観測可能にする（原則4）。

## 14.2 Telemetry Model（§14）

各 flow node が保持する観測値（`rivus-runtime/src/telemetry.rs`）：

```rust
struct NodeTelemetry {
    node_id, label, kind,
    chunks_in, chunks_out, rows_in, rows_out, errors,
    busy: Duration,        // 処理時間
    mode: Mode,
    finished: bool,
}
// 派生: throughput_rows_per_sec(), selectivity()
```

計測は **エンジン側**で行う（operator に計測責務を持たせない）。これにより観測の
一貫性と「telemetry は中核」を担保。Phase 1 で latency 分布 / chunk rate /
synchronization state / memory pressure を追加。

## 14.3 可視化（§13）— MVP は ASCII

`rivus run` の出力（実物）：

```
▒ execution graph   final mode: normal
  Users                    open        0→8     ██████████████ done
    └─ Minors              filter      8→4     ███████░░░░░░░ done
    └─ Adults              filter      8→4     ███████░░░░░░░ done
      └─ Merged            merge       8→8     ██████████████ done

▒ error stream      (empty)

▒ Merged  (8 rows)
  name  age  country
  ----  ---  -------
  ben   15   US
  ...
```

bar は rows_out を最大値で正規化。indent はトポロジカル深さ。mode と errors（`!N`）
を併記。これは spec の例（`├─ parse ███████░░`）と同系。

## 14.4 表示形式のロードマップ

| 形式 | 段階 | 入力 |
|---|---|---|
| ASCII graph / table | MVP | `RunResult` |
| TUI dashboard（live） | 次 | `RuntimeHandle::subscribe` |
| SVG animation | 次 | `RuntimeSnapshot` → SVG |
| `rivus live` Markdown | 将来 | PKC 統合 |

いずれも共通中立表現 `RuntimeSnapshot`（11 §11.4）を入力にする。

## 14.5 synchronization awareness（§11）

branch/join は同期コストを持つ。runtime は buffering / blocking / sync latency を
観測し、ユーザへ明示する：

```
WARN: synchronized join detected — estimated buffering: 4 chunks
```

これは「hidden materialization 禁止」の徹底：暗黙にバッファするのではなく、必ず
可視化・警告する。

## 14.6 replayable / queryable

- **queryable**: `monitor` / `watch` / `visualize`（11 §11.3）で runtime を問い合わせ。
- **replayable**: chunk meta（id, mode, corrupt）と error stream を記録すれば、
  実行を再構成・再生できる（`stream Label`）。checkpoint は Phase 1。
- **visualizable**: 上記の通り。

## 14.7 PKC Markdown Integration（§16）

Markdown を Executable Structural Document にする。

````
```rivus live
Import
```
````

このブロックは Rivus runtime に接続し、live SVG / AA rendering / animated
telemetry を埋め込む。実装方針：Markdown レンダラが `rivus live` ブロックを検出
→ `RuntimeHandle::subscribe` を張り → `RuntimeSnapshot` を周期的に SVG/AA 化して
差し替える。ドキュメントが「動く」。

## 14.8 optimizer の可観測性

最適化も観測対象（08 §8.7）。`explain --opt` で適用 rule とコスト変化、前後の
再生成 source を見せる。opaque optimizer はアンチパターン。

### 段階表

| | Observability |
|---|---|
| MVP | per-node telemetry / ASCII graph・table / error stream 表示 / explain |
| 次 | TUI live / SVG / RuntimeSnapshot 公開 / sync 警告 / latency 分布 |
| 将来 | `rivus live` Markdown / 分散の集約ダッシュボード / トレース連携(OTel) |
