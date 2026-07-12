# Design 40 — 三面フロー（Three-Plane Flow）: 制御・テレメトリ・データの統一

**Status:** 提案（統括指示 2026-07-09 を受けた設計）。**批准必須・自己マージ禁止。**
**Directive:** 「コントロールフロー、テレメトリフロー、データフロー全てをフローと
するのだ」「全てが流れでなくてはいけません」「opentelemetry などのメトリクス概念と
QUIC 論争を再燃させ、カリカリのチューニングを行う」「他のソリューションに負ける
ことは許さない」。依存方針 v2（外部モジュール積極導入可・単一バイナリ・供給網明示）
を前提とする。

---

## 40.1 命題 — 「Everything is Flow」を三面に拡張する

物理法則 #1（Everything is Flow）は今日まで**データ面**にしか本気で適用されて
いない。テレメトリは engine が計測して最後に吐く「レポート」であり、制御（mode
遷移・stop/monitor・route）は「コマンド」である。§34 が既に三チャネル分離
（Control / Data / Telemetry のフレームタグ）を分散実行に導入したが、これを
**ローカル実行を含む全実行の一次概念**に昇格させる：

> **1つの実行 = 3つの並行フロー。**
> - **データフロー**: 今日の chunk ストリーム（不変）。
> - **テレメトリフロー**: ノード毎の計測値（rows/busy/selectivity/errors…）を
>   実行**中**に chunk として流す — 「実行後レポート」ではなく「実行と同時の
>   ストリーム」。エラーストリーム（continue-first）はその一部。
> - **制御フロー**: mode 遷移・backpressure クレジット・stop/pause・再構成が
>   同じフレーム/イベント形式で流れる。
>
> 三面は**同じ Chunk/イベント表現・同じ IR 語彙**で記述され、`rivus run` の
> ローカル実行と `--on` の分散実行で**同じ三面**が流れる（interpret == distribute
> の三面版）。

これは §14（observability）・§33/§34（分散・トランスポート）・§28.12.2④
（credit bounded pull）の**統合完成形**であり、新奇性ではなく収束である。

## 40.2 テレメトリフロー = OpenTelemetry 互換メトリクス

現状の `--json`（1行1イベントの独自 JSONL）を保ちつつ、**OTel の概念モデル**
（Resource / Metric / DataPoint / Span）に写像する：

| Rivus | OTel |
|---|---|
| 実行（flow run） | Trace |
| ノード（operator） | Span（親=グラフ辺） |
| `rows_in/out`, `busy_ms`, `selectivity` | Metric (Counter/Gauge/Histogram) |
| エラーストリームのイベント | Span Event / ログレコード |
| chunk メタ（mode, provenance） | Attributes |

- **スライス T1（依存ゼロ・即着手可）**: `--json` を OTLP/JSON 互換スキーマに
  整形するエクスポータ層。ネットワーク送信なし＝ファイル/stdout に流すだけで
  Grafana/Collector が食える形。
- **スライス T2（方針 v2 で解禁）**: `opentelemetry` + `opentelemetry-otlp`
  crate による OTLP/gRPC push（feature `otel`）。SUPPLY-CHAIN 審査必須。
- **スライス T3**: テレメトリフローを**Rivus 自身で処理可能**にする —
  `monitor` 系構文でテレメトリストリームを通常のフローとして `|?`/`|#` できる
  （メトリクスの自己ホスト集計。dogfooding＝テレメトリもデータ）。

## 40.3 QUIC 再燃 — §35 の勧告を方針 v2 で再審する

§35 は「WireGuard 委譲で足りる・QUIC は不要」と勧告し B2 を close した。
その論拠のうち**方針 v2 で失効するもの**を明示する：

| §35 の論拠 | v2 での再評価 |
|---|---|
| 依存ゼロ既定を壊せない | **失効**（外部モジュール許可） |
| pure-Rust QUIC 未成熟 | 再検証：quinn 0.11+/rustls 0.23（ring不要の aws-lc-rs / RustCrypto provider）は成熟が進んだ。要 deny/監査 |
| 遅延はフロー実行支配 | **有効なまま**（0.97ms vs 8.6ms はコネクション再利用で縮む見込みだが、シリアル実行が支配的な限り二次要素） |
| チャネル分離は論理タグで足りる | 三面フロー時代は**ストリーム=面**の物理分離が活きる：QUIC の多重ストリームに Control/Telemetry/Data を1本ずつ載せ、**テレメトリの head-of-line blocking をデータから隔離**できる（TCP 1本では不可能） |

**勧告の更新案**: B2 を「close」から「**再開（条件付き）**」へ。条件＝
(a) コネクション再利用実装後のレイテンシ再計測で TCP+WG 比の劣位が 2× 以内、
(b) 三面フローの面分離が実測で意味を持つ（テレメトリ遅延の p99 改善）、
(c) `cargo deny` + SUPPLY-CHAIN 審査緑。①②いずれか不成立なら §35 維持。
**計測が裁く。**

## 40.4 ストリームIO第一（圧縮ストリーム）— 実装済みの土台

統括指示「ディスクIOを支配させない・圧縮ストリームとストリームIOの積極採用」は
本 PR（#237 第5弾）で最初の具現が入った：

- gzip/zstd を**デフォルト機能**へ昇格（純Rust デコーダ・供給網審査済）。
- `read` が `.gz`/`.zst` を**ストリームのまま** decode（`CompressedCsvReader`
  / `StreamJsonlReader`＝sample 推論・単一パス。decompress-to-buffer 禁止）。
- 圧縮ストリームはレンジ分割不能 → **ファイルレベル並列**（波状・uri順スロット）
  で 10M×9ファイル圧縮が平文と同速以上に（csv.gz 6.1→4.7s、jsonl.gz 11.3→6.5s、
  全出力 bit 不変）。

**含意（正直に）**: 圧縮経路は sample 推論なので「sample 後に型が広がる列」は
平文の全走査推論と型が異なり得る（source の圧縮/HTTP 経路と同一の文書化済み
トレードオフ）。本フィクスチャでは最終出力 bit 一致を実測。全走査推論との統一
（推論もストリームで2回流す vs sample で1回）は T4 として批准対象。

## 40.5 カリカリのチューニング — 次のレバー（計測済み棚卸し）

10M×9ファイル標準での現在地（warm best-of-3・出力は全て DuckDB と行一致）:

| | rivus | DuckDB | 残差の正体 |
|---|---:|---:|---|
| csv | 4.8s | 0.9s | 直列パイプライン（join 1.4s・group 0.8s…） |
| csv.gz | 4.7s | 1.3s | 同上 |
| jsonl | 7.4s | 1.5s | JSON 2重パース＋直列 |
| jsonl.gz | 6.5s | 1.8s | 同上 |
| peak RSS | ~1.1–1.3GB | 0.24–0.4GB | read 全バッファ＋blocking join |

1. **パイプライン並列**（本命）: 整数/decimal レーンの結合的集約は byte-identity
   安全。既存 #41 経路の一般化。
2. **ストリーミング read / stream-probe join**（メモリ天井）: 小側 hash 構築後、
   大側 chunk を到着順に probe して即 emit — 「全てが流れ」のエンジン側の本丸。
3. **JSONL 単一パース**（infer と decode の融合）。
4. **join/group キーの型レーン直接ハッシュ**（テキスト描画排除）。

## 40.6 批准依頼事項

- **Q1**: 三面フロー（40.1）を §00 北極星に追記してよいか。
- **Q2**: OTel T1（依存ゼロ整形）即着手・T2（otel crate, feature `otel`）の
  SUPPLY-CHAIN 審査開始の可否。
- **Q3**: QUIC B2 の条件付き再開（40.3 の3条件）の可否。
- **Q4**: 圧縮経路の sample 推論トレードオフ（40.4）の追認、または全走査推論
  への統一指示。
