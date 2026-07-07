# 36. sliding window ＝ 派生キーの複数化 — `hops(ts, size, hop)` ＋ explode（§30.4 の続き・批准依頼）

> **状態：提案（先行研究・プロトタイプ実装済み・批准待ち）。** §30（#157 裁定）は
> tumbling を「窓＝派生グループ化キー」1 個（`bucket`）に還元して close し、
> **sliding / session を「別スライス・別批准」として残した**（§30.4）。本メモはその
> sliding 側の提案。§30 の裁定（watermark／非有界解除＝永続対象外、§30.5）は
> **一切覆さない**。session は §36.5 の設計スケッチのみ（実装しない）。

## 36.0 結論（先出し）

sliding window は新しい演算子ではなく、**「派生キーが複数になった tumbling」**である。

```
|> (hops(ts, "2m", "1m")) as w price    -- 各行が属する窓開始の LIST を導出
explode w                                -- 行を窓ごとにファンアウト（既存 op）
|# w avg:price                           -- 以降は普通の group-by（既存）
```

- **新規機構は関数 1 個**：`hops(ts, size, hop)` — `ts` を含む全 sliding 窓の
  開始 datetime を **List で返す**（epoch 整列・左閉右開・昇順）。`bucket` の
  自然な一般化：`hop == size` で `bucket` に退化（テストで pin）。
- explode・List レーン（§32 s3a）・group-by・並列 merge は**全て既存**。
  §30 の哲学「窓は新機構ではなく派生キー」が sliding にもそのまま延長される。

## 36.1 意味論

`hops(ts, size, hop)` = `{ w | w ≡ 0 (mod hop), w ≤ ts < w + size }`（`w` は
epoch からの整数 tick、`ts.unit` で正確に表現できない size/hop は `bucket` と
同じ契約で Null → 行ごと continue-first）。

- `hop == size` → tumbling（= `bucket`、要素 1 の List）。
- `hop < size` → 重複窓：各行は **⌈size/hop⌉ 個**の窓に属する（一定・入力非依存）。
- `hop > size` → 間隙：窓間の行は**空 List** → explode が 0 行に落とす
  （「どの窓にも属さない」は真の答えであってエラーではない）。
- 負 tick（1970 以前）は floor 除算で正しく整列（`bucketed` と同一の数論）。

## 36.2 不変条件との整合（実測済み）

| 不変条件 | 論拠・実測 |
|---|---|
| **byte-identity**（serial==parallel==chunk-size） | `hops` は行毎の純関数（状態なし）、explode は行内展開、group キーが増えるだけ（§30.6「窓は並列ハザードを追加しない」）。**200k 行・size=10m/hop=5m で実測一致**（stress `sliding_window_serial_parallel_chunk_size_byte_identical`） |
| **bounded** | 窓状態を持たない：メモリは既存 group-by の群基数バウンドそのもの（アクティブ窓概念が不要）。ファンアウト率は ⌈size/hop⌉ で静的 |
| **continue-first** | 不正な ts/size/hop → Null（`bucket` と同契約）。空 List → 0 行 |
| **依存ゼロ・IR 可逆** | std-only の関数 1 個。`to_source` は既存の Func 経路（`hops(ts, "2m", "1m")` がそのまま往復） |
| **#41（f64 非結合）** | 窓でも同じ：exact 集計（count/min/max/decimal 和）のみ並列、f64 sum/avg は既存分岐どおり直列 or `--exact` |

## 36.3 実装（このスライスで入るもの）

- `rivus_core::DateTime::hop_starts(size, hop) -> Option<Vec<i64>>`（`bucketed`
  と同じ単位変換・floor 数論、i128 で `t - size` のオーバーフローを防止）
- `Func::Hops`（IR・parse・to_source・did-you-mean 候補）
- eval：スカラ＝`Value::List<DateTime>`、列＝`column_from_values` の **List
  アーム新設**（子列を再帰的にレーン型付けし offsets を構築 — `hops` に限らず
  List を返す将来の関数すべての受け皿）
- schema_prop：`Hops → DataType::List`（explode が要素レーンに剥がす）
- stress 3 本：オラクル一致（chunk-size ループ）／gap・退化ケース（`hops(s,s) ≡
  bucket`）／200k 行 serial==parallel==chunk-size

## 36.4 #60 との整合（要リスコープ）

#60（2026-06-02 起票）は §30 裁定（#157）より**古い**。§30 で確定済みの部分を
反映すると、#60 の残タスクは：

| #60 の項目 | 状態 |
|---|---|
| tumbling | **済**（`bucket`＋group、§30.3） |
| sliding / hopping | **本提案**（`hops`＋explode） |
| session | 未（§36.5 スケッチ、別批准） |
| window-close emit・unbounded 解除・watermark | **§30.5 で永続対象外と裁定済み**（本提案も触れない） |

## 36.5 session window 設計スケッチ（実装せず・将来の批准材料）

session（gap 境界）は「キー導出」では表せない**真に新しい意味論**（§30.4 判定の
とおり）：session id は**同一グループ内の前行の ts** に依存する。最小設計案：

- 新 op `Sessionize { ts, gap, by }`（ストリーミング・非 blocking）：グループ毎に
  `last_ts` だけ保持（bounded = O(グループ基数)）、`ts - last_ts > gap` で
  session 連番を増やし **`session` 列を行に付与**して流す。以降は普通の group-by。
- **時刻昇順前提**（#60 の契約）：逆行を検知したら error stream に計数 surface
  （continue-first）。ソート済み入力では決定的＝byte-identity。**並列は初期
  スライスでは直列限定**（順序依存のため）— これも「窓は派生キー」に載らない
  ことの帰結で、別批准が必要な理由そのもの。

## 36.6 GUIDE 追補（本 PR 同梱）

時系列集計の節に sliding の 3 行イディオムを追加（tumbling=`bucket` の隣）。

> 批准は統括の専権。本メモ＋プロトタイプは研究成果＝設計提案であり、自己マージしない。
