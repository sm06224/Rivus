# Design 43 — Rolling row-window aggregates (#63)

**Status:** 提案（裁可待ち）。**Author:** 先行研究担当.
**Decision:** 統括／レビュー兼指揮（ratify per-item; 現運用 #240）.

Track C の残り最後の大物。`lag`/`lead`/`diff`/`pct_change`（点参照）と
`session`（キー導出）は design/38 P3 の窓 item 文法で着地済み。残るのは
**移動集計** — pandas の `rolling(N).mean()`、SQL の
`ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW` に当たる形。各項は独立に
採否できます。

---

## 43.1 Grammar — P3 窓 item 族の素直な延長（新キーワードなし）

```
|> * (rolling_avg(price, 5) over sym) as ma5
|> sym price (rolling_sum(qty, 20) over sym) as v20
```

- 第 1 スライスの関数族: **`rolling_sum` / `rolling_avg` / `rolling_min` /
  `rolling_max`**。すべて既存の窓 item 文法（`(FN(col, N) [over BY…]) as
  ALIAS`・alias 必須・`|> *` は keep-all）に乗り、**キーワードは 1 語も
  増えない**（`lead` と同じ — `rolling_avg` という名の列は `(` が続かない
  限り通常の射影）。
- N は **行数**（trailing・当該行を含む）。`over` グループ毎・source 順。
- **不採用案**: `rolling(avg(price), 5)` のネスト呼び出し文法。表現力の
  加算なしに parse・schema 伝播・エラー文言の複雑さだけ足すため、
  `pct_change` 前例のフラット名族を推奨（統括が逆を好む場合は文法だけの
  入替えで済む — 意味論は §43.2 のまま）。

## 43.2 Semantics

- **窓**: 直近 N 行（当該行含む）。グループの先頭 N−1 行は **null**
  （lag の先頭 N null・lead の末尾 N null と同じ端点規則。pandas でいう
  `min_periods=N` 固定 — `min_periods` オプションは第 1 スライス対象外）。
- **窓内の null セル**: sum/avg/min/max から除外（`|#` 集計と同じ SQL
  意味論）。窓内が全 null → 出力 null。
- **レーン**: `|#` の規則を鏡写しにする —
  `rolling_sum` は exact レーン維持（i64→i64・decimal(s)→decimal(s)・
  duration→duration、内部 i128 蓄積）・f64→f64。`rolling_avg` → f64
  （decimal avg の決定的丸めの細部は `|#` 実装と同一規則を実装時に pin）。
  `rolling_min`/`rolling_max` → source レーン維持（数値・datetime・
  duration・decimal。str は第 1 スライス対象外 — 需要が見えてから）。
- **直列・chunk-size 非依存**: order 依存 → `Op::Shift` と同じ直列分類。
  グループ毎に直近 N 値のリング（lag と同じ機構クラス）— trailing 窓は
  **過去しか見ない**ので lead の遅延 emission は不要、streaming per-chunk
  emit のまま。

## 43.3 f64 の罠（#41 圏 — ここが本メモの核心）

滑り累算（add/subtract accumulator）は f64 では**禁止**:
`acc += x[i]; acc -= x[i-N]` の桁落ち・非結合性により、窓を都度再計算した
値と ULP 級で乖離し、しかも入力履歴に依存して**再現不能にドリフト**する。

- **規則（第 1 スライス）**: f64 の `rolling_sum`/`rolling_avg` は
  **窓を毎行再計算**（リング上の N セルを固定順で fold・O(N)/行）。
  値は「窓の内容だけの純関数」になり、byte-identity・chunk-size 非依存が
  **構成的に**成立する。N が実用域（数〜数百）なら実測上十分速い想定
  （feature スライスにつき perf 主張はしない — 実測は実装 PR で正直に添付）。
- **exact レーン（i64/decimal/duration）は滑り可**: 整数加減算は結合的・
  可逆なので、滑り累算 == 再計算が **bit で**成り立つ（oracle test で
  滑り vs 再計算の一致を pin してから滑りを既定にする）。
- 将来: f64 で N が大きい用途が実測で出たら、design/37 の精神で
  「正準木の窓版」（セグメント木・O(log N)/行・純関数）を別スライスで
  検討。**bench が勝ちを示すまで実装しない。**
- `rolling_min`/`rolling_max` は単調 deque（O(1) 償却/行）が値純関数の
  まま使える（比較のみ・数値誤差なし）— 第 1 スライスから採用可。

## 43.4 IR / 実装スケッチ

- `Op::Rolling { col, func: RollFunc, n, by, out }` — `Shift` の拡張では
  なく独立 op（Shift の kind は点参照・Rolling は集計を担ぐ）。
  `RollFunc = Sum | Avg | Min | Max`。
- `to_source`: `|> * (rolling_sum(col, N) over by…) as out`（N 常時表示・
  lag/lead と同型）。schema_prop は §43.2 のレーン規則を鏡写し。
- runtime: グループ毎リング（VecDeque、容量 N）＋ exact レーン用 i128
  滑り累算 / f64 用 fold 再計算 / min-max 用単調 deque。未知列・未知 by
  列は既存 Shift と同じ warn＋passthrough／empty-group。
- engine: 直列分類は `Op::Rolling => None`（Shift と同じ 1 行）。

## 43.5 スライス計画・受入

1 スライス（parser＋IR＋op＋docs）。受入 gate:

- parser round-trip（N 既定なし — **N 必須**。窓幅の暗黙既定は事故のもと。
  alias 必須・冪等・列名非衝突 pin）
- stress oracle: 端点 null・グループ interleave・窓内 null・全 null 窓・
  レーン pin（i64/decimal/f64/datetime min-max）・**cz 掃引**・
  exact レーンの滑り vs 再計算 bit 一致・f64 の「窓内容の純関数」性
  （同一窓内容 → 同一 bytes を並べ替え fixture で pin）
- 常設 gate（fmt/clippy/test 両 feature/依存樹）・破壊的変更なし・新規依存なし

## 43.6 裁可時の確認点（open questions）

1. **名前族**: フラット（`rolling_avg`）推奨 — ネスト（`rolling(avg(…))`）
   への inversion は文法のみの変更で可。
2. **N 必須**（既定なし）で良いか。
3. `min_periods`・str min/max・`rolling_count`/`rolling_std` は**保留**
   （`rolling_std` は f64 モーメントの決定性設計が design/37 圏 — 別メモ）。
4. f64 再計算方式（§43.3）の受諾 — 滑り累算は exact レーン限定。
