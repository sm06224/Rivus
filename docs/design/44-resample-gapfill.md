# Design 44 — Resample / gap-fill: 欠損時間バケットの行生成（#62）

**Status:** 提案（裁可待ち）。**Author:** 先行研究担当.
**Decision:** 統括／レビュー兼指揮（ratify per-item; 現運用 #240）.

Track C の最終ピース。`bucket(ts, dur)` ＋ `|#` で event-time バケット集計は
一級になった（design/30・#157 裁定「窓＝派生グループ化キー」）が、**行の無い
バケットは出力に存在しない** — 「1 時間ごとの売上（売上ゼロの時間も 0 行で
欲しい）」が表せない。欠損バケットを補うには**行を作る**必要があり、これは
Rivus 初の row-creating op ＝ sessionize と同じ「真に新しい意味論」圏
（§30.4 判定）なので、実装前に本メモで批准を仰ぐ。各項は独立に採否可能。

---

## 44.1 提案 — `resample` は行生成**だけ**・埋めは既存 `fill` との合成

```
S: open sales.csv (ts:datetime amount:int)
 |> (bucket(ts, "15m")) as b amount
 |# b sum:amount
 resample b "15m"
 fill sum_amount "0"
 sort b ;
```

- **新 verb `resample KEY "STEP" [over COL…]`**（1 語のみ追加）: `KEY`
  （datetime レーンの列 — 典型は `bucket`/`trunc`/`date_bin` の出力）の
  **観測 min..max を STEP 刻みの格子**とみなし、格子上に行が無い点へ
  **行を 1 本ずつ生成**する。生成行は `KEY`（＝格子点）と `over` 列以外
  **すべて null**。
- **gap-fill は新機構を作らない**: 生成行の値埋めは既存の
  `fill col "0"` / `fill col ffill` / `bfill` / `mean` の**合成**で表す
  （design/38「新キーワードより合成」・fill の意味論は不変のまま再利用）。
  count 系を 0 にしたいのか、計測値を ffill したいのかはユーザーの選択で、
  resample が決め打ちしない。
- `over COL…` は P3 と同じ**パーティション句**: 部分系列（銘柄毎など）毎に
  独立の min..max 格子を張る。省略時は全体で 1 格子。

**不採用案（比較のため記録）**:
- **(B) `|#` への句の埋め込み**（`|# (resample(b,"15m")) sum:v`）— group 文法に
  行生成を密輸する形。集計と生成が癒着し、`|#` 以外の入力（既に集計済みの
  CSV を読んだだけのフロー）に使えない。却下推奨。
- **(C) `range` 生成ソース＋left join の合成** — 合成度は最高だが、境界を
  データから導けず（明示 from/to 必須）、別フロー＋join が定型文になる。
  明示境界が要る用途の**将来拡張**としては (A) と両立する（`resample … from
  … to …` を後から足せば同じ表現力に到達）。

## 44.2 Semantics

- **格子**: パーティション毎に `[min(KEY), max(KEY)]` を STEP 刻み
  （min 起点・**閉区間**）。観測値の min/max はデータの純関数なので格子も
  純関数 — 決定性・byte-identity は構成的。境界の明示指定（`from`/`to`）は
  第 1 スライス対象外（§44.5）。
- **生成行**: KEY = 格子点・over 列 = パーティション値・他列 = **null**。
  観測行は 1 バイトも変えない（値も順序内相対位置も）。
- **出力順**: `(over…, KEY)` 昇順に**整列して emit**（blocking — min/max が
  要る時点で全入力を見る必要があり、時系列の自然な出力形でもある。観測行の
  間に生成行を差し込む以上、何らかの順序決定は不可避 — 明示の sorted 契約が
  最も予測可能）。後段の `sort` は不要になるが、書いても冪等。
- **格子外の観測行**（KEY が min 起点 STEP 刻みに乗らない — 例: KEY が
  `bucket` 出力でない生 ts だった）: 行は**そのまま保持**し（never-silent に
  行を消さない）、**計数して finish で 1 回 surface**（「N row(s) off the
  STEP grid — did you mean bucket(ts, STEP)?」）。格子生成は乗っている観測
  点集合に対してのみ判定。
- **null KEY の観測行**: 格子に参加せず保持・計数 surface（sessionize の
  null ts と同じ扱い）。
- **STEP**: bucket/hops/session と同じ duration 語彙（ts 単位で正確に表現
  可能であること — 同じ検査を再利用）。
- **生成行数の可観測性**: 「resample: N gap row(s) created」を finish で
  1 回 surface（Info/Recoverable 級・never-silent の裏面「無から行が湧いた
  ことも黙らない」）。爆発ガード: 格子点数がパーティションあたり
  上限（例 10^7）を超える場合は **Fatal**（`min..max` と STEP の取り違え
  — 例: 単位ミス — で数億行生成する事故を教えるエラーで止める）。
- **直列・blocking**: sort と同じ分類（order 生成 op）。chunk-size 非依存は
  「全入力の純関数」なので自明。

## 44.3 レーン・スキーマ

- 出力スキーマ = 入力スキーマ（列の追加・削除なし — 行だけ増える）。
  schema_prop は identity。
- KEY が datetime レーンでない場合: **教えるエラー**（parse 時は列型不明
  なので runtime で Fatal ではなく… → 第 1 スライスは runtime で
  warn＋passthrough〔行生成なし〕。sessionize の非 datetime ts と同じ型）。

## 44.4 IR / 実装スケッチ

- `Op::Resample { key: String, step: String, by: Vec<String> }`。
  to_source: `resample key "step" [over by…]`。keyword は `resample` 1 語
  （`is_keyword` 追加 — flip 後の初の新 verb。retired 語とは無関係）。
- runtime: blocking — パーティション毎に (観測行バッファ・KEY tick 集合・
  min/max) を蓄積、finish で格子走査して生成行を差し込み `(over…, KEY)`
  昇順 emit。メモリは入力全量（sort と同等・既知の blocking 特性）。
- engine: 直列分類（`Op::Resample => None`）・列参照検証は Shift と同型。

## 44.5 スライス計画・受入

メモ批准 → 実装 1 スライス。受入 gate:

- parser round-trip（STEP 必須・duration 検査・over 句・冪等）
- stress oracle: 中抜けバケットの生成（単一/複数パーティション）・観測行
  無改変 pin・`fill "0"`/`ffill` 合成の end-to-end・格子外/null KEY の
  計数 surface・生成 0 行（隙間なし）で入力 sorted 出力一致・cz 掃引・
  爆発ガードの Fatal・`resample`→`fill`→`|#` 再集計の整合
- 常設フル gate・破壊的変更なし・新規依存なし

## 44.6 裁可時の確認点（open questions）

1. **新 verb `resample` の追加可否**（design/38 後の初の新キーワード。
   行生成は窓 item でも派生キーでも表せない — §30.4 の「真に新しい意味論」
   判定に該当すると考える。名は pandas 語彙・`gapfill` との比較で
   `resample` 推奨 — 生成が本体で埋めは fill の合成のため）。
2. **埋めを既存 `fill` 合成に委ねる**方針（resample 自体は null 行のみ）。
3. **出力順 = `(over…, KEY)` 昇順の sorted 契約**（blocking）で良いか。
4. 境界の明示指定（`from`/`to`）・`range` 生成ソースは**保留**で良いか。
5. 爆発ガードの上限値（パーティションあたり格子点数、案 10^7・Fatal）。
