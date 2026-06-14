# 30. 窓スライス — 有界 event-time 窓 ＝ 派生グループ化キー

> **本書は設計先行（doc-first・phase-0）。批准前に実装に入らない（§25.10）。
> 自己マージ禁止。** 本版は **#157 裁定（issuecomment-4700132036・統括/チャット経由）を
> 反映した書き直し版**で、#156 で main に着地した初版（`over` 句・非有界 6c・arrival 6d を
> 含む案）を**置換**する。#157 が覆した点:
>
> 1. **`over` 句は却下** — SQL の `OVER` は合成しない後付け。窓は **既存文法の派生グループ化
>    キー**（`trunc`/`bucket`）で表す（新キーワード・新 `Op` なし）。
> 2. **スコープは有界窓のみ** — 非有界＋watermark＋late（旧 6c）と arrival/processing
>    （旧 6d・#154 (c)）は **対象外**（"そこまでは専用ストリーム処理／SQL エンジンの領分"）。
>    Rivus の byte-identity 純度を保つため背負わない。
>
> 既存の正しさ機械（byte-identity・continue-first・never-silent・IR 可逆・zero-dep・
> null モデル）は **保存して載せ替える**。

関連：§0.13（有界/非有界＋時間＋状態）・§0.14（決定性の境界）・§23（datetime レーン・
`trunc`）・#41（f64 集約の非結合性）・**#157（本書の裁定）**・#154（(c)＝arrival は本書で
対象外確定）。

---

## 30.0 狙いと位置づけ

窓（windowing）は「時間で区切った範囲での集約」。Rivus が背負うのは **有界の時系列集計**
だけに絞る（#157 裁定）：

- **対象**：基本的に**揃っているブロック内**、または**実行時に参照可能なレコード**の集約。
  「1時間ごとの売上」「日次のエラー率」のような **event-time バケット集計**を、ファイル等の
  **有界**入力に対して一級にする。これは §0.13「時間：窓を IR の段に」の最小具体化であり、
  未着手の ts Epic #56・#60〜#67 に直接効く。
- **対象外**：非有界ストリーム上の **watermark／late-data**（旧 6c）と **到着順依存集約**
  （旧 6d・#154 (c)）。これらは専用ストリーム処理／SQL エンジンの領分で、Rivus は背負わない
  （§30.5・理由は二分原則と byte-identity 純度）。`watch` 下流の窓無しブロッキング集約を
  実行前に拒否する §28.12 の挙動（#155 で文言 clean）は**据え置き**。

**設計の要（#157）**：tumbling 窓は「行 → 時間バケツ」の**純関数**だから、**窓は新機構では
なく派生グループ化キー**で表せる。`trunc(ts, "hour")` を計算列にして `|#` で集約する——
それだけ。`over` 句も `Window` enum も `Op` への slot 追加も**要らない**。byte-identity は
「ただの group-by」なので自明に保たれる。

**スコープ外（本書では設計しない）**：sliding/session の新意味論の綴り（§30.4 に方向だけ）／
socket/http transport（§28.12.5）／分散シャッフルでの窓再分配（Phase 3）／GPU 窓集約（§22）。

---

## 30.1 二分原則 ＝ スコープ線引きの根拠（裁定 A・縮約）

窓は**締める基準**で2系統に分かれ、この線が「Rivus が背負うか」を決める：

- **event-time 窓**（締める基準＝**タイムスタンプ列＝データの値**）：窓割り当ては ts 値の
  純関数。同じ入力に同じ割り当て＝到着順・実時刻・並列分割に**非依存** → **byte-identity
  保持**・§0.14 の決定的契約内。**有界源ならこれが本書の実装対象**。
- **arrival / processing 窓**（締める基準＝**到着順・実時刻**）：実行ごとに割り当てが変わり得る
  ＝**本質的に非決定**。#154 (a) 却下の一般化（`take N` は終了性を回復するが決定性を回復
  しない／arrival 窓も同様に区切りは与えるが決定性は与えない）。

二分の帰結（#157）：Rivus は **event-time × 有界**のみを実装する。arrival は非決定で
byte-identity 哲学に乗らないので**対象外**。event-time でも**非有界**は watermark/late が
到着順非決定を持ち込むので**対象外**（§30.5）。＝**実装対象は「有界源上の event-time 窓」
一点**に収束する。これが surface を「ただの group-by」に落とせる理由でもある。

---

## 30.2 surface ＝ 派生グループ化キー（`over` 句を却下・新 `Op` なし）

tumbling 窓は **「ある行 → その行が属する時間バケツ」の純関数**。Rivus には既にその関数が
ある：`trunc(ts, "hour")`（`crates/rivus-ir/src/expr.rs:112` `Func::Trunc`・year/month/day/
hour/minute/second 境界へ切り捨て・同 unit の datetime を返す・Design 23）。窓集約は
**派生列を作って既存 `|#` で集約する**だけで表せる：

```
# 1時間ごとの売上合計（有界・event-time tumbling）
sales.csv |> open |> (trunc(ts, "hour")) as hour |# hour sum:amount

# 日次のエラー率（count は |# が常時産む）
logs.csv  |> open |> (trunc(ts, "day")) as day  |# day count
```

**なぜ `over` を採らないか（#157）**：SQL の `OVER (PARTITION BY … ORDER BY … ROWS BETWEEN …)`
は集約関数への**後付け**で、別文法を丸ごと背負い**合成しない**。Rivus の合成原理
（everything is flow）に反する。上の綴りは:

- **新キーワード・新 `Op`・`Window` enum・`Op::GroupBy` への slot 追加が一切不要**。窓は
  `|>`（`Op::ProjectExpr` の派生列）＋ `|#`（`Op::GroupBy`）という**既存ノードの合成**。
- **byte-identity 自明**：`trunc`/`bucket` は決定的な純関数、`|#` は既存の group-by。窓を
  入れても「ただの group-by」なので serial==parallel==chunk-size は既存の保証がそのまま効く。
- **IR 可逆**：新しい to_source 規則ゼロ（既存の `trunc(...)` 計算列と group-by の round-trip
  をそのまま使う）。
- **explain 不変**：窓は派生列＋group-by として既に可視。

> 補足（キーの形）：現状 `|#` のキーは `keys: Vec<String>`（bare 列名）。よって窓キーは
> **一旦 `|>` で材化**してから `|#` で参照する（上記）。`|#` がキー位置で式を直接受ける糖衣
> （`|# (trunc(ts,"hour")) sum:amount`）は **§31（構文 v2・#158）でキーを path/式へ一般化**
> する流れに乗せるのが自然で、本書では**必須としない**（材化経由で完全に表現できる）。

---

## 30.3 `bucket(ts, dur)` — 任意幅の小拡張（唯一の新規）

`trunc` は year/month/day/hour/minute/second の**固定境界**のみ。`15m`／`90m`／`6h` のような
**任意幅**の tumbling には、`trunc` と同型の純関数 `bucket(ts, dur)` を1つ足す（これが本
スライス唯一の新規機構）：

```
# 15分バケツ
metrics.csv |> open |> (bucket(ts, 15m)) as w |# w avg:latency
```

- **意味論**：`bucket(ts, dur)` = `floor(ts.ticks / dur.ticks) * dur.ticks`（epoch 起点の
  固定グリッド）を返す（同 unit の datetime）。`dur` は core の `Duration` リテラル（`15m`/
  `90m`/`6h`）。`trunc(ts,"hour")` は `bucket(ts, 1h)` の特例に一致（境界が暦単位に乗るときの
  別名）。
- **byte-identity / 境界ハザード無し**：datetime は exact i64 ticks（`DtColumn{ticks:Vec<i64>,
  unit}`・`crates/rivus-core/src/chunk.rs:175`）。`bucket` は **i64 の整数除算・乗算のみ**＝
  浮動小数を通らない。単位整合は cast レーンと同じ widening（`dur` を `ts.unit` の tick へ
  正規化＝無切捨て）。左閉右開 `[start, start+dur)`。
- **IR / 可逆**：`Func::Bucket` を `expr.rs` に追加（`Trunc` の隣）。parse `bucket(ts, 15m)`／
  to_source 復元＝**常時 std**。評価は datetime レーンの整数演算のみ（feature 不要）。
- **新 `Op` なし**：`bucket` は計算列の関数。窓集約は §30.2 と同じく `|> (bucket(...)) as w |# w …`。

オフセット付きグリッド（`bucket(ts, 1h, offset)`）やカレンダー対応（月またぎ等）は **`trunc`/
`bucket` の引数拡張**で後日対応可（新ノード不要）。本スライスは epoch 起点の固定幅まで。

---

## 30.4 sliding / session ＝ 真に新しい意味論（後送り・本スライス対象外）

派生グループ化キー1個で表せるのは **tumbling まで**（1 行が**ちょうど1つ**のバケツに入る）。
次の2つは「キー1個」では表せない真に新しい意味論なので、**綴りは必要になった段で詰める**
（本スライスでは設計しない）：

- **sliding（重複窓）**：`hop < size` のとき 1 行が**複数**窓に属する＝**ファンアウト**。派生列
  （行→値1個）では表せず、`explode` 的な行増殖か専用機構が要る。
- **session（動的境界）**：窓の境界が固定グリッドでなく**データ依存**（連続 ts の間隔が `gap`
  超で切れる）。事前の純関数キーにできず、隣接行を見る走査が要る。

いずれも有界源なら決定的に実装可能（exact i64 ticks）だが、**機構が tumbling と別**。
tumbling（§30.2/30.3）を landed させ、実需が出た段で sliding/session を別スライス＋別批准に。
（§31 のパス式キー一般化や `explode` が入れば sliding はその上に自然に乗る可能性が高い。）

---

## 30.5 スコープ外の明記 — 非有界 watermark/late（旧 6c）と arrival（旧 6d）

**#157 で確定**：以下は Rivus が**実装しない**（"そこまでは専用ストリーム処理／SQL エンジンの
領分"）。byte-identity 純度を保つための線引きで、§30.1 二分原則の直接の帰結：

- **非有界＋watermark＋late-data（旧 6c）**：非有界源では「全データが揃う」が成り立たず、
  watermark で締めると**どの行が間に合うか**が到着順依存＝非決定。§0.14 の決定的契約の外。
  → **対象外**。将来やるなら独立批准だが、**現方針は「やらない」**。
- **arrival / processing 窓（旧 6d・#154 (c)）**：締める基準が到着順＝本質的に非決定。
  → **対象外＝実装しない**。`watch` 下流の窓無しブロッキング集約を実行前拒否する §28.12 の
  挙動（#155 で `take N` 誘導を除去済み）は**そのまま据え置き**で、これが arrival 集約の
  受け皿を**塞いだまま**にする正しい状態。
- **帰結**：**#154 (c) は「スコープ外＝実装しない」で実質決着**。#154 は本書（§30 rewrite）が
  main に着地した時点で、その旨を記して **close してよい**（tracking 役目を終える）。

この線引きにより、初版にあった `Discovery`/arrival 由来の**決定性タグ一般化（`unbounded_nodes`
を非決定タグへ拡張）は不要**になる（§30.6）。

---

## 30.6 正しさの継承（#41 f64 制約・決定性・メモリ）

窓は集約の**範囲**を変えるだけ＝「ただの group-by」なので、既存 group-by の正しさ機械を
そのまま継承する。窓固有の新ハザードは無い。

- **#41 f64 集約制約の継承**：窓内であっても、f64 レーンの `sum`/`avg`/`std` は並列分割→
  マージで加算順が変わり ULP がずれる＝byte-identity が壊れる（#41 と同根）。よって**直列維持**
  か **decimal レーン（`--exact`・§21）**。順序非依存＝並列安全な exact reduction（`min`/`max`/
  `count`/`count_distinct`/`first`/`last`/`percentile`、整数・decimal の `sum`/`avg`）は窓内でも
  結果不変。AggFunc 一覧（`graph.rs:132`・窓でも同一集合）：Sum/Avg/Min/Max/Std/Count/
  CountDistinct/First/Last/Pct(u8)。**これは初版 §30.5 から不変**。
- **決定性タグ＝不要**：有界 event-time 窓は**派生列＋group-by**で、源が有界なら元々
  `unbounded_nodes`（`graph.rs:1524`）のタグが立たない＝決定的・byte-identical。6c/6d を切った
  ので arrival 由来の種付けも無く、**`unbounded_nodes` の一般化・改名は行わない**（slice 5 の
  現状のまま）。
- **メモリ**：窓集約のメモリは**既存 group-by の群基数バウンド**そのもの（窓キーが群キーに
  なるだけ）。`count_distinct`/`percentile` が群×窓ぶん値を貯める pipeline-breaker なのも既存
  どおり。**有界専用なので「同時開窓数ノブ」「watermark 解放」のような非有界向け機構は不要**。
  巨大入力での群基数上限は既存 group-by と同じ運用（初版 §30.6 の bounded-block 協調は
  非有界前提だったので**撤回**）。

---

## 30.7 決定分岐（→ 批准・#143/#149 形式・大幅縮約）

#157 で surface とスコープが裁定されたため、初版の①〜⑦は大半が解決。残る分岐のみ：

1. **`bucket` Func の採否と綴り（§30.3・未確定）**：`bucket(ts, dur)`（epoch 起点固定グリッド・
   `dur`＝Duration リテラル）を `Func` に追加してよいか。名前（`bucket`/`floor`/`window`…）。
   `trunc(ts,"hour")` と `bucket(ts,1h)` の重複は別名として許容するか。
   - 推奨：`bucket` 追加・`trunc` は暦境界の別名として併存。
2. **キー位置の式糖衣（§30.2・未確定）**：`|# (bucket(ts,1h)) sum:amount` のように `|#` が
   キー位置で式を直接受ける糖衣を本スライスで入れるか、§31（パス式キー一般化）に委ねるか。
   - 推奨：**§31 に委ねる**。本スライスは材化経由（`|> … as w |# w …`）で完結。
3. **sliding/session（§30.4・未確定＝後送り確認）**：本スライス対象外で合意か。
   - 推奨：対象外（実需が出た段で別スライス＋別批准）。
4. **#154 のクローズ（§30.5・未確定＝手続き確認）**：本 rewrite 着地時に #154 を「(c) 対象外」で
   close してよいか。
   - 推奨：close。

①②（裁定済み確認）／③④⑤⑥⑦（初版）は #157 により**却下・対象外・不要**で決着済み。

---

## MVP / 次 / 将来

- **MVP（本書批准の対象）**：有界 event-time 窓を **派生グループ化キー**で表す設計確定
  （`trunc` 再利用＋`bucket` 小拡張）・#41 継承・決定性/メモリは既存 group-by 継承・
  6c/6d 対象外の明文化。
- **次**：批准後 **6a＝`bucket(ts, dur)` Func の実装**（`trunc` 最小延長・datetime i64 整数演算・
  parse/to_source・byte-identity stress・英日ガイドに時系列集計の節）。これが最初で唯一の
  実装スライス。
- **将来**：sliding/session（実需が出たら別スライス・§31 のパス式キー/`explode` の上に）／
  キー位置の式糖衣（§31 一般化に同乗）。**非有界 watermark/late・arrival は永続的に対象外**
  （#157）。
