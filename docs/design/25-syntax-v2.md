# 25. Unified Flow Syntax v2 — flowing authoring + IR tidy

> 統括方針（2026-06-03, 申し送り経由）: 構文 v2。**記号は最小**、**再利用は名前付き
> フローを名前でパイプ**、**スコープ継続で書き継ぐ**、**`$x` 値ホール（プリペアド方式・
> injection-safe）**、**シグネチャ＝強制される意味契約**、**コメントは inert trivia として
> IR 保持（round-trip 不変）**、**fmt/tidy/explain は IR ベース清書**。
> **本書は #87 = phase-0（設計先行）。批准前に実装に入らない。** Epic #86。
> 構文/IR の方向を決めるため #85 同様 **レビュー批准必須・自己マージ禁止**。

## 25.1 目的 / 原則

「**書き手は流れを止めない**」。authoring は前→後ろへ素直に流して書け、整形（fmt/tidy）
は機械が **IR から清書**する。構文は人間の流れを優先し、正規化は IR の責務に寄せる。
§01 8 法則（特に *Everything is Flow* / *IR Reversible* / *Text is stream*）と、IR が単一の
真実（§04）・`to_source` 可逆（§04, §08）に完全準拠。v1（§10）を壊さず上位互換に保つ。

## 25.2 記号は最小（転送 4 種）

| 記号 | 意味 |
|---|---|
| `\|?` | filter（述語・AND はカンマ） |
| `\|>` | project / computed columns |
| `\|#` | group / aggregate |
| `\|!` | validate（§24・契約強制・never-silent） |

source/sink/scope は**語**（`open`/`save`/`scope:` … `;`）で表す。記号の乱立を避け、
転送はこの 4 つに集約。**代数的シジルや `@` パッチ等の追加記号は却下**（§25.5）。

## 25.3 `$x` 値ホール（プリペアド方式・injection-safe）

リテラルを埋める箇所は **`$x` プレースホルダ＋束縛**で渡す（SQL プリペアドと同型）。
文字列連結でフローを組ませない → **injection-safe**。

```
F: open data.csv |? country == $c |? age >= $min ;     # $c, $min を束縛で渡す
```

- 値は IR の**定数ノード**に束縛され、ソース文字列へは補間されない。
- CLI / 埋め込み API は束縛セットを**別経路**で渡す（解析後に束縛）。未束縛は明示エラー。
- `to_source` は `$x` を**そのまま保持**（束縛値は埋め込まない）→ round-trip 安全。

## 25.4 再利用＝名前付きフローを名前でパイプ

フローに名前を付け、**名前でパイプして合成**（関数合成的）:

```
clean : open data.csv |? age in $lo..$hi |! id required ;
report: clean |# country count ;          # clean の結果を名前で受けて続ける
```

- `def` キーワード / 末尾 `@` パッチ / 代数的シジルでの合成は **却下**（読みにくさ・goto 性）。
- 再利用は「**名前付きフロー → 名前で参照**」の一手に統一。IR 上は既存の scope/`StreamRef`
  ノードへ lower（新 IR 不要）。

## 25.5 スコープ継続で書き継ぐ / 結果ラベル廃止

- 既存フローへの追記は**同名スコープを継続**して書き継ぐ（行を足す）。
- 末尾に `@` で後付けする方式は **goto 的**でデータ流を壊すため **却下**。
- **フロー結果のラベルは廃止**（結果に明示ラベルを付けない）。下流参照は §25.4 の
  名前付きフロー参照で代替し、構文要素を 1 つ減らす。

## 25.6 シグネチャ＝強制される意味契約

名前付きフロー/ブロックは署名を持てる:

```
clean (lo:int, hi:int) expects(id required, age:int) "rows kept in [lo,hi]":
    open data.csv |? age in lo..hi |! id required ;
```

- 署名はコメント的に**見える**が、`expects(...)` は **強制される意味契約**で **§24 の
  validator に lower**（違反は never-silent disposition で surface）。
- `(params)` は `$x` 束縛の**型付き入口**、`"doc"` は説明文字列。ブロック化可。
- 「コメント風だが効力を持つ」点が #25.7 の inert コメントとの決定的な違い。

## 25.7 コメントは inert trivia（IR 保持・round-trip 不変）

- コメント `#{ ... }#` は**意味を持たない trivia**だが、**IR に保持**し `to_source` で
  位置ごと復元する（**round-trip 不変条件に含める**）。整形で書き手のメモが消えない。
- `expects`（enforced 契約）と `#{}#`（inert trivia）は明確に別物。

## 25.8 fmt / tidy / explain は IR ベース清書

- `rivus fmt` / `tidy`: パース → IR → **IR から清書**（流れ書きを正規形へ）。trivia 保持。
- `rivus explain`: IR を可視化（§14・最適化レポート同伴）。
- 書き手は流れを止めず素に書き、**機械が IR 経由で整える** → 自由な authoring と
  決定的な正規化を両立。

## 25.9 IR / round-trip への要件（v1 互換）

v2 は **IR（PlanGraph）を変えない**——同じ DAG に lower する“書き味”の層:

- `$x` 値ホール = 定数束縛ノード（解析後束縛）。
- 名前付きフロー参照 = 既存 scope / `StreamRef`。
- 署名 `expects(...)` = §24 `Op::Validate` へ lower、`(params)` = 束縛入口。
- コメント trivia = ノード/エッジに付帯保持（`to_source` 復元）。

`to_source` は `$x`・trivia・署名を**忠実に往復**。`optimizer_equiv`（§08）でバイト不変を
ゲートし、round-trip（trivia 含む）テストを必須化。

## 25.10 段階計画（Epic #86 / phase-0 #87）

| phase | 内容 | 状態 |
|---|---|---|
| 0 | 本設計ドキュメント（#87） | 本書（**批准待ち**） |
| 1 | `$x` 値ホール（lexer/parser ＋ 束縛 ＋ `to_source` 保持） | 批准後 |
| 2 | 名前付きフロー参照の整理 ＋ フロー結果ラベル廃止 | |
| 3 | 署名 `expects(...)` → §24 validator へ lower | |
| 4 | コメント `#{}#` の IR trivia 保持 ＋ `fmt`/`tidy`（IR 清書） | |

各 phase は `to_source` round-trip（trivia 含む）と `optimizer_equiv` をゲート。
v1 構文は壊さない（上位互換）。**構文/IR を変えるため批准必須・自己マージ禁止。**

## 25.11 将来スライス：`is null` / `is not null` と `null` リテラル（#81 連携）

§26（null モデル）が **§26.0 非目標**として「`is null` 述語の最終形・`null` リテラルは
§25 構文側で別途」と送った欠損の**明示選択**構文を、ここで正式に引き取る（埋もれ防止）。

- **位置づけ**：#81 null モデルは欠損を「**落とす/除外/補完/検出**」までを実現する
  （`dropna`・比較・`fill`・`coalesce`）。本スライスはその最後の 1 ピース＝欠損行を
  **明示的に“選び出す”** 述語（`|? x is null` / `|? x is not null`）と `null` リテラル。
- **着手時期**：**#81 null モデル完了後**（少なくとも STEP 2-② で述語が validity 対応に
  なってから）。それ以前に構文だけ入れても意味論の土台が無い。
- **設計指針（統括）**：Rivus の強みは「**SQL 同等処理＋フローらしい処理**」。SQL の
  `WHERE x IS NULL` を機械的に写すのではなく、**既存述語・`dropna` と一貫したフロー語彙**に
  馴染む形にする。例（候補・確定ではない）:
  - 述語：`|? x is null` / `|? x is not null`（`is` を最小の述語キーワードとして導入）。
  - `dropna` との一貫：`dropna` は「null 行を落とす」明示操作、`|? x is not null` は
    「null でない行を残す」述語——同じ欠損概念を**操作**と**述語**の両語彙で提供し、
    どちらも §26.2(a) の「null 等価規則」と矛盾しないこと。
  - `null` リテラル：式中の欠損定数（`fill x null` や比較の右辺など）。`to_source` 往復・
    `eval` の validity 対応（`column_from_values`/`const_column` は既に validity=0 を生成可能）。
- **不変条件**：IR（PlanGraph）は欠損を構文要素として持たない（§26.6）。`is null` は
  **validity 対応の述語** として eval に落ち、`null` リテラルは validity=0 の定数列に lower。
  `to_source` 忠実往復・`optimizer_equiv` バイト不変をゲート。**批准必須・自己マージ禁止。**
