# 31. 構文大改革 v2 — `.riv.md` Literate ＋ 構造化データ ＋ 設定スクリプト化 ＋ explain 生成器

> **本書は設計先行（doc-first・phase-0）。批准前に実装に入らない（§25.10）。
> 自己マージ禁止。** 本書は **#158（design brief・統括/チャット経由で方向裁可済み）を
> 忠実に反映した起草版**で、Rivus の authoring 形式・データモデル・設定の所在を v2 として
> 束ねて再設計する。未確定の決定分岐（§31.10）は **批准 issue（#143/#149 形式）**で裁可を
> 得てから段階1を実装する。
>
> 既存の正しさ機械（byte-identity・continue-first・never-silent・IR 可逆・zero-dep・
> null モデル・記号原則）は **保存して載せ替える**。**後方互換は無視してよい**（v2 は v1 を
> 壊してよい・統括裁定 #158）。

関連：§0.13（有界/非有界＋時間＋状態）・§0.14（決定性の境界）・§0.15（信頼境界・capability）・
§03（Chunk/Column・Arrow 後継）・§06（execution-lane typing）・§25（構文 v2・fmt/trivia/
round-trip）・§26（null モデル）・§28（I/O サブストレート・`source.uri`/provenance）・
§29（記号原則・`:` 定義チェーン・共用体ビュー・`base.name`）・§30（窓＝派生グループ化キー）・
**#158（本書の brief）**・#157（窓再スコープ）・#154（arrival 対象外）。

---

## 31.0 狙いと位置づけ — 再現可能ノートブック × DAG ネイティブ

Rivus v1 は「即時実行のフロー言語」だった。v2 は **authoring（書き残す）形式を一級にする**。
動機は #158 の3つの要請の合流：

1. **構造化データを一級に** — JSON/Parquet のネスト（オブジェクト・配列）を、現状の
   degrade-to-string（`crates/rivus-runtime/src/jsonl.rs:34` `JVal::Raw(String)`）から
   **typed nested（Arrow 流 Struct/List）**へ引き上げる。
2. **コマンド引数を抱えずスクリプトに設定を宿す** — 性能ヒント・スキーマ・要求 capability を
   フラグの山ではなく**ファイルに宣言**する。
3. **YAML frontmatter ＋ Markdown 寄せ** — 文芸的（literate）に、設定と散文と実行を1つの
   ドキュメントに収める。

**北極星 ＝ 再現可能ノートブック**。marimo / Pluto と同方向（プレーンテキスト・隠れ状態なし・
git 親和）だが、**両者が命令型コードから DAG を「推論」するのに対し、Rivus はプログラムが
最初から DAG**＝推論ゼロ・実行順依存なし・隠れ状態が構造上あり得ない（より強い再現性）。
Jupyter の `.ipynb` git 地獄・実行順依存も原理的に回避する。

**ポジショニング（統括裁定 #158）**：

- **`.riv`（単独フロー）＝ 即時実行 REPL 推奨形式**（awk/シェル one-liner・`rivus run -c '…'`・
  対話的探索）。
- **`.riv.md`（Rivus Literate）＝ 正式 authored 形式**（設定・スキーマ・構造化・文芸ドキュメント・
  生成 DAG/出力）。
- **両者は同一 IR の「対の表現」**（jupytext 流ペアリング）：`.riv.md` のフェンス群 ⇄ `.riv` を
  相互変換し、fmt が往復検証を契約とする（§31.5）。

**コア無改造の相乗り戦略**：レンダリング/ノート UI は下流（Quarto/Pandoc/Jupyter）に借りる。
Rivus の仕事は **「frontmatter ＋ フェンスを解釈し実行・DAG/出力を生成」**に絞る（供給網規律・
依存ゼロと整合・§31.7）。

**段階（big-bang 回避）**：1 `.riv.md` Literate ＋ explain 生成器（**意味論ゼロ改造＝最低リスク・
ここから**）→ 2 構造化データ → 3 設定スクリプト化 → 4 ノートブック（将来）。各段は前段に乗り、
決定性・byte-identity・zero-dep・never-silent・IR 可逆を終始保つ（§31.8）。

**本書の対象外**：MyST directive/相互参照/複数フォーマット出版（＝下流 Quarto/Pandoc に任せる）／
ノート UI/リアクティブ実行のフロントエンド（＝コア外・feature-gate/別ツール・§31.8 段階4）／
window/watch の非有界意味論（§30/#157 で確定済み・蒸し返さない）。

---

## 31.1 層分け ＝ 本改革の背骨（記号原則の拡張）

**最重要原則（#158・統括確定）**：**役割ごとに表現の層を分け、層を混ぜない**。これは §29 の
**記号原則**（`( )`＝式・`{ }`＝ブロック・`:`＝定義・軽→重）の素直な拡張であり、SQL の `OVER`
を却下（#157）したのと同じ理由——**埋め込み DSL の醜さを避ける**——で駆動される。

| 層 | 表現 | 役割 | 意味論 |
|---|---|---|---|
| **宣言の木** | YAML **frontmatter** | 設定・スキーマ・要求 capability | 宣言（値・型・構造） |
| **骨格・散文** | **Markdown**（見出し・段落） | 文書構造・説明 | **強化コメント＝意味なし・inert** |
| **実行** | ` ```flow ` **フェンス** | 実行されるパイプライン | 従来の `name:` スコープがフェンス内で従来どおり |

**鉄則：式・パイプラインを YAML/Markdown 散文に潰さない。** 書式を理解するパース／ロジック／
検証（datetime 書式・正規表現・契約・集約）は **必ずフロー側（`( )`・`{ }`・`|`）**に置く
（§29 記号原則と同一）。frontmatter に載るのは「宣言できる平坦な設定」だけ。

**Markdown は「強化版コメント」**：意味を持たない（inert）。§25.7 の **コメント trivia 保存**
（`crates/rivus-ir/src/graph.rs:1435` `Node::leading_comments`）を一般化し、Markdown 散文も
**IR に riding して round-trip 保存**する（fmt が散文を保存・§31.5）。**字下げは見た目だけ
（構文非依存）**＝有意字下げの脆さを踏まない。

**Markdown は strict サブセット**：frontmatter（`---` 区切り YAML）＋ ATX 見出し（`#`）＋
フェンス（```` ``` ````）＋段落（＋表）。パーサは **frontmatter と ` ```flow ` フェンスだけ**
拾えばよく、残りは trivia。**std で軽い・依存ゼロ維持**（YAML/Markdown の最小サブセットを
自前パース・既存 lexer 同様）。

---

## 31.2 `.riv.md` Literate 形式 — frontmatter ／ Markdown ／ flow フェンス

`.riv.md` は3層を1ファイルに収める。例：

````markdown
---
title: 日次売上集計
chunk_size: 65536          # (R) 資源ヒント（結果不変）
parallel: auto             # (R)
needs: [read:sales/*.csv]  # (C) capability の宣言（付与は運用者・§0.15）
---

# 日次売上の集計

このフローは時間別の売上を集計する。下の散文は **意味を持たない**（強化コメント）。

```flow
sales:
  sales.csv |> open |> (trunc(ts, "day")) as day |# day sum:amount
```
````

- **frontmatter** = 設定/スキーマ/要求 capability の宣言（§31.3）。
- **Markdown 本文** = 強化コメント（inert・round-trip 保存）。
- **` ```flow ` フェンス** = 実行されるパイプライン。複数フェンスは複数スコープ（従来の
  `name:` ラベル付きスコープがフェンス内でそのまま機能）。フェンス間は `+`/`->` ではなく
  **名前参照**（`| name` 再利用・§25.4）で繋ぐ。

**フェンスタグ**：` ```flow `（実行）を採る。` ```rivus ` を別名にするか・実行しないコード
ブロック（` ``` ` 素フェンス＝ただの表示）の扱いは §31.10 分岐⑤で詰める。

**拡張子**：`.riv.md`（Markdown ツール群が一級認識・Quarto/jupytext 互換）。素フローは `.riv`。

**IR への lowering**：1つの `.riv.md` は **既存 `PlanGraph` 1つ**に落ちる。frontmatter の (S)
意味設定はスコープ属性へ、Markdown 散文は **新しい trivia スロット**（`Node::leading_comments`
の文書版 — フェンス前後の散文ブロックを順序保存）へ riding。**IR の実行意味論はゼロ改造**＝
段階1 は parser/fmt/explain の拡張だけで完結（最低リスク）。

---

## 31.3 設定の所在 — カスケード ＋ 三分類 ＋ capability 外部付与

**設定カスケード（Quarto 流）**：

```
frontmatter（既定）  ←  #| ハッシュパイプ（セル/フェンス単位上書き）  ←  CLI（最外の薄い上書き）
```

per-cell オプションは **フェンス内先頭の `#|`**（Quarto のハッシュパイプ）で綴る：

````markdown
```flow
#| name: daily
#| cache: true
sales.csv |> open |> (trunc(ts, "day")) as day |# day sum:amount
```
````

**設定の三分類（#158・統括確定）** — どこに置けるかは「結果バイトを変えるか」で決まる：

| 分類 | 例 | 置き場所 | 決定性 |
|---|---|---|---|
| **(S) 意味設定** | exact/decimal・datetime 書式/locale/tz・parse-error ポリシー・validate disposition | **必ず in-script**（多くは既に `:decimal`/`:datetime`/`\|!` で native） | 結果バイトを**変える** |
| **(R) 資源/性能ヒント** | chunk_size・parallel(cpus/min-bytes/serial)・fd budget・window/watch budget | **frontmatter/`#\|`** へ集約。CLI/env は薄い上書き | **結果不変**（決定性契約の外） |
| **(I) 起動/観測** | telemetry 形式/宛先・`--serve`/`--tui`・`fmt --write` | **最小 CLI** に残す | — |
| **(C) capability** | watch パス allowlist・読める源/書ける沈 | **外部付与**（運用者・§0.15） | — |

**(R) が決定性契約の外**であることが要：chunk_size/parallel を変えても **byte-identity は不変**
（serial==parallel==chunk-size は既存の stress 契約）。だから安全に frontmatter へ移せる。逆に
**(S) は結果を変えるので絶対に frontmatter/CLI に出さない**（in-script のみ）——これが「設定を
スクリプトに宿す」の正しい線引き。

**capability は宣言と付与を分離（§0.15・統括確定）**：frontmatter の `needs:`（例
`needs: [read:sales/*.csv]`）は **要求の「宣言」止まり**。**付与は運用者**（外部）であり、
**スクリプトは自己認可しない**（信頼境界・§0.15）。capability 違反は fatal ではなく
**拒否イベント**として error stream に surface（never-silent・§0.15 既存方針）。

**現状 CLI（移行元）**：`crates/rivus-cli/src/main.rs` の `--chunk-size`（行135付近）・
`--memory`（行106）・`--no-opt`（行71）等の (R)/(I) フラグ。v2 では (R) は frontmatter 既定へ、
CLI は薄い上書きへ。`--json`/`--telemetry-addr`/`--serve`/`--tui` 等 (I) は CLI に残す。

---

## 31.4 `explain` を生成器に — Mermaid DAG ／ defaulted frontmatter ／ 正準フロー書き戻し

現状 `explain` は ASCII で IR を描く（`crates/rivus-cli/src/viz.rs:20` `render_snapshot_frame`・
`PlanGraph::to_source` の再生成 source）。v2 では **explain を `.riv.md` の生成器**に拡張する。
3つを生成し、**`--write` は生成領域だけを冪等に置換**する：

1. **Mermaid DAG**（埋め込み用）：IR から ` ```mermaid ` フェンスを生成。**出力専用＝戻さない**
   （round-trip 負担ゼロ・毎回 IR から再生成）。`viz.rs` の ASCII レンダラの隣に Mermaid
   エミッタを足す（純関数・I/O なし・unit-testable）。
2. **defaulted frontmatter**：書かれた設定（set）と既定値（default）を **区別して**出力
   （`# default` 注記）。`--write` は **set を保存し default 領域だけ**更新＝冪等。
3. **正準フロー**：`PlanGraph::to_source`（`graph.rs:1626`・既存の可逆再生成）を ` ```flow `
   フェンスへ。最適化後 IR を渡せば最適化済みフローも生成可能（`explain` の本領）。

**冪等性の機構（センチネル）**：生成領域は **センチネルで囲う**（例 `<!-- rivus:begin … -->` …
`<!-- rivus:end -->`）。`--write` は **囲いの中だけ置換**し、**手書き散文は保存**する。これは
§25 の fmt round-trip 契約（trivia 保存・`graph.rs:1681` の `leading_comments` 再出力）を
文書全体へ一般化したもの。**生成（戻さない Mermaid）と可逆（戻すフロー/frontmatter）を
センチネルで分離**するのが鍵。

> 設計上の対称性：**戻すもの**（フロー・frontmatter＝IR と等価・round-trip 必須）と
> **戻さないもの**（Mermaid・defaulted 注記＝IR の射影・毎回再生成）を明確に分ける。後者を
> round-trip 対象にしないことで「生成物を編集して壊れる」事故を構造的に防ぐ。

---

## 31.5 `.riv` ⇄ `.riv.md` ペアリング — jupytext 流・往復検証を fmt 契約に

`.riv`（素フロー）と `.riv.md`（Literate）は **同一 IR の対の表現**。jupytext の `--sync`/
`--test-strict` に倣い、**相互変換と往復検証を fmt の契約**にする：

- **`.riv.md` → `.riv`**：フェンス群を連結（散文/frontmatter は落ちる＝lossy・素フローは
  実行のみ）。
- **`.riv` → `.riv.md`**：フェンスに包む（散文なし・最小 frontmatter）。
- **往復検証**：`.riv.md` の **フェンス内容**は `parse → to_source → parse` が安定
  （§25 の既存 round-trip 契約）。frontmatter/散文の保存は §31.4 のセンチネル機構が担保。

**lossless の境界**：フェンス（実行）と frontmatter（設定）は lossless。Markdown 散文は
`.riv.md` 内で round-trip 保存されるが、`.riv` へ落とすと消える（素フローに散文の居場所が
ないため＝設計どおり）。Quarto/jupytext 互換は **`#|`/frontmatter キーの対応**まで保証し、
出版層（MyST directive 等）は対象外（§31.7）。fmt のテストはこの境界を pin する。

---

## 31.6 構造化データ一級化 — Arrow nested ／ パス式キー（段階2）

**構造化 ≠ 行指向ボックス**。現状の core はフラット・スカラ列（`crates/rivus-core/src/chunk.rs:300`
`ColumnData` の Bool/I64/F64/Dec/DateTime/…/Str/Resource）で、ネストは degrade-to-string
（`jsonl.rs:34` `JVal::Raw`）。v2 は **Arrow レイアウトのネスト列**を足す（依然 columnar・
SIMD フレンドリ・byte-identical）：

- **Struct ＝ 子列の束**（field 名 → 子 `Column`）。
- **List ＝ offsets ＋ 子列**（`i32` offsets ＋ 単一子 `Column`・可変長）。

`chunk.rs:6` が予告する **§03 の「Arrow-backed, zero-copy successor that slots in behind this
same API」**と同線。**core のフラット・スカラ列は無改造の最速路（退化形）として残す**——ネストは
それを子に持つ再帰構造。`DataType`（`crates/rivus-core/src/value.rs:1401`）に `Struct`/`List`
変種を、`Value`（`value.rs:1322`）に対応スカラを足す。

**アクセスは既存ドット構文の一般化**。現状アクセサ（`crates/rivus-ir/src/expr.rs`）：
`Expr::Field{name, access}`（`Access::Fast`=`$_.f`/`Deep`=`$_..f`/`Source`=`source.uri`・
行202-234）・`Expr::FieldAt(u32)`=`$_[i]`（行241）・`Expr::SubView{base,name,..}`=`base.name`
（行248・§29 char スライス）。これを **パス式へ一般化**：

```
user.age          # Struct フィールド
tags[0]           # List 添字
$_.user.age       # 現在行のパス
$_..age           # 深いパス（曖昧性ルールは §31.10 分岐③）
```

**IR のキーをパス式へ**：現状 group/sort/distinct/join のキーは **`Vec<String>`**
（`crates/rivus-ir/src/graph.rs`：`GroupBy{keys:Vec<String>}` 行724・`Sort{keys:Vec<(String,bool)>}`
行684・`Distinct{keys:Vec<String>}` 行689・`Join{left_keys,right_keys}` ・`FilterProject{fields:
Option<Vec<String>>}` 行732）。これらを **path 式（`Vec<PathExpr>` 的）へ一般化**し、bare name は
パスの退化形（深さ1）として吸収。runtime のキー索引（`operators/aggregate.rs` の `for k in
&self.keys`）も path 解決へ。**退化形（フラット列・深さ1パス）は既存の最速路を一切変えない**
ことを byte-identity 契約で pin。

**never-silent なネスト破損**：壊れたネスト（型不一致・欠損フィールド）は **型付き null ＋計上**
（§26 null モデル）。**不透明文字列退避（degrade-to-string）をやめる**——これが #158 の構造化の
肝。`explode`/`unnest`（List → 行増殖）と空/null の扱いは §31.10 分岐③で詰める。

> 段階2 は型ラティス（§06）に触れる本丸。本書は**方向と接続点**を定め、**詳細仕様は §32（構造化
> データ）として別 doc ＋別批准**に分けるのが妥当（§31.10 分岐①）。段階1（§31.2-31.5）は
> 構造化に依存せず単独で実装・批准できる。

---

## 31.7 先行事例と相互運用（jupytext / Quarto / marimo）

車輪の再発明を避け、`.riv.md` を **Quarto/jupytext のテキスト規約に準拠**させて既存生態系へ
相乗りする（#158）：

| 事例 | 取り入れる要素 | 取り入れない |
|---|---|---|
| **jupytext** | テキスト正準・リッチ表現は派生／`.riv`⇄`.riv.md` ペアリング／往復検証を fmt 契約に | percent/light 等の複数スクリプト方言（2形式に絞る） |
| **Quarto（.qmd）** | **`#\|` セルオプション**＋**設定カスケード**（frontmatter 既定←セル上書き） | MyST directive/相互参照/複数フォーマット出版（下流に任せる） |
| **marimo / Pluto** | 北極星の先行実証（DAG・隠れ状態なし・git 親和） | DAG を**推論**する点（Rivus は DAG ネイティブ＝より強い再現性） |

**相乗りの帰結**：`.riv.md` が Quarto/jupytext/Jupyter の一級ドキュメントになる → 任意の
Markdown/Quarto エディタで編集でき、将来「Quarto 用 Rivus エンジン」や jupytext ペアリングで
**レンダリング/ノート UI を自作せず借りる**。**Rivus の仕事は「frontmatter＋フェンスを解釈し
実行・DAG/出力を生成」に絞る**（コア無改造・依存ゼロ）。

参考：Jupytext（Notebooks as Markdown・CLI sync/test）／Quarto Execution Options／
marimo dataflow graph。

---

## 31.8 段階（big-bang 回避・各段でコア不変）

| 段階 | 内容 | リスク | doc/批准 |
|---|---|---|---|
| **1** | `.riv.md` Literate ＋ explain 生成器（Mermaid・defaulted frontmatter・正準フロー・`#\|` セルオプション・ペアリング） | **最低**（意味論ゼロ改造＝parser/fmt/explain 拡張のみ） | **本書 §31（ここから着手推奨）** |
| **2** | 構造化データ一級化（ColumnData に Struct/List・パス式キー・`explode`・JSON typed nested） | 中（型ラティス §06 に触れる本丸） | **§32 別 doc ＋別批准**（推奨・§31.10 分岐①） |
| **3** | 設定スクリプト化（frontmatter/`#\|` の (S)/(R) ＋ CLI/env 薄い上書き） | 中（1 と相乗） | **§33 別 doc or §31 拡張**（§31.10 分岐①） |
| **4** | 再現可能ノートブック（Quarto/Jupyter 相乗り・リアクティブ再実行・ライブプレビュー） | 高（フロントエンド） | 将来・**コア外**（feature-gate/別ツール・zero-dep コア無改造） |

各段は前段に乗り、**決定性・byte-identity・zero-dep・never-silent・IR 可逆**を終始保つ。
段階1は構造化（段階2）に依存しないので、**単独で実装・批准・着地できる**のが要点。

---

## 31.9 正しさの継承（不変条件）

本改革を貫く不変条件（毎スライス）：

- **既定ビルド依存ゼロ**：YAML/Markdown の strict サブセットは自前パース（既存 lexer 同様 std）。
  レンダリング/ノート UI は **コア外**（下流に借りる・段階4 は feature-gate）。
- **byte-identity**（有界部分木・serial==parallel==chunk-size）：構造化のネスト列も columnar を
  保ち、退化形（フラット列）は既存最速路を変えない。#41 の f64 非結合性のみ不変（datetime は
  exact i64）。
- **never-silent・continue-first**：壊れたネストは **型付き null ＋計上**（§26）、degrade-to-string
  をやめる。capability 違反・unbound hole は error stream に surface。
- **IR 可逆**（to_source/fmt round-trip・§25）：フェンス/frontmatter は lossless、Markdown 散文は
  trivia として保存、Mermaid/defaulted は生成専用（戻さない）。
- **英日両ガイド同時更新**（機能 PR 時。design doc は日本語のみ）。
- **capability は外部付与**（§0.15）：`needs:` は宣言止まり・自己認可不可。

---

## 31.10 決定分岐（→ 批准・#143/#149 形式）

#158 末尾の未確定分岐①〜⑧を整理する。各々に**推奨**を付す（批准 issue で裁可を得る）：

1. **doc 構成**：§31 一本に束ねるか、§31(literate)/§32(構造化)/§33(設定) に割るか。
   - **推奨：本書 §31 を傘＋段階1詳細**とし、**段階2（構造化）＝§32・段階3（設定）＝§33 を
     別 doc ＋別批准**に分ける（§28/§29/§30 と同じ粒度・段階1が単独で着地できるため）。
2. **構造化モデル範囲**：Struct＋List で JSON/Parquet を覆う。`Map`（動的キー）は後段か。
   - **推奨：段階2 は Struct＋List まで**。`Map` は実需が出た段で別スライス。
3. **パス解決の意味論**：`.` 自動解決・`..`（深い）の曖昧性ルール・`[i]`/スライス・list への
   集約定義・`explode` と空/null。
   - **推奨：§32（構造化 doc）で詳細裁定**。本書は「ドット構文の一般化」方向のみ確定。
4. **IR 変更**：group/sort/proj のキーを `Vec<String>` → path 式へ一般化する形。
   - **推奨：`PathExpr` 型を新設**し bare name を深さ1の退化形に。退化形は byte-identity 不変を
     pin。詳細は §32。
5. **`.riv.md` 詳細**：フェンスタグ（` ```flow ` / ` ```rivus `）・拡張子（`.riv.md`）・`#\|` の
   正準キー一覧・`explain --write` の書き戻し可否とセンチネル方式・表をインラインデータ源に
   するか。
   - **推奨：` ```flow ` ＋ `.riv.md`・センチネル方式で書き戻し可・表はインラインデータ源に
     しない（段階1）**。正準キー一覧は §31.3 三分類に従い段階1で確定。
6. **ペアリング規約**：`.riv`⇄`.riv.md` の往復規則（fmt 契約・どこまで lossless か）・
   Quarto/jupytext 互換の範囲。
   - **推奨：フェンス/frontmatter は lossless・Markdown 散文は `.riv.md` 内で保存（`.riv` 化で
     消える）・互換は `#\|`/frontmatter キー対応まで**（出版層は対象外）。
7. **設定キーの正準名**（frontmatter/`#\|` の (S)/(R) スキーマ）と CLI/env 上書きの優先順位。
   - **推奨：カスケード `frontmatter ← #\| ← CLI`（§31.3）で確定。(S) は in-script のみ・(R) は
     frontmatter 既定**。正準名一覧は段階1/3 で詰める。
8. **fmt/explain の round-trip 契約**（strict サブセット・冪等・生成領域 vs 手書き散文の分離）。
   - **推奨：センチネルで生成領域を囲い `--write` は中だけ置換・手書き散文は保存・Mermaid は
     生成専用**（§31.4）。

---

## MVP / 次 / 将来

- **MVP（本書 §31 批准の対象＝段階1）**：`.riv.md` Literate 形式（frontmatter/Markdown/
  ` ```flow ` フェンス）＋ explain 生成器（Mermaid DAG・defaulted frontmatter・正準フロー
  書き戻し・センチネル冪等）＋ `#|` セルオプション ＋ `.riv`⇄`.riv.md` ペアリング。
  **意味論ゼロ改造**（parser/fmt/explain 拡張のみ・IR 実行不変）。
- **次**：段階2 構造化データ（**§32 別 doc ＋別批准**：ColumnData に Struct/List・パス式キー・
  `explode`・JSON typed nested）→ 段階3 設定スクリプト化（§33）。
- **将来**：段階4 再現可能ノートブック（Quarto/Jupyter 相乗り・リアクティブ再実行・ライブ
  プレビュー）＝**コア外**（feature-gate/別ツール・zero-dep コア無改造）。`Map` 型・出版層は
  下流/別スライス。**非有界 window/watch は §30/#157 で確定済みの対象外を継承**。
