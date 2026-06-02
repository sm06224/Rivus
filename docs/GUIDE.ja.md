# Rivus — 構文・利用ガイド（日本語版）

Rivus はフロー指向・DAG ネイティブ・ストリーミングのデータランタイムです。
**フロー**（ソース → 変換 → シンク）を記述すると、Rivus がそれをチャンク単位で、
有界メモリで、オプティマイザとライブテレメトリ込みで実行します。

このガイドは実務リファレンスです（完全な構文・全オペレータ・コピペできる例集）。
設計思想は [`docs/design/`](design/README.md)、インストールは [README](../README.md)
を参照してください。英語版は [`docs/GUIDE.md`](GUIDE.md) です。

> このドキュメントは英語版ガイドと同じ内容を日本語でまとめたものです。実装が
> 進んだ際は両方を更新します。差異を見つけたら英語版（`GUIDE.md`）が正です。

---

## 1. 10 秒でわかるメンタルモデル

```
Scope:                 # 実行グラフ上の名前付きノード
    open data.csv      # ソース（フローの先頭）
    |? age >= 20       # 変換（フィルタ）
    |> name age        # 変換（射影）
    save out.csv       # シンク
;                      # スコープ終端
```

- プログラムは **スコープ** の集合。`Name: … ;` で 1 つ定義します。
- スコープの最初の行が **ソース**（`open …`）、残りは左から右へ適用される
  **変換** と **シンク** です。
- `|?` `|>` `|#` はパイプ演算子。`->` `+` `&` で DAG（分岐 / マージ / 結合）を
  組みます。スコープは名前で相互参照できます。
- 空白・改行は意味を持ちません（1 行でも複数行でも可）。`#` は行コメント
  （ただし `|#` はグループ演算子）。

---

## 2. フローの実行

```sh
rivus run flow.riv                 # ファイルから実行
rivus run -c 'U: open u.csv … ;'   # インラインで渡す
rivus run - <<'RIV' … RIV          # 標準入力（ヒアドキュメント）
rivus explain -c '…'               # DAG・IR・オプティマイザのレポートを表示
rivus check   -c '…'               # 構文チェックのみ
```

フラグ: `--chunk-size N`（チャンクあたり行数、既定 4096）、`--no-opt`
（オプティマイザ無効）、`--json`（ASCII 表示の代わりに機械可読な JSONL
テレメトリを stderr へ。stdout はクリーンなデータのまま）、
`--telemetry-addr HOST:PORT`（同テレメトリを TCP ソケットへ配信）。

**stdout と stderr。** 実行グラフ・テレメトリ・エラーストリームは **stderr** へ、
`save stdout` / `save -` シンクは **stdout** へクリーンなデータを書きます。
だから Rivus はそのままシェルパイプに差し込めます。

---

## 3. ソース（フローの先頭）

| 構文 | 読み込むもの |
|---|---|
| `open PATH` | 拡張子で形式判定（`.csv`→CSV、`.jsonl`/`.ndjson`/`.json`→JSON、`.tsv`/`.tab`→TSV） |
| `open PATH as FMT` | 形式を強制（`csv` \| `tsv` \| `json` \| `jsonl` \| `ndjson`） |
| `open PATH.gz` / `PATH.zst` | **gzip / zstd 圧縮**された CSV/TSV。`--features gzip` / `--features zstd` でビルドした時のみ。直列・単一パス・有界メモリ。既定（依存ゼロ）ビルドでは `--features …` を促すエラー |
| `open PATH noheader` | ヘッダ行なし CSV。列名は `c0, c1, c2, …`、先頭行からデータ |
| `open PATH (col[:type] …)` | **スキーマ宣言**：列名を位置で与え（ヘッダ / `c0…` を上書き）、任意で型を固定。`int`/`i64`, `float`/`f64`, `str`/`string`, `bool`, `decimal(N)`（厳密固定小数点）, `datetime[("fmt")]`（厳密な時刻、§6 参照）。例 `open f.csv (id:int zip:str age)` は `zip` の先頭ゼロを保持。`open sales.csv (id amount:decimal(2))` は `amount` を厳密に読む。`open log.csv (ts:datetime("yyMMddHHmmss"))` は `ts` を時刻として読む |
| `readcsv PATH` / `readjson PATH` | 形式を明示する動詞 |
| `readbin PATH [le\|be] [packed\|aligned] (name:type …)` | 固定長バイナリレコード（C 構造体ダンプ） |
| `open stdin` / `open -` | 標準入力から CSV（または `as FMT`）を読む |
| `stream NAME` | 名前付きフローを再生（MVP: 参照） |

形式判定は拡張子を過信しません。拡張子が嘘をつくときは `open data.dat as json`、
ひと目で分かるようにしたいときは `readcsv`/`readjson` を使ってください。

**対応形式（現状）:** CSV（引用フィールド対応・`noheader`・スキーマ宣言・TSV/
カスタム区切り）、JSON Lines / JSON 配列 / NDJSON、固定長バイナリ、gzip/zstd
圧縮入力（feature-gated）。

---

## 4. 変換

### `|?`（`where`）— フィルタ

```
|? age >= 20
where age >= 20, country == "JP"      # カンマ = AND（`and` と同じ）
|? country == "JP" and active == true
|? score > 90 or age < 18
|? (score / age) > 3                  # 括弧内で算術（§6 参照）
```

### `|>` — 射影 / 列の計算

各項目は次のいずれか：

| 項目 | 意味 |
|---|---|
| `name` | 列 `name` を残す |
| `name as alias` | 残してリネーム |
| `(expr) as alias` | **計算列**（括弧内に式） |

`(expr) as name` は算術（`+ - * / %`）・文字列/数値/述語関数・型キャスト
`expr:type` を使えます（§6）。

### `|#` — グループ化（1 つ以上のキー）

キー列で分割し集約します。`count` は常に出力。`func:col` で集約列を追加。
複数キーは**列のタプル**で分割します（各キーが `count` の前に独立した列になる）。

- 数値: `sum avg min max std`（std は標本、ddof=1）
- パーセンタイル: `median` と `pNN`（`p50 p90 p99 …`、線形補間）
- 異なり数: `count_distinct`（別名 `nunique`）
- 位置: `first last`（ソース順で最初/最後の非空値）

```
|# country                          # → country, count
|# country region sum:score         # 複数キー → country, region, count, sum_score
|# country sum:score avg:age        # → country, count, sum_score, avg_age
|# country median:score p90:score   # → country, count, median_score, p90_score
```

出力列名は `count` と `<func>_<col>`（例 `sum_score`）。`std`/パーセンタイルは
グループの値をバッファ（`sort` 同様のパイプラインブレーカ、グループ濃度で有界）、
他は O(1) メモリでストリームします。

### `take` / `limit` / `head` — 行数を制限

```
take 100
```

### `sort` — 1 つ以上のキーで整列

ストリーム全体の安定ソート（ブロッキング段）。同値はソース順を保持。複数キーは
各キーで順に、各々に方向を指定できます。

```
sort age              # 昇順（既定）
sort age asc
sort score desc
sort team score desc  # team 昇順、その中で score 降順
```

### `distinct` — 重複除去

```
distinct                 # どの列でも一致する行を除去（行全体）
distinct city region     # 指定列で重複除去
```

最初の出現を残します（ソース順で決定的、チャンク非依存）。

### `dropna` / `fill` — 欠測値

```
dropna                 # いずれかの列が空の行を落とす
dropna city region     # 指定列が空の行を落とす
fill city "UNKNOWN"    # `city` の空セルを定数で埋める
fill price ffill       # 前方補完：直前の非空値を繰り下げ
fill price bfill       # 後方補完：次の非空値を繰り上げ
fill score mean        # 列平均で埋める（数値セル）
fill score median      # 列中央値で埋める
```

「欠測」セルは空文字列です。数値列は空を保持できない（パース時に 0 になる）ので、
空を検出/クリーニングしたい列は `:str` 宣言してください。`ffill`/`bfill` は
チャンク境界をまたいで最近傍を運びます（先頭の空は前方補完元なし、末尾の空は
後方補完元なし）。`bfill` は finish までバッファ（`sort` 同様のパイプライン
ブレーカ）、`ffill` は完全ストリーミング。`mean`/`median` は非空数値セルの
全列統計を計算して空に代入します（これもパイプラインブレーカ）。整数結果は
末尾 `.0` を付けません。すべての `fill` メソッドは非空セルを変更しません。

### `describe` — 1 パスの列サマリ

ストリームを列ごとの要約に置き換えます（pandas `.describe()` / SQL `DESCRIBE`
相当）：`column`, `type`, `count`、数値列は `min`, `max`, `mean`。単一パス。

```
open data.csv describe save stdout as csv
# column,type,count,min,max,mean
# id,i64,1000,1,1000,500.5
```

### `rename` / `drop` / `reorder` / `cast` — 列の形

ステートレスでストリーミングな列操作（`|>` 不要）：

```
rename age years city loc   # その場でリネーム：age→years, city→loc
drop zip notes              # 列を削除、残りは順序維持
reorder name id             # name,id を先頭へ、残りは元の順
cast age:int price:f64      # 列をその場で再型付け
```

`rename` は位置・型・値を保持（未知名は警告）、`drop` は指定列を削除（未知名は
無視）、`reorder` は指定列を先頭に浮かせる純粋な並べ替え（未知名は無視・重複は
除去）、`cast` は名前付き列をその場で再型付け（位置・名前は保持、値を再 coerce）。
いずれも `to_source` で round-trip します（型名は正規化、`int` → `i64`）。

---

## 5. DAG：分岐・マージ・結合

- `-> Child: body ;` — **分岐（tee）**：各チャンクを子へも転送。
- `A + B [+ C …]` — **マージ**：指定ストリームの和集合。
- `A & B on key` — **内部結合**（`on lkey:rkey` で左右別名）。出力 = 左の全列 +
  右の全列（結合キーを除く。左と衝突する名前は `_r` を付与）。
- **複合キー：** `on k1 k2 …` は列のタプルで結合（例 `A & B on country region`）。
  各キーは別名なら `lk:rk`、混在も可（`on a x:y`）。下記すべての結合種で有効。
- `A &left B on key` — **左外部結合**：左の全行を保持。右が一致しなければ右列を
  型デフォルト（`0` / `0.0` / `false` / 空文字）で埋める。
- `A &right B on key` — **右外部結合**：右の全行を保持（左列をデフォルト埋め）。
  結合キー列は右キーを保持するので、孤立した右行もキーを失いません。
- `A &full B on key` — **完全外部結合**：両側の全行。未マッチ側はデフォルト埋め。

```
# 2 つの CSV を id で内部結合
Users:  open users.csv ;
Orders: open orders.csv ;
Joined: Users & Orders on id  |> name amount  save out.csv ;

# 左結合：注文がないユーザーも残す（amount → 0）
AllUsers: Users &left Orders on id  |> name amount  save out.csv ;
```

スコープは付けた名前で参照します。結合はブロッキング段（両入力をバッファ）で、
`sort`/`|#` と同じくパイプラインブレーカです。

---

## 6. 式

`|?` 述語と `(…)` 計算列で使います。

**値**

| 種類 | 例 |
|---|---|
| 整数 / 浮動小数 | `42`, `3.14` |
| 文字列 | `"JP"`（エスケープ: `\n \t \" \\`） |
| 真偽値 | `true`, `false` |
| 現在行のフィールド | `age`（裸）, `$_.age`（明示） |
| 深い / 動的フィールド | `$_..age`（再帰）, `item("age")`（動的） |
| 親スコープのフィールド | `$_:1.country`（`$_:0` = 現在、`$_:1` = 親 …） |

**関数**

- *文字列* — `upper(s)`, `lower(s)`, `trim(s)`, `len(s)` → int,
  `substr(s, start, len)`, `replace(s, from, to)`, `split_part(s, sep, n)`
  （1 始まりのフィールド）, `concat(a, b, …)`。
- *述語*（→ bool）— `contains(s, sub)`, `starts_with(s, p)`, `ends_with(s, p)`,
  `like(s, pat)`, `glob(s, pat)`、および（`--features regex` 時）`regexp(s, re)`。
- *数値* — `abs(x)`, `round(x)`（0 から離れる丸め）, `floor(x)`, `ceil(x)`。
  結果が整数なら整数、そうでなければ浮動小数を返します。
- *null 合体* — `coalesce(a, b, …)`：テキストが空でない最初の引数（SQL/pandas の
  null-coalesce）。

```
|? contains(email, "@gmail")
|> (upper(name)) as NAME (len(name)) as nlen (substr(zip, 0, 3)) as area
|> (round(price * 1.1)) as gross (coalesce(nick, name)) as display
```

**比較** — `==  !=  <  <=  >  >=`
**論理** — `and`, `or`
**算術**（括弧内）— `+  -  *  /  %`。`* / %` が `+ -` より強く結合、括弧で入れ子。

**型キャスト** — `expr:type` は値のレーンを再解釈（`int`/`i64`, `float`/`f64`,
`str`/`string`, `bool`, `decimal(N)`）、最も強く結合：

```
|? age:int >= 20            # 文字列列を数値として比較
|> id (price:f64 * 1.1) as gross
|> (age:str) as age_text    # add-property キャスト（3 つ目の型付け方法）
cast age:int price:f64      # cast 動詞：列をその場で再型付け
```

**厳密 decimal レーン（`decimal(N)`）** — 浮動小数の丸めが許されない場面（金額、
byte-identical な並列合計）向けのオプトイン固定小数点レーン（`i128` を小数 `N`
桁でスケール）。値が整数なので `0.1 + 0.2` は厳密に `0.3`、加算は**結合的**＝
並列の partition→merge が直列と 1 ビットも違わない（f64 では不可能）。読み取り段で
宣言すると **テキスト→`i128` を f64 非経由で厳密**に読み、式キャストもできます：

```
open sales.csv (id amount:decimal(2))   # "12.5" を 12.50 として厳密に読む
|? amount >= 19.99                       # i128 で厳密比較（浮動小数を経由しない）
|> id amount
```

スケールは現状必須（`decimal(2)`、bare `decimal` 不可）。`N` 桁を超える小数は
**round-half-even** で決定的に丸め、不足は 0 詰め、解釈不能セルは `0`（continue-first）。
既定は従来どおり `i64`/`f64` の高速レーン — `decimal` は「速度より正確性」を
*選ぶ* ときだけのオプトインです。

**日時レーン（`datetime[("fmt")]`）** — 固定幅 / ISO のタイムスタンプを、文字列でも
近似 float でもなく**厳密な時刻**（Unix epoch からの秒数 `i64`、UTC）として読みます。
`decimal` と同じく整数表現ゆえ厳密・**結合的**なので、日時の `min`/`max`/`count` や
日付バケットの group-by は並列でも byte-identical。書式を宣言するか、よくある形を
自動推論します：

```
open log.csv (ts:datetime("yyMMddHHmmss") msg)  # "260601143000" を厳密にパース
|? ts >= "2026-06-01"                            # リテラルも同じレーンへパース
|> (format(trunc(ts, "day"), "yyyy-MM-dd")) as day msg
|# day count:msg                                 # 日次の件数（時系列集計）
```

- **書式トークン**（`strptime` の最小部分集合、std のみ）：`yyyy` `yy` `MM` `dd`
  `HH`/`hh` `mm` `ss`。それ以外の文字は一致必須のリテラル。2 桁年は
  `00–68 → 20xx`・`69–99 → 19xx` で決定的にピボット。bare `:datetime`（書式なし）は
  `yyyy-MM-ddTHH:mm:ss` → `yyyy-MM-dd HH:mm:ss` → `yyyy-MM-dd` → `yyyyMMddHHmmss`
  → `yyMMddHHmmss` → `yyyyMMdd` の順で自動推論。
- **比較**はリテラルを同じレーンへパースして時刻同士で比較（`ts >= "260601000000"`）。
  どの書式にも一致しないセル/リテラルは epoch `0` / 非時刻として継続（continue-first。
  そのリテラルに対しては `!=` のみ真）。
- **関数**：`year(ts)` `month(ts)` `day(ts)` `hour(ts)` `minute(ts)` `second(ts)`
  （整数）、`trunc(ts, "day"|"hour"|"minute"|"month"|"year")`（日時バケットキー）、
  `format(ts, "fmt")`（文字列）、`ts2 - ts1`（秒差）。既定の整形は ISO-8601
  `yyyy-MM-ddTHH:mm:ss`。

**条件** — `case when … then … [else …] end`：

```
|> name (case when age >= 65 then "senior"
              when age >= 18 then "adult"
              else "minor" end) as band
```

整数同士の算術は整数のまま（`/` は常に浮動小数、SQL/pandas 同様）。文字列は
算術が必要なときベストエフォートで数値解釈。0 除算/剰余は NaN/0 を返します
（停止しない＝continue-first）。

---

## 7. シンク（フローの末尾）

| 構文 | 書き込むもの |
|---|---|
| `save PATH` | 拡張子で形式判定（`.tsv`/`.tab`→タブ区切り、`.json`→JSON 配列、`.jsonl`/`.ndjson`→NDJSON） |
| `save PATH as FMT` | 形式を強制（`csv` \| `tsv` \| `json` \| `jsonl` \| `ndjson`） |
| `writecsv PATH` / `writejson PATH` | 明示動詞（`writejson` = NDJSON） |
| `save stdout` / `save -` | 標準出力へ |
| `print` | 画面プレビュー用にキャプチャ |

```
… save out.csv
… save out.json              # 単一の JSON 配列: [{…},{…}]
… save out.jsonl             # NDJSON: 1 行 1 オブジェクト
… save - as json             # JSON 配列を stdout へ（パイプ向き）
… save out.tsv               # タブ区切り
```

「読める形式は書ける」：CSV/TSV、JSON 配列、JSON Lines はすべて対称です。
**`as json` は単一の角括弧配列**、**`as jsonl`/`.jsonl`** は 1 行 1 オブジェクト
（`writejson` が出すもの）。どちらも有界メモリでストリーム。空結果は `[]`（json）
または行なし（jsonl）です。

---

## 8. ライフサイクルフック（continue-first）

```
on error severity >= critical: transition emergency ;
```

`Severity::Fatal` のみがグラフを停止します。それ以外はエラーストリームに流れ、
フローは走り続けます（continue-first）。

---

## 9. ワンライナー集

Rivus は `awk`/`jq` のように——インライン・パイプ・ヒアドキュメントで使えます。

```sh
# フィルタ + 射影 を stdout へ
rivus run -c 'U: open users.csv |? age >= 20 |> name age save stdout as csv ;'

# CSV → JSONL 変換（1 行 1 オブジェクト）
rivus run -c 'U: open users.csv save stdout as jsonl ;' > users.jsonl

# CSV → JSON 配列（単一の [{…},{…}]、そのまま jq へ）
rivus run -c 'U: open users.csv |? age >= 20 save - as json ;' | jq '.[].name'

# 計算列で上位 5 件
rivus run -c 'S: open sales.csv |> product (qty * price) as total sort total desc take 5 save stdout as csv ;'

# グループ + 集約
rivus run -c 'G: open sales.csv |# region sum:amount avg:amount save stdout as csv ;'
```

**Unix フィルタ短縮形。** *変換のみ* のプログラム（パイプ `|…` か変換動詞で始まる）
は自動的に「stdin から CSV を読み stdout へ CSV を書く」形に包まれます——
スコープ不要で `awk`/`jq` のように差し込めます：

```sh
cat data.csv | rivus '|? age >= 20 |> name age'   # フィルタ + 射影
cat data.csv | rivus 'sort age desc'              # ソート
cat data.csv | rivus 'describe'                    # サマリ
cat data.csv | rivus '|# country sum:amount'       # グループ + 集約
```

**巨大ファイルを即プレビュー** — シンクなしの実行は *プレビュー* です。Rivus は
スキーマをサンプリングし、15 GB のファイルでも有界メモリで先頭行を表示します
（全行処理するには `save` を付ける）：

```sh
rivus run -c 'B: open big.csv ;'        # 瞬時に head、~10 MiB RAM
```

---

## 9b. 実践例（少し難しいもの）

DAG・結合・グループ化・クリーニングを組み合わせた実フロー。各例は完結した
プログラムです（`.riv` に保存するか `-c` で渡す）。

**注文を顧客で補強し、(国, ティア) ごとの売上。** 複合結合 → マルチキーグループ
（複数集約 + パーセンタイル）：

```
Customers: open customers.csv ;        # id, country, tier
Orders:    open orders.csv ;           # cust_id, amount, status

Revenue:
    Orders &left Customers on cust_id:id   # 全注文を保持、欠けた顧客は埋める
    |? status == "paid"
    |> country tier (amount:f64) as amount
    |# country tier sum:amount avg:amount p90:amount count_distinct:cust_id
    sort sum_amount desc
    save revenue.csv
;
```

**雑なエクスポートを整え、バケット化して集計。** 型宣言・補完・`case` バケット・
グループ——pandas に手を伸ばしたくなる類の処理：

```
Clean:
    open raw.csv (id age:str score:str region:str)
    cast age:int score:f64                 # 文字列列を再型付け
    fill region ffill                      # 空の region を直前値で補完
    fill score mean                        # 欠測スコアを平均で補完
    |> id age region score
       (case when age >= 65 then "senior"
             when age >= 18 then "adult"
             else "minor" end) as band
    |# region band avg:score median:score std:score
    save out.json                          # 単一 JSON 配列
;
```

**ログをセッション化し、ユーザー内でランク付け。** ソースを分岐し、各側で計算、
ダッシュボード向けに JSON 出力——ライブテレメトリをソケットへ：

```
Events:
    open events.csv.gz                     # gzip 入力（--features gzip が必要）
    |? status == "ok"
    |> user ts (bytes / 1048576.0) as mib
    sort user mib desc                      # user 昇順、その中で mib 降順
    |> user (round(mib)) as mib (concat(user, "@", ts)) as event_id
    save - as json
;
```
```sh
rivus run sessions.riv --telemetry-addr 127.0.0.1:9000   # メトリクスをライブ配信
```

---

## 10. パフォーマンス

- **ストリーミング・有界メモリ。** CSV ソース/シンクはストリームします。1.1 GB /
  4800 万行のファイルを `open |? age>=50 |> name age save out.csv` で処理して
  **~10 MiB** の RAM（ファイルを丸読みしない）、**DuckDB より ~1.45 倍速く
  ~40 倍少ないメモリ**（3.0 秒 vs 4.4 秒 / 407 MiB）、awk の ~3.8 倍、Python の
  ~10 倍——詳細は [`docs/BENCHMARKS.md`](BENCHMARKS.md)。
- **既定で並列。** `save` シンク付きの単一 CSV **または JSONL** が **8 MiB** 以上なら、
  自動で CPU コア横断のストリーム処理（改行境界のバイト範囲ワーカー → 順序付き出力）。
  JSONL も有界メモリでストリーム（全文読み込みを廃止）、**group-by も並列化**。
  171 MiB のフィルタで直列 ~1.6 秒 → 並列 **~0.4 秒**。`RIVUS_PARALLEL_MIN_BYTES`
  （バイト、`0` で常時）で調整、`RIVUS_NO_PARALLEL=1` で直列強制。圧縮入力
  （`.gz`/`.zst`）はシーク不可なので直列。
- **`--memory low|auto|fast|unbounded`。** メモリ/速度の knob。`low`＝直列強制
  （最小資源）、`auto`（既定）＝CPU数・入力サイズで自律調律、`fast`＝より積極的に
  並列（閾値を下げる）— **この3つは有界メモリのまま**。`unbounded` は**明示的に**
  有界を犠牲に速度を取るオプトイン: 分割不可ソース（圧縮/JSONL/binary）も入力を
  materialize して並列化（peak メモリ O(入力)）。4 つとも結果は **byte-identical**で、
  違うのはメモリ/速度だけ。**group-by** も並列化: byte-identical な集計
  （`min`/`max`/`count`/`count_distinct`/`first`/`last`/percentile と exact-`decimal`
  の `sum`/`avg`）は `auto`/`fast` で有界並列、`unbounded` で分割不可ソースにも拡張。
- **ライブ進捗。** 対話的な `rivus run` は長いジョブ中、stderr に
  `… N rows  T s  R rows/s` を表示。
- **機械可読テレメトリ。** `rivus run … --json` でノードごとの JSONL
  （rows in/out, busy_ms, rows/s, selectivity, mode）+ エラー + サマリを stderr へ
  （stdout はクリーン）。`--telemetry-addr HOST:PORT` で TCP ソケットへ配信。
- **オプティマイザは既定で動作**（ソース重複排除・filter+project 融合・射影
  プッシュダウン・リーダーへのフィルタプッシュダウン）。`rivus explain` で何を
  したかを表示し、最適化後 IR からソースを再生成。`--no-opt` で無効化。正しさは
  `optimizer_equiv` テストでバイト単位に保証。

---

## 11. CLI リファレンス

```
rivus run     <program> [--chunk-size N] [--no-opt] [--json]  フローを実行
rivus explain <program> [--no-opt]                    DAG IR + オプティマイザレポート
rivus check   <program>                               構文チェックのみ
rivus gen     <shape>   [--rows N --seed S --ratio R] シード付きデータを stdout へ

PROGRAM:
  <file.riv>                 ファイルからプログラムを読む
  -c, --command <STRING>     インライン文字列で渡す
  - | stdin                  標準入力から読む（ヒアドキュメント）

GEN SHAPES（決定的・シード付き — ベンチ/デモ用、awk 不要）:
  clean         整形済み id,name,age,score,country,active CSV
  error-heavy   ~ratio の不正行（既定 0.1）— continue-first ストレス
  mixed         ~ratio の型混在セル（既定 0.1）
  jsonl         1 行 1 フラット JSON オブジェクト
```

```sh
# 自己完結ベンチ：生成してフィルタ — 外部ツール不要
rivus gen clean --rows 1000000 | rivus '|? age >= 50 |> name age'
```

---

## 12. 文法クイックリファレンス

完全な実装済み文法は英語版 [`docs/GUIDE.md`](GUIDE.md) の §12 を参照してください
（こちらは同一の文法を共有します）。§9 のワンライナーから始めて育てていくのが
おすすめです。
