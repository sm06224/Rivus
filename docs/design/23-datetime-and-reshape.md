# 23. Datetime lane・list 集計・pivot — 時系列とリシェイプ

> 統括方針（2026-06-01）: **(1) 日時型**（`yyMMddhhmmss` タイムスタンプを扱う）、
> **(2) 集計の配列化**（group-by で値を集める list 集計）、**(3) 最終的に pivot**。
> 三者は連続した設計 — datetime はピボットの行/列キーになり、list 集計は pivot の
> 構成要素。いずれも `06-type-system.md`（型 = 実行レーン）と原則（continue-first・
> chunk-native・byte-identical・依存ゼロ）に乗せる。

---

## 23.1 Datetime lane（日時型）

### 動機
`yyMMddhhmmss`（例 `260601143000` = 2026-06-01 14:30:00）のような固定幅
タイムスタンプを、文字列でも近似 f64 でもなく **厳密な時刻**として扱いたい。
比較・差分・切り捨て（日/時/分単位の group-by 키）・整形が必要。

### 表現 — epoch 整数（scaled、decimal lane と同系）
日時は **基準時刻からの整数オフセット**で持つ。丸め誤差ゼロ・比較は整数比較・
**加算/差分が結合的**なので並列集計でも byte-identical。

```
Column::DateTime(DtColumn { ticks: Vec<i64>, unit: TimeUnit, tz: Tz })
TimeUnit = Sec | Milli | Micro | Nano   // 既定 Sec（yyMMddhhmmss は秒精度）
Tz       = Naive | Utc | FixedOffset(i32 sec)  // MVP は Naive/Utc
```

- `ticks` = 基準（1970-01-01T00:00:00）からの整数。`i64` 秒で約 ±2920 億年、
  ナノでも約 ±292 年 → 実用上十分。
- **decimal lane（21）と同じ思想**: 整数表現ゆえ厳密・結合的。日時の min/max/
  range/差分の合計が並列でも一致する。

### パース（continue-first）
- **書式指定**: `open log.csv (ts:datetime("yyMMddhhmmss"))`。`strptime` 風の
  最小トークン集合（`yyyy yy MM dd HH mm ss SSS`）を std だけで実装（依存ゼロ）。
- **自動推論**: フラグ `--dates` または列注釈 `:datetime` で、よくある書式
  （ISO8601 `yyyy-MM-ddTHH:mm:ss`、`yyMMddhhmmss`、`yyyyMMdd`）を順に試す。
  どれにも一致しない値は **warning + Null 相当（既定 epoch 0）で継続**（原則2）。
- 2 桁年 `yy` のピボット規約（例 00–68→20xx, 69–99→19xx）を明示・固定（決定的）。

### 演算
| 操作 | 構文（案） | 意味 |
|---|---|---|
| 比較 | `|? ts >= "260601000000"` | リテラルは同 lane にパースして整数比較 |
| 差分 | `(ts2 - ts1) as secs` | i64 差（秒）。結合的 |
| 切り捨て | `trunc(ts, "day")` / `trunc(ts,"hour")` | group-by キー用。整数除算で決定的 |
| 部分取り出し | `year(ts) month(ts) day(ts) hour(ts)` | i64 を返す（既存 computed-column 20 に関数追加） |
| 整形 | `format(ts, "yyyy-MM-dd")` | 出力時に文字列化 |

`trunc`/`year`/`hour` 等は **時系列 group-by の鍵**（「日ごと集計」「時間帯別」）。
これらは整数演算なので並列集計が byte-identical。

### 段階実装
1. コア型 `DataType::DateTime{unit,tz}` / `Column::DateTime` / `Value::DateTime`、
   `value_at`/`gather`/`Display`（既定 ISO8601 整形）。等価ユニットテスト。
2. パーサ：`:datetime[("fmt")]` 注釈 ＋ `--dates`。std-only な strptime/strftime
   最小実装。書式往復（`to_source` 可逆・原則5）。不正値 continue-first テスト。
3. 演算子：比較（リテラル同 lane 化）、`trunc`/`year`/`month`/`day`/`hour`/`diff`、
   `format`。`optimizer_equiv` 等価ゲート、chunk-size 非依存。
4. 時系列 group-by の例とベンチ（日次/時間帯別集計）。

---

## 23.2 List 集計（集計の配列化）

### 動機
group-by で「各グループの値を**集めて配列にする**」。SQL の `array_agg` /
pandas の `GroupBy.agg(list)` / `group_concat`。pivot の構成要素でもある。

### 型と集計
配列を持つ **list lane** を導入（要素 lane を内包）:

```
Column::List(ListColumn { offsets: Vec<u32>, values: Box<Column> })  // Arrow ListArray 同型
DataType::List(Box<DataType>)
AggFunc::List      // グループの値をソース順に集める → List<elem>
AggFunc::Set       // 重複除去して集める（順序: 初出順 or ソート、決定的に固定）
AggFunc::Join(sep) // 文字列連結（group_concat、sep 指定）
```

- **`offsets + values`** は `StrColumn` と同じ「平坦バッファ＋オフセット」方式で、
  per-row 割り当てなし。要素が数値なら `values: Column::I64/F64/...`。
- **並列マージ（#41）**: `List` は **worker 順に values を連結**するだけ → source 順
  保存で byte-identical。`Set` は初出順を保つなら worker 順マージで決定的。
  `Join` も同様（区切り文字で連結）。**すべて結合的＝並列安全**。

### 構文（案）
```
open events.csv
  |# user list:item            # user ごとに item を配列化 → list_item 列
  |# user join:item            # user ごとに item を連結（既定区切り ","）
;
```
`AggFunc::parse` に `list`/`set`/`join`（必要なら `join(";")`）を追加。出力列の
dtype は `List<elem>`（または `Join` は `Str`）。

### 段階実装
1. コア型 `Column::List`/`DataType::List`/`Value::List`、`value_at`（`[a, b, c]`
   整形）/`gather`/`append`。
2. `AggFunc::List`/`Set`/`Join` と `AggAcc` の対応アキュムレータ（values バッファ）。
   serial group-by で動作＋並列マージ等価テスト（worker 順連結 = single-pass）。
3. パーサ `list:col`/`set:col`/`join:col`。`to_source` 可逆。
4. 出力（CSV は `[a,b,c]` セル、JSON は本物の配列 → `18-io-formats` の JSON 経路と接続）。

---

## 23.3 Pivot（最終目標）

### 動機
「行 → 列」への展開。例: `date, country, sales` を **country を列に**広げて
`date × {JP, US, DE, ...}` のクロス表にする。datetime（行キー）＋集計（セル値）＋
列キーの直積 = pivot。`19-interactive` の対話グリッドとも相性が良い。

### 意味論
```
pivot rows:<keys> cols:<key> values:<agg:col>
```
- `rows` … 残す行キー（例 `trunc(ts,"day")`）。
- `cols` … 列に展開するキー列（例 `country`）。その**distinct 値が出力列名**になる。
- `values` … 各セルの集計（例 `sum:sales`、`avg:price`、`list:item`）。

`pivot` は **group-by(rows + cols) → 列方向に再配置** と等価。実装は 2 段:
1. 内部で `group-by [rows..., cols] agg` を実行（既存 GroupBy を再利用）。
2. **列ピボット段**: `cols` の distinct 値を集め、`rows` ごとに 1 行へまとめ、
   各 distinct 値を 1 列に置く（欠損セルは Null/0、決定的）。

### chunk-native の難所と方針（hidden full materialization 禁止・アンチパターン）
- pivot は本質的に **pipeline-breaker**（全行を見て初めて列集合が決まる）。
  `sort`/`group` と同じ「materializing boundary」として扱い、`finish()` で emit。
- **列集合（cols の distinct）は group-by 段で確定**してから列を組む → 出力スキーマが
  実行時に決まる（動的スキーマ）。列数が爆発する高基数 cols は **上限＋
  `other` 集約**（または warning）で端末/メモリを守る（`14-observability` の巨大 DAG
  対策と同系）。
- **並列**: 内部 group-by が #41 の並列対象なら（decimal/整数/順序非依存 agg）pivot も
  その上で並列化可能。f64 sum 等は serial or `--exact`。
- **byte-identical**: 列順は cols distinct の**ソート順に固定**、行順は rows キーの
  ソート順に固定 → 並列でも決定的。

### 逆操作 unpivot（melt）も対で計画
`unpivot cols:<c1 c2 ...> into:(name,value)` で wide → long。pivot/unpivot は
`to_source` 可逆性（原則5）の観点でも対称に設計する。

### 段階実装
1. IR に `Op::Pivot { rows, col_key, value_agg }`（と将来 `Op::Unpivot`）。`name()`、
   `to_source` 可逆、`explain` 表示。
2. ランタイム `Pivot` 演算子（内部で GroupBy を駆動 → 列再配置、`finish` emit、
   動的スキーマ、列上限ガード）。等価テスト（pivot then unpivot 往復、chunk-size
   非依存、高基数ガード）。
3. パーサ `pivot rows:... cols:... values:...`。CLI 例＋ベンチ（日次×国の sales）。
4. 並列 pivot（内部 group-by が並列安全な集計のとき）＋ `--exact` 連携。

---

## 23.4 三者の連結（時系列ピボットの最終像）

```
open sales.csv (ts:datetime("yyMMddhhmmss") amount:decimal)
  |> (trunc(ts,"day")) as day country amount
  pivot rows:day cols:country values:sum:amount
;
# → day, JP, US, DE, ...（各セルは厳密な decimal 合計、並列でも byte-identical）
```

- **datetime**（行キー）＋ **decimal**（厳密セル値・並列安全）＋ **pivot**（列展開）が
  噛み合う。list 集計を values にすれば「日×国ごとの明細配列」も作れる。
- すべて opt-in lane / 明示演算子で、**既定の f64・行指向・依存ゼロ経路はゼロ回帰**。

## 23.5 実装順序（小さな測定付き PR）

1. datetime コア型＋パース（21 decimal の後、同じ scaled-integer 流儀で着手しやすい）。
2. list 集計（group-by 拡張・並列マージ等価）。
3. pivot（内部 group-by 再利用・動的スキーマ・高基数ガード）。
4. unpivot・時系列糖衣（`trunc`/`year`…）・JSON 配列出力の接続。

各段は operator/eval 境界の裏（原則「Operator boundary stays thin」）、`cargo deny`
緑・依存ゼロ維持、`docs/BENCHMARKS.md` に before/after。
