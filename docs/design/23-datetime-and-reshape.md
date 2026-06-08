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
| 部分取り出し | `year(ts) month(ts) day(ts) hour(ts) minute(ts) second(ts)` | i64 を返す（既存 computed-column 20 に関数追加） |
| 整形 | `format(ts, "yyyy-MM-dd")` | 出力時に文字列化 |

### 精度の契約（#53・#44 系譜 — exact レーンは比較で精度を黙って落とさない）
decimal（#44）と同じく、**datetime は比較・min/max を f64 経由にしない**。
`2^53 ns ≒ 1970+104日` なので現実の ns tick（~1.7e18）は f64 で隣接値が潰れる。
よって：
- **比較は exact i64**：interpreter の `dt_cmp` が
  - datetime×datetime → i128 cross-unit、
  - datetime×テキストリテラル → 同 lane に `parse_auto` して比較、
  - datetime×**整数**リテラル → 整数を「列 unit の生 tick」として i64 比較。
  kernel は `num_col` で datetime を**除外**（f64 経路を撤廃）→ 全 datetime 比較が
  interpreter に集約され、kernel==interpreter が自明に一致。
- **min/max は exact i64 tick で集計し DateTime 型を保持**（`AggAcc.dt_min/dt_max`、
  f64 列に落とさない）。整数ゆえ結合的 → 並列 group-by でも byte-identical。
- **datetime の算術（`ts2-ts1`）は #57 Duration 型で exact i64 化済み**（下記）。
  datetime 自体の sum/avg は依然 f64 だが、時刻の sum/avg は意味のある instant では
  ないため許容（min/max は exact、上記）。
- 受け入れ: ns・>2^53 を跨ぐ敵対的テスト（`eval::dt_cmp_tests`・
  `operators::agg_merge_tests::datetime_minmax_is_exact_i64_and_type_preserving`）。

### Duration / TimeSpan 型（#57 — 時刻差は符号付き i64・第一級型）✅ landed
`DateTime − DateTime` の結果を **`Duration{ticks:i64, unit}`**（`Value::Duration`/
`Column::Duration`/`DataType::Duration`）として instant と型で区別。整数ゆえ厳密・
結合的で、instant と対照的に **sum/avg が有意味かつ並列 byte-identical**。
- **型代数**（`eval::temporal_op`、row-wise/columnar 共有で byte-identical）:
  `DT−DT→Dur`、`DT±Dur→DT`、`Dur±Dur→Dur`、`Dur×int→Dur`、`Dur÷Dur→f64 比`。
  cross-unit は細かい unit へ lift、overflow は飽和（continue-first）。
- **比較**: `dt_cmp` が Dur×Dur（i128）・Dur×テキスト（`parse_at`）・Dur×整数（生 tick）
  を exact i64 で処理（f64 経路なし）。
- **集計**: `AggAcc.dur_sum`(i128)/`dur_min`/`dur_max` で sum/avg/min/max を exact 化、
  `Duration` 型を保持。`group_parallel_safe` は Dur 列の sum/avg を**並列許可**
  （decimal と同じく結合的）。avg は i128 を round-half-even。
- **リーダー**: `(d:duration)` で人間可読 `[-][Nd ]HH:MM:SS[.frac]` を厳密パース
  （format 不要、unit=Sec）。`to_source` 可逆。整形は `format(dur[, "iso"])`。
- 受け入れテスト: `eval::dt_cmp_tests`（ns diff exact・型代数・Dur 比較）、
  `agg_merge_tests::duration_aggregates_are_exact_and_parallel_safe`（single==partitioned、
  ns>2^53）、`stress::duration_groupby_parallel_matches_serial`（serial==parallel×chunk）、
  `stress::duration_read_roundtrip_and_diff`。MVP スコープ外: tz・暦演算・sub-tick。

`trunc`/`year`/`hour` 等は **時系列 group-by の鍵**（「日ごと集計」「時間帯別」）。
これらは整数演算なので並列集計が byte-identical。

### 段階実装
1. ✅ **landed** コア型 `DataType::DateTime{unit}` / `Column::DateTime` /
   `Value::DateTime`、`value_at`/`gather`/`Display`（既定 ISO8601 整形）、
   厳密な i128 跨ぎ比較。等価ユニットテスト。（MVP は `unit=Sec`・naive UTC、
   `tz` は未導入。）
2. ✅ **landed** パーサ：`:datetime[("fmt")]` 注釈。std-only な strptime/strftime
   最小実装（`yyyy yy MM dd HH/hh mm ss`、2桁年ピボット 00–68→20xx）。書式は
   `OpenCsv.dt_formats` に載せて `to_source` 往復（原則5）。直列 CSV リーダー
   （圧縮含む）で `ColBuilder::DateTime`。不正値→epoch 0（continue-first）。
   （`--dates` フラグは未実装＝列注釈で代替。）
3. ✅ **landed** 演算子：比較（リテラルを同 lane へ auto-parse）、`year`/`month`/
   `day`/`hour`/`minute`/`second`、`trunc`、`format`、`diff`（`ts2-ts1`）。
   並列バイト範囲リーダーも `DtSpec` を `Arc` 共有して対応 → serial/parallel ×
   chunk-size sweep の等価テスト（`tests/stress.rs`）。
4. ⏳ 時系列 group-by の例は GUIDE と stress テストに収録済み。専用ベンチ（日次/
   時間帯別の大規模集計）は **未追加**（機能であり最適化ではないため後回し）。

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

## 23.6 cast/式での datetime パース書式（BUG-D）— 批准用 RFC

> ステータス: **設計（批准待ち）**。実装はこの節の批准後に着手する（IR・構文を
> 触るため design-doc 先行・批准制）。byte-identity / continue-first / IR 可逆 /
> 依存ゼロ / 英日ガイド同時更新の不変条件下で行う。

### 問題（再現）
`:datetime("fmt")` の **パース書式は reader schema 経路でしか効かない**。
- ✅ `open f.csv (ts:datetime("yyMMddHHmmss"))` — 効く。書式は `Codec::Csv.dt_formats`
  側テーブル（`crates/rivus-ir/src/graph.rs:362`）→ `build_dt_specs`
  （`crates/rivus-runtime/src/csv.rs:1173`）→ `DtSpec`（`csv.rs:1165`）で `DtSpec::parse_opt`
  に届く。
- ❌ `cast ts:datetime("yyMMddHHmmss")` — **書式が黙って捨てられる**。パーサは
  `finish_type` で `("fmt")` を `self.last_dt_fmt` に拾うが、cast 構築
  （`crates/rivus-parser/src/lib.rs:1247-1260`）は `last_dt_fmt` を**回収しない**。
  eval は `cast_value`（`crates/rivus-runtime/src/eval.rs:521`）で
  `DateTime::new(to_i64(v), unit)` ＝ **値を生の epoch ticks 扱い**。`"260601120000"`
  → ticks 260601120000 → 西暦 10228。
- ❌ 計算列 `(ts:datetime("fmt")) as a` も同根で書式喪失。

### 根因
**`DataType::DateTime { unit }`（`value.rs:1124`）も `Expr::Cast { expr, ty }`
（`expr.rs:256`）も書式を保持しない。** reader だけが列名→書式の側テーブルを別に
持っているため、cast/eval 経路には書式が届かない非対称。

### 設計原則 — 書式は「型の同一性」ではなく「パース操作」の関心事
パース後の datetime 列は `ticks + unit` のみ。**同じレーン**であり、由来の入力書式は
保存・比較・出力に一切無関係。`DateTime{Sec,"yyMMdd"}` と `DateTime{Sec,"yyyy-MM-dd"}`
は **同一の型**であるべき。したがって書式は **str→datetime の変換操作** に属し、
**型（`DataType`）には載せない**。

### 候補比較
| 案 | 概要 | 評価 |
|---|---|---|
| **A: `DataType` に載せる** `DateTime{unit, format}` | 型に書式を持たせ cast も schema も同経路 | ❌ 型等価の意味論を壊す（同レーンが別型に）。`DataType` は `Copy`/`Eq`/`Hash` 前提で広く使われ（sort `make_cmp`・Field・最適化）、`String`/`Arc` 追加で `Copy` 喪失＝全域に波及。**却下** |
| **B: cast 操作に載せる** `Expr::Cast{expr, ty, format: Option<Arc<str>>}` | 書式は cast ノードに同伴、`DataType` は不変 | ✅ 関心の所在が正しい・型は綺麗なまま・reader の側テーブルと同じ「型と書式の分離」を**インライン**で実現・to_source 可逆が自然。**推奨** |
| C: 側テーブルを cast にも | reader と同じ列名→書式表を式側にも | ❌ 非対称の元凶（側テーブル）を増殖。却下 |
| D: 専用関数 `parse_datetime(x,"fmt")` | cast と別の式関数 | △ cast の `:datetime("fmt")` と二系統に分裂。reader の `(ts:datetime("fmt"))` 構文と不一致。却下（一貫性優先） |

### 推奨表現（案B）
```rust
// crates/rivus-ir/src/expr.rs
Expr::Cast {
    expr: Box<Expr>,
    ty: DataType,
    format: Option<Arc<str>>,   // パース書式（現状 datetime のみ解釈）。None=現行どおり
}
```
- `format` は **汎用の optional パース書式**（当面 datetime ターゲットのみが解釈、
  将来 decimal/独自 date 書式へ拡張可）。`Arc<str>` は最適化での `Expr` clone を安価に。
- `DataType` は無改変（`Copy`/`Eq`/`Hash` 維持、全域ゼロ波及）。

### 構文（reader と一貫）
`:datetime("fmt")` を **reader schema・cast 動詞・計算列**で同一に通す:
```
open f.csv (ts:datetime("yyMMddHHmmss"))      # 既存（dt_formats 経由・不変）
… cast ts:datetime("yyMMddHHmmss")            # 新規: Cast.format に載る
… |> (ts:datetime("yyMMddHHmmss")) as t       # 新規: 計算列も同経路
```
パーサ変更は cast 構築点で `self.last_dt_fmt.take()` を `format` へ回収するだけ
（`finish_type` の書式取得は既存・再利用）。

### eval セマンティクス
`cast_value`/`cast_column` に `format: Option<&str>` を渡す。ターゲットが `DateTime{unit}` のとき:
- **`format = Some(fmt)`（BUG-D 本丸）**: 値を文字列表現にして
  `DateTime::parse_with_format(s, fmt, unit)`。失敗は **null（continue-first）**。
  → `"260601120000"`（int でも str でも）を `yyMMddHHmmss` で正しく解釈。
- **`format = None`**: **現行どおり**（数値→ticks 再解釈）で**挙動不変**＝既存
  byte-identity 完全維持。

> **批准サブ論点①（任意拡張）**: `format=None` かつ **入力が Str** のとき、reader と
> 同様に `parse_auto`（`AUTO_FORMATS`）で解釈すべきか？ 現状は `to_i64`→0 で
> 事実上壊れている（`cast str:datetime` 単体）。一貫性では「Str かつ None →
> `parse_auto`」が望ましいが**挙動変更**。既定は **現行据え置き（None は不変）**とし、
> 統括が一貫性拡張を望む場合のみ別途取り込む。

### continue-first / never-silent
パース失敗セルは **null**（原則2）。なお **既存の cast（`cast x:int` 等）は失敗を
黙って 0/null にしており**、cast 経路に error-stream チャネルが無いのは**既存仕様**。
datetime cast の失敗 surface（reader の `parse_failures` 相当）は **本 BUG-D の範囲外・
tracked follow-up**（cast 全般の never-silent 化として別途）。これにより BUG-D を最小に保つ。

### 不変条件への影響
- **byte-identity**: cast は決定的・行単位・浮動小数縮約なし → serial==parallel==
  chunk-size を自明に満たす。`format` は IR に載り各 worker に clone される（差異なし）。
- **IR 可逆（to_source）**: `Expr::Cast` の to_source（`expr.rs:409`）を、
  `format=Some` のとき `{expr}:{ty}({fmt:?})`（reader の `to_src_line`
  `graph.rs:780` と同じ `{:?}` クォート）で描画。`None` は現行 `{expr}:{ty}`。
  再パースで `format` が復元され round-trip 一致。
- **optimizer_equiv**: 最適化は cast を不透明に扱う。`format` はノードに同伴して
  移動するので等価性不変。`Expr::Cast { .. }` の全 match 箇所はフィールド追加で
  コンパイラ誘導により網羅更新（pushdown/dedup 等）。
- **依存ゼロ**: 新規 crate なし。`parse_with_format` は既存 std 実装。

### reader 整合（範囲外メモ）
`dt_formats` 側テーブルはそのまま（動作中）。将来、宣言スキーマ表現に書式を畳んで
側テーブルを退役させる統一は可能だが **BUG-D の範囲外**（別スライス）。

### 影響範囲（file:line）
1. `crates/rivus-ir/src/expr.rs:256` — `Expr::Cast` に `format` 追加。
2. `crates/rivus-ir/src/expr.rs:409` — `to_source` で書式描画。
3. `crates/rivus-parser/src/lib.rs:1247-1260` — cast 構築で `last_dt_fmt` 回収。
4. `crates/rivus-runtime/src/eval.rs:511-538/585-633` — `cast_value`/`cast_column` が
   `format` を受けて datetime を `parse_with_format`。
5. `Expr::Cast { .. }` の全 match 箇所（optimizer 等）— フィールド追加対応（コンパイラ誘導）。
6. テスト: `cast ts:datetime("fmt")` と計算列 `(ts:datetime("fmt")) as a` の
   acceptance（round-trip＋eval 結果＋chunk-size 独立）、to_source 可逆、optimizer_equiv。
7. `docs/TEST-AUDIT.md` BUG-D → RESOLVED、`docs/GUIDE.md`/`GUIDE.ja.md`（cast での書式指定）。

### 段階実装（批准後）
1. IR: `Expr::Cast.format` 追加 ＋ to_source ＋ 全 match 更新（コンパイル緑）。
2. パーサ: cast での書式回収 ＋ round-trip テスト。
3. eval: `format` を datetime パースに接続 ＋ acceptance テスト（int/str 両入力・
   chunk-size 独立・byte-identity）。
4. ドキュメント（TEST-AUDIT/GUIDE 英日）。

### 批准ポイント（要・統括判断）
1. **案B（`Expr::Cast.format`、書式は型でなく操作に載せる）**で進めてよいか。
2. **サブ論点①**: `format=None` かつ Str 入力で `parse_auto` を効かせる一貫性拡張を
   **入れる／据え置く**のいずれか（既定＝据え置き）。
3. never-silent な cast 失敗 surface を **範囲外（tracked）**とする方針でよいか。
