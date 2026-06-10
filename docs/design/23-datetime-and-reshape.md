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
  （`DateTime::AUTO_FORMATS` = ISO8601 `yyyy-MM-ddTHH:mm:ss`、`yyMMddhhmmss`、
  `yyyyMMdd` 等）を順に試す（first match wins）。
  どれにも一致しない値は **warning + Null 相当（既定 epoch 0）で継続**（原則2）。
- 2 桁年 `yy` のピボット規約（例 00–68→20xx, 69–99→19xx）を明示・固定（決定的）。
- **不変条件：`AUTO_FORMATS` は互いに素**（区切り文字＋全消費の桁数で、任意入力に
  一致する書式は高々1つ）。`parse_with_format` が末尾までの完全消費を要求するため、
  桁数の異なる純数字書式（8/12/14桁）も区切り付き書式も互いに重ならない。これは
  byte-identity の前提：高々1書式しか一致しないので**試行順は結果を変えない**。
  書式を追加するときはこの不変条件を維持すること（`auto_formats_disjoint` テストが固定）。
- **最適化：move-to-front（#135）**。実世界の datetime は非ISOが主流（`yyMMddHHmmss`・
  `yyyyMMdd`・ログ系）で、ISO 先頭の試行順だと非ISO列は毎行 ISO 形式の失敗試行を払う。
  `parse_auto_sticky` は「直前にヒットした書式」を次回最初に試し、外れたら従来どおり
  全書式 fallback する。列内が均一書式なら2行目以降1試行に縮む。上の互いに素不変条件
  により **byte-identical**（試行順のみ変化）。状態（hint）は**列ごと・worker ごと**に
  持ち（スレッド間で共有しない）、serial == parallel を維持。均一列で win、強い交互
  混在のみ move-to-front の宿命で僅かに不利（実列は均一なので実害なし、`BENCHMARKS`）。

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

## 23.6 式 cast の source-aware 化と書式の所有者（BUG-D）— 確定方針

> ステータス: **設計確定（統括批准済み・2026-06-08）**。**型システムの新設は一切しない。**
> 不変条件: byte-identity（serial==parallel==chunk-size）／ never-silent・continue-first
> ／ IR 可逆（to_source round-trip＋optimizer_equiv）／ 依存ゼロ（既存 parse ヘルパ再利用）
> ／ 英日ガイド同時更新。

### 問題（再現）
`:datetime("fmt")` の書式は **reader schema 経路でしか効かない**。式 cast は壊れている:
- ✅ `open f.csv (ts:datetime("yyMMddHHmmss"))` — reader schema が書式を持ち、読み時に
  exact text path でパース。`Codec::Csv.dt_formats`（`graph.rs:362`）→ `build_dt_specs`
  （`csv.rs:1173`）→ `DtSpec::parse_opt`（`csv.rs:1165`）。**最速・byte-identity。ここは不変。**
- ❌ `cast ts:datetime` / `(ts:datetime) as t` — `cast_value`（`eval.rs:521`）が
  `DateTime::new(to_i64(v), unit)` ＝ **Str を to_i64→0**、数値を生 ticks 再解釈。
  `"2026-06-01"` は 0（epoch）、`"260601120000"` は西暦 10228。**str→datetime が黙って壊れる。**
- ❌ `cast ts:datetime("fmt")`（式位置の明示書式）— `finish_type` が `("fmt")` を
  `last_dt_fmt` に拾うが cast 構築（`lib.rs:1247-1260`）が**回収せず黙って捨てる**。

### 確定方針 — 書式の唯一の所有者は「スキーマ宣言」（方針「い」）
**書式（`datetime("yyMMdd")` 等）を持てるのは reader スキーマ宣言だけ。** 式 cast は
**書式を持たない別用途**として許容する。両者は用途が完全に異なる:

| | スキーマ宣言 `(ts:datetime("fmt"))` | 式 cast `… cast ts:datetime` |
|---|---|---|
| 用途 | データをその型に**読み込む/正規化** | 計算の途中で**その場で型を変える** |
| 書式 | あり（明示／auto） | **なし**（auto のみ） |
| 位置 | source の宣言スキーマ | 主に `\|>`、`\|?` でも可 |
| 速度 | 最速（exact text path） | 劣後（行単位 eval）— **用途が違うので許容**（統括明言） |

プロジェクションは元来データ変換の場なので、そこで型を変えるのは自然。速度は劣後するが
用途が違うため許容する。

### byte-identity 契約 — 2 つの cast は「意味同一・経路のみ差」
同じ型変換は **どこで行っても結果バイトが同一**でなければならない（経路＝速度だけが違う）。
場所で結果が変わるのは byte-identity 違反＝バグ。**今の BUG-D（reader では正しくパースされる
str→datetime が、式 cast では 0/誤値になる）はまさにこれ**で、直す対象。式 cast の auto
パースは方針「い」の既定として正しい（reader の auto 推論と同じ意味）。

### 却下（実装しない・蒸し返さない）
- ✗ **案B**: `Expr::Cast` に `format` フィールド追加 → 不正状態（式に書式）を表現可能にし
  シンプリシティを壊す。**却下**（旧 RFC を撤回）。
- ✗ `ParseTemporal` 等の新 IR ノード → 構文と意味をねじる。却下。
- ✗ `LaneCodec` trait 全面刷新 → 過剰。却下。
- ✗ `type` キーワード / struct lane / UserDefinitionType の新設 → 不要。named 再利用は
  既存 §25.4 flow-reuse ＋ 将来 `as Sale` の小追加（後続スライスB・任意）で賄う。

### スライスA = BUG-D 本丸（最小・今回実装）
1. **式 cast を source-aware に**（`eval.rs` `cast_value:511` / `cast_column`）:
   ターゲットが `DateTime`/`Date`/`Time` で **source が Str → 正しくパース**（既存
   `DateTime::parse_auto` / `Date::parse` / `TimeOfDay::parse_at` を再利用）。**数値 source は
   現状の ticks 再解釈のまま**。これで `cast str:datetime` の silent-wrong が直る。
2. **cast 失敗の never-silent surface**（統括裁定: silent はしない、原則。実装難度や
   破壊的変更を理由に削らない）: パース/変換失敗 → **null（continue-first）＋ error stream に
   surface**。配管を cast/eval 経路に通し、**datetime 系を第一実装**、他 lane（int/decimal 等の
   失敗）も同じ配管で広げる方向。**serial==parallel で同一 ErrorEvent**（BUG-F と同じ作法）。
3. **式位置の明示書式を never-silent エラー化**（parser `lib.rs:1247-1260` parse_cast）:
   式/cast 位置で `:datetime("fmt")`（`last_dt_fmt` が Some）を検出したら黙って捨てず
   **parse エラー**にし「書式は（reader）スキーマで宣言せよ」と案内。**reader スキーマ位置の
   `(ts:datetime("fmt"))` は従来どおり有効（不変）**。
4. **`Expr::Cast` の構造は不変**（`format` を足さない）→ `to_source` も現状 `{expr}:{ty}` の
   まま、round-trip 不変。

### never-silent 配管（実装スケッチ）— 唯一の非自明点
現状 `cast_value`/`cast_column` は error channel を持たない純粋関数。失敗を operator まで
運んで raise する機構を最小で足す:
- **cast 関数に失敗カウンタを out-param で渡す**: `cast_column(col, ty, fails: &mut u64)` /
  `cast_value(v, ty, fails: &mut u64)`。**非 null 入力がパース/変換失敗 → null（validity=0、
  continue-first）＋ `*fails += 1`**。null 入力は null のまま（カウントしない）。
- **列指向経路の配管**: `eval_column` は公開シグネチャを温存し（`fails` を捨てる薄い
  wrapper）、内部実装 `eval_column_acc(expr, chunk, &mut fails)` が再帰（Cast/Arith/Func）と
  `cast_column` に `fails` を通す。`ProjectExpr` は `_acc` 版を使い、出力列（alias）ごとに
  失敗総数を**チャンクをまたいで蓄積**。`cast` 動詞 operator は `cast_column` を直接呼ぶので
  そのまま蓄積。
- **finish で一度だけ surface**（chunk-size 独立）: reader の `parse_failures` と同作法で
  `「N value(s) in '<col>' could not be cast to <type>; set to null」`（`Severity::Recoverable`）。
- **serial==parallel の契約**: per-row のカウントなので、**並列では各 worker が自分の
  partition の partial を surface し、その総和が serial の総数に一致**する（既存の never-silent
  契約＝`parse_failures` / `validate_reject_parallel_summary_counts_sum_to_total` と同じ。
  単一の同一イベントではなく「総和一致」）。**受入テストはこの総和一致を最初に固定**して
  under-build を防ぐ。
- **スカラ経路（`|?` 述語・func 引数内の cast）も拡張済み（スライスA-2）**：`eval`→
  `eval_acc`、`eval_predicate`→`eval_predicate_acc`（＋ `call_func`/`arith_value`/
  `compare_fast` に `fails` を貫通）。`Filter`/`Validate`/`FilterProject` が予測詞内の
  cast 失敗を finish で一度 surface（`「N value(s) could not be cast in |? <pred>; set to
  null」`）。値は従来どおり null（continue-first）で並列は per-worker partial が serial 総和に
  一致。`ProjectExpr` は func 引数/case 内の cast も同 accumulator で拾う。

### 挙動変更の明示（doc 必須）
`str→datetime`/`date`/`time` の式 cast 結果が「0/誤値 → 正しいパース値」に変わるのは
**壊れの修正**だが、**既存挙動の変更**として GUIDE/CHANGELOG に明記する。byte-identity は
serial==parallel==chunk-size で維持（行単位・決定的）。

### 受入テスト
- 式 cast の str→datetime/date/time パース（**int/str 両入力・chunk-size 独立・byte-identity**）。
- 失敗の **null＋surface**（**serial==parallel 同一 ErrorEvent**）。
- **式位置の明示書式 `:datetime("fmt")` が never-silent エラー**。
- **reader スキーマ位置 `(ts:datetime("fmt"))` は不変**（既存テスト緑）。

### 後続（tracked・スライスA 範囲外）
- **スライスB（任意）**: named schema 再利用 = 既存 §25.4 flow-reuse ＋ `as Sale` の小追加
  （`type` 新設なし）。
- **最適化メモ**: source 直後の cast を codec schema に畳む **pushdown**（式経路を exact text
  path に縮約＝速度差を消す）。byte-identity 不変・before/after 必須。
