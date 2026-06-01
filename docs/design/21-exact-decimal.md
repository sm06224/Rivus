# 21. Exact decimal lane — COBOL 的固定小数点による「速度を犠牲にした正確性」

> 統括方針（2026-06-01）: **ユーザーオプトインで、速度を犠牲にしてでも正確性
> （byte-identical・並列安全）を担保できる数値レーンを用意する。** これは
> `06-type-system.md` §6.3 が予定していた **Decimal lane（scaled integer）** の
> 実装可能詳細であり、Epic #38（攻めの構造ベット）と #41（並列集計）の
> 「結果不変の堀」を *型レベルで* 満たす根本解。

## 21.1 なぜ必要か（測定された動機）

f64 の加算は **非結合的**（`(a+b)+c ≠ a+(b+c)`）。実測（`crates` 外の検証、
`docs/BENCHMARKS.md` 参照）:

| データ | f64 アキュムレータで partition→merge | 一致 |
|---|---|---|
| 整数値（合計 < 2^53） | 厳密 | ✅ |
| 整数値（合計 > 2^53, バラバラ） | 最終 ULP ズレ | ❌ |
| 小数値 | 最終 ULP ズレ | ❌ |

つまり並列集計（#41）を byte-identical にしようとすると、**小数列の sum/avg/std
が原理的に壁**になる。これは「より賢い縮約順序」では消えない（f64 の表現自体が
丸める）。**10進固定小数点（scaled integer）にすれば加算が整数演算になり、
厳密かつ結合的** → 並列マージで 1 ビットも変わらない。金額計算の正しさ（`0.1 +
0.2 = 0.3` が厳密）という独立した価値も同時に得る。

これは「速度 < 正確性」をユーザーが選べる、という原則（`06` の gradual typing と
原則7「型 = 実行戦略」）の自然な帰結である。

## 21.2 表現 — scaled integer

`decimal(scale=s)` の値は **整数 `unscaled: i128` と固定スケール `s`** で表す。
意味は `unscaled × 10^(−s)`。例: `12.34` は `scale=2` で `unscaled=1234`。

```
Column::Dec(DecColumn { unscaled: Vec<i128>, scale: u8 })
```

- **i128 を採用**: 18 桁の i64 では金額（兆円 × 銭）で桁あふれしやすい。i128 は
  約 38 桁、`unscaled` の総和が overflow する現実的ケースはまず無い。overflow は
  continue-first で検知し（§21.7）、`bigdecimal`（§6.3 の最遅レーン）昇格は将来。
- **scale は列単位で 1 つ**（chunk 内・列内で均質）。混在は読み取り時に最大スケールへ
  そろえる（§21.4）。これにより加算は `unscaled` 同士の **i128 整数加算**になる。

## 21.3 ユーザーインターフェース（列ごと＋フラグ一括の両対応）

統括決定: **両方**用意する。

1. **列ごとの明示指定**（細粒度・既定は従来 f64）:
   ```
   open sales.csv (price:decimal amount:decimal(2))
   ```
   - `decimal` … スケール自動推論（§21.4）
   - `decimal(2)` … スケール明示 2 桁
   - 既存の `(id:int name:str)` 注釈構文（`parser`）に lane を 1 つ足すだけ。

2. **フラグで一括**（手軽・全小数列を decimal 化）:
   ```
   rivus run flow.riv --exact            # 全 F64 推論列を decimal に
   rivus run flow.riv --exact=auto|N     # スケール自動 or 一律 N 桁
   ```
   - `--exact` は「速度を犠牲にしても正確に」という明示オプトイン。
   - 既定（無指定）は**完全に従来通り f64**＝ゼロ回帰・速度最優先。

## 21.4 スケール推論（自動＋明示の両対応）

統括決定: **両対応**。

- **明示**（`decimal(2)` / `--exact=2`）: その桁で固定。読み取り時、各セルを
  `round_half_even` でそのスケールにそろえる（決定的）。
- **自動**（`decimal` / `--exact` / `--exact=auto`）: 2 パス読み取り（既存の
  グローバル型推論と同じ pass）で、その列に現れた **最大の小数桁数** を採用。
  例: `12.5` と `3.14` 混在 → scale=2。`06` の「最初のチャンク生成前にグローバル
  型を確定」原則に乗るので **chunk-size 非依存**。
- スケールは確定後 schema に載る（`Field { name, DataType::Decimal { scale } }`）。
  `to_source()`（IR 可逆性・原則5）は `decimal(s)` を復元する。

### 21.4.1 比較は決して無言で丸めない（会計契約 — 最重要）

統括方針（2026-06-01）: **会計用 decimal は別格の契約**。丸めが起きるのは
**格納時のみ**（明示スケールへの `round_half_even`）であって、**比較では一切
丸めない**。具体的には:

- 比較リテラルは **f64 を経由させず、書かれたとおりの自然スケールの exact
  Decimal** として保持する（`19.995` は scale=3 の `Decimal{19995,3}`、f64 の
  `19.99499…` ではない）。数値リテラルに小数点があれば `Value::Dec` に lex する。
- `decimal 列 OP リテラル` は **`max(列スケール, リテラルスケール)` で i128 比較**
  （`Decimal::partial_cmp`）。どちらのオペランドも丸めない。i128 が溢れる時だけ
  f64 ビューに degrade（kernel・interpreter で同一）。
- 反例（やってはいけない）: リテラルを列スケールへ量子化すると `amount > 19.995`
  が `> 20.00` になり `20.00` を**黙って落とす**。これは契約違反。
- 受け入れ: `> 19.995` は `20.00` を残し、`== 0.305` は scale-2 列で 0 件、
  `> 0.299` は `0.30` を残す。kernel と interpreter で byte-identical
  （`decimal_filter_no_silent_rounding` で gate）。**会計の正確さ ≠ f64 の近似**。

## 21.5 集計の意味論（avg/std は「高精度で割って丸める」）

統括決定: **高精度で割って決定的に丸める**。

| 集計 | decimal lane での扱い | 並列マージ |
|---|---|---|
| `sum` | `unscaled` の i128 総和（厳密） | ✅ byte-identical |
| `min`/`max` | i128 比較 | ✅ |
| `count`/`count_distinct` | 整数 | ✅ |
| `first`/`last` | 値そのまま（source 順） | ✅ |
| `avg` | **厳密 sum ÷ count を、出力スケール `s_out` で `round_half_even`** | ✅（sum も count も厳密だから商の丸めも決定的） |
| `std` | Σx² も i128（`unscaled²` は i256 一時、または検算済み i128 範囲）→ 分散を有理数で評価し `s_out` で丸め | ✅ |
| `percentile` | 値をバッファ→ソート→補間を**有理数/scaled**で評価し丸め | ✅ |

要点: **割り算の結果だけが丸め対象**で、丸めは「決めた桁数で round_half_even」と
*決定的*。並列でも分子（厳密 sum）・分母（厳密 count）が同じなので商も同じ。
`avg` のデフォルト出力スケールは「入力スケール + 既定追加桁（例 +6）」とし、
`--exact=N` 等で上書き可能。std の Σx² は i128 で足りない超大規模では overflow
検知（§21.7）→ degraded で f64 にフォールバックし warning（continue-first）。

## 21.6 並列集計（#41）との接続 — これが本命

decimal lane では `sum`/`avg`/`std`/`min`/`max`/`count*`/`first`/`last`/`pct`
の **すべてが partition→merge で byte-identical**。よって #41 の並列 group-by は
**decimal 列に対しては無条件で安全**に有効化できる。f64 列の sum/avg/std だけが
「serial 維持 or 許容誤差」の対象として残る（それも `--exact` で decimal に倒せば
解消）。

```
group-by 並列可否（集計列の lane で決まる）:
  decimal 列      → 常に並列 OK（厳密・結合的）
  i64 列          → 並列 OK（合計が i64/i128 範囲なら厳密）
  f64 列 min/max  → 並列 OK（結合的）
  f64 列 sum/avg/std → serial 維持（既定）/ --exact で decimal 化して並列
```

実装は `AggAcc` に decimal 用アキュムレータ（`i128 unscaled_sum`, `i128
unscaled_sum_sq` 等）を足し、§operators の merge（worker 順 fold）を decimal でも
実装する。byte-identical は `partition_then_merge_equals_single_pass`（decimal 版）
で gate。

## 21.7 continue-first / overflow（原則2）

- パース不能セル（`abc`）: 既存の数値同様 warning + 既定 0（`unscaled=0`）で継続。
- スケール超過の桁: `round_half_even` で丸め（情報損失は warning しない＝明示桁数の
  意味どおり）。`--exact` 自動スケールなら損失は起きない。
- **i128 overflow**: 加算/二乗で検知（`checked_add`/`checked_mul`）。検知したら
  その集計を **degraded** にし f64 で続行 + warning（停止しない）。telemetry に
  `decimal_overflow` を 1 件計上（観測可能・原則4）。

## 21.8 段階実装（小さな測定付き PR の連結）

1. **コア型**: `DataType::Decimal{scale}`, `Column::Dec`, `Value::Dec` と
   `value_at`/`gather`/`append`/`Display`（`round_half_even` 整形）。等価性ユニット
   テスト。**ここだけで既存経路はゼロ回帰**（新 variant は誰も生成しない）。
2. **読み取り**: `csv.rs` の推論/ビルドに decimal lane（明示スケール先行、自動は
   2 パス最大桁）。`parser` に `:decimal[(n)]` 注釈、CLI に `--exact[=auto|N]`。
   読み取り等価テスト（同じ値が f64 と decimal で表示一致、丸め決定性）。
3. **集計（serial）**: `AggAcc` の decimal 経路（sum/min/max/count/avg/std/pct）。
   f64 集計との数値整合＋ decimal の厳密性テスト。
4. **集計（並列）= #41**: decimal 列で並列 group-by を有効化、`merge` を worker 順
   fold で実装、`partition_then_merge_equals_single_pass`（decimal）を gate。
   `docs/BENCHMARKS.md` に並列 group-by の before/after（decimal 列）。
5. **演算子横断**: filter/arith（`+−×`）の decimal 経路（×はスケール加算、÷は
   §21.5 の丸め）。`optimizer_equiv` 等価ゲート。

各 PR は `06-type-system.md` の「operator は dtype 分岐を増やすだけ」を守り、
operator boundary を厚くしない。既定ビルドは**依存ゼロのまま**（i128 は std）。

## 21.9 GPU との関係

GPU backend（`22-gpu-backend.md`）は SIMD lane（i64/f64）の大規模カーネルを狙う。
decimal lane（i128 整数）は GPU でも厳密に走らせ得る（整数加算）が、優先度は低い
（金額計算は CPU で十分速く、GPU の主目的は f64/i64 の大規模スキャン）。両者は
直交し、どちらも operator/eval 境界の裏に差し込む。
