# 26. Null model — 縦の「絶対に黙って落とさない」を支える欠損表現

> 統括方針（2026-06-04, 申し送り経由）: 最優先は **#81 null モデル**。横（書き味・構文
> v2 #86）は phase-3 前半まで進んだが、縦の核＝「**サイレントはダメ**」（§01）を支える
> **欠損(null)の一級表現**が未着手。根本原因は **null / empty(空文字) / 0 を区別できない**
> こと。本書は #81 の設計先行（docs のみ／STEP 1）。
> **data-model の方向を決めるため、§24/§25 同様レビュー批准必須・自己マージ禁止。批准前に
> 実装に入らない。**
>
> **BUG-A の受け入れテストは既にリポジトリに実在する**（レビュー指摘②の事実確認）:
> `crates/rivus-runtime/tests/stress.rs:3134` の `dropna_drops_blank_numeric_rows_bug_a` が
> `#[ignore = "BUG-A: dropna blind to blank in inferred-numeric column (null model); …"]` 付きで
> 存在（#91 で追加。`id,age\n1,25\n2,\n3,40\n4,\n` を `dropna age` が落とせず現状 fail ＝
> ignore）。STEP 2-② で**この既存テストを un-ignore して緑**に反転させる（新規追加ではなく
> 反転）。なお `docs/TEST-AUDIT.md` の表記は `BUG_A`（大文字）だが実テスト名は小文字 `bug_a`
> （grep 取りこぼしの原因と思われる）。

## 26.0 なぜ今・スコープ

Rivus は「continue-first（落ちない）」「never-silent（黙って捨てない）」を掲げるが、現状の
`Column`（`crates/rivus-core/src/chunk.rs`）は**型ごとの密な `Vec` レーン**で、欠損を表す
余地がない:

```rust
pub enum Column { Bool(Vec<bool>), I64(Vec<i64>), F64(Vec<f64>),
                  Dec(DecColumn), DateTime(DtColumn), Duration(DurColumn),
                  Date(Vec<i32>), Time(Vec<i64>), Str(StrColumn) }
```

結果として:

- 空欄の numeric セルは **`0` に潰れる**（読み取り時）。`dropna`/`fill`/`required` は
  「欠損」を `0` と区別できず効かない（**BUG-A**）。`null`・`empty("")`・`0` が同一視される。
- parse 失敗は #80 で **件数は surface** されるが、値は**型のデフォルト（0/epoch 0）に黙って
  化ける**。「数は見えるが値は失われる」状態で、never-silent の理念に対し中途半端。

本書は欠損を **一級の値**として表現し、入力収斂・演算伝播・集約・sink・byte-identity・並列
マージまで一貫させる設計を決める。**cross-cutting**（core `Column`・全 operator・集約・sink・
並列）ゆえ、実装前に方向を 1 本で批准する。

非目標（本 doc では決めない／別 issue）: SQL 三値論理の完全実装、`NULLS FIRST/LAST` の
ユーザ指定構文、`is null` 述語構文の最終形（§25 構文側で別途）。本書は **データモデルと
既定セマンティクス**を確定し、構文糖衣は最小限に留める。

---

## 26.1 表現 — 列ごとの validity（null bitmap、推奨）

**決定（推奨）: 各 `Column` レーンに「妥当性ビットマップ（validity bitmap）」を持たせる。**
sentinel 方式は採らない。

```rust
/// 1 bit/row。bit=1 が「値あり(valid)」、bit=0 が「null(欠損)」。
/// `None` は「この列に null は一つも無い」= ゼロオーバーヘッド（高速パスを温存）。
pub struct Validity(Option<Box<[u64]>>);   // 省メモリのワード詰めビットマップ

pub struct Column {
    data: ColumnData,     // 既存の型別 Vec レーン（値は型のデフォルトで埋まる）
    validity: Validity,   // None = all-valid（既定）
}
```

- **なぜ bitmap か**:
  - **null / empty / 0 を構造的に区別**できる（sentinel は「0 を null と区別できない」問題を
    別の特殊値に押し付けるだけで、decimal/datetime/全レーンに安全な番兵が無い）。
  - **既存の密レーンと値は据え置き**（null 行の裏値は型デフォルトのまま）→ SWAR/SIMD
    スキャン・exact 整数レーン（decimal/datetime/duration、§21/§23）の高速パスを壊さない。
  - `validity = None`（null 皆無）のとき**完全ゼロコスト**。null を持つ列だけが 1bit/row を払う。
  - Arrow 互換の素直な形（将来 backend を差し替えても写像が自明、§01「Operator boundary
    is thin」）。
- **empty("") vs null**: `Str` レーンは `""`（**長さ 0 の実在文字列**）を従来どおり値として
  保持。`null` は validity=0 で表す。両者は別物（§26.3 の入力規則で作り分ける）。
- **0 vs null**: `I64`/`F64`/`Dec`/`Date`/`Time` の `0`/epoch0 は実在値。`null` は validity=0。
  裏の `data` が 0 でも validity=1 なら「実在の 0」、validity=0 なら「欠損（表示・演算で 0 を
  使わない）」。
- **API（core）**: `Column::is_null(row) -> bool`、`value_at(row) -> Value`（null 行は
  `Value::Null` を返す。`Value::Null` は既存）、構築系（push_value / push_null）、
  `gather`（行選択）・`append`（チャンク連結）・`take` が **validity を同伴**で運ぶ。

---

## 26.2 意味論 — 比較・順序・伝播・集約

「決定的・byte-identical・continue-first」を満たす**実務的な二値寄り**の規則を既定とする
（完全な SQL 三値論理は非目標）。

### (a) 比較・等値（**文脈依存**）

`null == null` の答えは**文脈で変わる**。これを取り違えると §26.2(d) の「`count_distinct`
は null を数えない」と矛盾するので、明示する:

| 文脈 | `null == null` | `null == 非null` | 効果 |
|---|---|---|---|
| **述語 / join**（値比較） | **false** | **false** | null 行は述語不成立。`x > 5` は null を残さない。join キーが null の行はマッチしない |
| **group-by / distinct / dedup**（キー等価） | **true** | false | null どうしは**単一の等価キーに畳む** |

- 述語側: 既定のフィルタ/比較は **`null` を「述語不成立(false)」**として扱う。`|? x > 5` は
  `x` が null の行を**残さない**（drop）。`==`/`!=` も `null == 任意 → false`、
  `null != 任意 → false`（null は何とも「等しくも異なりもしない」）。`x != null_literal` で
  全行を拾う事故を防ぐ。将来 `is null` / `is not null`（§25 構文側で別途）でのみ明示判定。
- **キー等価側**: `group-by` のキー・`distinct`/`dedup` の重複判定では **`null == null → true`**。
  すなわち:
  - **group-by キーが null の行は落とさず、1 つの「null グループ」に畳む**（COUNT(\*) に
    数えられる）。
  - `distinct`/`dedup` は複数の null 行を 1 行に畳む。
  - これが §26.2(d) の `count_distinct`（**非 null の異なり値**を数える）と整合する: 「グループ
    キーとしての null は 1 つ」だが「異なり値としての null は数えない」は別レイヤの規則で、
    互いに矛盾しない。
  - 実装: グルーピングの等価キーはこの規則で正規化する（現状の
    `crates/rivus-runtime/src/operators.rs` のキー組み立て＝`BTreeMap` キーに、null を表す
    決定的な番兵キーを 1 つ用意し、全 null 行を同一キーへ写す。**値の `0`/`""` とは衝突しない
    別表現**にすること）。

### (b) 順序（sort）
- **nulls last（昇順で末尾・降順で先頭）を既定**とする。決定的で安定ソート（§ 既存 `sort`
  の安定性）を保ち、**chunk-size 非依存**。`NULLS FIRST` 等のユーザ指定は将来構文。

### (c) 伝播（算術・関数）
- **null in → null out**（SQL 流）。`(a + b)` は a か b が null なら null。文字列関数・数値
  関数・`cast` も入力 null → 出力 null。
- 例外 `coalesce(a, b, …)`: **最初の非 null** を返す（既存の「空文字でない最初」から「非
  null」へ意味を整流。空文字 `""` は非 null なので coalesce では拾われる点に注意 → §26.7 の
  移行注記）。
- 0 除算・overflow は従来どおり continue-first（NaN/飽和）で、これは null ではない（演算は
  成立している）。**panic しない**。

### (d) 集約（group-by / describe）
- `sum`/`avg`/`min`/`max`/`std`/`median`/`pNN`: **null を skip**（非 null のみで計算）。
  全要素 null のグループは結果 null（`avg`/`sum` とも null。`sum` の「空＝0」は採らない：
  「値が無い」と「合計 0」を区別する）。
- **COUNT の区別**:
  - `count`（現状の暗黙カウント, `|#` が常に出す）= **全行数 = COUNT(\*)**（null 含む）。
  - `count:col`（将来。列指定カウント）= **非 null 数 = COUNT(col)**。
  - `count_distinct:col` = **非 null の異なり数**（null は数えない）。
- `first`/`last`: **最初/最後の非 null**（既存「非空」から「非 null」へ整流）。
- **exact レーンの結合性は不変**: null skip は対象集合を絞るだけで、整数/decimal/datetime の
  加算・min/max の**結合性は保たれる** → 並列 partition→merge は依然 byte-identical（§26.4）。

---

## 26.3 入力 — parse 失敗・空欄は null に収斂（one-off を増やさない）

**決定: 「欠損」の発生源を null に一本化**し、#80（parse 失敗 surface）・#82（`required`
validator）と**同じ経路**に乗せる。新しい特殊扱いを増やさない。

| 入力 | 結果 |
|---|---|
| CSV の**引用なし空フィールド**（`a,,b`） | **null**（全レーン共通。numeric も `0` でなく null） |
| CSV の**引用空文字** `""`（`a,"",b`） | **empty string `""`**（`Str` の実在値。null ではない） |
| 型宣言レーンで**パース不能**な非空セル（例 `(age:int)` に `"x"`） | **null** ＋ #80 と同様に**件数を surface**（never-silent。「default 0 に化けた」をやめ「null 化した」に統一） |
| JSON の `null` | **null** |
| JSON の欠落キー | **null** |

- これで **BUG-A が解ける**: 空欄 numeric が null になり、`dropna`/`fill`/`required` が型に
  依らず効く（§26 STEP2-②で受け入れテスト un-ignore）。
- #80 の surface 文言は「`… could not be parsed; kept as default 0`」から「`… set to null`」
  へ整流（never-silent は維持、損失の説明が正確になる）。空セルは従来どおり「欠損
  （surface しない）」、パース不能非空セルは「失敗（surface する）」の区別を維持。
- **引用 vs 非引用の空**で null/empty を作り分ける規則は、CSV で唯一移植可能な「欠損 vs
  空」の表現。これを正準とする（§26.5 の sink 出力と round-trip で対称化）。

---

## 26.4 byte-identity を null 込みで再定義

§01 の不変条件「**serial == parallel == chunk-size でバイト一致**」を **validity 込み**で成立
させる。

- **validity は決定的**: ある行が null かは入力位置だけで決まり、chunk 分割・並列ワーカー
  数に依存しない。
- **並列マージ**: バイト範囲ワーカーは各自 `Column{data, validity}` を構築し、コーディネータ
  が**順序どおり連結**（`append` が validity を同伴）。null 位置は連結後も同一。
- **集約**: null skip 後の集合に対する exact 整数/decimal/datetime の `sum`/`avg`/`min`/`max`/
  `count` は結合的 → partition→merge が直列とバイト一致（§26.2(d)）。f64 の `sum`/`avg` は
  **従来どおり非結合ゆえ直列**（#41 の既存規則を踏襲、null で変えない）。
- **テスト**: 既存 `tests/stress.rs` の byte-identity スイートに **null を含むデータ**の
  serial==parallel==chunk-size ケースを追加（STEP2 各段でゲート）。

---

## 26.5 sink 出力と round-trip

| sink | null の書き方 | round-trip |
|---|---|---|
| CSV/TSV | **引用なし空フィールド**（`a,,b`） | 再読込で **null**（§26.3）。対称 ✔ |
| CSV/TSV の empty `""` | **引用空** `""` | 再読込で **empty string** ✔ |
| JSON 配列 / JSONL | **`null`**（裸） | 再読込で null ✔ |
| JSON の empty string | `""` | empty string ✔ |

- 「**書ける形は読める**」（§07 対称性）を null/empty 込みで維持。CSV は引用の有無で null と
  `""` を区別、JSON は `null` と `""` で区別。
- 数値整形（末尾 `.0` など）の既存規則は非 null 値にのみ適用。null は常に空（CSV）/`null`
  （JSON）。

---

## 26.6 to_source / round-trip / 並列マージへの影響

- **to_source**: null は**データ値**であって IR/ソースの構文要素ではない（フローに `null`
  リテラルは現状無い）。よって `to_source` は原則不変。将来 `is null` 述語や `null` リテラルを
  入れる場合のみ §25 構文側で別途（本 doc の対象外）。
- **round-trip（IR 可逆）**: 影響なし（IR は欠損を表現しない；欠損は実行時のデータ性質）。
  `optimizer_equiv`（byte-identity ゲート）は §26.4 のデータ side で担保。
- **並列マージ**: §26.4 のとおり validity 同伴連結のみ。新たな IR ノードは不要。

---

## 26.7 移行・互換の注意（実装段で守る）

- **意味変化の明示**: 「空欄 numeric = 0」→「= null」は**観測可能な挙動変更**。`sum`/`avg` が
  null skip になり、`dropna` が numeric にも効く。**英日ガイドに before/after を明記**し、
  必要なら `--blank-as-zero` 等の互換フラグは**設けない**（理念優先。旧挙動が要るなら
  `fill col 0` を明示）。
- `coalesce` の「空文字でない」→「非 null」整流で、`""` の扱いが変わる（`""` は非 null ゆえ
  coalesce で拾われる）。テストとガイドで固定。
- **continue-first 厳守**: null 由来で panic しない（`value_at` の null、null 同士の演算、
  全 null グループの集約、すべて安全）。

---

## 26.8 段階計画（STEP 2 以降・批准後）

各段 = **完結した能力で 1 PR（半割りしない）**。共通ゲート: 等価性テスト先行・**null 込み
byte-identity**・ローカル全緑（`fmt` / `clippy -D warnings` / `test` default＋`--all-features` /
`gitleaks` / `cargo deny`）・**依存ゼロ**・continue-first（panic しない）・**英日両ガイド同時
更新**。**自己マージ禁止**（data-model 方向）。

| STEP | 能力（1 PR） | 完了条件 |
|---|---|---|
| 1 | **本 design doc**（docs のみ） | review 批准（**現在地**） |
| 2-① | core `Column` の validity（構築・`gather`/`append`/`value_at`・基本演算）＋ reader が parse 失敗/空欄を null 化 | null を保持・運搬・surface 整流。既存テスト緑＋null 構築/連結テスト |
| 2-② | operators（filter/project/cast/sort/distinct/**dropna**/fill）が null を正しく扱う | **既存の `dropna_drops_blank_numeric_rows_bug_a`（stress.rs:3134, 現 `#[ignore]`）を un-ignore して緑**。group-by/distinct のキー null 等価（§26.2a）。null 込み chunk-size 非依存 |
| 2-③ | 集約が null skip（COUNT(\*) vs COUNT(col) 区別、first/last/distinct 整流） | null skip の oracle ＋ exact レーンの結合性維持 |
| 2-④ | sink の null 出力（CSV 空 / JSON `null`）＋ round-trip | §26.5 の対称性テスト（read→write→read） |
| 2-⑤ | 並列マージの null 込み byte-identity | serial==parallel==chunk-size を null データで固定（stress） |

各段は前段に積み、null 表現が「軌道に乗る」まで構文 v2 phase-3 後半以降は保留（申し送り）。

---

## 26.9 既存設計との関係（レビュー確認用）

- **§01 8 法則**: 「Continue First」「Observable First（never-silent）」の縦の核を埋める。
  「Byte-identical across execution strategies」を validity 込みで再定義（§26.4）。
- **#80（parse 失敗 surface）**: 「default 0 に化けた」→「null 化した」に文言整流。surface 経路は
  流用（one-off を増やさない）。
- **#82 / §24（validation layer）**: `required` validator は **null を判定**して disposition で
  surface（§26.3 と統一）。`dropna` は null を落とす（§26.2(a) の述語 drop とは別の明示操作）。
- **§21 decimal / §23 datetime/duration**: exact 整数レーンの**裏値は据え置き**、validity を
  上掛け。結合性・byte-identity は不変（§26.4）。
- **#41（f64 並列の非結合）**: 既存どおり f64 `sum`/`avg` は直列維持。null skip は集合を絞る
  だけで結合性規則を変えない。

## 26.10 レビューへの確認事項

1. 表現は **null bitmap（列ごと validity, `None`=all-valid）** でよいか（sentinel 不採用）。
2. **「引用なし空＝null / 引用空＝empty / 0＝実在」**の三区別と、CSV/JSON の入出力規則
   （§26.3/§26.5）を正準としてよいか。
3. 既定セマンティクス: 述語 null=false・**nulls last**・null in→null out・集約 null skip・
   **COUNT(\*) vs COUNT(col)** 区別、で方向 OK か。
4. 互換の非対称な意味変化（空欄 numeric=0 → null、`coalesce` 整流）を**互換フラグなし**で
   進める（理念優先）方針でよいか。
5. STEP 2 の段割り（①core→②operators/BUG-A→③集約→④sink→⑤並列）と各段ゲートで進めてよいか。
