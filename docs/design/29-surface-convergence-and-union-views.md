# 29. Surface 収束 ＋ 共用体的ユーザー型 — `:` 定義チェーンと多重ビュー

> 統括方針（2026-06-09, 設計対話で確定）: cast / rename / projection が同一作用に複数の
> 入口を作り、ユーザーを幻惑させている（`|>` は既に select / rename / compute の3役を持つのに
> rename / cast verb が重複）。**ユーザー動線のシンプリシティを回復する**。IR が作用上同一に
> 解釈するのは構わない（むしろ良い）。あわせて、CSV 固定長 ID と binary C-struct を統一的に
> 扱う **共用体的ユーザー型**（「物理1列＋多重論理ビュー」）の方向を据える。
>
> **本書は phase-0（設計先行）。批准前に実装に入らない。** 構文/IR を変えるため、§00/§24/
> §25/§26/§28 同様 **レビュー批准必須・自己マージ禁止**（§25.10）。big-bang 禁止・段階スライス。
> 既存の正しさ（byte-identity・continue-first・IR 可逆・zero-dep・null モデル）は**保存して
> 載せ替える**。

---

## 29.0 目的とスコープ

### 動機（surface の発散）
今の Rivus には、列を変換する入口が複数ある:

| 作用 | 現状の入口 | 重複 |
|---|---|---|
| 選択（残す列を選ぶ） | `\|> a b c` | — |
| 改名 | `\|> (x) as y` ／ rename verb | `\|>` と verb の二重 |
| キャスト | `(x:type)` ／ cast verb | `\|>` と verb の二重 |
| 計算 | `\|> (x+y) as z` | — |

`|>`（`Op::ProjectExpr`）は元来「列を変換する場」で **select / rename / compute の3役**を担う。
そこへ rename / cast の独立 verb が重なり、**同じ作用に複数の綴り**が生まれている。書き手は
「どれを使うべきか」で迷い、`to_source` の正規形も揺れる。**動線を一つに収束**する。

### 記号原則（統括確定・本書の背骨）
収束の基準は記号の役割を一意に固定すること:

- `( )` = **式**（評価して値を出す）。
- `{ }` = **ブロック**（サブフロー／構造ビューの束）。
- `:` = **定義**（名前・型・構造を左から積む）。
- **即値化できるもの**（プリミティブ変換＝改名・型キャスト）＝ **型キャスト `:` 一本**に寄せる。
- **書式を理解するパース／ロジック／検証**（datetime 書式・正規表現・契約）＝ **式 `( )`・
  ブロック `{ }`・サブフロー `|`** で表す（`:` には書式を載せない＝§23.6 確定方針と整合）。
- 優先度は **軽負荷 → 重厚** の順（**選択 < 改名 < cast < 計算**）。`|>` を読むとき、左ほど
  軽い作用、右ほど重い作用、と一目で分かる並びにする。

### スコープ
- **§29.2** 「`:`」定義チェーン：cast / rename verb を `|>`（`Op::ProjectExpr`）への糖衣として
  lower し、mental model を一つに収束。**IR は既存 `Op::ProjectExpr`、byte-identity 不変**。
- **§29.3** 共用体的ユーザー型：「物理1列＋多重論理ビュー」。**struct lane を物理新設しない**。
- **§29.4** テキスト複合 vs 構造体複合の **3軸差異**（境界・サブ型・エンコーディング・
  エンディアン）と、既存 §28 binary（`Codec::Binary`/`BinType`/`Endian`）への統合方針。
- **§29.5** 統括に批准させる分岐（記法・単位・null・to_source 規則・拡張・新演算子・厳密度段階）。

### 非目標（本書では設計しない）
- コンパイル backend（§00 Phase 2）・分散・制御プレーン。
- `as Sale` named schema 再利用（§23.6 スライスB）は **本収束（cast/rename/`as` overload 整理）の
  後**に回す（§29.5-7）。

---

## 29.1 surface 収束の不変条件（先に固定）

1. **作用は IR で同一に解釈してよい**（むしろ収束の目的）。cast / rename / select / compute は
   すべて `Op::ProjectExpr { items: Vec<(Expr, String)> }`（`graph.rs:487`）へ lower する。
   新 IR ノードを足さない。
2. **byte-identity 完全不変**：糖衣化は parse → 同一 IR への lower のみ。serial == parallel ==
   chunk-size を `tests/stress.rs`・`tests/optimizer_equiv.rs` で固定。
3. **`to_source` 可逆**：収束後の正規形（`|>` の `:` チェーン）と、後方互換で残す verb の両方を
   忠実に往復する。round-trip（trivia 含む）テストを必須化。
4. **v1 を壊さない上位互換**：既存の cast / rename verb・`(x:type)`・`as` は当面据え置き（alias
   として正名へ解決、`to_source` は正規形を出す＝§25.2a verb 命名ポリシーと同型）。

---

## 29.2 「`:`」定義チェーン（cast / rename の収束）

### 形
`|>` の各列項を、左から定義を積む **`:` チェーン**で書く:

```
col :name :type(arg) [:...]
```

- `:name`（後続が**識別子**）＝ **改名**。
- `:type(arg)`（後続が**型語**）＝ **キャスト**（`int` / `decimal(2)` / `datetime` / `str` …）。
- 左→右に積むので「**改名してから cast**」が自然な語順で表せる。

例（収束前 → 収束後）:

```
# 収束前（入口が複数・揺れる）
open sales.csv |> rename amount:amt |> (amt:decimal(2)) as amt ;

# 収束後（|> の : チェーン一本）
open sales.csv |> amount :amt :decimal(2) ;
#                 └ select  └rename └cast   ……軽→重の順で並ぶ
```

### IR への lower（既存 `Op::ProjectExpr`）
`:` チェーンは `(Expr, alias)` の組へ lower する。

| 綴り | lower 先 `(Expr, String)` |
|---|---|
| `col` | `(Expr::Column("col"), "col")` |
| `col :amt` | `(Expr::Column("col"), "amt")`（alias だけ変わる＝改名） |
| `col :decimal(2)` | `(Expr::Cast(Column("col"), Decimal(2)), "col")` |
| `col :amt :decimal(2)` | `(Expr::Cast(Column("col"), Decimal(2)), "amt")` |

IR・runtime・optimizer は一切変わらない。`Expr::Cast` の構造は **不変**
（`format` フィールドを足さない＝§23.6 案B却下と整合）。書式が要る型変換（datetime 書式）は
**reader スキーマ宣言**が唯一の所有者のまま（§23.6 方針「い」）。

### verb は desugar **しない**（s1 実装で確定・doc 当初案を修正）
当初案は「cast/rename verb も `Op::ProjectExpr` へ desugar」だったが、**意味が保存できない**ため
撤回する：verb（`rename OLD NEW`／`cast COL:type`）は**全列保持の in-place 演算**
（`Op::Rename`/`Op::Cast`）であり、`ProjectExpr` は**列選択**（列挙した列だけ残る）。パース時に
スキーマは未知なので、通過列を列挙する形に書き換えられない。よって:

- **verb は現行どおり**（`Op::Rename`/`Op::Cast`・全列保持・上位互換）。
- **収束の実体は `|>` 内の `:` チェーン**：projection の文脈では select / rename / cast / compute
  が一箇所で書け、そこが正規形になる。verb は「行全体を保ったまま少数列を直す」用途に残る
  （重複ではなく semantics が違う）。

### `to_source` 可逆性（§29.5-4・s1 実装で確定）
`:` の後続トークンの文脈解決を **互いに素**に固定した（`rivus_ir::is_type_word` が単一の真偽源）:

- 後続が **型語**（`is_type_word`：`int`/`i64`/…/`decimal`/`datetime` 等、別名込み）→ **常に cast**。
- 後続が **`{ … }`** → 構造ビュー（§29.3・s2、未実装）。
- それ以外の**識別子** → 改名。
- 順序は厳格に**「改名 → cast」**（軽→重）。cast の後に続く `:` は明示エラー（never-silent）。

「別名が型語と衝突する」場合（例：列を `int` という名に改名）はチェーンでは表せず、**括弧形
`(col) as int` がエスケープハッチ**。`to_source` も同じ述語でガードし、衝突別名は括弧形で出す
（`:int` と出すと cast に再パースされるため）。正規形は `:` チェーン（型名は正規化 `int`→`i64`）、
旧綴り（`(col:type) as x`・`(col) as x`）は同一 IR ゆえ正規形へ収束し、round-trip をテストで固定
（`colon_chain_is_the_canonical_form_and_round_trips` ほか）。

---

## 29.3 共用体的ユーザー型 — 「物理1列＋多重論理ビュー」

### 動機（complexId 例）
固定長 ID（例：27文字の `complexId`）を、

- (a) **全体プリミティブ**（`string(27)`）としても、
- (b) **内部サブフィールド**（`cls` / `departmentId` / `equipmentId`）としても

参照したい。同一データの **union / overlay**（重ね合わせ）である。

### 方針 — struct lane を物理新設しない（確定）
物理表現は**既存のまま**：CSV 固定長は `StrColumn`、binary は固定幅レコード 1 本。complexId は
**「オフセットマップ＋ビュー」という型付随メタデータ**として持ち、サブ参照は **zero-copy
スライス**（物理バイト/文字列を複製しない）。`DataType` に重い struct 表現を増やさず、既存
レーンの上に**論理ビュー**を被せる。

### サブフィールドは substr でなく「オフセット/インデックス指定」
サブフィールドは部分文字列演算（`substr`）ではなく、**C-struct のフィールドオフセットと同型**の
**オフセット/長さ（または index）指定**で定義する。これにより:

- **CSV 固定長 ID**（文字オフセット）と **binary C-struct**（バイトオフセット）を**統一的**に扱える。
- ビューは静的に解決でき（typed-IR・§00 0.12）、実行時は zero-copy スライス。

定義形（**統括批准 2026-06-10・issue #137**）— 範囲形 `@start..end`（半開区間）:

```
# 全体ビュー string(27) に、サブビューを { @start..end } 範囲で重ねる
open ids.csv |> complexId :string(27) :{ cls@0..3 departmentId@3..11 equipmentId@11..27 } ;
```

- `:string(27)` … 全体プリミティブ（ビュー a）。
- `:{ … }` … サブビュー束（ビュー b）。**`@start..end`（半開区間 `[start, end)`）** で物理位置を指定。
  `:` は定義記号（§29 記号原則）ゆえ offset/len 区切りには使わず、既存 `Tok::DotDot` の範囲形を用いる
  （lexer 変更ほぼゼロ）。端点規約（半開・単位は型族自動）は s2 design で明記。
- 全体ビューとサブビューは**同一物理列**を指す union（どちらでも参照可）。**重なり許容・全幅網羅不要**
  （隙間＝padding 可）。

### 参照記法 — `.` アクセサ採用（統括批准 2026-06-09）
サブフィールドの参照は **`.` アクセサ（`complexId.cls`）** を採用する。`.` は本質的に可逆不能では
ない——**式文脈（`|> ( … )` の内側）では既に可逆**で、provenance の `source.uri`（§28.6）と同一機構
（round-trip 済み：parser `source_accessor_parses_and_round_trips`）。

可逆／非可逆の境目は lexer の **depth-aware な語規則**（`lexer.rs:428` `word_part`）にある:

- **式文脈（`depth > 0`）**：語は `[A-Za-z0-9_]+` のみ。`.` は語に含まれず独立 `Tok::Dot`
  （`lexer.rs:265`）になるので `complexId.cls` は `Word "complexId" Dot Word "cls"` に分解され、
  `Expr::Field` へ parse・`to_source` が忠実に復元する（**可逆**・`lib.rs:1395`）。
- **flow 文脈（`depth == 0`）**：語は `.` `/` `-` を含む（`lexer.rs:446` `is_word_part`）。これは
  **裸のファイルパス**（`open users.csv`・`data/out.parquet`）を 1 トークンで読むための仕様。
  ゆえに裸の `a.b` はファイル名／ドット付き列名と**字面が同一**で区別できず、`to_source` が忠実に
  往復できない。#123 が裸 dotted を明示エラー化した（`lib.rs:1413`）のはこのため——
  **アクセサが irreversible なのではなく、その字面スロットがパスに属している**。

→ 結論：union サブフィールド参照は **式文脈の `.` アクセサ**（`|> (complexId.cls) as cls`）に置く。
既存 `source.uri` と同じ場所・同じ機構で、**ゼロ lexer 変更・既に round-trip 済み**。裸 flow 位置の
`.` はパス曖昧性ゆえ従来どおり明示エラーのまま（never-silent）。

### s2 lowering（実装確定 2026-06-10・issue #137 裁定を受けて）

§29.5-1 の「残る細目」を次のとおり確定し、s2 の最小スライス（**text／char 限定**）を実装する:

- **定義の格納**：`Op::ProjectExpr` に `views: Vec<ViewDef>` を加える（既存 project は空＝**挙動不変**）。
  `ViewDef { col, width: Option<u32>, subs: Vec<SubView> }`・`SubView { name, start, end }`（**半開・char**）。
  `complexId :string(27) :{ … }` の `(27)` は `ViewDef.width` に保持する（`DataType::Str` は幅を持たない
  ので、cast 自体は `Str` のまま・幅は views メタに置く）。`to_source` は item の `:string(width)` と続く
  `:{ name@start..end … }` を views から忠実に復元（可逆）。
- **参照**：式文脈の `base.name` は `Expr::SubView { base, name, start, end }` に lower（parser が**直前まで
  に見た定義を状態で解決し範囲を inline**）。`to_source` は `base.name`。`source.uri` と同じ式文脈 `.` 機構。
- **eval（zero-copy）**：`SubView` は `column(base).get(row)` の `&str` を char 範囲 `[start, end)` で
  **借用スライス**（部分文字列を複製しない＝§29.3 のゼロコピー）。char 境界が UTF-8 コードポイント境界を
  割る／範囲が幅を超える場合は **never-silent エラー**（continue-first・error stream へ）。
- **解決規則**：`base` が直前までに定義された view 列で `name` がそのサブビュー名のときだけ accessor 化。
  未知の `base.name` は従来どおり明示エラー（never-silent・`lib.rs:1413`）。同名サブビューは定義時にエラー。
- **byte-identity**：`SubView` は純粋な行ごとスライス＝serial == parallel == chunk-size 不変。
- **binary（byte 単位・`char[N]` BinType）は後続コミット**（§29.4・§29.5-3）。本スライスは text（char）限定。

---

## 29.4 テキスト複合 vs 構造体複合 — 3軸差異（必須節）

共用体型は「文字列複合（CSV 固定長）」と「構造体複合（binary C-struct）」の両方を担う。両者は
本質的に異なる軸を持ち、型はこれらを**パラメータ**として持つ:

| 軸 | 文字列複合（CSV 固定長） | 構造体複合（binary C-struct） |
|---|---|---|
| **境界** | 文字単位（UTF-8 可変幅・全角/半角） | バイト単位（＋ align / pad） |
| **サブ型** | 部分文字列（substr ビュー） | 任意（`i32` / `f64` / `char[N]` …） |
| **エンコーディング** | テキストデコード要 | 生バイト → 型解釈 |
| **エンディアン** | 無関係 | 数値に LE / BE |

→ 共用体型は **「境界単位（char / byte）・エンコーディング・エンディアン・サブ型」** を
パラメータに持つ一つの型族として設計する。

### バイナリモードは §28 の既存機構と統合（新設しない）
構造体複合は **§28 の既存 `Codec::Binary`** に統合する（新しい binary 機構を作らない）:

- `Codec::Binary { fields: Vec<(String, BinType)>, endian: Endian, c_align: bool }`（`graph.rs:372`）。
- `BinType`（`graph.rs:188`：`I8..F64` / `Bool`）。`Endian { Little, Big }`（`graph.rs:23`）。
- **現状 `BinType` に固定長文字列サブ型（`char[N]`）が無い**ため、構造体複合のサブ型として
  `char[N]`（生バイト→テキストデコード）を**追加**する（§29.5-3 で確定：全 padding は値保持・可変長は範囲外）。
  `c_align`（C `repr(C)` 自然アラインメント）と `endian` は既存フィールドをそのまま使う。

### テキストモードは UTF-8 境界を割らない
文字列複合は **char / byte 境界を明示**し、**UTF-8 のコードポイント境界を割らない**
（全角/半角・マルチバイトを跨ぐオフセットは never-silent でエラー化＝continue-first）。
オフセット単位（char か byte か）は §29.5-2 で確定（型族から自動導出・綴りは範囲形 `@start..end`）。

---

## 29.5 統括に批准させる分岐（design doc で列挙）

> **批准済（統括裁定 2026-06-10・issue #137）。** 当初は候補併記だったが、裁定結果を本節へ反映済み。
> ②③が確定したため **s2 着手可**（⑤＝s3・⑥＝s4 着手時の前提）。

1. **共用体ビューの参照記法 — 確定：式文脈の `.` アクセサ（統括批准 2026-06-09）**
   - `complexId.cls` を **式文脈（`|> ( … )`）** で使う。`source.uri`（§28.6）と同一機構で
     **既に可逆**（`lexer.rs:428` の depth-aware 規則・§29.3 参照）。ゼロ lexer 変更。
   - 裸 flow 位置（`|?` 述語・verb 直下）の `.` は**パス字面と衝突**するため従来どおり明示エラー
     （`lib.rs:1413`）。サブを裸で回したい場合は `|> (complexId.cls) as cls` で兄弟列に材化して
     から参照（`source.uri` の運用と同型）。
   - 残る細目（s2 で確定）：サブビュー名の名前空間（同名衝突規則）／式文脈で複数サブを一括展開する
     糖衣（`|> complexId.{cls departmentId}` を `(complexId.cls) (complexId.departmentId)` へ
     desugar するか）の要否。

2. **オフセット単位・重なり・網羅 — 確定（統括批准 2026-06-10・issue #137）**
   - **単位は型族から自動導出**：文字列複合（CSV 固定長）＝**char**、構造体複合（binary C-struct）
     ＝**byte**。明示綴りは増やさない（型から自明な情報の二重指定を避ける）。
   - **綴りは範囲形 `@start..end`（半開区間 `[start, end)`）**。`@offset:len` は不採用——`:` は定義記号
     （§29 記号原則）であり offset/len 区切りとの二重役を避ける。lexer には既に `Tok::DotDot` があり
     追加コストはほぼゼロ。端点規約（半開・char/byte 単位での端点）は s2 design で明記。
   - **重なり許容**（サブビューが物理範囲を重複してよい＝union/overlay の核）。
   - **全幅網羅は不要**（サブビューが全幅を覆わなくてよい・隙間＝padding 可）。
   - char 単位で UTF-8 コードポイント境界を割るオフセットは **never-silent エラー**（§29.4）。

3. **null / 可変長 / `char[N]` の null 表現 — 確定（統括批准 2026-06-10・issue #137）**
   - **可変長フィールド**（length-prefixed・delimiter 区切り）は **s2 範囲外**（固定長サブビューのみ・
     将来スライス）。zero-copy ＋ 静的オフセットの足場を崩さないため。
   - 固定長サブビューの **null** は §26 null モデル（validity）に従い、全体列の validity を継承。
   - 構造体複合の `char[N]` サブ型（§29.4）の **全 padding は値として保持**（null にしない）。**空セルのみ**
     §26 null。「空という値」と「null」の二義性を避け byte-identity / round-trip を保つため。

4. **「`:`」チェーンの `to_source` 完全可逆 — 確定（s1 実装済・§29.2 参照）**
   - 後続は **型語（cast・常に優先）／`{…}`（構造・s2）／識別子（改名）** で互いに素。順序は
     「改名 → cast」固定・超過は明示エラー。型語衝突の別名は**括弧形 `(col) as int` が
     エスケープハッチ**（`to_source` も同述語でガード）。単一の真偽源は `rivus_ir::is_type_word`
     （parser の型表とテストで同期固定）。
   - `optimizer_equiv` バイト不変 ＋ round-trip をテストでゲート済み。

5. **書式 / ロケール / タイムゾーン拡張 — 確定（統括批准 2026-06-10・issue #137）**（別スライス s3・依存ゼロ）
   - 曜日 `ddd`・`[ja-jp]` 等**ロケール**・**サブ秒** `nnnnnn` の追加。日本語曜日は **std-only な
     小テーブル**で依存ゼロを死守。
   - **タイムゾーンも s3 範囲に含める**（統括明言「ロケールも TZ もありあり」）。固定オフセット
     （`+09:00`）は **#93 で既に正規化済み**。named zone（IANA）を扱う場合、フル tzdata は依存・
     データサイズの供給網判断（§SUPPLY-CHAIN チェックリスト）が要る——**std-only の範囲（固定
     オフセット＋限定的な既知略称テーブル等）か tzdata 取り込みかは s3 design で選択肢＋推奨を提示して
     再確認**する。
   - **`AUTO_FORMATS` 互いに素性の再検証**（§23.1 不変条件・`auto_formats_disjoint` テスト）を
     書式追加のたびに行う（必須）。
   - **非 UTF-8（SJIS 等）は s3 範囲外**（将来・encoding 依存の判断を別途／**既定ビルド std-only を死守**）。

6. **新演算子 / リテラル — 確定（統括批准 2026-06-10・issue #137）**
   - `~`（regex 中置）・`'…'`（regex リテラル）・`$_[i]`（位置参照）・`|!` 複数検証 ＋ `{}` サブフロー。
   - **`~` / `'…'` regex リテラルの parse / to_source は常時 std**（IR に保持・可逆＝§04）。**評価のみ**
     off-by-default の `regex` feature を必須とし、**feature off 時は never-silent エラー**（実行不可を
     明示）。＝**既存 `Func::Regexp` と同一構成**。既定ビルドは regex crate を引かない＝依存ゼロ。

7. **「どこまで厳密にするか」の段階**
   - (1) その場 ad-hoc な `:type{…}`（無名・即席）→ (2) **名前付き再利用 / 外部 DSL 流用**。
   - slice B の **`as Sale`（§23.6）はこの収束の後・`as` overload 整理後**に回す（`as csv|tsv|…`
     形式指定との曖昧性解消を先に固める）。

---

## 29.6 スライス分割（批准後）

各スライス＝1完結能力 PR・ローカルゲート緑・依存ゼロ・英日両ガイド・`to_source` round-trip ＋
`optimizer_equiv` 緑・byte-identity 保存。**糖衣化（move-only/lower）コミットと挙動コミットを分ける**。

| # | スライス | 主要素 | 正しさゲート |
|---|---|---|---|
| **s1** | **「`:`」定義チェーン（landed）** | `:` チェーンを `Op::ProjectExpr` の `(Expr, alias)` へ lower。`to_source` 正規形＋後方互換往復。`Expr::Cast` 構造不変。**verb は desugar しない**（in-place 全列保持で semantics が別・§29.2 で確定） | **byte-identity 不変**（IR=既存 ProjectExpr）・round-trip・optimizer_equiv・既存テスト緑 |
| **s2** | **共用体ビュー**（範囲形オフセットサブフィールド・zero-copy・§28 binary 統合）【批准済 #137】 | 「物理1列＋多重論理ビュー」。**範囲形 `@start..end`（半開）** サブビュー（単位は型族自動＝char/byte・重なり許容・全幅網羅不要）。§28 `Codec::Binary`/`BinType`/`Endian` に統合（`char[N]` サブ型追加・**全 padding は値保持**・可変長は範囲外）。`.` アクセサ参照（式文脈・§29.5-1 確定） | UTF-8 境界不割（never-silent）・zero-copy・null（§26）・serial==parallel==chunk-size・round-trip |
| **s3** | **書式 / ロケール / TZ 拡張**（曜日 / ロケール / サブ秒 / タイムゾーン・互いに素性再検証）【批准済 #137】 | `ddd`・`[ja-jp]`・`nnnnnn` ＋ **TZ**（固定オフセットは #93 済・named zone は std-only か tzdata 取込かを s3 design で再確認）。日本語曜日は std-only テーブル。`AUTO_FORMATS` 互いに素性を再検証。非 UTF-8 は範囲外 | `auto_formats_disjoint` 再固定・byte-identity・**依存ゼロ** |
| **s4** | **`~` / regex リテラル / `\|!` 複数検証 / `{}` サブフロー**【批准済 #137】 | `~`（中置）・`'…'`（regex リテラル）・`$_[i]`・複数検証＋`{}`サブフロー。**parse/to_source は常時 std（IR 可逆）・評価のみ `regex` feature**（off＝never-silent エラー）＝`Func::Regexp` と同構成 | 既定ビルド依存ゼロ（regex feature off）・never-silent・round-trip |

---

## 29.7 不変条件（毎スライス）

- **byte-identity**（serial == parallel == chunk-size）— `tests/stress.rs`。
- **IR 可逆**（`to_source` round-trip ＋ `optimizer_equiv`）。
- **依存ゼロ**（既定ビルド `rivus-*` のみ。重い物＝regex 等は feature-gate）。
- **never-silent・continue-first**（不正入力は error stream へ surface、Fatal 以外は継続）。
- **英日両ガイド同時更新**。
- push 前ゲート（CLAUDE.md）：`cargo fmt --all -- --check` ／ `RUSTFLAGS="-D warnings" cargo clippy
  --workspace --all-targets --all-features`（=0）／ `cargo test --workspace [--all-features]`
  （FAILED=0）／ 依存ゼロ（`cargo tree -p rivus-cli --edges normal`）／ `gitleaks`。
- **設計 / IR / 構文変更は design doc 先行・批准制**（§25.10）。

---

## 29.8 段取り

1. **本 §29 design doc を 1 本起こして PR 化**（本 PR）。
2. **統括批准**（§29.5 の各分岐を確定）— ②③⑤⑥ **批准済 2026-06-10・issue #137**。
3. **s1 から単一 PR ずつ**。各 PR は **レビュアー（私）が実機検証 → COMMENT**、**統括が
   squash-merge**。
4. 各 PR は §29.7 の不変条件を実測で裏取りして承認 → merge。

---

## 29.9 確定境界（蒸し返さない）

設計対話と §23.6 で確定済み・本書でも維持する境界:

- **書式の所有者は reader スキーマ宣言**（方針「い」・§23.6）。`:` には書式を載せない。
- **式 cast は書式なし・source-aware**（str→datetime を正しく auto-parse・§23.6 スライスA landed）。
- **struct を物理 lane 新設しない**（共用体は「物理1列＋多重論理ビュー」で実現・§29.3）。
- **共用体サブフィールド参照は式文脈の `.` アクセサ**（`source.uri` と同一機構で可逆・統括批准
  2026-06-09）。裸 flow 位置の `.` はファイルパス字面と衝突するため明示エラーのまま（never-silent・
  `lib.rs:1413`）。
- **共用体サブビューの綴りは範囲形 `@start..end`（半開）**・単位は型族自動（char/byte）・重なり許容・
  全幅網羅不要（統括批准 2026-06-10・issue #137）。`@offset:len` は不採用（`:` 二重役回避・`Tok::DotDot` 流用）。
- **`char[N]` の全 padding は値として保持**（null にしない・空セルのみ §26 null）。可変長サブビューは
  s2 範囲外（統括批准 2026-06-10・issue #137）。
- **s3 にタイムゾーンを含む**（固定オフセットは #93 済・named zone は std-only か tzdata かを s3 design で
  再確認）。非 UTF-8（SJIS 等）は s3 範囲外（統括批准 2026-06-10・issue #137）。
- **regex（`~` / `'…'`）は IR 常時 std・可逆／評価のみ `regex` feature**（off＝never-silent エラー・
  `Func::Regexp` と同構成・統括批准 2026-06-10・issue #137）。
- **却下（実装しない）**：`Expr::Cast.format`（案B・§23.6）／`ParseTemporal` 新 IR ノード／
  `LaneCodec` trait 全面刷新／`type` キーワード・UserDefinedType 新設（§23.6 却下）。

---

## MVP / 次 / 将来

- **MVP（本書批准の対象）**：記号原則・`:` 定義チェーン収束・共用体ビュー（物理1列＋多重ビュー）・
  3軸差異・§29.5 批准分岐・s1〜s4 の設計確定。
- **次**：s1（`:` チェーン＋cast/rename 糖衣化・IR=ProjectExpr・byte-identity 不変の足場）→
  s2（共用体ビュー・§28 binary 統合）→ s3（書式/ロケール拡張）→ s4（regex/複数検証/サブフロー）。
- **将来**：named schema 再利用（`as Sale`・§23.6 スライスB）を `as` overload 整理後に。外部 DSL
  流用（strptime 互換・正規表現方言）の段階拡張。
