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

**cast / rename の独立 verb は同じ `(Expr, alias)` へ desugar** する（parser 段で `Op::ProjectExpr`
に畳む）。よって IR・runtime・optimizer は一切変わらない。`Expr::Cast` の構造は **不変**
（`format` フィールドを足さない＝§23.6 案B却下と整合）。書式が要る型変換（datetime 書式）は
**reader スキーマ宣言**が唯一の所有者のまま（§23.6 方針「い」）。

### `to_source` 可逆性（§29.5-4 で批准する規則の要点）
`:` の後続トークンの文脈解決を **互いに素**に固定する:

- 後続が **型語**（既知の型名集合に属す）→ cast。
- 後続が **`{ … }`** → 構造ビュー（§29.3）。
- それ以外の**識別子** → 改名。

「列名が型語と衝突する」場合（例：`int` という名の列を別名 `int` にする）の曖昧性解消規則を
**§29.5-4 で確定**する（候補：rename 側を明示する小記号／型語は予約語として列別名に使えない、等。
未確定）。`to_source` は正規形（`:` チェーン）を出し、round-trip 不変をゲートする。

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

定義の候補形（**確定ではない・§29.5-1/2 で批准**）:

```
# 全体ビュー string(27) に、サブビューを { オフセット:長さ } で重ねる（候補）
open ids.csv |> complexId :string(27) :{ cls@0:3 departmentId@3:8 equipmentId@11:16 } ;
```

- `:string(27)` … 全体プリミティブ（ビュー a）。
- `:{ … }` … サブビュー束（ビュー b）。`@offset:len`（または index）で物理位置を指定。
- 全体ビューとサブビューは**同一物理列**を指す union（どちらでも参照可）。

### 参照記法（⚠️ `.` アクセサは却下済み）
サブフィールドの**参照記法**は最初に決める批准点（§29.5-1）。**`.` アクセサ（`id.cls`）は #123 で
round-trip 不能ゆえ却下済み**（discovery の `word.field` を明示エラー化した経緯と同じ：flow-mode
lexer が `a.b` を 1 識別子に畳むため、可逆性が壊れる）。代替の**可逆記法**を確定する（候補は
§29.5-1）。

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
  `char[N]`（生バイト→テキストデコード）を**追加**する（§29.5-3 で null/可変長と併せて批准）。
  `c_align`（C `repr(C)` 自然アラインメント）と `endian` は既存フィールドをそのまま使う。

### テキストモードは UTF-8 境界を割らない
文字列複合は **char / byte 境界を明示**し、**UTF-8 のコードポイント境界を割らない**
（全角/半角・マルチバイトを跨ぐオフセットは never-silent でエラー化＝continue-first）。
オフセット単位（char か byte か）の指定方法は §29.5-2 で批准する。

---

## 29.5 統括に批准させる分岐（design doc で列挙）

> 以下は **確定ではない**。各項に候補を併記するが、最終形は統括の批准で確定する。

1. **共用体ビューの可逆な参照記法**
   - ⚠️ `.` アクセサ（`id.cls`）は **#123 で round-trip 不能ゆえ却下済み**。
   - 代替候補（未確定）：(a) インデックス風 `complexId[cls]`／(b) サブビューを**兄弟列に昇格**
     （`cls` / `departmentId` を独立列名として参照）／(c) `:` 経由の参照 `complexId:cls`。
   - 決め手：**`to_source` 完全可逆**であること・flow-mode lexer と衝突しないこと。

2. **オフセット単位（char / byte）の指定方法・サブフィールドの重なり可否/網羅の要否**
   - 単位：文字列複合は **char**、構造体複合は **byte** が既定だが、明示する綴り（`@`／`bytes`／
     `chars` 修飾など）を確定。
   - **重なり可否**：サブビューが物理範囲を重複してよいか（union/overlay の本質は重なり許容だが、
     検証・最適化の都合で制限するか）。
   - **全体網羅の要否**：サブビューが全幅を覆い切る必要があるか（隙間 padding を許すか）。

3. **null / 可変長フィールドの扱い**
   - 固定長前提のサブビューに **null**（validity=0・§26 null モデル）をどう載せるか。
   - **可変長フィールド**（length-prefixed・delimiter 区切り）を扱うか／本スライスの範囲外とするか。
   - 構造体複合の `char[N]` サブ型追加（§29.4）の null 表現（全 padding を null とみなすか）。

4. **「`:`」チェーンの `to_source` 完全可逆**
   - `:` の後続が **識別子（改名）／型語（cast）／`{…}`（構造）** の **3 文脈を互いに素に固定**する
     規則。特に「列名が型語と衝突」する場合の曖昧性解消（型語を別名に使えない予約とするか、
     改名側を明示する小記号を置くか）。
   - `optimizer_equiv` バイト不変 ＋ round-trip（trivia 含む）をゲート。

5. **書式 / ロケール拡張**（**別スライス s3・依存ゼロ**）
   - 曜日 `ddd`・`[ja-jp]` 等**ロケール**・**サブ秒** `nnnnnn` の追加。日本語曜日は **std-only な
     小テーブル**等で依存ゼロを死守。
   - **`AUTO_FORMATS` 互いに素性の再検証**（§23.1 不変条件・`auto_formats_disjoint` テスト）を
     書式追加のたびに行う。
   - **非 UTF-8（SJIS 等）**は範囲と依存を要設計（**既定ビルド std-only を死守**）。範囲に含めるか
     どうかを批准。

6. **新演算子 / リテラル**
   - `~`（regex 中置）・`'…'`（regex リテラル）・`$_[i]`（位置参照）・`|!` 複数検証 ＋ `{}` サブフロー。
   - **regex は既存 feature-gate を崩さない**（既定ビルド依存ゼロ）。regex リテラルを入れても
     既定ビルドが regex crate を引かない構成（feature 無効時は明示エラー or 不可）を確定。

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
| **s1** | **「`:`」定義チェーン＋cast/rename verb 糖衣化** | `:` チェーンを `Op::ProjectExpr` の `(Expr, alias)` へ lower。cast/rename verb を同 IR へ desugar。`to_source` 正規形＋後方互換往復。`Expr::Cast` 構造不変 | **byte-identity 不変**（IR=既存 ProjectExpr）・round-trip・optimizer_equiv・既存テスト緑 |
| **s2** | **共用体ビュー**（オフセットサブフィールド・zero-copy・§28 binary 統合） | 「物理1列＋多重論理ビュー」。オフセット/index サブビュー（char/byte）。§28 `Codec::Binary`/`BinType`/`Endian` に統合（`char[N]` サブ型追加）。可逆参照記法（§29.5-1） | UTF-8 境界不割・zero-copy・null（§26）・serial==parallel==chunk-size・round-trip |
| **s3** | **書式 / ロケール拡張**（曜日 / ロケール / サブ秒・互いに素性再検証） | `ddd`・`[ja-jp]`・`nnnnnn`。日本語曜日は std-only テーブル。`AUTO_FORMATS` 互いに素性を再検証。非 UTF-8 は範囲を批准 | `auto_formats_disjoint` 再固定・byte-identity・**依存ゼロ** |
| **s4** | **`~` / regex リテラル / `\|!` 複数検証 / `{}` サブフロー** | `~`（中置）・`'…'`（regex リテラル）・`$_[i]`・複数検証＋`{}`サブフロー。regex は **feature-gate を崩さない** | 既定ビルド依存ゼロ（regex feature off）・never-silent・round-trip |

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
2. **統括批准**（§29.5 の各分岐を確定）。
3. **s1 から単一 PR ずつ**。各 PR は **レビュアー（私）が実機検証 → COMMENT**、**統括が
   squash-merge**。
4. 各 PR は §29.7 の不変条件を実測で裏取りして承認 → merge。

---

## 29.9 確定境界（蒸し返さない）

設計対話と §23.6 で確定済み・本書でも維持する境界:

- **書式の所有者は reader スキーマ宣言**（方針「い」・§23.6）。`:` には書式を載せない。
- **式 cast は書式なし・source-aware**（str→datetime を正しく auto-parse・§23.6 スライスA landed）。
- **struct を物理 lane 新設しない**（共用体は「物理1列＋多重論理ビュー」で実現・§29.3）。
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
