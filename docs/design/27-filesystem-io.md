# 27. Filesystem integration — 実 ETL 移行のためのファイル入出力拡張

> 統括方針（2026-06-06, 申し送り経由）: **DuckDB ベースの実 ETL を Rivus に移行**できる
> ようにする「移行トラック」テーマ2。テーマ1（DuckDB 件数パリティ＝null 扱いで件数を
> ズラさない）は landed（join null キー非マッチ＋パリティ・オラクル）。本書はテーマ2の
> **設計先行（docs のみ）**。実 ETL の痛点（PowerShell `gci -re` / DuckDB の `filename` /
> `PARTITION BY` 相当）を 6 項目で捉える。
>
> **入出力の境界とユーザ可視構文を増やすため、§24/§25/§26 同様レビュー批准必須・自己マージ
> 禁止。批准前に実装に入らない。** 批准後、`27.8` の順でスライス実装（各=1完結能力 PR・
> ゲート緑・英日両ガイド同時更新）。
>
> **設計指針（統括）**: DuckDB / PowerShell の機械的な写しにしない。**Rivus のフロー語彙**
> （`open`/`save`・`@Label`・`|>`・既存の述語/関数）と一貫させ、**reversible**（`to_source`
> 影響を各項目で明記）・**continue-first**（壊れたファイルで止めない）・**既定は依存ゼロ**
> （glob/再帰は std の `std::fs` で実装可能）を守る。

## 27.0 なぜ今・スコープ

実 ETL は「**多数のファイルを再帰的に集めて 1 ストリームにし、由来を保持したまま処理して、
キー別に複数ファイルへ書き分ける**」形が中心。今の Rivus は単一 `open PATH` / 単一 `save
PATH` しか持たず、ここが**移行の実ブロッカー**（速度＝SIMD CSV scan より優先、ROADMAP 近期
順序）。

**スコープ（本書で設計する 6 項目）**

1. `filename` 暗黙カラム（由来パスを行に付与）
2. 再帰グロブ＋フィルタ入力（`gci -re` 相当）
3. 動的出力ファイル名（データから出力パス生成）
4. 動的・分割出力（DuckDB `PARTITION BY` 相当）
5. 長パス対応（Windows 拡張長パス等）
6. Unicode / 日本語のパス・列名

**非目標（本書では扱わない）**: ネットワーク/オブジェクトストレージ（S3 等, doc 18）・
圧縮の新形式（`.zip`/tar, §A）・対話的に入力を求める progressive IO（テーマ3, 設計後段）。
これらは別 doc/別項目。

## 27.1 `filename` 暗黙カラム（provenance）

複数ファイルを 1 ストリームに束ねたとき、各行が**どのファイル由来か**を保持する。DuckDB の
`read_csv(..., filename=true)` 相当。

- **構文（フロー語彙）**: 入力に opt-in 修飾子を付ける —
  `open "data/*.csv" with filename` （`with filename` で `filename` 列を末尾に付与）。
  既定では付けない（列を増やすため・ゼロ回帰）。列名衝突時は `filename_r`（join と同規則）。
- **意味論**: `filename` は各行の**ソース絶対パス**を持つ `Str` 列。チャンク境界・並列読みでも
  正しい由来を保つ（reader が chunk 生成時にソースを付帯）。単一ファイル `open` でも使える。
- **continue-first**: 読めないファイルは error stream に surface してスキップ（§13）、`filename`
  は健全なファイル行にのみ付く。
- **reversibility**: `Op::Open` に `with_filename: bool`（＋ §27.2 の glob）フィールドを増設。
  `to_source` は `with filename` を忠実に往復。IR は列追加を明示（optimizer は不変）。
- **代替案と棄却**: 関数 `source_path()` を式から呼ぶ案は、行が「どの読みノード由来か」を式
  評価時に解決できない（チャンクは由来を持つが式は行値のみ）ため棄却。reader が列として
  付与するのが素直。

## 27.2 再帰グロブ＋フィルタ入力（`gci -re` 相当）

ディレクトリを**再帰列挙**し、パターンで絞った**ファイル集合**を 1 入力ストリームに連結する。

- **構文**: `open` のパスにグロブを許す —
  - `open "logs/**/*.csv"` … `**` = 再帰、`*`/`?`/`[...]` = 既存 glob（`like`/`glob` 関数と
    同じ文字クラス語彙を流用し、語彙を統一）。
  - 追加フィルタは**既存の述語語彙**で: `open "logs/**/*" where name like "*2026*"` の形（パス
    に対する `where`）。これにより「DuckDB の写し」でなくフロー述語で絞れる。
- **意味論**: マッチしたファイルを**決定的順序**（パスのバイト昇順）で連結＝1 ストリーム。
  全ファイルは**同一スキーマ前提**（CSV は先頭ファイルのヘッダ/推論を共有、不一致は continue-
  first で warn）。`with filename`（§27.1）と併用で由来保持。
- **実装/依存**: `std::fs::read_dir` の再帰で std-only（**依存ゼロ維持**）。glob マッチも
  std で実装（`**` の再帰は自前）。シンボリックリンクはループ防止のため既定で辿らない。
- **continue-first**: 1 ファイルが壊れても全体を止めない（そのファイルの bad rows を surface
  して次へ）。0 件マッチは warn（空ストリーム）。
- **reversibility**: `Op::Open { path }` の `path` をグロブ文字列として保持＋`where` 述語は
  既存 `Filter` ノードに lower（パス述語専用ノード `Op::SourceFilter` を新設し reader 前段で
  評価、`to_source` 往復）。**批准ポイント**: パス述語を専用ノードにするか `open` 属性にするか。
- **大規模**: 列挙はストリーミング（全パスを一括メモリに載せない）。`take`/`|?` のプッシュ
  ダウンは将来（まず正しさ）。

## 27.3 動的出力ファイル名

固定 `save PATH` でなく、**データ/列値から出力パスを生成**する（テンプレート）。

- **構文**: `save` にテンプレートを許す —
  `save "out/{country}_{year}.csv"` … `{col}` は列値で展開。`{}` 内は**式**も可
  （`save "out/{substr(id,22,4)}.csv"`）で、既存の式語彙を流用。
- **意味論**: テンプレートに**グループ化列のみ**を許す形を基本に（行ごとに違うパスは §27.4 の
  分割出力で扱う）。単一テンプレートで全行同一パスに解決する場合は普通の `save`。
- **continue-first**: 生成パスが不正/書けない → surface してスキップ（その行群を落とさず error
  stream に）。
- **reversibility**: `Op::Save { path_template, … }`。`to_source` はテンプレート文字列を忠実に
  往復（`{}` のエスケープ規則を定義：リテラル `{` は `{{`）。
- **§27.4 との関係**: 動的ファイル名は「1 ストリーム→1 パス（データ依存）」、分割出力は
  「1 ストリーム→キー別 N パス」。前者は後者の退化形として実装してもよい（批准ポイント）。

## 27.4 動的・分割出力（DuckDB `PARTITION BY` 相当）

フィルタ/キーごとに**出力先ファイルを分ける**（write-side の group-by）。

- **構文（フロー語彙）**: `save` にパーティションキーを付ける —
  `save "out/" by country region` … `out/country=JP/region=13/part.csv` のような階層
  （DuckDB 互換の `key=value` レイアウトを既定、`as flat` で `out/JP_13.csv` 形も選べる）。
  あるいは §27.3 のテンプレートと統合: `save "out/{country}/{region}.csv" by country region`。
- **意味論**: パーティションキーで行を振り分け、キー別に別ファイルへ。**パイプラインブレーカ**
  （sort/group 同様、キー基数ぶんのライタを保持）。**決定的**（キー順・行順を固定）で、
  並列でも byte-identical（書き分けは順序非依存キー＝決定的）。高基数ガード（ファイル数上限・
  超過は warn + 単一ファイルfallback か error）。
- **null キー**: パーティションキーが null の行 → DuckDB 同様 `key=__HIVE_DEFAULT_PARTITION__`
  相当の「null パーティション」へ（§26 と整合：null は単一グループに畳む）。**批准ポイント**。
- **依存/実装**: 複数ライタの open/flush を OpCtx 経由で管理（std-only）。長パス（§27.5）と連携。
- **reversibility**: `Op::Save { partition_by: Vec<String>, layout: Hive|Flat, … }`。`to_source`
  で `by …`/`as flat` を往復。

## 27.5 長パス対応

Windows の拡張長パス（`\\?\` プレフィクス, 260 文字超）等で破綻しない。

- **意味論**: パスは**プラットフォームに委譲**（`std::path`）。Windows では長パスを自動で
  `\\?\` 正規化（または OS の長パス有効化前提を明記）。Unix は PATH_MAX を尊重。
- **実装/依存**: std の `std::fs`/`std::path` で大半は透過。Windows 固有の `\\?\` 正規化のみ
  `#[cfg(windows)]` で std API を使い実装（**依存ゼロ維持**）。
- **continue-first**: 長すぎ/不正パスは surface してスキップ、run は継続。
- **reversibility**: 構文に影響なし（パス文字列をそのまま保持）。

## 27.6 Unicode / 日本語のパス・列名

パス・ヘッダ・列名の非 ASCII（日本語等）を端から端まで破綻なく扱う。

- **意味論**: 内部は UTF-8（`text is stream`, §06）。パスは `std::path`（OsStr）で保持し、
  表示/`to_source` は UTF-8 lossless。**列名**は日本語可（`|>`・`|#`・述語で日本語識別子を
  許す — lexer の識別子規則を Unicode 対応へ拡張）。
- **BOM / encoding（§A 連携）**: 先頭 UTF-8 BOM 除去（ROADMAP §A の BOM 項目と統合）。UTF-16
  入力は将来（warn + 継続）。
- **continue-first**: 不正 UTF-8 は lossy デコード + warn（落とさない）。
- **reversibility**: 日本語識別子・パスを `to_source` が忠実に往復（クォート規則を定義：空白/
  記号を含む列名は `"…"` で囲む）。**批准ポイント**: 識別子の Unicode 範囲（XID_Start/Continue
  か、ゆるく「空白・区切り以外」か）。
- **依存**: `unicode-xid` を入れるか std の `char::is_alphanumeric` 系で済ますか（後者なら依存
  ゼロ）。**批准ポイント**。

## 27.7 IR / reversibility への要件（v1 互換）

- 入出力の拡張は **IR（PlanGraph）の `Op::Open`/`Op::Save` を拡張**する（新ノードは最小限：
  パス述語 `Op::SourceFilter` の要否は §27.2 の批准ポイント）。
- **`to_source` は新属性（`with filename`・glob・`where`・テンプレート・`by`・`as flat`）を
  忠実に往復**。`optimizer_equiv`（§08）でバイト不変をゲート。
- continue-first（§13）・byte-identity（§26.4：分割出力も決定的）・依存ゼロ（std-only）を全項目で
  維持。
- 既存単一 `open`/`save` は**完全上位互換**（無修飾は現挙動）。

## 27.8 段階計画（批准後のスライス順）

各スライス＝**1 完結能力の PR**・ゲート緑（fmt/clippy/test/deny/gitleaks・依存ゼロ）・
英日両ガイド同時更新・`to_source` round-trip ＋ `optimizer_equiv` 緑・**批准必須/自己マージ禁止**。

| # | スライス | 主要素 | 依存 |
|---|---|---|---|
| 1 | **`filename` カラム**（§27.1） | `open … with filename`・reader が由来付与・round-trip | 単独で価値・他項目の土台 |
| 2 | **再帰グロブ＋フィルタ入力**（§27.2） | `open "**/*.csv"`・std 再帰列挙・パス `where`・決定的連結 | 1 と併用で由来保持 |
| 3 | **動的・分割出力**（§27.3＋§27.4） | `save "…/{k}.csv" by k`・複数ライタ・Hive/flat・null パーティション | 2 の後（多→多が実 ETL の核） |
| 4 | **長パス / Unicode**（§27.5＋§27.6） | `\\?\` 正規化・日本語パス&列名・BOM 連携 | 仕上げ（全経路の堅牢化） |

> 段表は §26.8 同様、移動/設計コミットと挙動コミットを分け、レビューしやすく刻む。

## MVP / 次 / 将来

- **MVP（本書批准の対象）**: 上記 4 スライスの設計確定。
- **次**: スライス 1→2→3→4 を順に landed。各スライスで DuckDB/PowerShell パリティ（テーマ1の
  oracle に「複数ファイル収集→分割書き出し」ケースを追加）。
- **将来**: glob のプッシュダウン（`|?`/`take` を列挙に押し下げ）・オブジェクトストレージ
  （doc 18）・progressive/interactive 入力（テーマ3）。
