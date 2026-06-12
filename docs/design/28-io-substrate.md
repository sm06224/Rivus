# 28. I/O substrate (Pillar 1) — handle 値型 ＋ discovery-as-flow ＋ 形式非依存 codec

> 統括方針（2026-06-06, North Star 批准後）: §00 North Star **ピラー1** の設計先行。
> ファイル中心 I/O 結合（`OpenCsv`/`OpenJsonl`/`OpenBinary` 等の形式別バリアント）を
> **壊して**、I/O を **Discovery → Transport → Codec → Provenance の直交4層**に再建する。
> **既存の正しさ（byte-identity・continue-first・IR 可逆・zero-dep・null モデル）は保存して
> 載せ替える**（§00 0.5/0.8）。**big-bang 禁止・段階スライス**。
>
> **入出力境界とユーザ可視構文・IR を変えるため、§24/§25/§26/§00 同様レビュー批准必須・
> 自己マージ禁止。批准前に実装に入らない。** §27（filesystem-io）は本書に吸収・一般化する。

## 28.0 目的とスコープ

今の I/O は「形式ごとに `Open*` ノード」で、源（どこから）・運搬（どう読む）・解釈（どの形式）・
由来（どのファイル）が 1 ノードに結合している。これでは North Star の 5 つの姿
（特にサービス＝transport 差し替え・オーケストレーション＝遠隔リンク・provenance）が出ない。

**本書のスコープ**: 入出力を**直交4層**＋ **handle 第一級値型 `Resource`** に再建する。
- **Discovery**: handle の**ストリーム**を産む（ls / glob / 再帰 / `list(s3)` / `watch` /
  `subscribe`）。「探索＝フロー」。
- **Transport**: handle → バイトストリーム（file / mmap / stdin / http / socket）。
- **Codec**: バイト → `chunk(+schema)`（csv / tsv / json / jsonl / binary / parquet…）。
- **Provenance**: 各 chunk/行に handle を結びつける（`with source` ＝ `filename` の一般化）。
- 出力は**鏡像**: encode → route（動的/分割名） → transport（write / POST / publish）。

**非目標（本書では設計しない）**: コンパイル backend（ピラー2・§00 Phase 2）・分散リンク
（ピラー3）・制御プレーン（ピラー4）。ただし本書の型（`Resource`・typed-IR・決定性境界）は
それらの前提なので、整合する形で据える。

## 28.1 `Resource` — handle 第一級値型（§00 0.10① 確定）

**`Resource` のみを第一級の値型**にする（§00 0.10① の (c)）。Stream・Chunk は構造のまま。

```
Resource {
  uri:    String,          // "file:///data/a.csv" / "s3://b/k" / "http://…" / "-"(stdin)
  scheme: Scheme,          // File | Stdin | Http | S3 | … (transport の選択キー)
  // 任意メタ（discovery が埋める。無ければ None）:
  size:   Option<u64>,
  mtime:  Option<DateTime>,
  // codec ヒント（拡張子等から。明示 codec で上書き可）:
  hint:   Option<Codec>,
}
```

- **第一級値**: 式・列・関数に渡せる（`Value::Resource`）。`Column::Resource` レーン（`uri` を
  StrColumn 風に保持＋メタ）。これにより **discovery の結果を普通の列として述語で絞れる**
  （`name`/`size`/`mtime`）。
- **typed-IR（§00 0.12）**: `Resource` は型のある値。`DataType::Resource` を追加（既存 lane に
  並ぶ）。`to_source` は `Resource` リテラル/列を忠実に往復。
- **provenance の単位**: chunk/行に紐づく由来は `Resource`（パス文字列でなく handle）。

## 28.2 直交4層

```
Discovery: () or Resource-pattern  →  Stream<Resource>
Transport: Resource                →  Stream<Bytes>      (scheme で選択)
Codec:     Stream<Bytes> (+schema) ⇄  Stream<Chunk>      (形式、双方向)
Provenance: Chunk × Resource       →  Chunk (+source 列/メタ)
```

直交＝**組み合わせ自由**: 「(discovery で集めた) Resource 列 → transport → codec」を任意形式・
任意源で一律に組める。現 `open f.csv` は「Discovery=単一固定 Resource ／ Transport=File ／
Codec=Csv ／ Provenance=off」の**特殊形**に過ぎない（後方互換の sugar として残す）。

## 28.3 Discovery-as-flow

探索を**ソース段**として IR に置く。handle のストリームを産み、**普通の述語で絞る**。

```
ls "logs/**/*.csv"                      # → Stream<Resource>（再帰 glob）
|? size > 1mb, name like "*2026*"       # 既存述語で絞る（discovery 述語）
|> read as csv with source              # 各 Resource を transport+codec で開き、由来付与
```

- **`ls` / `list` / `watch` / `subscribe`** が discovery verb（scheme で実装差し替え：
  ローカル fs / s3 / http index / inotify / pubsub）。**std-only**（ローカル fs 再帰）が既定、
  リモートは feature-gate。
- **述語プッシュダウン（§00 0.6）**: `name`/`size`/`mtime` の述語は discovery に押し下げて
  列挙段で枝刈り（大ディレクトリで効く）。最適化器が安全に押し下げ（決定的・副作用なし）。
- **有界/非有界（§00 0.13）**: `ls`/`glob` は**有界**（完了する）、`watch`/`subscribe` は
  **非有界**。型で区別し、非有界は背圧・窓の対象。
- **`read`** verb: `Stream<Resource>` を受け、各 Resource を transport で開き codec で chunk 化、
  **`union-by-name` でスキーマを整合**して 1 ストリームに連結する。**「warn して継続」はしない**
  — 列集合は名前で和（欠けは null・型は安全昇格）し、**整合不能な行/ファイル（型衝突・必須欠落
  等）は `reject`/`quarantine` として error stream に surface**（never-silent・§13/§24）。
  **決定的順序**（Resource を uri バイト昇順で連結）。dead-letter 先（`quarantine(sink)`）は §24
  と整合。slice 3 の前提。
- **供給元非依存（批准 2026-06-07）**：`read` が消費する Resource 列の供給元は問わない —
  `ls`/glob・マニフェスト（`open m.csv |> (resource(filepath)) as path`）・計算パス
  （`resource(式)`）・将来の sqlite/http discovery、さらに**制御プレーンからの引数注入**
  （§0.7・値ホール束縛の発展）。read は既定 `path` 列、無ければ最初の `Resource` 型列を消費し、
  transport は scheme でディスパッチ（file 前提を焼き込まない）。

## 28.4 Transport（Resource → Bytes）

scheme で選択。`Transport` トレイト（境界は薄い・§01）:

```
trait Transport { fn open(&self, r: &Resource) -> io::Result<Box<dyn ByteSource>>; }
```

- 実装: `File`（mmap/seek 可＝byte-range 並列の前提）・`Stdin`・`Compressed`（gzip/zstd,
  feature-gate）・`Http`/`Socket`（feature-gate、サービス化の土台＝§00 0.2-3）。
- **byte-identity 保存**: File transport は現 reader の byte-range 分割をそのまま提供
  （seekable のみ並列、非 seekable は serial）。Provenance ＋並列の両立は §28.6 で扱う。

## 28.5 Codec（Bytes ⇄ Chunk、形式非依存）

形式を `Codec` トレイトに**直交化**。decode（読み）と encode（書き）は対。

> **実装同期（slice 1a-②③ landed）**: 当初スケッチは `decode(&[u8]) -> Vec<Chunk>`
> だったが、現リーダーは**有界メモリのストリーミング pull**（全バイトを抱えない）なので、
> Decoder は**chunk 単位の pull**として実装した。推論（§06 two-pass の pass 1）は各形式の
> `open`/`plan_parallel` 側に残し（確定スキーマと decoder を返す）、本トレイトは pass 2 の
> decode ＋ source が surface する診断を担う。dispatch は**行単位でなく chunk 単位**なので
> パース hot path は monomorphic のまま（`rivus_runtime::codec`）。

```rust
trait Decoder {                                   // pass 2: streaming chunk-pull
    fn decode_chunk(&mut self) -> Option<Vec<Column>>;   // None = stream/range 終端
    // source が一度だけ surface する診断（形式により既定値）:
    fn inferred(&self) -> &[(String, DataType, bool)] { &[] } // A4 推論結果
    fn rows_prefiltered(&self) -> u64 { 0 }                    // pushdown で除外した行
    fn parse_failures(&self) -> &[u64] { &[] }                 // null 化した parse 失敗数
}
// 推論（pass 1）は形式側 open が担い、(Schema, Box<dyn Decoder>) を返す。
trait Encoder { fn encode(&mut self, chunk: &Chunk) -> Vec<u8>; … }  // 出力は鏡像
```

- 実装: csv/tsv（delim）・jsonl/json・binary・(feature) parquet。**null モデル（§26）・型推論
  （§06/§23）・continue-first は codec 内に閉じる**（現 `csv.rs`/`jsonl.rs` の中身を Decoder に
  移植）。①②と直交＝**全形式で null/型/エラーが一律**。
- **§06 two-pass 推論を保持（🟡 slice 1a 必須）**: Decoder は**推論フェーズ（`infer`）と
  decode を分離**する — 現 `CsvChunker` の two-pass グローバル推論（サンプルで lane 決定 →
  確定スキーマで全体を decode）を**等価移植**し、宣言スキーマ時は `infer` を省略。これにより
  typed-IR（§00 0.12）が codec の出力スキーマを静的に解け、かつ推論結果は既存テストで固定して
  byte-identity を保つ。
- **既存の正しさ保存**: CSV Decoder は現 `CsvChunker` のロジックを**移設のみ**（byte-identity・
  null・SWAR parse を保つ）。形式判定は拡張子/明示で `Codec` を選ぶ（現 `resolve_format` 相当）。

## 28.6 Provenance（`with source`）

`filename`（§27 slice 1, park）を**全形式・handle ベース**に一般化。

```
read as csv with source                 # 各行/chunk に由来 Resource を付与
… |> name (source.uri) as path          # Resource 列から uri を取り出す（関数/フィールド）
```

- **chunk 単位**で由来 `Resource` を保持（行展開は `source.uri` 等のアクセサで列化）。**列を
  増やすのは opt-in**（`with source`）。`with filename` は `(source.uri)` の sugar alias。
- **並列との両立**: byte-range ワーカは「どの Resource のどの範囲か」を知っているので、由来は
  worker 起点で正しく付く（§27 slice 1 が serial に落ちた制約を、handle を chunk メタに持たせる
  ことで**並列でも**解消）。byte-identity を保つ（由来は位置依存メタ）。

## 28.7 出力（鏡像）

`save` を **encode → route → transport** に分解（§27.3/27.4 を一般化）:
- **route**: 動的出力名（テンプレート）・分割（`by key`）＝ discovery の逆。出力先 `Resource` を
  データから生成。
- **transport(write)**: file/POST/publish。サービス/分散の出力側。
- 決定的・byte-identity（分割キーは順序非依存・決定的）。
- **s4b 具象（#143 批准・統括裁定 2026-06-11）**：正準形 `save TEMPLATE [by KEY…] [as flat]`。
  テンプレートのプレースホルダ＝キー（§27.3 は §27.4 の退化形・テンプレート外 `by` キーはエラー・
  `{{}}` エスケープ）。プレーンパス＋`by`＝Hive `k=v/part.ext`（DuckDB 互換）・`as flat`＝`v1_v2.ext`。
  null キー＝`__HIVE_DEFAULT_PARTITION__`・キー値は `%` 込みパーセントエスケープ（**単射**）。
  **基数上限 Fatal なし＝書き切る**（silent fallback 禁止・資源圧は LRU/spill 等の工学で吸収・
  書込不能はパーティション単位 Recoverable で他継続）。IR＝`Route::Template{template, by, flat}`・
  runtime コア＝`rivus_runtime::route`（serial と並列 single-write merge が同一コア＝各ファイル
  byte-identical serial==parallel==chunk-size・パーティション内行順＝入力順・part 名固定
  `part.<codec拡張子>`）。**serial は bounded-memory streaming**（`RouteWriter`：LRU で開きファイル
  上限 `RIVUS_ROUTE_FD_BUDGET`（既定 512）・evict→reopen は append＝ヘッダ一度・JSON `[`/`]` を
  メタ追跡・buffered `write_routed` とバイト一致）。並列マージ経路は手元の merged chunks を
  `write_routed` で書く（既に在荷）。式プレースホルダ `{expr}` は s4c で実装（snippet を射影 1 項にラップして本体文法でパース＝
  文法の二重化なし・`Route::Template.exprs`・評価失敗は counted→null パーティション）。

## 28.8 IR への落とし方（typed-IR・既存ノードの再編・正しさ保存）

- 形式別 `Op::OpenCsv/OpenJsonl/OpenBinary` を、**`Op::Source { discovery, transport, codec,
  provenance }`**（合成可能な直交フィールド）へ段階的に置換。`Op::Sink` も対称に。
- **後方互換**: `open f.csv` / `readbin` 等は parser で「単一 Resource discovery ＋ File ＋
  拡張子 codec」に desugar（`to_source` は元の sugar 形に戻す＝可逆）。**v1 構文は壊さない**。
- **typed-IR（§00 0.12）**: `Resource` 値・codec が決める出力スキーマを IR 上で静的に解く。
- **byte-identity（§00 0.5/0.14）**: 各スライスで `optimizer_equiv` ＋ stress（serial==parallel
  ==chunk-size・null 込み）を緑に保つ。codec/transport の移設は**挙動不変**を機械で固定。

## 28.9 決定性・有界/非有界との関係（§00 0.13/0.14）

- **有界・決定的**: ファイル discovery（uri バイト順）＋ File transport ＋ codec ＝ 決定的 op
  集合の内側 → interpret==compile==distribute 契約に乗る。
- **非有界・非決定的**: `watch`/`subscribe` ＋ socket/http ＝ 到着順依存 → 契約の**外側**
  （§00 0.14）。背圧・窓（§00 0.13）で扱い、byte-identity 契約は要求しない（明示）。
- **ケイパビリティ（§00 0.15）**: 各 transport/discovery は付与された権限内でのみ動く
  （読める源・到達できる遠隔）。違反は拒否イベントで surface（never-silent）。

## 28.10 段階スライス（批准後）

各スライス＝1完結能力 PR・ローカルゲート緑・依存ゼロ・英日両ガイド・`to_source` round-trip ＋
`optimizer_equiv` 緑・byte-identity 保存。**移設（move-only）コミットと挙動コミットを分ける**。

| # | スライス | 主要素 | 正しさゲート |
|---|---|---|---|
| **1a** | **Codec/Transport トレイト抽出（純移設）** | 現 csv/jsonl/binary を `Decoder`/`Transport` トレイト裏へ**移すだけ**（§06 two-pass 推論は `infer`/`decode` 分離で等価移植）。`Op::Open*` の内部表現は据え置き、IR/構文/`to_source` **不変**。値型は導入しない | **全既存テスト緑・byte-identity 完全不変**（純移設・挙動ゼロ変更） |
| **1b** | **`Resource` 値型** | `DataType::Resource`/`Value::Resource`/`Column::Resource` 追加（=全 exhaustive match に新アーム・別 PR）。`Resource` リテラル/列・`to_source` 往復。決定性は §00 0.14（`uri`=契約内、`mtime`/`size`=契約外） | 新 match 緑・round-trip・既存挙動ゼロ回帰 |
| 2 | **Provenance `with source`**（全形式・並列対応） | chunk に由来 Resource メタ；`source.uri` アクセサ；§27 slice 1（park）の機構を cherry-pick・並列で由来保持 | null/byte-identity 込み・`with filename` sugar 等価 |
| 3 | **Discovery-as-flow（ローカル fs）** | `ls`/`glob`/再帰（std-only）→ `Stream<Resource>`；述語プッシュダウン；`read … with source` で多ファイル連結（§27.2 を吸収） | 決定的順序・continue-first・chunk-size 非依存 |
| 4 | **動的/分割出力（route）**（§27.3/27.4 を吸収） | `save` の encode→route→transport 分解・`by key`・テンプレート | 決定的・byte-identity・分割 |
| 5 | **非有界 transport の骨組み**（feature-gate） | `watch`/socket/http の Transport/Discovery 骨組み・背圧・窓の入口（§0.13）。サービス化の土台 | 有界部の byte-identity 不変・非有界は契約外明示 |

## 28.11 §27 の吸収マップ

| §27 項目 | §28 での落とし所 |
|---|---|
| 27.1 `filename` カラム | §28.6 Provenance `with source`（`filename`=sugar）。slice 2 |
| 27.2 再帰グロブ+フィルタ | §28.3 Discovery-as-flow（`ls`+述語プッシュダウン）。slice 3 |
| 27.3 動的出力名 / 27.4 分割出力 | §28.7 出力 route。slice 4 |
| 27.5 長パス | Transport(File) の実装詳細（slice 1/2 で吸収） |
| 27.6 Unicode/日本語パス&列名 | `Resource.uri`（UTF-8）＋識別子 Unicode（横断・別途） |

## MVP / 次 / 将来

- **MVP（本書批准の対象）**: 直交4層・`Resource` 値型・段階スライス（28.10）の設計確定。
- **次**: slice **1a**（Codec/Transport トレイト移設・byte-identity **完全不変**の純移設）→
  **1b**（`Resource` 値型）→ 2（provenance `with source`・並列対応）→ 3（discovery-as-flow・
  union-by-name）→ 4（route 出力）→ 5（非有界骨組み）。
- **将来**: ピラー2（コンパイル backend、typed-IR を単型化）・ピラー3（分散＝transport を
  ネットワークに）・ピラー4（制御プレーン）。本書の `Resource`/typed-IR/決定性境界がその前提。
