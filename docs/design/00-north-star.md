# 00. North Star — Rivus のビジョンと根底アーキテクチャ

> 統括ビジョン（2026-06-06）。Rivus が最終的に何であるかの**正典**。個別 design doc(§01..)
> はこれから派生する。**設計先行・批准必須・自己マージ禁止。**
>
> Rivus は「クエリ言語」ではなく **データフロー特化の言語＋実行/可視化/制御の統合基盤**。
> 既存の正しさ機械（byte-identity・continue-first・never-silent・IR 可逆・null モデル）は
> **保存して載せ替える**。big-bang 全書換は禁止、段階で進める。

## 0.1 テーゼ

**IR(PlanGraph) を唯一の通貨とし、データプレーンも制御プレーンも Rivus のフロー。実行は
「解釈 or コンパイル」でバイト同一。エッジ（源/沈/遠隔リンク）は同一の直交基盤。ゆえに
可視化・制御・実行が一つに融ける。**

## 0.2 Rivus の5つの姿＝一つの芯

1. **シェル**（nushell 的・オブジェクト/カラムナーパイプ・その場で動く）
2. **アドホック ETL/分析**（awk/jq/duckdb 的ワンライナー）
3. **サービス**（通信層に I/O を差し替えると常駐サービス化）
4. **コンパイル済み専用 ETL**（同一 Rivus コードを LLVM で完全最適化した実行バイナリ）
5. **オーケストレーション＋制御プレーン**（遠隔/同一機を跨ぎフロー接続。可視化・全体最適・
   調速・微分かつ無停止のスケール・部分更新）

これらは別機能でなく**同じ IR の“縁の差し替え”**で出る。

## 0.3 5つの軸

| 軸 | 役割 |
|---|---|
| ① 表層 Frontend | シェル/ワンライナー/フルプログラム → 全部 IR に落ちる |
| ② エッジ Source/Sink 基盤 | Discovery→Transport→Codec→Provenance（直交）。Transport を通信に=サービス化。Discovery=自分で探す |
| ③ 実行モード | 解釈（即時）/ コンパイル（LLVM 完全最適化バイナリ）— 同一 IR の二経路 |
| ④ トポロジ | フロー間リンク=ネットワーク transport の源/沈。遠隔/同一機でフロー接続=オーケストレーション |
| ⑤ 制御プレーン | フローを制御するフロー。テレメトリ→制御ループ。p2p シグナリング |

## 0.4 唯一の通貨：IR

全表層 → IR、IR ↔ Rivus ソース（可逆）、IR を解釈 or コンパイル。ソース/沈/遠隔リンク/制御は
特別な組込みでなく、IR 上の合成可能な段。

## 0.5 不変条件（既存資産＝linchpin。保存して載せ替える）

- **byte-identity（serial==parallel==chunk-size）→ 解釈==コンパイル==分散 の契約**。これが
  無ければ最適化も分散も信用できない。ビジョンの土台。
- **IR 可逆**（IR↔source）＝アドホック/コンパイル/転送で同じコード。
- **continue-first / never-silent / error-as-stream** ＝ サービス・分散の障害モデル。
- **chunk-native columnar** ＝ 高速データプレーン。
- **テレメトリ流（`--json`/`--serve`）** ＝ 制御プレーンの胚。
- **zero-dep core / feature-gate** ＝ 移植性。

## 0.6 直交 I/O 基盤（②の詳細）

- **Discovery**: handle のストリームを産む（`ls`/`gci -re`・glob・`list(s3)`・`watch`・
  `subscribe`）。探索＝フロー、結果を普通の述語で絞る（name/size/mtime…）。**探索述語は
  プッシュダウン**で枝刈り。
- **Transport**: handle→バイト（file/mmap/http/socket/stdin）。
- **Codec**: バイト→chunk(+schema)（csv/tsv/json/jsonl/binary/parquet…）。①②と直交＝全形式一律。
- **Provenance**: 各 chunk/行が handle を持つ（`with source`＝`filename` の一般化、パス→列復元）。
- **handle は第一級の値型 `Resource`**（パス/URL/offset/mtime）。計算・格納・関数に渡せる。
- 出力は鏡像：encode→route（動的/分割名=探索の逆）→transport（write/POST/publish）。

## 0.7 制御プレーン＝フローを制御するフロー（⑤の詳細）

テレメトリ流入 → 制御フロー → 走行フローを変異、の閉ループ。p2p シグナリング。統括制御で
全体最適・調速（背圧の制御信号化）・微分スケール（変化率でスケール）・無停止の部分更新
（subgraph ホットスワップ）。制御プレーン自体が Rivus フロー。

## 0.8 破壊と再建の原則

壊す＝ファイル中心 I/O 結合（`OpenCsv`/`OpenJsonl` 等の形式別バリアント）。保存＝0.5 の不変
条件。big-bang 禁止。各ピラーを design-doc→批准→段階で載せ替え、全段で byte-identity/
continue-first/zero-dep を維持。

## 0.9 派生ピラー（各 design-doc-first・批准制）

1. **I/O サブストレート**（0.6）— §27 を吸収・一般化（形式非依存・handle 値型・
   discovery-as-flow）
2. **IR 通貨＋コンパイル backend**（Cranelift→LLVM）— 解釈==コンパイル byte-identity をゲート
3. **分散**：フロー間リンク=ネットワーク transport（順序/背圧/byte-identity を跨いで保つ）
4. **制御プレーン**：テレメトリ⇄制御フロー・p2p・無停止更新/スケール
5. **表層統一**：シェル/ワンライナー/言語の単一フロントエンド

## 0.10 最初に決める杭（ここを練る）

1. handle/Chunk/Stream を第一級の値型にする度合い
2. 解釈/コンパイルの byte-identity 契約（#41 f64 非結合をコンパイル側でどう守るか）
3. テレメトリ⇄制御を一級フローにする境界
4. 分散の最小核（フロー間リンクの還元しきり）
5. 段階戦略（0.11）

## 0.11 段階戦略（big-bang を避ける）

- **Phase 0**: 本 North Star 批准。
- **Phase 1**: I/O サブストレート再建（ピラー1）。既存の正しさを保持して I/O だけ載せ替え。
  §27 はこの一部に再編。形式非依存・handle 値型・discovery-as-flow。
- **Phase 2**: コンパイル backend（ピラー2）＋「解釈==コンパイル」byte-identity ゲート。
- **Phase 3**: 分散リンク（ピラー3）。
- **Phase 4**: 制御プレーン（ピラー4）。
- 表層統一（ピラー5）は各 phase に併走。

各 phase = design doc → 批准 → 1完結スライス、ローカルゲート緑・依存ゼロ・英日両ガイド。

---

## 付記：現状資産との対応（この正典が「保存する」もの）

本 North Star は更地からの再設計ではなく、既に landed した正しさ機械の**上に**ビジョンを
据える。下表は 0.5 の不変条件が現リポジトリのどこに実在するか（再建時に壊さない基準）。

| 不変条件 | 現在の実体 |
|---|---|
| byte-identity | `tests/stress/byte_identity.rs`・`tests/stress/null.rs`（serial==parallel==chunk-size、null 込み）・`optimizer_equiv` |
| IR 可逆 | `rivus_ir::PlanGraph::to_source`（§04・round-trip テスト） |
| continue-first / never-silent | `rivus_core` error stream・`Severity`・`|!` validator（§13/§24） |
| null モデル | §26（validity bitmap・null/empty/0 区別・DuckDB 件数パリティ #110） |
| chunk-native columnar | `rivus_core::{Chunk, Column, ColumnData}`（§03） |
| テレメトリ流 | `--json`/`--tui`/`--serve`（§14・§19） |
| zero-dep core | 既定ビルド rivus-* のみ（`cargo deny`・feature-gate） |

§27（filesystem-io）は本ビジョンでは**ピラー1（I/O サブストレート）の一部**に再編される
（形式非依存 codec・handle 値型・discovery-as-flow への一般化）。§27 のスライス2以降は
Phase 1 の design doc 批准後に進める（slice 1 = `filename` 列は `with source` の特殊形として
残る）。
