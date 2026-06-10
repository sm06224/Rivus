# セッション・ハンドオーバー（次セッションの実装担当へ）

最終更新: 2026-06-07 ／ ブランチ `claude/eager-bohr-HsrXO`（指定）／ 版数
**`1.3.0-dev`**。dev→main は**常に唯一の開いた PR**・maintainer が squash/merge →
こちらは `git fetch origin main && git reset --hard origin/main` して継続。

> **現フォーカス＝§28 I/O サブストレート（ピラー1）の段階スライス。**
> design は `docs/design/00-north-star.md`（正典）/ `docs/design/28-io-substrate.md`
> （批准済）。レビュアー＝統括（人間）。各スライスは「**byte-identity 不変・
> to_source 可逆・依存ゼロ**」を実測で裏取りして承認 → maintainer merge。

---

## 0. まず読むもの
1. `CLAUDE.md`（運用契約・規律）— **拘束力あり**。特に「Tool & edit discipline」
   （依存編集を並列発行しない／小バッチ＋都度ゲート／ディスク信頼）。
2. `docs/design/00-north-star.md`・`docs/design/28-io-substrate.md`（§28.6/§28.8/§28.10）。
3. 本ファイル（直近の文脈・次タスクの実装契約）。

---

## 1. §28 進捗（landed＝main マージ済み／本PR＝レビュー待ち）

| slice | 内容 | 主な成果物 |
|---|---|---|
| **1a** | Codec/Transport トレイト抽出（純移設・挙動ゼロ） | `crates/rivus-runtime/src/transport.rs`（`Scheme`/`FileTransport`/`read_whole`/`open_compressed`）・`codec.rs`（`trait Decoder`）。全 reader を裏へ移設、重複分類を `Scheme` に集約 |
| **1b** | `Resource` 値型 ＋ `resource("uri")` リテラル | core `Resource{uri,size?,mtime?}`（**同一性は uri のみ**＝§0.14）・`Value/DataType/ColumnData::Resource`・`Column::resource`。parser `resource("uri")`→`Expr::Literal`、to_source 往復（uri のみ） |
| **2-①** | provenance 配線（挙動ゼロ） | IR `Provenance{Off,Source,Filename}` を `Op::OpenCsv/OpenBinary/OpenJsonl` に追加。parser `with source`/`with filename`（全形式・`with`未知=Err）、to_source 可逆。**runtime は `..` で無視＝byte-identity 完全不変** |
| **2-②a** | provenance 活性化（アクセサ＋付与） | core `ChunkMeta.source: Option<Resource>`（加算的）。ir `Access::Source`（field 名を焼き込まない汎用）・`Access::is_column()`・`Provenance::source(path)`・to_source `source.<field>`。parser `source.<field>`（`.field` が続く時だけ予約）。runtime eval `source.uri`/`source.scheme`＝`resource_field`（slice 3 の Resource 列と共有）・provenance 無し→null。source op が serial＋全並列ワーカで同一ハンドルを stamp＝**byte-identity（serial==parallel==chunk-size）**。optimizer の prefilter/projection pushdown は Access::Source を非列として除外 |
| **2-②b** | `with filename` 材化 ＋ ガイド | `with filename`＝`(source.uri) as filename`：source op が行末に `filename` 列（=path・Str）を材化。衝突時 `filename_r`（join 規則）。`with source` は handle のみ（列ゼロ）。英日ガイド（§3 Sources＋§6 アクセサ）。stress: 材化・衝突・並列 byte-identity |
| **Op::Source 統一** | 形式別3変種統一（move-only・#122 マージ済） | `Op::Source{discovery:Discovery::Fixed, transport:Transport::Local, codec:Csv/Jsonl/Binary, provenance}`。`OpenCsv/OpenJsonl/OpenBinary` を撤去。parser は `open`/`readcsv`/`readjson`/`readbin` を desugar、`to_source` は同一文字列を復元。**挙動ゼロ・byte-identity 不変**（注意1＝再石化回避） |
| **3a** | discovery-as-flow（`ls`・#123 マージ済） | `ls "glob"`(+alias `gci`/`dir`)＝`Op::Source{Discovery::Glob, Codec::Discover}`。std 自前 glob（`**`/`*`/`?`/`[…]`・symlink 非追従・uri 昇順・0件→warn）。**bare-columns** `{path:Resource, name:str, size:int, mtime:datetime}`（accessor 不採用＝可逆性確保）。述語の dotted `word.field`（`source.uri` 含む）は明示エラー（never-silent＋可逆性ガード）。size/mtime は §0.14 契約外 |
| **3b** | discovery 述語プッシュダウン（#124 マージ済） | optimizer `discovery_prefilter`：`ls` の単一 FilterProject 消費者から `name` の必須サブ文字列（`==`/`contains`/`starts_with`/`ends_with`/`like` 先頭）を抽出し `Codec::Discover{name_prefilter}` へ。enumeration walk が **stat 前**に basename で枝刈り（大ディレクトリで syscall 節約）。superset prune＋filter 権威＝結果不変（optimizer_equiv 固定）。size/mtime は stat 必須で利得なしのため非対象。決定性文言を精緻化 |
| **3c-①** | `resource(式)`（#125 マージ済） | `resource(EXPR)`：文字列リテラルは Resource リテラル（1b 維持）、それ以外は **cast `EXPR:resource`** へ desugar（マニフェスト列・計算パス）。parser `decl_type` に `resource` 追加（1b の `:resource` cast の parser 欠落も解消）。＋ canon メモ（§0.7/§28.3/ROADMAP） |
| **3c-②** | `read` 多ファイル union-by-name（**本PR**） | `read [as fmt] [with source/filename]`＝`Op::Read{fmt, provenance}`。Resource 列（既定 `path`、無ければ最初の Resource 型列、無ければ Fatal）を消費し全ファイルを open+decode。**union-by-name**（first-seen 列順・欠損→null）・**数値 widening**（int⊆float⊆decimal⊆str＝無切捨て）。開けない/壊れ→**quarantine**（Recoverable surface・スキップ・継続）。bad_rows も surface。provenance で各ファイル handle を行に（`source.uri`/`filename` 行ごと）。uri 昇順・chunk-size 非依存・**MVP=serial**・CSV+JSONL。`operators/read.rs` |

ゲートは各 commit で全緑（fmt / clippy `--all-features -D warnings` 0 / 全テスト /
stress serial==parallel==chunk-size / optimizer_equiv / 依存ゼロ）。各 slice は CLI で
e2e 確認済（`open`/`ls`/`gci`/`resource(式)`/`read`/`with source`/`with filename`/`explain` 往復）。

**Verb 命名ポリシー（恒久・§25.2a）**：Verb のみ・`Verb-Noun` 不採用・PowerShell 動詞/別名
語彙・短縮は alias（正名へ解決、to_source は正名）。

**リリース**：提案タグは 2-② → **`v1.3.0-dev.7`**、Op::Source → **`v1.3.0-dev.8`**、
3a → **`v1.3.0-dev.9`**、3b → **`v1.3.0-dev.10`**、3c-① → **`v1.3.0-dev.11`**、
3c-② → **`v1.3.0-dev.12`**（カットは統括判断）。

---

## 2. 次タスク＝slice 4（route 出力）＋ 3c フォローアップ

slice 3（discovery-as-flow）は **完了**：3a（`ls`）・3b（pushdown）・3c-①（`resource(式)`）・
3c-②（`read` union-by-name・本PR）。§28.10 の次は **slice 4（動的/分割出力 route・§28.7/§27.3-4）**：
`save` を encode→route→transport に分解、動的出力名・`by key` 分割、決定的・byte-identity。

**3c フォローアップ（モデルはこのまま乗る・rework なし）**：
- sqlite/http/s3 の Transport プラグイン（§28.4／slice 5）。`read` は scheme dispatch 前提。
- パス→パーティション列 materialize（Hive 部分読み・gap B）。
- binary codec で `read`、並列多ファイル＋bounded-memory streaming（現 MVP は serial・全 buffer）。
- `read` の per-column cast 失敗 surface（現状 widening で原則発生せず・reader の bad_rows は surface 済）。

実装の足場（本セッションで整えた共有機構）:
- `Op::Source{Discovery::Glob, Codec::Discover}`＝`ls`、`Op::Read{fmt, provenance}`＝`read`。
- discovery glob は `crates/rivus-runtime/src/discovery.rs`（std・`glob_paths`）。
- `read` は `crates/rivus-runtime/src/operators/read.rs`（per-file decode は `csv::CsvChunker::open`/
  `jsonl::JsonlChunker::open` を直接駆動＝Fatal-on-open を避け quarantine 化。widening は `widen()`）。

---

## 2b. §29 surface 収束（現セッション・PR #136・ブランチ `claude/design-29-surface-convergence`）

`docs/design/29-surface-convergence-and-union-views.md`（phase-0 → s1 実装済み）。

- **批准済（統括 2026-06-09）**：共用体サブフィールド参照＝**式文脈の `.` アクセサ**
  （`|> (complexId.cls) as cls`・`source.uri` と同一機構でゼロ lexer 変更・既に可逆）。裸 flow
  位置の `.` はパス字面と衝突するため従来どおり明示エラー（下記 tracked 🟡 と同根）。
- **s1 landed（本ブランチ）**：`|>` の **`:` 定義チェーン** `col [:alias] [:type]`
  （`parse_colon_chain`・既存 `Op::ProjectExpr` へ lower・IR/runtime/optimizer 変更ゼロ）。
  正規形＝チェーン（旧綴りは fmt で収束）。型語判定の単一真偽源＝`rivus_ir::is_type_word`
  （parser 型表とテスト同期）。衝突別名は `(col) as int` エスケープハッチ。
  **verb（rename/cast）は desugar しない**（in-place 全列保持＝semantics が別・§29.2 で確定）。
- **未批准（§29.5）**：2=オフセット単位/重なり・3=null/可変長・5=書式/ロケール・
  6=新演算子/regex・7=厳密度段階。**次＝s2（共用体ビュー・§28 binary 統合）は批准後**。

---

## 3. tracked
- ✅ **`Op::Source` 統一（注意1）＝done**（#122）。
- ✅ **3a の 🟡 列名問題は回避済**（discovery は bare 列）。
- 🟡 **handle field accessor は parens 内（computed column）限定**：flow-mode lexer が `a.b` を
  1 識別子に畳むため、述語の dotted `word.field` は明示エラー（never-silent）。
- 🟡 `dedup_sources` は現状 **CSV のみ**（path キー）。全 `Op::Source` 一般化は follow-up。
- 🟡 `read` MVP は **serial＋全ファイル buffer**。並列多ファイル＋bounded-memory は follow-up。

## 4. 以降のスライス順（§28.10）
2（provenance・**done**）→ \[Op::Source 統一・**done**\] → 3（discovery-as-flow・**done**：
3a/3b/3c-①/3c-②）→ **4（route 出力・次）** → 5（非有界骨組み・feature-gate）。

---

## 5. ローカルゲート（push 前に必須・数値で確認）
```sh
cargo fmt --all -- --check
RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features   # =0
cargo test --workspace            # 既存挙動ゼロ回帰
cargo test --workspace --all-features   # gzip/zstd オラクル・stress 含む
# byte-identity: tests/stress（serial==parallel==chunk-size・null・provenance 列）
# 依存ゼロ: cargo tree -p rivus-cli --edges normal  → rivus-* のみ
```
- 新 enum variant（`Value`/`DataType`/`ColumnData`/`Op` フィールド/`Access`）は
  **`cargo build --workspace --all-targets` の E0004/E0027/E0063 を潰し切る**（公開
  re-export の variant は dead-code 警告は出ない＝構築経路なしでも先行可）。
- force-push 不可。`reset --hard origin/main` 後の push は merge commit 経由で FF。
  上流 merge commit（committer `noreply@github.com`）は**amend しない**（公開済・乖離する）。

## 6. 環境メモ
- 4 vCPU。`gen` は6列固定（id,name,age,score,country,active）。
- 並列強制/抑止: `RIVUS_PARALLEL_MIN_BYTES=0` / `RIVUS_NO_PARALLEL=1`。
- ノード別 busy_ms: `rivus run f.riv --json 2>&1 | grep '"kind"'`。

---

## 旧文脈（§28 以前・参考）
SIMD-native parse（#71）・columnar（#40）・decimal/#41・日時レーン（doc23 §23.1）は
landed 済み。doc23 §23.2 list 集計 / §23.3 pivot は未着手バックログ。詳細は
`docs/BENCHMARKS.md` 末尾と git 履歴（〜#112）。
