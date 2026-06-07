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
| **[Op::Source]** | 形式別3変種統一（move-only・**本PR・レビュー待ち**） | `Op::Source{discovery:Discovery::Fixed, transport:Transport::Local, codec:Csv/Jsonl/Binary, provenance}`。`OpenCsv/OpenJsonl/OpenBinary` を撤去。parser は `open`/`readcsv`/`readjson`/`readbin` を desugar、`to_source` は同一文字列を復元。optimizer pushdown/`dedup_sources`・runtime `build`/`plan_parallel_source`・source ゲートは codec/discovery 分岐へ。**挙動ゼロ・byte-identity 不変**（注意1＝再石化回避） |

ゲートは各 commit で全緑（fmt / clippy `--all-features -D warnings` 0 / 全テスト /
stress serial==parallel==chunk-size / optimizer_equiv / 依存ゼロ）。2-②・Op::Source とも
CLI で e2e 確認済（`open`/`readcsv`/`with source`/`with filename`/`explain` 往復）。

**リリース**：1a/1b/2-① 分の提案タグ **`v1.3.0-dev.6`** はカット待ち。2-② マージ後は
**`v1.3.0-dev.7`**、Op::Source マージ後は **`v1.3.0-dev.8`**（カットは統括判断：
`git tag v1.3.0-dev.N && git push origin v1.3.0-dev.N`）。

---

## 2. 次タスク＝slice 3（discovery-as-flow・ローカル fs）

slice 2（provenance）も \[Op::Source 統一\] も **done**（後者は本PR・レビュー待ち）。
次は §28.10 **slice 3**：`ls`/`glob`/再帰（std-only）→ `Stream<Resource>`、述語
プッシュダウン、`read … with source` で多ファイルを **union-by-name** 連結（§27.2 吸収）。
決定的順序（uri バイト昇順）・continue-first・chunk-size 非依存。

実装の足場（本セッションで整えた共有機構）:
- 新 `Op::Source{discovery, transport, codec, provenance}` に discovery 段が乗る。
  slice 3 は `Discovery` に `Glob`/`Ls`/再帰の変種を**足すだけ**（`Op` の再形成は不要）。
- provenance アクセサ（`eval.rs` の `resource_field`/`uri_scheme`／`Access::Source`）は
  **base=meta.source → base=Resource 列**へ一般化できる形にしてある（discovery の
  Resource 列にそのまま効かせる）。
- 🟡 slice 3 で「**`ls` の Resource 列名が `source`**」の解決（実列優先 → provenance
  別名）を確定（注意2 の一般化）。

---

## 3. tracked
- ✅ **`Op::Source` 統一（注意1）＝done**（本PR）。`Discovery`/`Transport`/`Codec` 直交
  4層を IR に導入。`Transport::Local` は枠の予約（slice 5 で http/socket）、`Discovery`
  は `Fixed` のみ（slice 3 で glob/ls）。
- 🟡 `dedup_sources` は現状 **CSV のみ**（path キー）を踏襲（jsonl/binary は非対象）。
  全 `Op::Source` への一般化は follow-up（挙動変更になるため別途）。

## 4. 以降のスライス順（§28.10）
2（provenance・**done**）→ \[Op::Source 統一・**done**\] → 3（discovery-as-flow・
**次**・union-by-name）→ 4（route 出力）→ 5（非有界骨組み・feature-gate）。

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
