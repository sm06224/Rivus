# セッション・ハンドオーバー（次セッションの実装担当へ）

最終更新: 2026-06-07 ／ ブランチ `claude/stoic-cannon-D51Yl`（指定）／ 版数
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

## 1. §28 で main にマージ済み（このセッション）

| slice | 内容 | 主な成果物 |
|---|---|---|
| **1a** | Codec/Transport トレイト抽出（純移設・挙動ゼロ） | `crates/rivus-runtime/src/transport.rs`（`Scheme`/`FileTransport`/`read_whole`/`open_compressed`）・`codec.rs`（`trait Decoder`）。全 reader を裏へ移設、重複分類を `Scheme` に集約 |
| **1b** | `Resource` 値型 ＋ `resource("uri")` リテラル | core `Resource{uri,size?,mtime?}`（**同一性は uri のみ**＝§0.14）・`Value/DataType/ColumnData::Resource`・`Column::resource`。parser `resource("uri")`→`Expr::Literal`、to_source 往復（uri のみ） |
| **2-①** | provenance 配線（挙動ゼロ） | IR `Provenance{Off,Source,Filename}` を `Op::OpenCsv/OpenBinary/OpenJsonl` に追加。parser `with source`/`with filename`（全形式・`with`未知=Err）、to_source 可逆。**runtime は `..` で無視＝byte-identity 完全不変** |

ゲートは各 commit で全緑（fmt / clippy `--all-features -D warnings` 0 / 全テスト /
stress serial==parallel==chunk-size / optimizer_equiv / 依存ゼロ）。

**リリース**：1a/1b/2-① マージ分の提案タグ **`v1.3.0-dev.6`** はカット待ち（カットは
統括判断：`git tag v1.3.0-dev.6 && git push origin v1.3.0-dev.6`）。

---

## 2. 次タスク＝slice 2-②（provenance 活性化）— **統括批准済の実装契約**

`with source`/`with filename` を実際に効かせる。2-① で配線済（IR フラグ・parser・
to_source）なので、**runtime 活性化 ＋ `source.uri` アクセサ**を入れる。

### 2-②a（先）：アクセサ＋付与
- **core**: `ChunkMeta.source: Option<Resource>`（既定 None・加算的）。`Chunk.meta` は
  pub なので source operator が `c.meta.source = …` で設定。
- **アクセサ（批准済表現）**: `source.uri` を **Resource 値への汎用フィールドアクセサ**
  として実装する（**`.uri` を固定で焼き込まない**＝統括の強い要望・rework 回避）。
  - `source` → `chunk.meta.source` の Resource 値に解決。
  - `.uri`/`.scheme` → Resource 値への汎用アクセサ。**slice 3（`ls` の Resource 列）と
    同一機構を共有**できる形にする（base=meta.source を base=Resource 列に一般化可能に）。
  - **lexer 実測（重要）**: `word_part` は depth-aware（`crates/rivus-parser/src/lexer.rs:
    425`）。**括弧内（式モード）では識別子は `[A-Za-z0-9_]+`＝`.` を含まない**ので、
    `(source.uri)` は `Word("source") Dot Word("uri")` にトークン化される。よって
    parser は bare `source` の `.field` 末尾を明示パースする（`$_` の `parse_field_tail`
    が手本）。表現は `Expr::Field{name, access: Access::Source}`（新 `Access` 1モード＝
    match 波及最小）か、`Resource` 値への汎用アクセサ Expr。統括は前者の `Access::Source`
    を批准済だが、**`.uri`/`.scheme` を name に焼き込まず汎用化**すること。
  - **§0.14**: uri/scheme＝契約内（決定的）、mtime/size＝契約外。アクセサに「契約外
    フィールドは決定的集合の外」を内蔵（2-② は uri のみで可）。
  - provenance 無し chunk → **null 列**（continue-first）。
- **付与（最重要・byte-identity）**: `provenance != Off` 時、`Resource::new(path)` を
  各 `chunk.meta.source` に設定。**serial と byte-range 並列ワーカを必ず同時に**付与する
  （片方だけだと serial≠parallel で byte-identity が壊れる＝結合）。並列ワーカは各自 path
  から同一 Resource を作る＝**パーティション非依存で一致**。
  - 配線箇所: IR の `provenance` を `operators/mod.rs build` → `SourceCsv/SourceJsonl/
    SourceBinary::new`、並列は `from_stream/from_chunker` ＋ `csv_range_source/
    jsonl_range_source/bin_range_source`（`operators/mod.rs`・`operators/source.rs`、
    plan は `engine.rs plan_parallel_source`）へ通す。各 range source は path を持つので
    そこで `Resource::new(path)` を作って渡す。
- **ゲート**: e2e（`open f with source |> (source.uri) as p` が path 列）＋ **stress で
  serial==parallel==chunk-size の由来列一致**（`tests/stress/` に追加）＋ 既存全テスト
  不変 ＋ 依存ゼロ。

### 2-②b（後）：sugar ＋ 衝突 ＋ ガイド
- `with filename` ＝ `(source.uri) as filename` の sugar（材化）。
- **衝突規則**: 材化列 `filename` が既存なら `filename_r`（§27 流儀）。アクセサ
  `source.uri` は `source` 予約＋`.uri` 末尾で実列 `source` と判別。
- **英日両ガイド**（`docs/GUIDE.md` / `GUIDE.ja.md` §6 付近）に provenance を追記。

---

## 3. tracked（後続スライスの前に必ず）
- 🟠 **slice 3 の前に `Op::Source` 統一**（§28.8）：`provenance` を3つの形式別変種に
  足したが、discovery（slice 3）/route（slice 4）も同じ3変種に足すと**形式中心 I/O 結合
  の再石化**。`Op::Source{discovery, transport, codec, provenance}` への統一を**専用
  move-only スライス**として slice 3 の前に入れる（統括指示・注意1）。
- 🟡 slice 3 で「**`ls` の Resource 列名が `source`**」の解決（実列優先 → provenance 別名）
  を確定（注意2 の一般化）。

## 4. 以降のスライス順（§28.10）
2（provenance・**実装中**）→ \[Op::Source 統一\] → 3（discovery-as-flow・union-by-name）
→ 4（route 出力）→ 5（非有界骨組み・feature-gate）。

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
