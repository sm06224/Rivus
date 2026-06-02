# セッション・ハンドオーバー（次セッションの実装担当へ）

最終更新: 2026-06-02（夜）／ ブランチ `claude/stream-native-runtime-design-tReOj`
／ 版数 **`1.3.0-dev`**（次の開発版）。dev→main は**常に唯一の開いた PR**（直近は
#75）。次セッションは**レビュアーの確認結果から**始まる。

> **2026-06-02（夜）追記 — 正はここ＋BENCHMARKS/CHANGELOG**:
> - **#71 SIMD-native parse を 3 段 landed**（いずれも byte-identical・依存ゼロ・
>   等価性テスト先行）: ①AVX2 構造文字スキャン（32B/step、ランタイム検出、SWAR
>   フォールバック、1.72×）→②SWAR 整数 parse（exact i64、1.11–2.16×）→③SWAR 小数
>   parse（exact i128、共有 `rivus_core::numparse`、1.49–1.97×）。
> - **#40 columnar を再活性化し初レバー landed**: 述語カーネルの実測支配項＝
>   selection-vector 構築を**分岐レス**化（50% 選択率で 7.31×、選択率不問で一定）。
>   compare 自体は帯域律速で SIMD 無勝（#39 で計測済み）。
> - **次の #40 レバー（本命）**: 生存行 materialize の `Column::gather` 分岐レス/
>   SIMD 化（SIMD-native 経路＝#71 後で**計測してから**採否）。
> - **リリースは私（エージェント）がコントロール**（`docs/RELEASE.md`、maintainer
>   委任 2026-06-02）。版＝タグ。開発版は SemVer プレリリース `vX.Y.Z-dev.N`
>   （`release.yml` が `-` 付きタグを `--prerelease` 公開）。**既存の不整合リリース
>   `v1.2.0dev`（不正タグ・prerelease 誤フラグ・stable v1.2.0 より後に公開）は要削除
>   — 本セッションの MCP に release 削除手段が無く未実施、maintainer 側 UI 削除待ち**。
> - 旧 §23.1 日時レーン、decimal/#41 解禁は landed 済み。doc23 §23.2 list集計 /
>   §23.3 pivot は未着手バックログ。下記 4–7 節は旧文脈、状況は本追記が正。

---

## 0. まず読むもの

1. `CLAUDE.md`（運用契約・規律）— **拘束力あり**。特に「Tool & edit discipline」。
2. `docs/design/README.md`（8つの物理法則）と該当設計ノート。
3. 本ファイル（直近の文脈・未完タスク）。

---

## 1. このセッションで完了・push したもの

| commit | 内容 | 状態 |
|---|---|---|
| `c57c54c` | #39 述語カーネルを分岐なしバイトマスク化（~5%）。**手書きAVX2は計測で無効果と判明し不採用** | ✅ landed |
| `f477825` | 〃 ROADMAP 反映 | ✅ |
| `08abe66` | 設計ノート21（exact decimal）・22（GPU backend） | ✅ docs |
| `c1f6839` | 設計ノート23（datetime・list集計・pivot） | ✅ docs |
| `3997109` | **decimal型のコア実装**（`Value::Dec`/`Column::Dec`/`DataType::Decimal`、i128固定小数点、厳密加算）。既存挙動ゼロ回帰 | ✅ landed |
| `29a310c` | 1GB速度プロファイル＋新要件をROADMAPに記録 | ✅ docs |
| `e757168` | **BOM対応**（先頭UTF-8 BOMが第1列名を汚すバグ修正、全経路、テスト付き） | ✅ landed |

ゲートは各commitで全green（fmt / clippy default+all-features 0警告 / 15スイート /
deny / gitleaks / 依存ゼロ）。

---

## 2. レビュアーへの未解決の問い（#41）

GitHub Issue #41 にコメント済み（要返答）:
**並列 group-by を byte-identical にできるか** を試作で検証した結果、

- ✅ 一致: `min/max/count/count_distinct/first/last/percentile`（順序・結合則非依存）
- ❌ 一致しない: **f64 の `sum/avg/std`**（浮動小数加算が非結合 → 並列で最終ULPがズレる）
- ✅ ただし**整数列・decimal型なら厳密一致**（統括の指摘で確認済み。`docs/BENCHMARKS.md`
  と検証コードに記録）

→ 統括方針: **decimal型（doc 21）を入れて「速度を犠牲にした正確性」をオプトインで
担保**する。f64 の sum/avg/std だけ serial 維持 or `--exact` で decimal に倒す。
この設計判断の最終承認待ち。**試作コードは堀（結果不変）を守るため破棄済み**、
ブランチはクリーン。

---

## 3. 計測で確定した「速度の真相」（最重要・次の本命）

統括の 1GB/30M行 ≈ 22秒（DuckDB 10秒）問題を実測（`docs/BENCHMARKS.md` 末尾に詳細）:

| 処理 | 直列 busy_ms |
|---|---:|
| **open（CSVパース）** | **12,591** ← 支配的 |
| save（書き出し） | 6,897 |
| filter | 429（#39で既に安い） |

- **型を明示しても 12.5秒で不変** → 二度読み（推論）が原因では**ない**。
  **フィールド分割＋数値parse そのもの**が重い。
- 既定の並列で 16.9秒 → 6.8秒。DuckDB との差は**パース＋書き出しのスループット**。
- **次の本命レバー**: SIMD デリミタ走査（`,`/`\n` を `core::arch`、依存ゼロ）＋
  高速 int/float parse。次点で buffered 出力。**転送/実装の前後で必ず計測**。

---

## 4. 設計ノート（実装の青写真。すべて push 済み）

| doc | 内容 | 実装状況 |
|---|---|---|
| 21 exact-decimal | i128 固定小数点。`--exact`/`:decimal[(n)]`。avg/std は高精度で割って round-half-even | **コア型 landed**。リーダー/集計/並列は未着手 |
| 22 gpu-backend | feature-gate任意・CPU fallback・既定依存ゼロ。`--accel`。転送込みで測ってから採用 | 設計のみ |
| 23 datetime-and-reshape | 日時レーン（epoch整数、`yyMMddhhmmss`）/ list・set・join集計（配列化）/ pivot・unpivot | **日時レーン §23.1 landed**（コア型/リーダー/比較/関数/並列等価、`unit=Sec`・naive UTC）。list集計・pivot は未着手 |

---

## 5. 未着手タスク（ROADMAP に全記録済み・優先度順の私見）

1. **SIMD CSV スキャナ**（速度の本命。3節の計測が指す）— ROADMAP §E
2. **decimal リーダー対応**（`(price:decimal)`/`--exact`）→ **並列集計#41を解禁** — §A, §G, doc21 §21.8
3. ~~**日時レーンのリーダー対応**~~ ✅ **landed**（`(ts:datetime("fmt"))`、比較・
   `year/month/day/hour/minute/second/trunc/format/diff`、直列＋並列、等価テスト）。
   残: `--dates` 自動推論フラグ、sub-second `unit`、`tz`、専用ベンチ — doc23 §23.1
4. **list集計 → pivot/unpivot** — §D, doc23（**次の本命**：日時レーンが行/列キーに乗る）
5. **構文**（後方互換不要）: 任意の先頭`|`／フロー接頭辞`@Label`／**列生成+cast+rename を同一`|>`ブロックで** — §C
6. **書き出し高速化**（buffered formatting）— §E
7. GPU backend 骨組み — §F, doc22

---

## 6. 地雷・規律（CLAUDE.md と重複するが特に重要）

- **ツール乱発でセッションを壊した実績あり**。依存のある編集（Read→Edit→build,
  commit→push）を**並列発行しない**。1論理ステップ/ターン。出力が乱れたら実ファイルを
  Read で確認（記憶を信じない）。
- **新 enum variant（Column/Value/DataType）は exhaustive match を全部割る**。
  decimal で約8箇所直した。次に variant を足すなら `cargo build --workspace` の
  E0004 を潰し切る。
- **「速いは計測なしに主張しない」**。#39 の AVX2 はこれで不採用にした。各 perf PR は
  `docs/BENCHMARKS.md` に before/after。
- **push前に数値ゲート**: clippy警告=0, FAILED=0, 依存ゼロ（`cargo tree -p rivus-cli
  --edges normal` が rivus-* のみ）。
- commit メッセージは**ディスクにある事実だけ**（`git show HEAD:path` で確認できること）。
- force-push 不可。壊れたら上に積んで fast-forward。

---

## 7. 環境メモ

- 4 vCPU。`gen` は6列固定スキーマ（id,name,age,score,country,active）。
- 大ファイル: `./target/release/rivus gen clean --rows N --seed S > f.csv`
  （30M行 ≈ 1.13GB）。
- 並列強制/抑止: `RIVUS_PARALLEL_MIN_BYTES=0` / `RIVUS_NO_PARALLEL=1`。
  CPU/RAM上書き: `RIVUS_CPUS` / `RIVUS_RAM_BYTES`。
- ノード別 busy_ms: `rivus run f.riv --json 2>&1 | grep '"kind"'`。
