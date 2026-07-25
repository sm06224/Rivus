# セッション・ハンドオーバー（次セッションの担当者へ）

最終更新: **2026-07-25**（#240 承認済みキュー全消化: design/42 全段（CSV+JSONL）・
decode 列プルーニング・design/38 P1〜P4 移行リリース＋asof チェーン修正・#45 正準縮約木。
過去の詳細は git 履歴の本ファイル参照）。

> **2026-07-25 追記**: その後 **#251**（`.claude/` 運用資産）と **#253**（9-PR アーク後の
> 全体監査 hardening — 確定バグ 6 件封鎖・stress の env-race 実体化・重複一本化・
> コメント/CHANGELOG/design 台帳/GUIDE の現実同期。監査の全結果と却下/延期リストは
> PR #253 本文）が着地。**#252**（design/38 readbin 吸収 `open … as bin`・条件 8 点＋
> codec×オプション整合検査）は裁可待ちで open — 着地後は GUIDE 本文例の旧綴り
> 全面書換えが follow-up の適地（#253 で移行表のみ追加済み）。CLAUDE.md の乖離 3 点
> （dev ブランチ節・gate バイナリの CI 委譲・#41 旧記述）は #253 本文で maintainer へ
> 報告済み・未編集。

---

## 0. まず読むもの

1. `CLAUDE.md`（運用契約・ゲート・ツール規律）— **拘束力あり**。特に「依存する tool 呼び出しを
   並列発行しない／小バッチ＋都度ゲート／ディスク信頼」。
2. 本ファイル（現在地・運用体制・開いている判断）。
3. `docs/BENCHMARKS.md`（計測済み事実の台帳 — 「速い」はここに数字がある時だけ）・
   `docs/SUPPLY-CHAIN.md`（依存の審査台帳）。
4. issue **#240**（統合トラッカー v2 — #180 後継）— 裁可・GO・着地記録・申し送りの
   一元スレッド。冒頭の体制・ゲート・フロー・投稿規律が正典。

## 1. 運用体制（2026-07 現在）

- **役割分担**：統括（人間・最終決定）／レビュー兼統括指揮担当（#240 で GO）／実装主担当
  （着地）／先行研究員（本セッション群）。**自己マージは誰もしない。**
- **ブランチ運用（研究員）**：PR ごとに `claude/<topic>` を **origin/main 基点**で切る。
  着地後は fetch して次ブランチを新 main から。force-push 不可（recover forward —
  ただし指揮側が rebase を force-with-lease した実績あり: その場合は差分検証して追随）。
- **裁可フロー**：PR 作成 → **#240** に裁可依頼 1 コメント（実測・破壊的変更・ゲート数値）→
  指揮の独立 gate → GO → 実装担当が着地。**タグ提案は不要**（cut 保留・指揮管理）。
  1 イベント 1 コメント。セッション URL・モデル ID は GitHub に書かない。
- **ゲート（push 前・毎回・数値で確認）**：fmt --check clean／clippy default **と**
  `--all-features -D warnings` = 0／test 両 feature セット 0 failed／依存樹は
  policy v2「documented, not zero」（SUPPLY-CHAIN.md 台帳の 8 crate）。
  **注意: コンテナ再生成で cargo-deny / gitleaks バイナリが消えることがあり、
  proxy はリポ外 GitHub リリース DL を遮断する** — 依存変更ゼロの PR は CI の
  deny gate でカバーと明記して進めてよい（実績 2 回）。
- **GitHub API は希少資源**：CI をポーリングしない（webhook 購読）、コメントは束ねて 1 回。

## 2. main の現在地（#249 着地後・直近指揮ゲート 524/0・555/0・clippy 0/0）

**本アーク（2026-07-20〜24）で着地した PR**（すべて #240 に裁可記録あり）:

| PR | 内容 |
|---|---|
| #241 | design/42 (a): `ColumnData::StrDict`＋観測等価の全面配線＋property test 4 本 |
| #242 | design/42 (b)+(c) CSV: プラン連動辞書化・escape hatch・WPROF `dict=`／fused 整数 id 直引き（group slot memo＋join probe memo）— CSV group median −10% |
| #243 | decode 列プルーニング（対称方式・契約変更 CHANGELOG/design13 明記・explain surface）— ETL −5〜7% |
| #244 | design/38 P1+P2 移行リリース（別名族・project/filter 二重綴り→fmt 自動移行）＋codec round-trip 実バグ修正 |
| #245 | P3: over 窓関数統一（session/lag/diff/pct_change の `|>` item 化＋`|> *`） |
| #246 | P4: `&asof` 正則化（`A &asof B on k by ts [within]`） |
| #247 | asof チェーン to_source 修正（write_chain AsofJoin fan-in head — flip ブロッカー 1 号解消） |
| #248 | design/42 JSONL 側（批准スコープ完了）— JSONL group median −4〜5% |
| #249 | **#45 正準縮約木**（方式(b)・file-major 正準）— f64 sum/avg/std が並列化: f64 集計フロー wall 2.2〜3.4×・RSS 53× 改善 |

**エラー化リリース（P1〜P4 の旧綴り一括 flip）の前提**: 残るは `readbin`
（`open … as bin` 文法）の裁可のみ＝**統括専権**。asof チェーン描画は #247 で解消済み。

**標準フィクスチャの注意（2026-07-23）**: scratchpad がコンテナ再生成で消失し、
10M×9 files 標準は文書仕様（BENCHMARKS「standard fixture」節・dirty mix 込み）から
**再生成**した。以後の A/B は同窓で自己一貫だが、**絶対値は旧台帳エントリと非比較**
（再生成 JSONL は旧 606MB よりリーン）。DuckDB/Polars 対照スクリプトも消失 —
次に対 DuckDB 比を出すときは再セットアップが要る。

## 3. 設計アークの現在地

- **design/41（深層融合 worker）**: Stage A・C 着地済み・B（mmap）は計測負けで破壊済み。
- **design/42（辞書レーン）**: **批准スコープ完全着地**（(a)(b)(c) × CSV+JSONL）。
  producer 契約 2 点（空辞書×非空 codes 構成上不可能・append 物質化前の fused 消費）維持。
- **design/38（構文簡素化）**: P1〜P4 移行リリース着地（旧綴りは parse 可＋fmt 正典化、
  次リリースでエラー化）。P5 は使用調査待ち（統括判断）。
- **design/37／#45（正準縮約木）**: 方式(b) で着地。CanonTree（BLOCK=128）＋file-major
  spine。force-serial 時は plain-safe 集合→generic oracle（design/42 ガード保全）、
  f64 モーメント集合→同一機械 P=1（serial mirror）。**単一ファイル byte-range 経路は
  対象外のまま**（§37.5 プリパス＋carry = 将来スライス）。BLOCK 掃引（Q2）未実施。

## 4. 現在の実測プロファイル（2026-07-24・warm・再生成 10M 標準・4 コア箱）

| 形状 | wall | RSS | worker 内訳（/file） |
|---|---|---|---|
| CSV group | ~420ms | 10.1MB | decode 63-78ms・feed 36-44ms（idloop 発動 ~99.5%） |
| JSONL group | ~683ms | 10.1MB | **decode 171-174ms**・feed ~43ms |
| f64 集計（cast float sum/avg/std） | 675-718ms | 12.6MB | （#249 前は 1.5-2.5s / 670MB） |

- **decode が feed の 2〜4 倍 = 次のレバーは decode 側**。feed は id 直引きで実質床。
- **計測の罠（今回実測）**: fixture 再生成直後や cache eviction 後の初回 WPROF は
  decode が 20〜30× に膨らむ（cold page cache）。**必ず warm 2 周目以降で測る**。
  箱ノイズは日内 ±40% 級 — 比較は必ず同窓 interleave。

## 5. 開いている判断（勝手に決めない）

1. **readbin 文法（`open … as bin`）** — エラー化 flip の最後の前提・統括専権。
2. **P5（制御プレーン verb の整理）** — 使用調査待ち・統括判断。
3. **design/40 Q1-Q4**（OTel T1 / QUIC B2）— 引き続き裁可待ち。
4. **#45 の将来スライス**: 単一ファイル byte-range の file-major 化（§37.5）・BLOCK 掃引（Q2）。
5. #229 Parquet の full 搭載可否・`unbounded` full 搭載 — 従来どおり保留。

## 6. 次のレバー候補（優先順・2026-07-24 実測に基づく）

1. **JSONL decode（171ms/file — 最大単一レバー）**: scan_row_fast のテンプレート一致後も
   全バイト走査＋数値 parse が残る。スキャナ内 SWAR／値スパンの遅延 parse が候補。
2. **CSV decode 残差（63-78ms/file）**: field parse の μopt・辞書 intern コスト
   （+5-8ms/file — dict 列の probe 頻度削減や前行 memo は fixture 次第）。
3. fused 対応集合の拡張（複数 join・数値 coalesce — 適用面を広げる）。
4. 圧縮標準（csv.gz/jsonl.gz）の decode 側（Stage C 非対象だった領域）。
5. Track C 残り: resample/gap-fill（#62 agg 側）・rolling（#63）・lead（#65 follow-up）。
6. エラー化リリース本体（readbin 裁可後・fmt 移行網羅テストは P1〜P4 で pin 済み）。

## 7. 落とし穴（実際に踏んだもの）

- **依存する tool 呼び出しを並列発行しない**（CLAUDE.md 規律 — 破ると編集消失が起きる。実績あり）。
- **python heredoc の複数置換スクリプトは後段 assert 失敗で全置換が消える** — 1 スクリプト
  1 論理パッチ、書込後に grep 検証（このセッションで 2 回踏んだ）。
- **コンテナ再生成**で scratchpad（fixture・対照バイナリ・計測スクリプト）と
  cargo-deny/gitleaks が消える。fixture は文書仕様から再生成可（gen 手順は BENCHMARKS の
  fixture 節）。ツールはリポ外 DL が proxy 遮断される — CI 委任を明記して進む。
- **cold cache の初回プロファイルは decode を 20〜30× 過大評価**（§4）。
- ゲートスクリプトの多重起動（同一ログの取り合い）に注意。
- `fill` は `fill <col> <method>`（列が先）。sub-second を含む duration リテラルは文字列。
- fmt の canonical は `$_.col` 展開（#197 の pretty 化提案は未着手）。
- 出力ファイル名が入力 glob に一致するテスト fixture（`p*.csv` と `par.csv`）は
  2 回目の実行で自分の出力を読む — 命名を分ける（実績あり）。
