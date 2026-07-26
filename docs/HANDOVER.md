# セッション・ハンドオーバー（次セッションの担当者へ）

最終更新: **2026-07-26**（#257〜#265 アーク完了時点。過去の詳細は git 履歴の
本ファイル参照）。

> **2026-07-26 のアーク（全着地済み）**: **#257** design/38 flip（旧綴り 16 サイトの
> never-silent エラー化・recognize-but-refuse）／perf 3 連 = **#258**（JSONL scan_cell
> 残差 μopt・group −10%/ETL −19%）・**#259**（fused を join-less read→group へ拡張・
> plain group −26〜33%・dict 無しは負けるため runtime ガードつき）・**#260**（圧縮
> ストリーム JSONL の block walk 化・gz group −16%）／**#261** lead 窓関数（lag の
> 前方鏡像・遅延 emission）／**S1/S2 = #263/#264**（検証キャンペーン所見の封鎖 —
> **実装担当が起案**。役割原則「仕様確定済みの修正は執行側」が明文化された:
> #240 issuecomment-5083052015）／**#262+#265** design/43 rolling（メモ批准 →
> rolling_sum/avg/min/max 実装・f64 は毎行再計算の純関数方式）。タグは
> **v1.4.0-dev.9**（`6d36543` — 統括の cut が古い checkout に付いた経緯あり・
> 移動せず容認）と **v1.4.0-dev.10**（`e357410`）でキュー全消化。

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

**続くアーク（2026-07-25〜26）で着地した PR**: #250（HANDOVER 刷新）・#251〜#256
（前記）・**#257 flip 完結**（仕様正典 = issuecomment-5078743131・全 16 サイト
recognize-but-refuse・3 要素文言 pin）・#258/#259/#260（perf）・#261 lead・
#263/#264（S1 never-silent 封鎖＝数値比較の無音 false 修正・S2 テスト正直化 —
CI では環境理由 skip が fail）・#262/#265（design/43 rolling）。

**標準フィクスチャの注意（2026-07-23）**: scratchpad がコンテナ再生成で消失し、
10M×9 files 標準は文書仕様（BENCHMARKS「standard fixture」節・dirty mix 込み）から
**再生成**した。以後の A/B は同窓で自己一貫だが、**絶対値は旧台帳エントリと非比較**
（再生成 JSONL は旧 606MB よりリーン）。DuckDB/Polars 対照スクリプトも消失 —
次に対 DuckDB 比を出すときは再セットアップが要る。

## 3. 設計アークの現在地

- **design/41（深層融合 worker）**: Stage A・C 着地済み・B（mmap）は計測負けで破壊済み。
- **design/42（辞書レーン）**: **批准スコープ完全着地**（(a)(b)(c) × CSV+JSONL）。
  producer 契約 2 点（空辞書×非空 codes 構成上不可能・append 物質化前の fused 消費）維持。
- **design/38（構文簡素化）**: **P1〜P4 flip 済み**（#257 — 旧綴りは 3 要素を教える
  エラー・正典綴りのみ有効）。P5 は使用調査待ち（統括判断）。
- **design/43（rolling 窓集計）**: 批准済み・実装済み（#262/#265）。保留:
  `min_periods`・str min/max・`rolling_count`・`rolling_std`（f64 モーメント
  決定性 = design/37 圏の別メモが先）。
- **design/37／#45（正準縮約木）**: 方式(b) で着地。CanonTree（BLOCK=128）＋file-major
  spine。force-serial 時は plain-safe 集合→generic oracle（design/42 ガード保全）、
  f64 モーメント集合→同一機械 P=1（serial mirror）。**単一ファイル byte-range 経路は
  対象外のまま**（§37.5 プリパス＋carry = 将来スライス）。BLOCK 掃引（Q2）未実施。

## 4. 現在の実測プロファイル（2026-07-26・warm・再生成 10M 標準・4 コア箱）

検証キャンペーン⑤（@`520158a`・best wall / peak RSS・**DuckDB 対照なし**）:
CSV group **758ms/14.6MB**・JSONL group **714ms/9.9MB**・gz JSONL
**1004ms/12.2MB**・CSV ETL **629ms/12.5MB**・f64 moments **838ms/14.9MB**。
その後 #259/#260 で plain-group（cast 形状 −26〜33%）と gz JSONL（−16%）が
さらに短縮（BENCHMARKS の同窓 interleave が正）。JSONL per-file decode 中央値は
#258 後 **~112ms**（#255 前 222 → #255 後 170 → #258 後 112）。

- decode は 3 スライスで大きく回収済み。feed は id 直引き＋join-less fused で実質床。
- **計測の罠（今回実測）**: fixture 再生成直後や cache eviction 後の初回 WPROF は
  decode が 20〜30× に膨らむ（cold page cache）。**必ず warm 2 周目以降で測る**。
  箱ノイズは日内 ±40% 級 — 比較は必ず同窓 interleave。

## 5. 開いている判断（勝手に決めない）

1. **P5（制御プレーン verb の整理）** — 使用調査待ち・統括判断。
2. **design/40 Q1-Q4**（OTel T1 / QUIC B2）— 引き続き裁可待ち。
3. **#45 の将来スライス**: 単一ファイル byte-range の file-major 化（§37.5）・BLOCK 掃引（Q2）。
4. **`|#` の static/runtime 乖離 2 件（台帳報告済み・裁定待ち）**: decimal avg
   （schema_prop=F64 vs runtime=Decimal(scale+6)）・diff 非 datetime 分岐
   （schema_prop=src vs runtime=I64/Decimal 維持他 F64）。rolling は
   この乖離を持ち込まない設計にした（#265 裁可依頼参照）。
5. #229 Parquet の full 搭載可否・`unbounded` full 搭載 — 従来どおり保留。
6. **rolling 保留リスト**（design/43 §43.6 ③）: `rolling_std` は design/37 圏の
   決定性メモが先行条件。

## 6. 次のレバー候補（優先順・2026-07-24 実測に基づく）

1. **Track C 最終: resample/gap-fill（#62）** — 欠損時間バケットの行生成という
   新領域（行を「作る」最初の op）。design/44 メモ → 批准 → 実装の 2 段（先行研究が
   メモ起案予定）。
2. **DuckDB/Polars 対照の再セットアップ** — 検証キャンペーンで「対照なし」が
   続いている。比率主張の再開にはコンテナへの導入経路（proxy 制約）の解決が要る。
3. CSV decode 残差（薄い — parse_i64_fast 済み・辞書 intern +5-8ms/file の μopt 域）。
4. fused 適用面の続き（複数 join・数値 coalesce）。
5. f64 大窓 rolling の正準木窓版（design/43 §43.3 — **bench が要ると示すまで作らない**）。
6. #45 将来スライス（§37.5 file-major 単一ファイル・BLOCK 掃引）・#197 fmt pretty。

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
- **stale バイナリとの interleave**（#260 で実際に踏んだ）: 「main バイナリ」は
  **どの commit から build したか**を毎回確認する。平文 decode 中央値のような
  既知プロファイルと照合すると混入を検出できる（153-160 vs 110-116ms/file で発覚・
  全数取り直し）。branch 切替を挟む計測は特に危険。
- **`git checkout <file>` は未 commit の採用済み編集も巻き添えで消す**（負け案の
  破壊時に勝ち案まで消した実績 — #258 作業中）。負け案の revert は hunk 単位で。
