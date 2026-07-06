# セッション・ハンドオーバー（次セッションの担当者へ）

最終更新: **2026-07-06**（先行研究セッションによる全面刷新 — 旧版は §28 時代の文脈で
陳腐化していた。過去の詳細は git 履歴の本ファイル参照）。

---

## 0. まず読むもの

1. `CLAUDE.md`（運用契約・ゲート・ツール規律）— **拘束力あり**。特に「依存する tool 呼び出しを
   並列発行しない／小バッチ＋都度ゲート／ディスク信頼」。
2. 本ファイル（現在地・運用体制・開いている判断）。
3. `docs/BENCHMARKS.md`（計測済み事実の台帳 — 「速い」はここに数字がある時だけ）・
   `docs/SUPPLY-CHAIN.md`（依存の審査台帳）。
4. issue **#180**（着地トラッカー）— 裁可・マージ判断のスレッド。**#217**（棚卸し）。

## 1. 運用体制（2026-07 現在）

- **役割分担**：統括（人間・最終決定）／**レビュー兼統括指揮担当**（マージ裁可、#180 で
  GO を出す）／実装主担当（着地）／**先行研究員**（本セッション群・Antigravity セッション
  も同格の研究員）。**自己マージは誰もしない。**
- **ブランチ運用（研究員）**：PR ごとに `claude/<topic>` を **origin/main 基点**で切る。
  マージ後は `git fetch origin main` して次ブランチを新 main から。force-push 不可
  （recover forward）。CLAUDE.md の「単一 dev ブランチ」節は実装主担当向けの旧運用で、
  研究員は branch-per-PR が現行の合意。
- **裁可フロー**：PR 作成 → #180 に裁可依頼コメント（実測・破壊的変更・ゲート数値を明記）
  → 指揮が squash-merge → **マージ毎にタグ提案**（`v1.4.0-dev.N`、カットは統括専権）。
- **ゲート（push 前・毎回・数値で確認）**：fmt --check clean／clippy default **と**
  `--all-features -D warnings` = 0／test 両 feature セット 0 failed／
  `cargo tree -p rivus-cli --edges normal` = rivus-* のみ。gitleaks / cargo-deny は
  コンテナに無ければ CI に委ねる（CI は `cargo deny check --all-features`）。
- **GitHub API は希少資源**：CI をポーリングしない（webhook 購読）、コメントは束ねて1回。

## 2. main の現在地（v1.4.0-dev.7 = #220 以降）

- **Wave-1 正しさ債（#188 官能テスト起票分）ほぼ完済**：#218 decimal overflow surface・
  #219 to_source 可逆性・#220 pushdown 一般化（1.57×）・#221 式レーン never-silent
  （checked cast・div/0→null 破壊的）・#222 fill mean/median 実装・#223 **Plan
  Validation Gate**（未知列 did-you-mean・空プログラム・route 拒否・log フック実装・
  未知関数）— すべてマージ済み。
- 分散実行（§33/§34）・null model・decimal レーン・SWAR/AVX2 CSV scan・部分 route 出力は
  以前から landed（詳細は BENCHMARKS / design docs）。

## 3. 裁可待ち PR（2026-07-06 時点・5本）

| PR | 中身 | 特記 |
|---|---|---|
| **#224** | sink 書式高速化 2 弾：数値 LUT＋f64 exact fast path（save 1.80×）／temporal・decimal LUT＋**Display 委譲**（datetime 3.58×） | byte-identity は旧バイナリ A/B cmp＋オラクル 20万件で実証 |
| **#226** | UX 債バッチ：#203 map/裸ブロック明示拒否・#194 explain node サマリ・#199 basename/stem/dirname＋split_part 負 index＋**式の単項マイナス** | — |
| **#227** | **sliding window** = `hops(ts,size,hop)`（窓開始 List）＋既存 explode＋group | §36 メモの批准込み。column_from_values に List アーム新設 |
| **#228** | **session window** = `sessionize ts gap "30m" by user`（session 開始 datetime を付与） | **#227 の上にスタック** — #227 squash 後にリベース要 |
| **#229** | **Parquet リーダ**（feature `parquet`・arrow 抜き 22 crate・C ビルド皆無） | **RUSTSEC-2024-0436（paste unmaintained）の文書化例外を1件持ち込み** — 裁可時に明示確認。`full` 搭載は統括の配布判断 |

マージされたら：#227→#60 リスコープコメント（tumbling=済・sliding/session=着地・
watermark 系=§30.5 で永続対象外）、#228→リベース、タグ提案を忘れずに。

## 4. 計測済みの知見（BENCHMARKS.md に台帳あり）

- **1GB プロファイル**：open（parse）支配 → SWAR/AVX2 split 着地済み。**save が第2コスト
  かつ並列時の Amdahl 支配項**だった → #224 で書式コストを解消（レーン別 5M 行 save：
  datetime 1045→292ms・f64 625→296・date 435→152・decimal 328→190・int 113ms）。
- **読み側の f64 は std が既に Eisel-Lemire**（rust-lang/rust#86761）— 置換不要。
  `std::simd` は 2026 年も nightly — SIMD は `core::arch`＋実行時検出が正解のまま。
- **否定結果（#225 に記録）**：collected 一括書き出しの並列レンダは**どのホット経路にも
  乗らず無効果 → リバート済み**。sink 経路の地図：大ファイル streaming-parallel は
  per-worker part file で既に並列／blocking op 後の merge ストリームだけが直列面
  （sort→save で save≈760ms が残存標的）。
- 裸 `|# d count` は**幻の第2キー**になる parser quirk があった（#223 の Gate が教育的
  エラーで捕捉するようになった）。

## 5. 開いている設計判断（勝手に決めない）

1. **§36（sliding/session）の批准** — #227/#228 のマージ＝批准の理解で依頼中。
2. **Parquet の `full` 搭載可否**＋ **paste 例外の受容**（#229）。
3. **#41 の残り半分＝#45 正準縮約木**：f64 並列 sum/avg/std の byte-identity 化。
   pairwise/固定ブロック縮約は **serial の出力値も変える**（ULP レベル）ので、
   「バージョン間の値変化を許すか」という**批准事項**。次の研究の本丸候補。
4. §30.5 の裁定（watermark・unbounded 集計解除＝永続対象外）は**現行有効** — 窓 3 種が
   揃った今も、これを覆すには再批准が要る。
5. `unbounded` feature を `full` に入れるか（旧 issue の統括判断待ちのまま）。

## 6. 次のレバー候補（優先順は裁可の流れ次第）

- **#45 正準縮約木**（上記 5-3。設計メモ→プロトタイプ→計測の王道パターン）。
- **Parquet write 側**＋ row-group メタデータへの述語/射影 pushdown（#229 の残タスク）。
- sort→save の直列 merge ストリーム並列レンダ（#225 の提案、期待 ~2.5×）。
- Ryū/Dragonbox 移植（長仮数 f64 の std fallback テール）・Duration LUT。
- Track C 残り：resample/gap-fill（#62）・rolling（#63）・as-of join（#64）・lead/lag（#65）。
- Housekeeping：#188 サブの状態同期・GUIDE の窓 3 種チュートリアル節。

## 7. 落とし穴（実際に踏んだもの）

- **依存する tool 呼び出しを並列発行しない**（CLAUDE.md 規律 — 破ると編集消失・過剰主張
  commit が起きる。実績あり）。
- ゲートスクリプトの多重起動に注意（同一ログ/一時ファイルを取り合って偽 FAIL を出す）。
- `fill` は `fill <col> <method>`（列が先）。sub-second を含む duration リテラルは
  文字列（`"30m"`・bare `15m` は未実装＝§30.7①未確定）。
- fmt の canonical は `$_.col` 展開（#197 が pretty 化を提案中 — 未着手）。
- stress の一時 CSV はプロセス毎の名前だが、**並行 cargo test 二重起動**では衝突しうる。
