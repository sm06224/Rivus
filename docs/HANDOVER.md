# セッション・ハンドオーバー（次セッションの担当者へ）

最終更新: **2026-07-17**（design/41 Stage A＋Stage C 全段が #239 ブランチに着地。
過去の詳細は git 履歴の本ファイル参照）。

---

## 0. まず読むもの

1. `CLAUDE.md`（運用契約・ゲート・ツール規律）— **拘束力あり**。特に「依存する tool 呼び出しを
   並列発行しない／小バッチ＋都度ゲート／ディスク信頼」。
2. 本ファイル（現在地・運用体制・開いている判断）。
3. `docs/BENCHMARKS.md`（計測済み事実の台帳 — 「速い」はここに数字がある時だけ）・
   `docs/SUPPLY-CHAIN.md`（依存の審査台帳）。
4. issue **#240**（統合トラッカー v2 — #180 後継）— 裁可・GO・着地記録・申し送りの
   一元スレッド。冒頭の体制・ゲート・フロー・投稿規律が正典。**#217**（棚卸し）。

## 1. 運用体制（2026-07 現在）

- **役割分担**：統括（人間・最終決定）／**レビュー兼統括指揮担当**（マージ裁可、#180 で
  GO を出す）／実装主担当（着地）／**先行研究員**（本セッション群・Antigravity セッション
  も同格の研究員）。**自己マージは誰もしない。**
- **ブランチ運用（研究員）**：PR ごとに `claude/<topic>` を **origin/main 基点**で切る。
  マージ後は `git fetch origin main` して次ブランチを新 main から。force-push 不可
  （recover forward）。CLAUDE.md の「単一 dev ブランチ」節は実装主担当向けの旧運用で、
  研究員は branch-per-PR が現行の合意。
- **裁可フロー**：PR 作成 → **#240** に裁可依頼 1 コメント（実測・破壊的変更・ゲート数値を
  明記）→ 指揮の独立 gate → GO → 実装担当が着地。**タグ提案は不要**（cut 保留・
  未タグ蓄積は指揮管理）。1 イベント 1 コメント。
- **ゲート（push 前・毎回・数値で確認）**：fmt --check clean／clippy default **と**
  `--all-features -D warnings` = 0／test 両 feature セット 0 failed／依存樹は
  **policy v2 基準「documented, not zero」**（default は gzip/zstd の pure-Rust 8 crate
  常時搭載・SUPPLY-CHAIN.md が台帳）。gitleaks / cargo-deny はコンテナに無ければ
  CI に委ねる（CI は `cargo deny check --all-features`）。
- **GitHub API は希少資源**：CI をポーリングしない（webhook 購読）、コメントは束ねて1回。
- **裁可スレッドは #240 へ移行済み（2026-07-20）**: #180 は close（履歴保存）。
  想起義務は果たされた — 以後この注記は履歴。

## 2. main の現在地（`67fdc78`・v1.4.0-dev.9 まで cut 済み・以降のタグは指揮管理で保留）

**着地済み（dev.8 以降）**: #237（perf 第1-18弾＋R1/R2）→ #232 shift → #234 date_bin →
#233 as-of → **#239（design/41 Stage A〜C・投機オープン・C-eq・narrow-keep・kernel
マスク 他）** → #236（§38 構文簡素化 docs・破壊的変更許可）。#235 は #237 包摂 close。
指揮の独立ゲート直近値 = test **503/0・534/0**・clippy 0/0・依存樹 documented 8 crate。

**10M×9ファイル標準（汚れ入り・等価契約）の現在地（wall / peak RSS、
2026-07-17〜18 の同窓 interleave — #239 ブランチ、Stage C＋narrow-keep＋
kernel マスク＋キー・プレフィクス込み）**:

| 形状 | rivus | DuckDB 同窓 | 比 |
|---|---|---|---|
| CSV group | **440-478ms** / 9.4MB | 653-660ms | **0.69× 勝ち** |
| JSONL group | **611-720ms** / 8.2MB | 1257ms | **~0.50× 勝ち** |
| CSV ETL | ~712-754ms / 9.7MB | 1459ms（前日窓） | **~0.5× 勝ち** |
| CSV.gz group | **711-903ms** | 1177ms | **0.60× 勝ち**（旧 0.86×） |
| JSONL.gz group | **999-1119ms** | 1777-1922ms | **0.56× 勝ち**（旧 0.90×） |

**全 5 形状で DuckDB の 0.50-0.70×**（byte-identity 証明付き・一桁 MB RSS）。
gz 2 形状は個別最適化なしで narrow-keep/kernel マスク/prefix の恩恵が
自動適用された結果（fused loop はデコーダ非依存 — 計測 2026-07-18）。
残る未踏峰は Polars eager ETL 583ms（契約違反実装）— 現在 ~730ms 級で射程内。
箱ノイズが大きい（同一バイナリで日内 ±40% 変動）ため、比較は必ず同窓
interleave で（絶対値の日跨ぎ比較は無意味）。

**#237 で入った主要機構**（詳細は BENCHMARKS.md 第1-18弾の各節）:
ファイル毎 worker 並列（read→group / read→sink）・BroadcastProbe・ブロック歩行
decode/推論（CSV/JSONL）・列指向セルバッチ（`push_many`）・`fast_trim`・
reconcile/Cast のムーブ意味論・Str↔数値の列指向変換・in-tree FxHasher
（`fxhash.rs`、group scratch＋seal() / JoinTable — 出力順はハッシュ非依存を構造保証）・
preface（安全性サンプルの推論二周目排除）・WPROF（worker/op/phase 分解、env-gated）・
R1/R2 並列 identity ガード（`tests/stress/parallel_read_identity.rs`）。

## 3. 主計画 design/41 の現在地（Stage A・C 着地済み／B 未着手）

`docs/design/41-deep-fused-worker.md` の3段計画のうち、#239 ブランチ
（`claude/perf-join-groupkey`、レビュー再ゲート待ち）に着地済み:

- **Stage A（着地）**: A-1 probe projection pushdown（`fused_used_columns`）＋
  A-2 FusedReadGroup（join→pred→キー符号化→observe_row の単一行ループ、
  worker 毎 lossless フォールバック）。JSONL RowTemplate（decode 側＋infer 側）も同梱。
- **Stage C（着地・3 コミット 5df2b7e/fe621f7/cb01f7b）**: 投機 sample 開＋矛盾検出＋
  局所再走。§5 の C-eq が理論核（キーと書き出しセルは Display-safe、値消費は cast 正規化、
  →Str 拡幅のみ保持可・数値拡幅は直列 bail）。CSV は parse 失敗が検出器（Bool sample は
  不適格→正準）、JSONL は構文型なので lane_mismatches が完全検出器（Bool 例外なし）。
  group driver＋sink driver 両方、発動は strategy 接尾辞 "…, speculative open"。
  ガード: 単体 8 本＋R3/R3j/R3b/R4/R4b（矛盾あり/なし×byte-identity×bail×発動 assert）。
- **Stage B: mmap 窓トランスポート（実装→計測→不採用・破壊済み 2026-07-17）** —
  全 reclaim 設定（DONTNEED 無効含む）で CSV group ~8% 負け。敗因は soft page
  fault 経路 > 256KiB buffered copy（cgroup 箱・1 パス化済みでページ再利用なし）。
  再訪条件は BENCHMARKS の負の結果節。ETL の Polars 残差は decode の**計算**
  （field split＋lane parse）であってカーネル→ユーザコピーではない。
- 負の結果（BENCHMARKS 台帳）: sink 側融合は不採用・セル原語チューニング枯渇・
  StreamJsonlReader（read_line）を投機に転用すると open の勝ち分を decode で返上
  （投機デコーダは正準と同じ block-walk であること）・mmap 窓は上記。

## 4. 計測済みの知見（BENCHMARKS.md が台帳）

- worker コスト表（CSV group、20MB/file）: decode 110 / feed 165（probe 51・group 65・
  project 28）/ reconcile 33 ms。open（推論）210ms が最大の直列ブロック
- **負の結果（再発掘防止）**: cast の read 押し下げは意味論非保存（cast_value の文字列→int
  は f64 切り捨て "1.5"→1、リーダ I64 レーンは null）／0行チャンクでのスクラッチ型付けは
  式型導出の保証が無く #41 の罠（誤 approve で byte-identity 破壊リスク）
- JSONL の残る 1.34× はスキャナ自体（scan_row 全バイト×2パス）— ループ構造ではなく
  **スキャナ内 SWAR** が次のレバー
- ベンチ計測は bench_io.py 型（VmHWM ポーリングに sleep 必須・インターリーブ対照・
  ボックスノイズに注意）。旧バイナリ対照は git stash → build → cmp が定石

## 5. 開いている判断（勝手に決めない）

1. **#236 構文簡素化 P1+P2 — GO 済み**（受入条件は #236 コメント: 各スライス単独
   フル gate・fmt 自動移行 round-trip pin・CHANGELOG・既存テスト資産の変換維持）
2. **design/42 辞書レーン — 批准済み・条件 4 点**（①§2 不変条件の property test
   ②高カード escape hatch ③段階ごとガード＋計測 ④発動可観測性）→ 第 (a) 段実装中
3. **decode 列プルーニング — 対称方式で承認済み**（serial にも同一 used-set・
   契約変更を CHANGELOG＋docs 明記・explain surface）
4. **#45 f64 並列 byte-identity**: Q1（一度きり ~1 ULP シフト）は統括許容済み —
   実装 PR は CHANGELOG 明記＋decimal/`--exact` 無影響が条件
5. **design/40 Q1-Q4**（OTel T1 / QUIC B2）は引き続き裁可待ち
4. #229 Parquet の `full` 搭載可否（配布判断）・`unbounded` の full 搭載
5. FxHash は「性能ツールであり防御境界ではない」で指揮承認済み（SipHash 復帰は
   JoinTable/scratch の型1行）

## 6. 次のレバー候補（優先順・2026-07-18 改）

残レバーは全て ≤30ms 級（decode floor 実質到達・reconcile 0 化・述語 kernel 化・
キー prefix 事前計算まで完了）。次の 2 倍は構造変更から:

1. **辞書化 group-by（次の研究アーク）**: Str レーンの dictionary encode →
   group キーを整数 ID 組へ（composite String＋hash probe ~40-50ns/row の根治）。
   join probe の右表引きも ID 化で無料化しうる。IR/Chunk 層の設計判断を伴う
   ため design doc → 批准 → 実装の順。
2. fused 対応集合の拡張（Or 述語・数値 coalesce・複数 join — 適用面を広げる）
3. ETL 残差（Polars 583ms vs ~730ms）: prefilter の行毎 f64 parse を SWAR
   数字列比較へ（保守的意味論は維持）／typed agg リーダ（Value 往復除去、
   各 ~10-30ms 級）
4. 圧縮標準（csv.gz/jsonl.gz）の decode 側最適化（Stage C 非対象だった）
5. Track C 残り: resample/gap-fill（#62 の agg 側）・rolling（#63）
6. #45 正準縮約木の実装スライス（Q1 許容済み）

## 7. 落とし穴（実際に踏んだもの）

- **依存する tool 呼び出しを並列発行しない**（CLAUDE.md 規律 — 破ると編集消失・過剰主張
  commit が起きる。実績あり）。
- ゲートスクリプトの多重起動に注意（同一ログ/一時ファイルを取り合って偽 FAIL を出す）。
- `fill` は `fill <col> <method>`（列が先）。sub-second を含む duration リテラルは
  文字列（`"30m"`・bare `15m` は未実装＝§30.7①未確定）。
- fmt の canonical は `$_.col` 展開（#197 が pretty 化を提案中 — 未着手）。
- stress の一時 CSV はプロセス毎の名前だが、**並行 cargo test 二重起動**では衝突しうる。
