---
name: research-pr
description: 先行研究の PR 提出プレイブック。研究スライスの切り方・裁可依頼の型・計測と正直申告の規律。研究 PR の作成・裁可依頼・ベンチ記録を頼まれたときに使う。
---

# 研究 PR の型（先行研究）

branch-per-PR・**origin/main 基点**（`git fetch origin main` してから切る）。
自己マージはしない。裁可はトラッカー（#240 系）で受ける。

## スライスの切り方

- **1 PR = 1 検証可能な主張**。大きな機構は段階着地（(a) 配線→(b) producer→(c) 回収）
  に割り、各段に独立の gate とガードを持たせる。
- **投資段（実測退行）は単独マージを求めない**。回収段を同 PR に積み、net win を
  実測してから再裁可（同 PR 積み増しは段別コミットを維持）。
- 表現（representation）の追加は**観測等価を第 1 段で property test 化**し、
  producer より先に配線を通す（dict レーン (a) の型）。

## 裁可依頼コメントの型（トラッカーへ 1 コメント）

- **内容**（何を・なぜ・設計判断）／**破壊的変更の有無**／**新規依存の有無**
- **gate 実測値**（fmt/clippy/test 両 feature/依存樹——数値で）
- **実測**（同窓 interleave・ペア勝敗・median・fixture 条件）
- **並行性メモ**（審査中の他 PR との衝突面——「CHANGELOG のみ・後着側で機械解消」等）
- **裁可時に確認してほしい解釈・設計判断**は自分から列挙する（隠れた解釈が
  一番高くつく——`|> *` 新設・read* 正典=open の実例）

## 計測と申告の規律

- 「速い」は**同窓 interleave の数字がある時だけ**。before/after を BENCHMARKS.md に
  記録（fixture 条件・箱・rounds・除外 round を明記）。
- **負の結果は destroy して台帳へ**（作って・計測して・破壊した記録＋再訪条件）。
  塩漬けブランチを残さない。mmap 窓・固定幅パックキー・構造マーク走査が先例。
- **正直申告**: fixture 再生成で旧絶対値と非比較になった・ローカルにツールが無い・
  設計メモの想定が実測で外れた——はそのまま書く。レビューはこれを加点評価する。
- ガードには**発動 assert**を必ず付ける（strategy 文字列 / WPROF / 発動行数）。
  発動 assert の無いガードは無音 fallback で空洞化する（R5 の教訓）。
- 敵対 fixture を使う: ULP 感受性は小数値・汚れは sample 窓の外・空/巨大/多バイト/
  引用符/改行セル・chunk-size 掃引（cz=1,2,3,4096 級）。

## 触ってはいけないもの

- exact レーン（i128 int/decimal/duration）の bytes を変える変更は #57/#41 圏＝
  事前に設計メモ→批准。f64 の値シフトは Q1 級の統括裁可事項。
- 「default 依存」は policy v2 の documented set が台帳（SUPPLY-CHAIN.md チェック
  リスト無しに crate を足さない）。
