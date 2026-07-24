---
name: landing-exec
description: 実装担当（着地専門）のプレイブック。レビューの merge GO を受けてから squash-merge・記録・同期までの型。PR の着地、衝突解消、着地報告を頼まれたときに使う。
---

# 着地実行の型（実装担当）

merge してよいのは**レビュー兼指揮の GO コメントが付いた head だけ**。GO の無い head、
GO 後に動いた head は着地しない（動いていたら中止してトラッカーに報告）。

## 手順（1 PR あたり）

1. **GO の確認**: トラッカー最新の指揮ログ → PR の GO コメント。条件付き GO なら
   条件充足の確認コメントが出てから。
2. **head 不変確認**: `git fetch` して PR head sha が GO 時と一致すること。
3. **本機フル gate**: `/gate <head>`。GO コメントの見込み値（test 件数）と一致するか。
   base が古い場合は**現 main との合流木を作ってから gate**（gate した木がそのまま
   main になるように）。
4. **squash-merge** → **bit 同一確認**: `git diff <gate済みsha> origin/main` が空。
5. **記録は 1 コメント**: main sha・gate 実測値（表）・bit 同一・dev 同期、を
   トラッカーへ。**タグ提案は書かない**（タグキューは指揮が管理・cut は統括専権）。
6. ローカル dev を新 main に同期。

## 衝突解消の規律

- 解消は**加算的**（CHANGELOG の両項目併記など・字句改変なし）に限る。
  push 後は**merge せず停止**し、指揮の差分確認（interdiff）を待つ。
- 解消がコードに及ぶ場合は自分で判断せず、再 gate 依頼としてトラッカーへ。
- force-push は force-with-lease のみ・PR ブランチに限る。main の歴史は書き換えない
  （recover forward）。

## 禁じ手（実績のある失敗様式）

- GO の無い merge／head 確認を省いた merge。
- gate の見込み値と実測が違うのに「概ね一致」で進める（数値は一致か、差の説明か）。
- 衝突解消 merge に紛れた実質変更（interdiff で露見する——正直に別コミットにする）。
- 着地報告に測っていない数値を書く。
