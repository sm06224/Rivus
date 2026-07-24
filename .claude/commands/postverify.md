# /postverify <gated-sha> — 着地後 main が gate 済みの木と bit 同一かを検証する

squash-merge の事後検証。着地報告の数値を鵜呑みにせず、木そのものを照合する。

## 同世代 squash（base が着地時の main と同一）

```sh
git fetch origin main
git diff <gated-sha> origin/main --stat   # 出力が空 = bit 同一 = 検証完了
```

## 世代跨ぎ（PR base が 1 世代以上前の main で、無衝突 merge で着地した場合）

gate 済み合流木をローカルで**再構築**して照合する:

```sh
git checkout --detach <着地直前の main sha>
git merge --no-edit <gated-sha>      # 無衝突であること自体も検証の一部
git diff HEAD origin/main --stat     # 空 = 合流木と bit 同一
```

## 解消 re-push の差分確認（衝突解消後の再 push を着地させる前）

- コード部の同一性は**パッチ SHA 比較**が第一候補:
  `git diff <旧base>..<旧head> -- crates/ | sha256sum` と
  `git diff <新base>..<新head> -- crates/ | sha256sum`
- **SHA 不一致でも即 NG ではない**。基底が同じファイルを触っていると hunk の
  行番号だけがズレる。`diff <(git diff …) <(git diff …)` の interdiff で
  「@@ 行と index 行のみの差＝内容行の差ゼロ」を確認できれば同一と判定してよい
  （#242→#246 で実証済みの手順）。
- docs（CHANGELOG 等）の解消は**加算的**（両項目併記・字句改変なし）を目視確認。

## 乖離した場合

diff が空にならない・merge が衝突する場合は着地報告との乖離＝事故。**即 #240 に
実測値つきで報告**し、以後の着地を止める。force-push での歴史書き換えはしない
（recover forward）。
