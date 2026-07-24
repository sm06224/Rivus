# /gate [ref] — 独立フル gate を実走し、数値チェックポイントで報告する

引数 `ref`（sha/ブランチ）が与えられたら `git fetch origin <ref>` 後に
`git checkout --detach <ref>`（作業樹が汚れていたら中止して報告）。無指定なら現在の HEAD。

以下を**この順**で実走する。gate は数値チェックポイントであり、vibe ではない
（CLAUDE.md）。**依存する呼び出しを並列発行しないこと**（read→edit→build の並列化は
過去に破損 push を生んだ実績のある失敗様式）。

```sh
cargo fmt --all -- --check                                        # clean 以外は即 NO-GO
cargo clippy --workspace --all-targets 2>&1 | grep -cE "^(warning|error)"   # = 0
RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features   # exit 0
cargo test --workspace 2>&1 | grep -E "^test result" \
  | awk '{p+=$4; f+=$6} END {print "PASSED="p" FAILED="f}'        # FAILED=0
cargo test --workspace --all-features 2>&1 | grep -E "^test result" \
  | awk '{p+=$4; f+=$6} END {print "PASSED="p" FAILED="f}'        # FAILED=0
cargo tree -p rivus-cli --edges normal --prefix none | sed 's/ (.*//' | sort -u
```

判定基準:

- **ビルド失敗は「0 passed」であって green ではない**。test result 行の合算前に
  ビルドが通っていることを確認する。
- 依存樹は rivus-* を除いた残りが `docs/SUPPLY-CHAIN.md` の documented set と
  **完全一致**（policy v2「documented, not zero」——多くても少なくても NO-GO）。
- 新規のガードテスト（発動 assert 付き）は**名指しで単独実行**し、skip せず
  発動していることを確認する（`available_parallelism` ガードで無音 skip する
  テストは、通っても何も守っていない）。
- gitleaks / cargo-deny がローカルに無ければ「CI で充足を確認」と明記する
  （代わりに済ませたふりをしない）。

報告は表 1 枚（項目 / 実測値 / 判定）。PR 本文の申告値と自分の実測値は**必ず区別して**
書く。1 項目でも赤なら NO-GO で、赤の生ログを添える。
