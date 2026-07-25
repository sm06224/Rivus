# CLAUDE.md — operating contract for autonomous work on Rivus

This file is the durable memory for how to develop Rivus. Read it first. It is
binding unless the user overrides it.

## Addressing the user (experimental)

- When speaking to the user, address them as **「統括」** (e.g. 「承知しました、
  統括」). Japanese replies, per the user's standing preference.

## Mode: autonomous

- **Do not ask for confirmation.** Proceed. When something is ambiguous, consult
  the philosophy below, decide, rewrite as needed, and keep going. Surface
  decisions in PR descriptions, not as blocking questions.
- **Keep momentum.** Land work as a chain of small, reviewable PRs.

## Workflow: branch-per-PR ＋ 裁可フロー, squash-merge（改訂 2026-07-25・統括裁可）

体制は 4 役（統括＝最終決定・裁可・タグ cut 専権／レビュー兼指揮＝独立 gate → GO 判定・
事後検証／実装担当＝着地実行／先行研究＝PR 作成）。**自己マージは誰もしない。**
指揮拠点イシュー（現行 #240）が裁可・GO・着地記録の一元台帳。

- **branch-per-PR・origin/main 基点。** `git fetch origin main` してから
  `claude/<topic>` を切る。並行 PR は互いのファイル衝突面を裁可依頼に明記
  （後着側が merge-forward・解消は加算的に限る）。
  ※旧「単一 dev ブランチ・Exactly ONE open PR」運用は 2026-07 に廃止。
- **1 PR の型**: 研究が PR ＋指揮拠点へ裁可依頼（実測・破壊的変更・gate 数値明記）→
  指揮が独立 gate → GO（PR 1 コメント＋拠点記録）→ 実装担当が head 不変確認・
  本機 gate・squash-merge・拠点に 1 コメント → 指揮が事後検証（bit 同一）。
  詳細は `.claude/skills/`（landing-review / landing-exec / research-pr）。
- **Release tag cut は統括専権。** 未タグの蓄積キューはレビュー兼指揮が管理し、
  cut 再開時にその時点の HEAD で 1 本提示する（着地毎のタグ提案は書かない）。
  手順は `docs/RELEASE.md`。
- **GitHub API is the scarce resource** (secondary rate limit on PR/comment
  creation). So: never poll CI via API — rely on the `<github-webhook-activity>`
  events; don't open/close PRs in bursts; don't repeatedly edit PR bodies.
- **Do not wait on GitHub CI.** Guarantee green *locally* before every push.
- **Local gate (must pass before every push):**
  ```sh
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets   # (CI uses -D warnings; keep zero)
  cargo test --workspace
  # Policy v2: gzip/zstd are default features. Still-gated code (regex, parquet,
  # net/quic, unbounded) is invisible to the two lines above; CI compiles it,
  # so the gate must too (#79's gzip break is the cautionary tale).
  RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features
  cargo test --workspace --all-features    # runs the gzip/zstd oracle tests
  gitleaks detect --no-git --source .
  cargo deny check bans sources licenses    # advisories needs network → CI
  # gitleaks / cargo-deny がコンテナに無い場合（再生成で消える・proxy 制約で
  # 再導入不可のことがある）: 代替せず「CI gate で充足を確認」と明記して CI に
  # 委譲してよい（済ませたふりが最悪）。CI は cargo deny --all-features を走らせる。
  ```

## Tool & edit discipline (hard-won; violating this has shipped broken pushes)

Root cause of past breakage: firing many tool calls in one batch — especially
**dependent** read→edit→build chains in parallel — corrupts the output stream,
desyncs my view of disk state, and produces edits I *think* landed but didn't.
That has caused over-claiming commit messages and broken pushes. So:

- **Small batches, verify, then proceed.** One logical step per turn
  (a few *independent* calls at most). NEVER batch dependent calls
  (`Read`→`Edit`, `Edit`→`build`, `commit`→`push`) — each needs the prior result.
- **Trust disk, not memory.** Before editing, `Read` the exact lines; after a
  surprising result, re-Read rather than assume. A failed `Edit` (string not
  found) means the change did NOT apply — fix it before moving on, never paper over.
- **Gate is a numeric checkpoint, not a vibe.** Before every push confirm with
  counts: clippy `warning/error` count **= 0**, `test result` FAILED **= 0**,
  dependency tree audited (`cargo tree -p rivus-cli --edges normal` = rivus-*
  plus exactly the SUPPLY-CHAIN.md-documented adapters — under policy v2 the
  check is "documented", not "zero"). Build must
  succeed — a build failure makes `cargo test` report `0 passed`, which is NOT green.
- **Commit messages claim only what's on disk.** If a message says "hardens X",
  `git show HEAD:path` must contain that change. No aspirational wording.
- **Recover forward, don't rewrite history.** Force-push is denied here. If a
  broken commit was pushed, fix on top (or `reset --soft` onto the remote then
  re-commit) and fast-forward push. Note the supersession in the new message.
- **GitHub posts are expensive and permanent.** Get hashes/facts right the first
  time (read them from `git`, don't recall them); avoid bursts of corrective
  comments. One accurate comment beats three retractions.

## Benchmarking discipline

- Target the three regimes explicitly: **large**, **error-heavy**, **mixed-type**
  (and fan-out). Generators live in `rivus_runtime::gendata` (seeded, no `rand`).
- **Every optimization PR attaches before/after numbers** in `docs/BENCHMARKS.md`
  and keeps the correctness gate green (`tests/stress.rs`,
  `tests/optimizer_equiv.rs`). Correctness is the gate; speed is the reward.
- "Faster" is never asserted without a measured number.
- SIMD / assembler-level optimization is allowed **where a bench proves the win**.
- **Scale fixture is mandatory for perf claims（統括指示 2026-07-09）**: a
  single monolithic file is NOT a valid perf test. Minimum: **10M rows (CSV) and
  10M JSON objects (JSONL), split across ⌈2.2 × physical cores⌉ files** (this
  box: 4 cores → 9 files), with dirty data in the mix (malformed rows/lines, a
  file missing a column) so the never-silent contract is exercised at scale.
  Fan-out across files is what exposes serial bottlenecks and buffering-memory
  ceilings that a 1-file test hides. Compare against DuckDB/Polars equivalents
  (equal contract, verified row-identical) and report wall + peak RSS.
- **Stream IO first（統括指示 2026-07-09）**: so disk IO never dominates the
  scale fixtures, the standard fixtures also run **compressed** (gzip/zstd read
  as streams) — everything must flow（全てが流れ）. A path that materializes a
  whole file (decompress-to-buffer, read-whole) is a defect; decode must ride
  the decompression stream.

## Supply-chain vigilance

- **Policy v2（統括指示 2026-07-09）: external crates are ALLOWED, including in
  the default build.** The invariants are now: (a) **single-binary release**,
  (b) **license / dependency tree / supply chain explicitly documented** in
  `docs/SUPPLY-CHAIN.md`, (c) `cargo deny check` green. The old zero-dep-default
  rule is retired; dep-zero remains a nice property of individual crates, not a
  product constraint. Competitive performance outranks dependency purity —
  losing to other solutions is not acceptable（負けるな）.
- **Heavy/standard formats (compression, Parquet, pickle) use vetted crates**;
  feature-gating is now an *option* (niche backends), not a requirement.
  Compression (gzip/zstd) is **on by default** so compressed streams are
  first-class. Prefer mature, major, stable, pure-Rust.
- Before adding any crate, run the `docs/SUPPLY-CHAIN.md` checklist: needed?
  mature/major/stable (not obsolete, not a typosquat)? trusted maintainer?
  feature-gated? pin & vet *transitive* deps? permissive license? Verify with
  `cargo deny check --all-features`.
- Tools are installed from **official release binaries** and version-checked.
- Run `gitleaks` routinely; never commit secrets.

## Architecture invariants (the philosophy, in code terms)

The 8 "physical laws" (see `docs/design/README.md`): Everything is Flow ·
Continue First · DAG Native · Observable First · IR Reversible · Chunk Native ·
Execution-aware typing · Text is stream.

Concretely:
- **IR is the single source of truth.** `rivus_ir::PlanGraph`. Optimizer is
  IR-in/IR-out and never opaque (record every rule in `OptReport`, surface via
  `rivus explain`). Keep `to_source()` faithful (reversibility).
- **Operator boundary stays thin:** `process(from, chunk, ctx) -> Vec<Chunk>`.
  New execution backends (Arrow, JIT) slot behind it without touching the engine.
- **Telemetry is measured in the engine,** not in operators.
- **Continue-first:** only `Severity::Fatal` halts; everything else flows on the
  error stream. No panics on bad input.
- **Chunk-native & chunk-size independent:** results must not depend on
  `chunk_size` (stress-tested).
- **Byte-identical across execution strategies:** serial vs parallel vs any
  backend must produce the *same bytes*. Floating-point is the trap — f64
  addition is **non-associative**, so a naive partition-then-merge `sum`/`avg`/
  `std` drifts by a ULP (measured; #41). Exact reductions (`min`/`max`/`count`/
  `first`/`last`/`percentile`) and **integer / decimal** lanes *are* associative
  and always safe. Exact money math is the opt-in **decimal lane** (i128 scaled
  integer, `docs/design/21`): `--exact` / `:decimal`.
  **Resolved for f64 moments（#45/#249, 2026-07-23）**: f64 `sum`/`avg`/`std` now
  parallelize via the **canonical reduction tree**（fixed-block, file-major fold,
  `docs/design/37`）— the result is a pure function of the data (thread-count /
  chunk-size / serial-vs-parallel independent), so serial == parallel holds
  bit-for-bit **by construction**（強制直列は同一機械の P=1 mirror）。値は旧
  naive left fold から一度きり ~1 ULP 級シフト済み（Q1・統括裁可・CHANGELOG 記録・
  精度はむしろ向上）。exact レーンは 1 バイトも不変。原則は不変: **byte-identity を
  無言で緩める変更は今後も禁止**（新バックエンドは正準木と同じ「構成的に決定的」で
  あることを証明してから）。

## Roadmap (staged: MVP → optimize → JIT → distributed)

Live backlog with measured status is in `docs/BENCHMARKS.md` and
`docs/ROADMAP.md`. **Read `docs/HANDOVER.md` for the current cross-session
context** (what's landed, measured findings, next levers) — HANDOVER が正典で、
本節は静的スナップショットを持たない（過去に本節の focus 記述が実装から 2 世代
遅れて混乱を生んだため、2026-07-25 改訂で「HANDOVER を読め」に一本化）。

現在地の要約だけ最小限に: 10M×9 ファイル標準は全 5 形状で DuckDB の 0.5〜0.7×
（byte-identity 契約下・一桁 MB RSS）・#41/#45 は正準縮約木で解決済み・
辞書レーン design/42 全段着地・構文 design/38 P1〜P4 移行リリース済み（旧綴り
エラー化 flip が次の大物）。Heavy optional backends (Arrow, Cranelift JIT, GPU
`docs/design/22`) stay feature-gated behind the operator/eval boundary with a
CPU fallback.

## Repo map

```
crates/rivus-core       data model: Chunk/Column/Schema/Value/Mode/ErrorEvent
crates/rivus-ir         PlanGraph DAG, Op, Expr, to_source (reversible)
crates/rivus-parser     Unified Flow Syntax -> IR
crates/rivus-optimizer  semantics-preserving DAG transforms (IR-in/IR-out)
crates/rivus-runtime    single-thread chunk engine, operators, telemetry, gendata
crates/rivus-cli        rivus run|explain|check (ASCII viz)
docs/design/            17-section design  ·  docs/BENCHMARKS.md  ·  docs/SUPPLY-CHAIN.md
```

## 共有運用資産（`.claude/` — 全セッション共通の型）

運用の実証済みの型は `.claude/` に財産化してある。**該当する作業ではこれらを既定で使う**:

- **commands**: `/gate [ref]`（独立フル gate・数値チェックポイント）・
  `/postverify <sha>`（着地の bit 同一検証・interdiff 手順）・
  `/interleave A B flow`（same-window A/B 実測の規律）
- **skills**: `landing-review`（レビュー兼指揮の審査の型・判定原則）・
  `landing-exec`（実装担当の着地の型・禁じ手）・
  `research-pr`（研究 PR のスライス設計・裁可依頼・正直申告の規律）
- **agents**: `gate-runner`（読み取り専用の gate 並行実行係）

これらは実際の事故と成功から蒸留した規律（発動 assert・投資段は net win まで未マージ・
申告と disk の乖離は差し戻し理由・負の結果は台帳へ）を含む。更新は通常の PR フロー
（裁可 → gate → 着地）で行い、勝手に緩めない。
