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

## Workflow: ONE integration branch, squash-merge (minimize maintainer effort)

The maintainer squash-merges and wants near-zero merge effort. So:

- **Single long-lived branch `dev`, linear history.** Commit features
  sequentially on `dev` (never parallel feature branches → never internal merge
  conflicts). `git push` is free (not rate-limited); push often.
- **Exactly ONE open PR** (`dev` → `main`), kept updated by pushes. Do not open
  a second PR. After the maintainer squash-merges it, `git fetch origin main`
  then `git reset --hard origin/main` on `dev` and keep committing.
- **On every merge to `main`, surface the release tag.** Cutting a release is the
  maintainer's alone (`docs/RELEASE.md`), so after a merge tell them the suggested
  next tag — version + the `git tag … && git push origin <tag>` command — they
  decide when to cut it (standing request, 2026-06-03).
- **GitHub API is the scarce resource** (secondary rate limit on PR/comment
  creation). So: never poll CI via API — rely on the `<github-webhook-activity>`
  events; don't open/close PRs in bursts; don't repeatedly edit PR bodies.
- **Do not wait on GitHub CI.** Guarantee green *locally* before every push.
- **Local gate (must pass before every push):**
  ```sh
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets   # (CI uses -D warnings; keep zero)
  cargo test --workspace
  # Feature-gated code (compression: gzip/zstd, regex) is NOT in the default
  # zero-dep build, so a feature-only break (a struct field, a signature) is
  # INVISIBLE to the two lines above. CI compiles it, so the gate must too —
  # this is exactly how #79's gzip break should have been caught before push.
  RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features
  cargo test --workspace --all-features    # runs the gzip/zstd oracle tests
  gitleaks detect --no-git --source .
  cargo deny check bans sources licenses    # advisories needs network → CI
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
  zero-dep (`cargo tree -p rivus-cli --edges normal` = rivus-* only). Build must
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

## Supply-chain vigilance

- The **default build has zero third-party dependencies** (core/ir/parser/
  optimizer/runtime/cli are std-only with default features). Keep it that way;
  isolate tooling under `[dev-dependencies]`.
- **Heavy/standard formats (compression, Parquet, pickle) may use a vetted crate**,
  but only **off-by-default, feature-gated, and behind the source/sink trait** so
  the default build stays dep-free (maintainer-approved 2026-05; see selected
  adapters in `docs/SUPPLY-CHAIN.md`). Prefer mature, major, stable, pure-Rust.
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
  addition is **non-associative**, so a parallel partition-then-merge `sum`/
  `avg`/`std` drifts by a ULP and is NOT byte-identical (measured; #41). Exact
  reductions (`min`/`max`/`count`/`first`/`last`/`percentile`) and **integer /
  decimal** lanes *are* associative and safe to parallelize. Exact money math is
  the opt-in **decimal lane** (i128 scaled integer, `docs/design/21`): `--exact`
  / `:decimal`. Never silently relax byte-identity for f64 — keep it serial or
  route through decimal.

## Roadmap (staged: MVP → optimize → JIT → distributed)

Live backlog with measured status is in `docs/BENCHMARKS.md` and
`docs/ROADMAP.md`. **Read `docs/HANDOVER.md` for the current cross-session
context** (what's landed, the open #41 question, measured findings, next levers).

Measured current focus (the 1 GB profile points here): **SIMD CSV scan + faster
field parse** (parse is ~75% of wall, not inference) → buffered output → the
opt-in **decimal lane** at the reader (unblocks byte-identical parallel
group-by, #41) → datetime lane / list-agg / pivot (`docs/design/23`). Heavy
optional backends (Arrow, Cranelift JIT, GPU `docs/design/22`) stay
feature-gated behind the operator/eval boundary with a CPU fallback.

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
