# CLAUDE.md — operating contract for autonomous work on Rivus

This file is the durable memory for how to develop Rivus. Read it first. It is
binding unless the user overrides it.

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
- **GitHub API is the scarce resource** (secondary rate limit on PR/comment
  creation). So: never poll CI via API — rely on the `<github-webhook-activity>`
  events; don't open/close PRs in bursts; don't repeatedly edit PR bodies.
- **Do not wait on GitHub CI.** Guarantee green *locally* before every push.
- **Local gate (must pass before every push):**
  ```sh
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets   # (CI uses -D warnings; keep zero)
  cargo test --workspace
  gitleaks detect --no-git --source .
  cargo deny check bans sources licenses    # advisories needs network → CI
  ```

## Benchmarking discipline

- Target the three regimes explicitly: **large**, **error-heavy**, **mixed-type**
  (and fan-out). Generators live in `rivus_runtime::gendata` (seeded, no `rand`).
- **Every optimization PR attaches before/after numbers** in `docs/BENCHMARKS.md`
  and keeps the correctness gate green (`tests/stress.rs`,
  `tests/optimizer_equiv.rs`). Correctness is the gate; speed is the reward.
- "Faster" is never asserted without a measured number.
- SIMD / assembler-level optimization is allowed **where a bench proves the win**.

## Supply-chain vigilance

- The shipped runtime has **zero third-party dependencies** (core/ir/parser/
  optimizer/runtime/cli are std-only). Keep it that way; isolate tooling under
  `[dev-dependencies]`.
- Before adding any crate, run the `docs/SUPPLY-CHAIN.md` checklist: is it
  needed? who maintains it (trusted, active, widely used — beware typosquats and
  brand-new crates)? dev-only? pin & vet *transitive* deps? permissive license?
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

## Roadmap (staged: MVP → optimize → JIT → distributed)

Live backlog with measured status is in `docs/BENCHMARKS.md`. Current focus:
operator fusion → projection pushdown → vectorized/SIMD predicate kernels →
Arrow-backed columns → parallel scheduler. Then JIT (Cranelift), then distributed.

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
