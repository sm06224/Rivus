# Supply-chain policy

Module-based supply-chain attacks are real and trending. Rivus treats every
third-party crate as untrusted-until-vetted and minimizes the trusted surface.

## Posture

- **Zero third-party runtime dependencies.** The shipped binary (`rivus-cli`)
  and every library it links (`rivus-core`, `rivus-ir`, `rivus-parser`,
  `rivus-optimizer`, `rivus-runtime`) depend only on `std`. Verify with:

  ```sh
  cargo tree -p rivus-cli --edges normal   # only rivus-* internal crates
  ```

- **Dev-only dependencies are isolated.** `criterion` (benchmarks) and its
  transitive tree are `[dev-dependencies]`; they never ship in a release build.
  `criterion` is a widely-used, well-maintained crate (bheisler/criterion.rs).

- **No git or alternative-registry dependencies.** Only crates.io is trusted
  (`deny.toml [sources]`).

- **Pinned via `Cargo.lock`.** The lockfile is committed; CI builds from it.

## Enforcement (CI)

| check | tool | what it catches |
|---|---|---|
| secrets | `gitleaks` | committed credentials/tokens |
| advisories | `cargo-deny` | RUSTSEC vulnerabilities, yanked crates |
| licenses | `cargo-deny` | non-permissive / unknown licenses |
| bans | `cargo-deny` | version-wildcards, duplicate versions |
| sources | `cargo-deny` | non-crates.io origins (git/alt registries) |

Config lives in [`deny.toml`](../deny.toml). Run locally:

```sh
cargo deny check                 # all (advisories needs network)
cargo deny check bans sources licenses   # offline subset
gitleaks detect --no-git --source .
```

## Adding a dependency — checklist

Before adding any crate, especially a *new or latest-version* one:

1. **Is it needed?** Prefer `std` or a few lines of our own code. The optimizer,
   parser, and core were written dependency-free on purpose.
2. **Who maintains it?** Established author/org, active maintenance, broad
   downstream usage, source repo reviewed. Beware freshly-published crates and
   typosquats of popular names.
3. **Scope it.** If only for tests/benches, put it under `[dev-dependencies]`.
   Use `default-features = false` and enable only what's required.
4. **Pin & vet.** Commit the updated `Cargo.lock`; run `cargo deny check`.
   Review the *new transitive* crates the addition pulls in, not just the
   direct one.
5. **License.** Must be on the `deny.toml` allow-list (permissive only).

When a runtime dependency eventually becomes justified (e.g. Apache Arrow for
zero-copy columns, per design doc 03), it goes through this checklist and is
added behind the existing trait boundaries so the trusted core stays small.
