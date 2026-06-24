# Supply-chain policy

Module-based supply-chain attacks are real and trending. Rivus treats every
third-party crate as untrusted-until-vetted and minimizes the trusted surface.

## Posture

- **The default build has zero third-party runtime dependencies.** With default
  features, `rivus-cli` and every library it links (`rivus-core`, `rivus-ir`,
  `rivus-parser`, `rivus-optimizer`, `rivus-runtime`) depend only on `std`.
  Verify with:

  ```sh
  cargo tree -p rivus-cli --edges normal   # only rivus-* internal crates
  ```

- **Heavy/standard formats are opt-in, behind feature flags.** Some formats
  (compression, Parquet, pickle) are not reasonable to reimplement and need a
  mature ecosystem crate. These live behind **off-by-default cargo features**
  (e.g. `--features gzip,parquet`) and behind the existing source/sink trait
  boundary, so:
  - the default binary stays dependency-free and auditable;
  - a user opts in explicitly, pulling a *vetted* crate and its reviewed tree;
  - the core engine never depends on them.

- **Some opt-in features add *no* dependency at all.** The `net` feature
  (networking execution, §33 — `open "http://…"`) is implemented entirely on
  `std::net` (a minimal HTTP/1.1 GET client and a TCP `subscribe` dial), so it
  pulls **zero** third-party crates: `cargo tree -p rivus-runtime --features net
  --edges normal` shows only `rivus-*`. It is gated off-by-default purely so the
  *lean* build's surface is mechanically minimal; the transport itself is as
  dependency-free as the core. (Per §28.12.1a, deps are taken only when a
  capability can't be done in std — HTTP and a TCP feed can. The QUIC
  alternative — quinn/rustls/ring — is a later slice.)

- **Dev-only dependencies are isolated.** `criterion` (benchmarks) and its tree
  are `[dev-dependencies]`; they never ship in a release build.

- **No git or alternative-registry dependencies.** Only crates.io
  (`deny.toml [sources]`). **Pinned via `Cargo.lock`** (committed; CI builds from it).

## Enforcement (CI)

| check | tool | what it catches |
|---|---|---|
| secrets | `gitleaks` | committed credentials/tokens |
| advisories | `cargo-deny` | RUSTSEC vulnerabilities, yanked crates |
| licenses | `cargo-deny` | non-permissive / unknown licenses |
| bans | `cargo-deny` | version-wildcards, duplicate versions |
| sources | `cargo-deny` | non-crates.io origins (git/alt registries) |

Config lives in [`deny.toml`](../deny.toml). Feature-gated deps are checked with
`cargo deny check --all-features`. Run locally:

```sh
cargo deny check                          # default features
cargo deny check --all-features           # incl. optional format adapters
cargo deny check bans sources licenses    # offline subset
gitleaks detect --no-git --source .
```

## Vetting criteria for inviting a crate

A candidate must clear **all** of these (the maintainer's bar: *not obsolete,
major, stable, selectively verified*):

1. **Necessary** — `std` or a few lines of our own won't do (true for
   compression/Parquet/pickle; false for CSV/JSON, which we own).
2. **Mature & major** — multi-year history, large download counts, broad
   downstream use; **not** a fresh crate or a typosquat of a popular name.
3. **Stable** — a released `1.x` (or a long-stable `0.x` that is the de-facto
   standard); not abandoned, recent commits/releases.
4. **Maintained by a trusted org/author** — repo reviewed, issues triaged.
5. **Permissive license** — on the `deny.toml` allow-list (MIT/Apache-2.0/…).
6. **Transitive tree reviewed** — vet what it pulls in, not just the crate;
   prefer pure-Rust backends (no surprise C build) where credible.
7. **Isolated** — behind a feature flag and a trait boundary; `default-features
   = false`, enable only what's needed.

## Selected adapters (vetting log)

Approved for adoption when their format lands (each enters via the checklist and
a committed `Cargo.lock` + `cargo deny check --all-features`):

| need | crate | feature | why it clears the bar |
|---|---|---|---|
| **gzip / DEFLATE** ✅ *integrated* | [`flate2`](https://crates.io/crates/flate2) (pure-Rust `miniz_oxide` backend) | `gzip` | de-facto standard, ~100M+ downloads, rust-lang-adjacent maintenance, MIT/Apache-2.0, stable 1.x; pure-Rust backend avoids a C toolchain. **Adopted**: `default-features = false, features = ["rust_backend"]`, behind the source trait (`open *.gz`). Transitive tree (all pure-Rust, permissive): `crc32fast`→`cfg-if`, `miniz_oxide`→`adler2` (added `0BSD` to the license allow-list), `simd-adler32`. `cargo deny check --all-features` green. |
| **zstd (decode)** ✅ *integrated* | [`ruzstd`](https://crates.io/crates/ruzstd) | `zstd` | **pure-Rust decoder, no C**, MIT, established. **Adopted** for `.zst` input behind the source trait (`open *.zst`). Runtime tree (all pure-Rust, permissive): `ruzstd`→`twox-hash` (MIT). The encode-side [`zstd`](https://crates.io/crates/zstd) crate (C bindings, `zstd-sys`) is used **only as a `[dev-dependency]`** to write `.zst` test fixtures — it never ships in any build. `cargo deny check --all-features` green; default `cargo tree -p rivus-cli` stays rivus-only. |
| **file-change notification (`watch`)** ✅ *integrated (slice 5)* | [`notify`](https://crates.io/crates/notify) (notify-rs) | `unbounded` | **#149 ② amended ruling**: std polling rejected (needless OS load) → subscribe to the OS mechanism (inotify / kqueue / FSEvents / ReadDirectoryChangesW); 統括明言「依存ゼロは原則であって、依存なしで実装可能になるまでは依存ありで可」. `notify` is the de-facto standard watcher (notify-rs org, used across the ecosystem: cargo-watch / watchexec / rust-analyzer lineage), multi-year history, pinned to the **latest stable line 8.x (8.2.0 in `Cargo.lock`)** — the 9.x release candidates are *not* stable and are not taken. License **CC0-1.0** (already on the `deny.toml` allow-list). **Adopted**: `default-features = false, features = ["macos_fsevent"]` (its only default feature, kept explicitly so macOS uses FSEvents rather than one-fd-per-file kqueue), behind the `unbounded` feature (#149 ⑤) and the Discovery boundary — the core engine never references it. Linux transitive tree (all pure-Rust / FFI-decl only, no C build step; permissive): `inotify`→`inotify-sys`+`bitflags`, `libc`, `log`, `mio`, `notify-types`→`bitflags`, `walkdir`→`same-file`; platform-conditional: `fsevent-sys` (macOS), `kqueue`/`kqueue-sys` (BSD), `windows-sys` (Windows). Default `cargo tree -p rivus-cli` stays rivus-only; `cargo deny check --all-features` verified in CI. |
| **Parquet / Arrow** | [`parquet`](https://crates.io/crates/parquet) + [`arrow`](https://crates.io/crates/arrow) (apache/arrow-rs) | `parquet` | official Apache project, the standard, actively released; heavy transitive tree → strictly feature-gated and isolated |
| **Python pickle** | [`serde-pickle`](https://crates.io/crates/serde-pickle) | `pickle` | the established pickle crate, maintained, MIT/Apache-2.0 |

**Streaming note for compressed inputs:** a compressed stream can't be
arbitrarily seeked, so the byte-range *parallel* reader and the two-pass
seek-back don't apply. Compressed sources use a **serial, single-pass** path
(sample-infer the schema like preview, then stream-decode once) — still bounded
memory, just not parallel. This is a deliberate, documented trade-off.

## Adding a dependency — checklist (every time)

1. Confirm it's **needed** and on the selected-adapters list (or justify a new
   entry against the vetting criteria above).
2. Add it **optional + feature-gated**: `foo = { version = "1", optional = true,
   default-features = false }`, and a `feature = ["dep:foo"]`.
3. Commit the updated `Cargo.lock`; run `cargo deny check --all-features` and
   review the **new transitive** crates, not just the direct one.
4. Wire it behind a source/sink trait so the core never references it.
5. Update this vetting log and `deny.toml` if a new (permissive) license appears.

