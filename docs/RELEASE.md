# Releasing Rivus

Merging PRs and the pre-release gate (incl. the `gitleaks` secret scan) are
delegated to the autonomous agent; **cutting a release is the maintainer's alone**
(delegation updated 2026-06-03). The agent merges `dev → main` and guarantees the
gate is green; the **maintainer** then cuts the release by pushing a `v*` tag (or
running the **Release** workflow manually), and `.github/workflows/release.yml`
builds and publishes the binaries.

## Pre-release gate (must be green first)

Same as the per-push gate, plus a clean secret scan:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets        # zero warnings (CI uses -D warnings)
cargo test --workspace
cargo deny check bans sources licenses        # advisories needs network → CI
gitleaks detect --no-git --source .           # no secrets, ever
cargo build --release --locked -p rivus-cli   # default build stays dependency-zero
```

Never tag a release on a red gate.

## Target matrix (`release.yml`)

Each platform ships a **portable** (widest-compatibility) build and one or more
**tuned** (`-C target-cpu=…`) builds that are faster but require that CPU
generation or newer.

| Platform | Label | `target-cpu` | Notes |
|---|---|---|---|
| Windows 11 x64 | **`windows-x64-intel-tigerlake`** ★ | `tigerlake` | **Featured / recommended.** The maintainer's primary machine: 11th-gen Intel **Core i5, 4C/8T (Tiger Lake)**, Win11 64-bit. Enables AVX2 + **AVX-512** + BMI2 — exploited by the SWAR/SIMD CSV scan and the exact integer lanes (decimal/datetime/duration). |
| Windows 11 x64 | `windows-x64-portable` | `x86-64` | Any x64 (Intel & older AMD). |
| Windows 11 x64 | `windows-x64-amd-zen4` | `znver4` | AMD Ryzen 7000+. |
| Windows 11 x64 | `windows-x64-amd-zen3` | `znver3` | AMD Ryzen 5000+. |
| macOS arm64 | `macos-arm64` | (portable) | Apple Silicon M1+. |
| macOS arm64 | `macos-arm64-appleM-tuned` | `apple-m1` | Tuned, runs on all M1+. |

Other platforms: build from source (README → Installation).

### Release contents (each archive)

Every per-platform archive bundles, alongside the `rivus` binary: `README.md`,
`LICENSE`, `NOTICE`, and the **usage guides `GUIDE.md` (EN) + `GUIDE.ja.md`
(JA)** — so the docs ship with every release and there's no need to look up
usage separately (maintainer request 2026-06-06). The per-step gate already
requires the EN/JA guides updated together with each feature PR, so a release
always carries an accurate guide.

### Standing rule — favor the maintainer's lineup

When adding release artifacts, the maintainer's own machines are **first-class
and featured** (listed first, recommended in the release notes). Current
lineup: 11th-gen Intel Core i5 / Tiger Lake / Win11 x64 (★ above). Add new
machines here as they join the lineup and tune a dedicated `target-cpu` build
for each.

> Note: 11th-gen Core i5 at **4C/8T** is Tiger Lake (mobile, e.g. i5-1135G7) —
> hence `target-cpu=tigerlake`. (Rocket Lake desktop i5 are 6C/12T → would use
> `rocketlake` if that machine is ever added.)
