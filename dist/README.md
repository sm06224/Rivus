# dist/ — distribution & packaging

Rivus ships **pre-built binaries for macOS (Apple Silicon) and Windows 11+
(x64)**. Every other platform builds from source (it's a zero-dependency,
std-only Rust workspace, so `cargo build --release` is all it takes).

Two flavors are published per platform:

- **portable** — a multi-architecture baseline build that runs on the widest
  range of CPUs in that family (Intel x64 included).
- **tuned** — a microarchitecture-specialized build (`-C target-cpu=…`),
  faster but only runs on that CPU generation or newer.

## How releases are cut

A push of a `v*` tag (e.g. `v0.1.0`) triggers
[`.github/workflows/release.yml`](../.github/workflows/release.yml), which builds
and publishes these assets (each with `README`/`LICENSE`/`NOTICE` + `.sha256`):

| runner | target | `target-cpu` | asset label |
|---|---|---|---|
| `macos-14` (Apple Silicon) | `aarch64-apple-darwin` | *(baseline)* | `macos-arm64` |
| `macos-14` (Apple Silicon) | `aarch64-apple-darwin` | `apple-m1` | `macos-arm64-appleM-tuned` |
| `windows-latest` | `x86_64-pc-windows-msvc` | `x86-64` | `windows-x64-portable` |
| `windows-latest` | `x86_64-pc-windows-msvc` | `znver4` | `windows-x64-amd-zen4` |
| `windows-latest` | `x86_64-pc-windows-msvc` | `znver3` | `windows-x64-amd-zen3` |

macOS is **Apple Silicon only** (no legacy Intel Mac); the Windows portable
build is the one to use on **Intel** hardware. Publishing uses the official
`gh` CLI — no extra third-party Action is added.

```sh
git tag v0.1.0
git push origin v0.1.0      # → Release with macOS arm64 + Windows x64 assets
```

(You can also run it manually from the Actions tab via *workflow_dispatch*.)

## Local packaging

To produce the same archive layout for the machine you're on:

```sh
dist/build.sh               # → dist/rivus-v<version>-<host-target>.tar.gz
```

This is handy for a quick local hand-off; the published macOS/Windows
artifacts always come from CI on native runners.
