# dist/ — distribution & packaging

Rivus ships **pre-built x64 binaries for macOS and Windows**. Every other
platform builds from source (it's a zero-dependency, std-only Rust workspace,
so `cargo build --release` is all it takes).

## How releases are cut

A push of a `v*` tag (e.g. `v0.1.0`) triggers
[`.github/workflows/release.yml`](../.github/workflows/release.yml), which:

1. builds `rivus` on a **macOS Intel** runner (`x86_64-apple-darwin`) and a
   **Windows** runner (`x86_64-pc-windows-msvc`),
2. packages each as `rivus-<tag>-<target>.{tar.gz|zip}` together with
   `README`, `LICENSE`, `NOTICE`, plus a `.sha256` checksum, and
3. attaches them to the GitHub Release for that tag.

```sh
git tag v0.1.0
git push origin v0.1.0      # → Release with macOS + Windows x64 assets
```

(You can also run it manually from the Actions tab via *workflow_dispatch*.)

## Local packaging

To produce the same archive layout for the machine you're on:

```sh
dist/build.sh               # → dist/rivus-v<version>-<host-target>.tar.gz
```

This is handy for a quick local hand-off; the published macOS/Windows
artifacts always come from CI on native runners.
