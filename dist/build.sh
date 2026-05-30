#!/usr/bin/env sh
# Build a distributable Rivus package for the *host* platform.
#
# The canonical macOS / Windows x64 artifacts are produced by CI
# (.github/workflows/release.yml) on native runners. This script is for
# local/dev packaging on whatever machine you are sitting at — it produces
# the same archive layout so the steps in README "Installation" work for a
# locally built binary too.
#
# Usage:
#   dist/build.sh            # version read from workspace Cargo.toml
#   dist/build.sh 0.1.0      # explicit version label
#
# Output: dist/rivus-v<version>-<host-target>.{tar.gz|zip} (+ .sha256)
set -eu

cd "$(dirname "$0")/.."

VERSION="${1:-$(sed -n 's/^version[[:space:]]*=[[:space:]]*"\(.*\)".*/\1/p' Cargo.toml | head -n1)}"
TARGET="$(rustc -vV | sed -n 's/^host: //p')"
NAME="rivus-v${VERSION}-${TARGET}"

echo "==> building release binary for ${TARGET}"
cargo build --release --locked -p rivus-cli

BIN="target/release/rivus"
[ -f "${BIN}.exe" ] && BIN="${BIN}.exe"

echo "==> staging dist/${NAME}"
rm -rf "dist/${NAME}"
mkdir -p "dist/${NAME}"
cp "${BIN}" "dist/${NAME}/"
cp README.md LICENSE NOTICE "dist/${NAME}/"

case "${TARGET}" in
  *windows*)
    # zip via PowerShell if present, else fall back to `zip`.
    if command -v powershell >/dev/null 2>&1; then
      powershell -NoProfile -Command \
        "Compress-Archive -Path 'dist/${NAME}/*' -DestinationPath 'dist/${NAME}.zip' -Force"
    else
      ( cd dist && zip -qr "${NAME}.zip" "${NAME}" )
    fi
    ARCHIVE="dist/${NAME}.zip"
    ;;
  *)
    tar -C dist -czf "dist/${NAME}.tar.gz" "${NAME}"
    ARCHIVE="dist/${NAME}.tar.gz"
    ;;
esac

# Checksum (shasum on macOS/BSD, sha256sum on Linux).
if command -v shasum >/dev/null 2>&1; then
  ( cd dist && shasum -a 256 "$(basename "${ARCHIVE}")" > "$(basename "${ARCHIVE}").sha256" )
elif command -v sha256sum >/dev/null 2>&1; then
  ( cd dist && sha256sum "$(basename "${ARCHIVE}")" > "$(basename "${ARCHIVE}").sha256" )
fi

echo "==> done: ${ARCHIVE}"
