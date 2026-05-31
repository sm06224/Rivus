#!/usr/bin/env bash
# Search-pattern comparison: grep / ripgrep / (DuckDB if installed) vs Rivus,
# on self-generated data (no awk/python needed). Reproduces the matrix in
# docs/BENCHMARKS.md "Search-pattern matrix".
#
# Usage: bench/search.sh [ROWS]   (default 5_000_000)
set -euo pipefail
ROWS="${1:-5000000}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RIVUS="$ROOT/target/release/rivus"
DATA="$(mktemp -t rivus_search.XXXX.csv)"
trap 'rm -f "$DATA"' EXIT

cargo build --release -p rivus-cli --manifest-path "$ROOT/Cargo.toml" >/dev/null 2>&1
"$RIVUS" gen clean --rows "$ROWS" --seed 7 > "$DATA"
echo "data: $DATA ($(wc -l < "$DATA") lines, $(du -h "$DATA" | cut -f1))"

# median-of-3 wall time of a command (output discarded)
bench() {
  local label="$1"; shift
  local best=""
  for _ in 1 2 3; do
    local s e d
    s=$(date +%s.%N); "$@" >/dev/null 2>&1 || true; e=$(date +%s.%N)
    d=$(echo "$e - $s" | bc)
    if [ -z "$best" ] || (( $(echo "$d < $best" | bc) )); then best=$d; fi
  done
  printf "  %-30s %ss\n" "$label" "$best"
}
have() { command -v "$1" >/dev/null 2>&1; }

echo "== literal: country == JP =="
have grep && bench "grep -c ,JP," grep -c ",JP," "$DATA"
have rg   && bench "rg -c ,JP,"   rg -c ",JP," "$DATA"
bench "rivus contains" "$RIVUS" run -c "F: open $DATA |? contains(country, \"JP\") save - ;"
if have duckdb; then
  bench "duckdb LIKE" duckdb -c \
    "SELECT count(*) FROM read_csv('$DATA') WHERE country LIKE '%JP%';"
fi

echo "== prefix: name LIKE aki% =="
have rg && bench "rg ^...,aki" rg -ce '^[0-9]+,aki' "$DATA"
bench "rivus like aki%" "$RIVUS" run -c "F: open $DATA |? like(name, \"aki%\") save - ;"
if have duckdb; then
  bench "duckdb LIKE aki%" duckdb -c \
    "SELECT count(*) FROM read_csv('$DATA') WHERE name LIKE 'aki%';"
fi

echo "== IN-set: country in (JP,DE,BR) =="
have rg && bench "rg (JP|DE|BR)" rg -ce ',(JP|DE|BR),' "$DATA"
bench "rivus or-chain" "$RIVUS" run -c \
  "F: open $DATA |? country == \"JP\" or country == \"DE\" or country == \"BR\" save - ;"
if have duckdb; then
  bench "duckdb IN" duckdb -c \
    "SELECT count(*) FROM read_csv('$DATA') WHERE country IN ('JP','DE','BR');"
fi

echo "== typed (grep CANNOT express): age >= 50 =="
bench "rivus age>=50" "$RIVUS" run -c "F: open $DATA |? age >= 50 |> name age save - ;"
if have duckdb; then
  bench "duckdb age>=50" duckdb -c \
    "SELECT name,age FROM read_csv('$DATA') WHERE age >= 50;"
fi
