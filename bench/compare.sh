#!/usr/bin/env bash
# ETL comparison: Rivus vs DuckDB / awk / Python on the headline task
# (filter age>=50, project name,age, write CSV). Reproduces the
# "External comparison — vs DuckDB / awk / Python" table in docs/BENCHMARKS.md.
#
# Tools that aren't installed are skipped. Data is self-generated (no awk needed
# to build it). Usage: bench/compare.sh [ROWS]   (default 48_000_000)
set -euo pipefail
ROWS="${1:-48000000}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RIVUS="$ROOT/target/release/rivus"
DATA="$(mktemp -t rivus_etl.XXXX.csv)"
OUT="$(mktemp -t rivus_etl_out.XXXX.csv)"
trap 'rm -f "$DATA" "$OUT"' EXIT

cargo build --release -p rivus-cli --manifest-path "$ROOT/Cargo.toml" >/dev/null 2>&1
"$RIVUS" gen clean --rows "$ROWS" --seed 7 > "$DATA"
echo "data: $(wc -l < "$DATA") lines, $(du -h "$DATA" | cut -f1)"

timeit() { local label="$1"; shift; local s e; s=$(date +%s.%N); "$@" >/dev/null 2>&1 || true; e=$(date +%s.%N); printf "  %-12s %ss\n" "$label" "$(echo "$e - $s" | bc)"; }
have() { command -v "$1" >/dev/null 2>&1; }

timeit rivus "$RIVUS" run -c "F: open $DATA |? age >= 50 |> name age save $OUT ;"
have awk    && timeit awk    awk -F, 'NR>1&&$3>=50{print $2","$3}' "$DATA"
have python3 && timeit python3 python3 -c "
import csv,sys
w=csv.writer(open('$OUT','w'))
for r in csv.reader(open('$DATA')):
    if r and r[0]!='id' and r[2].isdigit() and int(r[2])>=50: w.writerow([r[1],r[2]])
"
have duckdb && timeit duckdb duckdb -c \
  "COPY (SELECT name,age FROM read_csv('$DATA') WHERE age>=50) TO '$OUT' (HEADER);"
