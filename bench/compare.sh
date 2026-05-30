#!/usr/bin/env bash
# External comparison: same logical task (read CSV, filter age >= 45, write the
# surviving `name` column) across Rivus and established tools, to ground Rivus's
# numbers in reality (anti-NIH; learn from collective-wisdom engines).
#
# Tools are used only if present. Nothing here is a runtime dependency of Rivus;
# this is a measurement harness. Reports the best of N wall-clock runs.
#
#   bench/compare.sh [ROWS] [RUNS]
set -euo pipefail

ROWS="${1:-1000000}"
RUNS="${2:-3}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
CSV="$TMP/data.csv"
RIV="$TMP/q.riv"
RIVUS="${RIVUS_BIN:-./target/release/rivus}"

echo "# External comparison — filter age>=45, project name"
echo "rows=$ROWS  runs=$RUNS (best wall-clock shown)"
echo

# --- deterministic CSV fixture (LCG in awk; same file for every tool) --------
awk -v n="$ROWS" 'BEGIN{
  print "id,name,age,score,country,active";
  s=12345; split("JP US DE FR BR",C," "); split("aki ben cho dee eri fum",NM," ");
  for(i=0;i<n;i++){
    s=(s*1103515245+12345)%2147483648; age=s%90;
    s=(s*1103515245+12345)%2147483648; sc=(s%10000)/100;
    s=(s*1103515245+12345)%2147483648; c=C[(s%5)+1];
    s=(s*1103515245+12345)%2147483648; act=(s%2)?"true":"false";
    print i","NM[(i%6)+1] i","age","sc","c","act;
  }
}' > "$CSV"
printf 'F:\n    open %s\n    |? age >= 45\n    |> name\n    save %s/out_rivus.csv\n;\n' "$CSV" "$TMP" > "$RIV"
echo "fixture: $(wc -l < "$CSV") lines, $(du -h "$CSV" | cut -f1)"
echo

# --- timing helper: best of RUNS, prints seconds ------------------------------
best() {
  local name="$1"; shift
  local best="" t0 t1 dt
  for _ in $(seq 1 "$RUNS"); do
    t0=$(date +%s.%N)
    "$@" >/dev/null 2>&1 || { printf "  %-22s FAILED\n" "$name"; return; }
    t1=$(date +%s.%N)
    dt=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.4f", b-a}')
    if [ -z "$best" ] || awk -v d="$dt" -v b="$best" 'BEGIN{exit !(d<b)}'; then best="$dt"; fi
  done
  local rps
  rps=$(awk -v r="$ROWS" -v s="$best" 'BEGIN{printf "%.2f", r/s/1e6}')
  printf "  %-22s %8ss   %7s M rows/s\n" "$name" "$best" "$rps"
}

run_rivus() { "$RIVUS" run "$RIV" --no-opt; }
run_rivus_opt() { "$RIVUS" run "$RIV"; }
run_awk() { awk -F, 'NR>1 && $3>=45 {print $2}' "$CSV" > "$TMP/out_awk.csv"; }
run_duckdb() { duckdb -c "COPY (SELECT name FROM read_csv_auto('$CSV', header=true) WHERE age>=45) TO '$TMP/out_duck.csv' (HEADER false);"; }
run_python() { python3 - "$CSV" "$TMP/out_py.csv" <<'PY'
import csv,sys
with open(sys.argv[1],newline='') as f, open(sys.argv[2],'w',newline='') as o:
    r=csv.reader(f); w=csv.writer(o); next(r,None)
    for row in r:
        if len(row)==6 and int(row[2])>=45: w.writerow([row[1]])
PY
}

command -v "$RIVUS" >/dev/null 2>&1 || RIVUS="rivus"
best "rivus (--no-opt)"  run_rivus
best "rivus (optimized)" run_rivus_opt
command -v awk     >/dev/null 2>&1 && best "awk (mawk)"      run_awk
command -v duckdb  >/dev/null 2>&1 && best "duckdb"          run_duckdb
command -v python3 >/dev/null 2>&1 && best "python (stdlib)" run_python

echo
echo "note: rivus reads the whole file + builds all 6 columns; awk/python stream;"
echo "      duckdb is vectorized + multi-threaded. Projection pushdown (optimized)"
echo "      lets rivus build only the columns the query needs."
