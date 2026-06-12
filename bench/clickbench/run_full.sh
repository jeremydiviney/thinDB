#!/usr/bin/env bash
# Full ClickBench 100M run: thinDB (warm server, :7880) vs DuckDB (per-process).
set -u
MYSQL="C:/Program Files/MySQL/MySQL Server 8.0/bin/mysql.exe"
DUCK="C:/Users/jerem/AppData/Local/Microsoft/WinGet/Packages/DuckDB.cli_Microsoft.Winget.Source_8wekyb3d8bbwe/duckdb.exe"
DUCKDB="bench/clickbench/duckdb/hits_full.duckdb"
QF="bench/clickbench/queries.sql"

printf "%-3s | %10s | %10s | %7s | %-7s | %s\n" "Q" "thinDB_ms" "duck_ms" "ratio" "status" "rows"
echo "----+------------+------------+---------+---------+------"

idx=0
t_total=0
d_total=0
fails=""
while IFS= read -r sql; do
  [ -z "$sql" ] && continue
  idx=$((idx+1))

  # thinDB (warm)
  ts=$(date +%s%3N)
  tout=$("$MYSQL" -h 127.0.0.1 -P 7880 -u root --database="${THINDB_DB:-clickbench_fsst__public}" -e "$sql" 2>&1)
  te=$(date +%s%3N)
  tms=$((te-ts))

  status="ok"
  # Anchored: a SELECT * header contains the HTTPError column name.
  if echo "$tout" | grep -q "^ERROR"; then
    status="FAIL"
    fails="$fails $idx"
  fi
  rows=$(echo "$tout" | grep -c .)
  rows=$((rows>0 ? rows-1 : 0))

  # DuckDB (per-process)
  ds=$(date +%s%3N)
  "$DUCK" "$DUCKDB" -c "$sql" >/dev/null 2>&1
  de=$(date +%s%3N)
  dms=$((de-ds))

  ratio="-"
  if [ "$dms" -gt 0 ] && [ "$status" = "ok" ]; then
    ratio=$(awk "BEGIN{printf \"%.2f\", $tms/$dms}")
  fi
  t_total=$((t_total+tms))
  d_total=$((d_total+dms))

  printf "%-3s | %10s | %10s | %7s | %-7s | %s\n" "$idx" "$tms" "$dms" "$ratio" "$status" "$rows"
done < "$QF"

echo "----+------------+------------+---------+---------+------"
printf "%-3s | %10s | %10s |\n" "SUM" "$t_total" "$d_total"
echo "thinDB total ${t_total}ms  DuckDB total ${d_total}ms"
[ -n "$fails" ] && echo "FAILED/declined queries:$fails" || echo "All 43 queries ran (no errors)."
