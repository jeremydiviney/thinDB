# ClickBench Results

End-to-end run of the 43 standard [ClickBench](https://benchmark.clickhouse.com/)
queries against thinDB over the **MySQL wire** (mysql2 driver).

**Setup:** 5,000,000-row `hits` table (105 columns), compacted to 6 segments.
ReleaseFast `thindb-server`, AMD Ryzen 9 9900X / Windows 11, query memory
budget 2 GiB. Times are wall-clock per query including wire round-trip +
result formatting.

**To reproduce:**

```
zig build clickbench -- bench/clickbench/data/hits_5m.tsv      # load (ReleaseFast)
zig build -Doptimize=ReleaseFast                               # build the server
./zig-out/bin/thindb-server --data-dir .clickbench-db --mysql-port 7880
# then, from tests/bun:
THINDB_MYSQL_PORT=7880 THINDB_DB=clickbench__public bun run clickbench/run_queries.ts
```

## Summary

| Metric | Value |
|---|---:|
| Queries passing | **43 / 43** |
| `COUNT(*)` (Q0) | **3 ms** (metadata-only, 0-column scan) |
| Median query | ~520 ms |
| Total (all 43) | ~51 s |
| Slowest | Q28 `REGEXP_REPLACE` — 22 s (regex-bound, CPU not memory) |
| Slowest excluding Q28 | Q34 `GROUP BY URL` — 3.5 s |

The high-cardinality `GROUP BY` queries (Q15-18, Q32-35) pass because of
**column pruning** (projection pushdown): the 105-column table is scanned
for only the 1–4 columns each query references, so blocking operators
(Sort, hash aggregate) buffer a fraction of the rows. Without pruning,
8 of these exceeded the 2 GiB budget. `COUNT(*)` short-circuits to a
manifest row-count with no segment decode at all.

## Per-query (ReleaseFast, 5M rows)

| Q | Time | Query |
|---|---:|---|
| Q00 | 3 ms | `SELECT COUNT(*) FROM hits` |
| Q01 | 170 ms | `COUNT(*) WHERE AdvEngineID <> 0` |
| Q02 | 178 ms | `SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth)` |
| Q03 | 190 ms | `AVG(UserID)` |
| Q04 | 325 ms | `COUNT(DISTINCT UserID)` |
| Q05 | 379 ms | `COUNT(DISTINCT SearchPhrase)` |
| Q06 | 174 ms | `MIN(EventDate), MAX(EventDate)` |
| Q07 | 175 ms | `AdvEngineID, COUNT(*) ... GROUP BY AdvEngineID` |
| Q08 | 500 ms | `RegionID, COUNT(DISTINCT UserID) GROUP BY RegionID` |
| Q09 | 607 ms | `RegionID, SUM/COUNT/AVG/COUNT(DISTINCT) GROUP BY RegionID` |
| Q10 | 225 ms | `MobilePhoneModel, COUNT(DISTINCT UserID)` |
| Q11 | 232 ms | `MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID)` |
| Q12 | 640 ms | `SearchPhrase, COUNT(*) GROUP BY SearchPhrase` |
| Q13 | 703 ms | `SearchPhrase, COUNT(DISTINCT UserID)` |
| Q14 | 684 ms | `SearchEngineID, SearchPhrase, COUNT(*)` |
| Q15 | 564 ms | `UserID, COUNT(*) GROUP BY UserID` |
| Q16 | 842 ms | `UserID, SearchPhrase, COUNT(*) GROUP BY UserID, SearchPhrase` |
| Q17 | 660 ms | `UserID, SearchPhrase, COUNT(*) (no ORDER BY)` |
| Q18 | 1081 ms | `UserID, minute(EventTime), SearchPhrase, COUNT(*)` |
| Q19 | 169 ms | `UserID WHERE UserID = <const>` |
| Q20 | 723 ms | `COUNT(*) WHERE URL LIKE '%google%'` |
| Q21 | 787 ms | `SearchPhrase, MIN(URL), COUNT(*) WHERE URL LIKE '%google%'` |
| Q22 | 1484 ms | `SearchPhrase, MIN(URL), MIN(Title), COUNT(*), COUNT(DISTINCT UserID)` |
| Q23 | 2417 ms | `SELECT * WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10` |
| Q24 | 374 ms | `SearchPhrase ORDER BY EventTime LIMIT 10` |
| Q25 | 415 ms | `SearchPhrase ORDER BY SearchPhrase LIMIT 10` |
| Q26 | 386 ms | `SearchPhrase ORDER BY EventTime, SearchPhrase LIMIT 10` |
| Q27 | 600 ms | `CounterID, AVG(length(URL)), COUNT(*) HAVING COUNT(*) > 100000` |
| Q28 | 21956 ms | `REGEXP_REPLACE(Referer, ...) GROUP BY k HAVING COUNT(*) > 100000` |
| Q29 | 1795 ms | `90 × SUM(ResolutionWidth + k)` |
| Q30 | 569 ms | `SearchEngineID, ClientIP, COUNT(*), SUM(IsRefresh), AVG(ResolutionWidth)` |
| Q31 | 516 ms | `WatchID, ClientIP, COUNT(*), ... WHERE SearchPhrase <> ''` |
| Q32 | 1313 ms | `WatchID, ClientIP, COUNT(*), ... (no WHERE)` |
| Q33 | 3088 ms | `URL, COUNT(*) GROUP BY URL ORDER BY c DESC LIMIT 10` |
| Q34 | 3501 ms | `1, URL, COUNT(*) GROUP BY 1, URL` |
| Q35 | 924 ms | `ClientIP, ClientIP-1..-3, COUNT(*) GROUP BY ...` |
| Q36 | 392 ms | `URL, COUNT(*) WHERE CounterID=62 AND EventDate range ...` |
| Q37 | 213 ms | `Title, COUNT(*) WHERE CounterID=62 ...` |
| Q38 | 121 ms | `URL, COUNT(*) WHERE ... AND IsLink/IsDownload ...` |
| Q39 | 824 ms | `TraficSourceID, SearchEngineID, AdvEngineID, CASE ... GROUP BY ...` |
| Q40 | 83 ms | `URLHash, EventDate, COUNT(*) WHERE ... RefererHash = <const>` |
| Q41 | 80 ms | `WindowClientWidth, WindowClientHeight, COUNT(*) WHERE URLHash = <const>` |
| Q42 | 115 ms | `DATE_TRUNC('minute', EventTime), COUNT(*) GROUP BY ...` |

**Notes**
- **Q28** is the lone slow query: `REGEXP_REPLACE` over 5M `Referer` values is
  CPU-bound in the Pike-VM regex engine. A Debug build exceeds the 120 s
  client timeout; ReleaseFast brings it to ~22 s. Optimizing the regex hot
  loop is the obvious next target for this query.
- The full ClickBench dataset is 100M rows; this run uses the 5M-row subset
  for fast iteration. The data file (`data/hits_5m.tsv`) is not checked in.
