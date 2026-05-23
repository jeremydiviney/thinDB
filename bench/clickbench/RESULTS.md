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
| `COUNT(*)` (Q0) | **<1 ms** (metadata-only, 0-column scan) |
| Median query | ~440 ms |
| Total (all 43, best-of-3) | **~24.8 s** (was 28.3 s pre-SIMD; Q29 1499→224 ms via fused col-op-const) |
| Slowest | Q28 `REGEXP_REPLACE` — 2.9 s; Q23 `SELECT * … ORDER BY LIMIT` — 2.5 s |
| Q28 `REGEXP_REPLACE` | 2.9 s — was 22 s before the regex bulk-skip; now GROUP-BY-bound, not regex-bound |

Recent wins folded into this number: **top-k aggregation fusion** (`GROUP BY
<key> ORDER BY <agg> LIMIT k` emits only the k groups — Q33 3.1 s → 0.9 s) and
the **constant-key routing fix** (`GROUP BY 1, URL` no longer reads the literal
key as unknown-cardinality and falls to the sort path — Q34 3.6 s → 1.1 s).
This is the **pre-SIMD baseline** (see the full table below); the SIMD pass
(#145) targets the numeric/scan inner loops next.

The high-cardinality `GROUP BY` queries (Q15-18, Q32-35) pass because of
**column pruning** (projection pushdown): the 105-column table is scanned
for only the 1–4 columns each query references, so blocking operators
(Sort, hash aggregate) buffer a fraction of the rows. Without pruning,
8 of these exceeded the 2 GiB budget. `COUNT(*)` short-circuits to a
manifest row-count with no segment decode at all.

## vs DuckDB (same machine, same 5M data, same queries)

DuckDB run in-process via the CLI (`.timer on`); thinDB over the MySQL wire.

| Engine | Mode | Total (43 queries) | vs thinDB |
|---|---|---:|---:|
| **thinDB** | 1 thread, MySQL wire | **28.3 s** | — |
| DuckDB | 1 thread (`SET threads=1`) | 9.5 s | thinDB **3.0× slower** |
| DuckDB | all cores | 3.5 s | thinDB **8.1× slower** |

thinDB is single-threaded today, so the apples-to-apples engine comparison
is vs DuckDB-1-thread (3.3×); the all-cores number just reflects DuckDB's
parallelism (the gap that auto-partitioned parallel execution, #144, would
close). Per-query, the gap is concentrated in high-cardinality **string**
GROUP BY / ORDER BY:

| Query | thinDB | DuckDB-1t | ratio |
|---|---:|---:|---:|
| Q28 `REGEXP_REPLACE` | **2866 ms** | **3604 ms** | **0.80× (thinDB wins)** |
| Q23 `SELECT * … ORDER BY LIMIT` | 2541 ms | 288 ms | 8.8× |
| Q29 `SUM(...)×~90` | 1905 ms | — | (compute/reduction-bound) |
| Q22 `MIN(URL), MIN(Title), COUNT DISTINCT` | 1530 ms | 289 ms | 5.3× |
| Q32 `WatchID, ClientIP GROUP BY` | 1460 ms | 420 ms | 3.5× |
| Q34 `GROUP BY 1, URL` | 1075 ms | 500 ms | 2.1× (was 3455 ms) |
| Q33 `GROUP BY URL` | 944 ms | 468 ms | 2.0× (was 3062 ms) |

thinDB **beats** DuckDB single-thread on the regex query (Q28 is DuckDB's
own slowest single-thread query). With the string-GROUP-BY queries now
fused/hash-routed, the remaining gap is spread across the scan + numeric
inner loops (Q29 reductions, Q23 wide materialize, filter/LIKE scans) — the
SIMD pass (#145) target — plus parallelism (#144) and spill sort (#240).

## Pre-SIMD baseline — full per-query (best of 3, 2 GiB)

Snapshot taken before the SIMD pass so before/after is attributable. ~5%
run-to-run variance on this machine; treat per-query SIMD deltas as
back-to-back measurements, not diffs against these absolutes.

| Q | ms | Q | ms | Q | ms | Q | ms |
|---|--:|---|--:|---|--:|---|--:|
| Q00 | 0 | Q11 | 222 | Q22 | 1530 | Q33 | 944 |
| Q01 | 256 | Q12 | 379 | Q23 | 2541 | Q34 | 1075 |
| Q02 | 211 | Q13 | 502 | Q24 | 390 | Q35 | 997 |
| Q03 | 208 | Q14 | 812 | Q25 | 443 | Q36 | 231 |
| Q04 | 391 | Q15 | 414 | Q26 | 395 | Q37 | 136 |
| Q05 | 435 | Q16 | 926 | Q27 | 614 | Q38 | 152 |
| Q06 | 187 | Q17 | 708 | Q28 | 2866 | Q39 | 1011 |
| Q07 | 186 | Q18 | 1180 | Q29 | 1905 | Q40 | 88 |
| Q08 | 604 | Q19 | 170 | Q30 | 575 | Q41 | 103 |
| Q09 | 663 | Q20 | 738 | Q31 | 571 | Q42 | 110 |
| Q10 | 220 | Q21 | 792 | Q32 | 1460 | **Total** | **28.3 s** |

### SIMD pass progress (#145)

**Step 1 — aggregate reductions (SUM/MIN/MAX/AVG, no-null fast paths).** Shared
`util/simd.zig` `@Vector` kernels (sum widens through i64 lanes; min/max/float
native), taken when a column has no nulls so the per-row validity branch is
gone. Correct + complete across the compatible aggregates, but **~neutral on
ClickBench**: only Q29 (90 sums) shows a clean gain (~1905→~1680 ms, ~12%); the
MIN/MAX/AVG queries (Q02/Q06/Q27/Q31/Q32) stay within run-to-run noise because
they're **decode/bandwidth-bound, not reduction-bound**. Suite total moved
inside noise (~26–27 s). Kept anyway — it removes branches from the hot loops
and seeds the window-SIMD refactor (#272).

**Step 2 — fuse `col op const` (the big win).** `--profile-ops` (a per-operator
timing flag added this pass) showed Q29's bottleneck wasn't the SUM (~2%) but
the **Compute** materializing 90 derived columns (~87%), via three per-element
passes (smallint→int cast, replicate-literal, add). Recognizing `col +/-/* const`
at resolve time and evaluating it as one fused widening SIMD pass collapsed it:
**Q29 1499 → 224 ms (~6.7×)**, suite **~26 → 24.8 s**, 43/43.

**What the profiler ruled out.** Late materialization for Q23 (`SELECT *` decodes
all 105 cols ×5M to emit 10): real but ~40%, capped by row-group decode
granularity + a Batch-struct-wide change — deferred (#273). SIMD string hashing
for the GROUP-BY family: profiled at ~4% (Q33 is half URL-decode in Scan, half
hash-aggregate; within the aggregate, hashing is ~7% — the rest is probe memcmp
+ cache misses, **memory-bound**). Built it, measured flat, reverted.

**Lesson:** thinDB's ClickBench cost is memory-bound — wide-column decode,
hash-table probe latency, string handling — not numeric inner loops. SIMD paid
off exactly where the work was compute-bound arithmetic (`col op const`); the
remaining levers are algorithmic (late materialization, hash-table layout),
not SIMD.

## Front-end overhead (wire + parser) — negligible

Measured so the 31.8 s is attributed correctly: it is essentially all
query execution, not protocol or parsing.

- **MySQL wire**, warm pooled connection: `SELECT 1` round-trips in
  **0.031 ms**, `COUNT(*)` in 0.095 ms. Result serialization is ~0.1 µs/row
  (returning 9000 more rows, `LIMIT 1000`→`10000`, added ~1 ms). Across all
  43 queries the wire adds ~1–2 ms total. (Loopback TCP; a real network
  adds RTT per query but the protocol itself is cheap.)
- **SQL parser** (text → IR, ReleaseFast): 0.31 µs for `COUNT(*)`, ~1 µs
  for a filter+GROUP BY+ORDER BY, 1.93 µs for Q28 (the most complex). That
  is 5–6 orders of magnitude below execution (170 ms–3.6 s) — effectively
  free.

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
| Q28 | 2718 ms | `REGEXP_REPLACE(Referer, ...) GROUP BY k HAVING COUNT(*) > 100000` |
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
- **Q28** (`REGEXP_REPLACE` host-extract over 5M `Referer` values) was the
  lone slow query at ~22 s, CPU-bound in the Pike-VM regex engine. A
  *stationary-run bulk-skip* (collapsing `[^/]+` / `.*$` loops into one
  forward scan instead of one Pike step per character) plus seed reuse,
  per-batch scratch reuse, a loop-class probe gate, and an unanchored
  first-byte prefilter brought it to **2.7 s** — below DuckDB's single-thread
  time for the same query (3.6 s) and no longer regex-bound. The residual
  cost is the high-cardinality string `GROUP BY` over the extracted hosts
  (compare Q33 `GROUP BY URL`, 3.1 s).
- The full ClickBench dataset is 100M rows; this run uses the 5M-row subset
  for fast iteration. The data file (`data/hits_5m.tsv`) is not checked in.
