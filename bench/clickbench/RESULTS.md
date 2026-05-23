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
| Total (all 43, best-of-3) | **~7.8 s** (24.8 → 21.1 pool → 18.6 hash-route → 18.0 MIN/MAX → 13.6 per-column reads → 12.1 LIKE → 11.4 flat agg state → 11.3 mask-guided AND → 10.9 single-row accumulate → 10.1 late materialization → 7.8 prefetch+int-key group table) |
| Slowest | Q28 `REGEXP_REPLACE` — 2.4 s (CPU-bound); Q23 `SELECT *` — 1.2 s; Q32 GROUP BY — 1.2 s |
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

## Buffer pool — cross-query decompressed-block cache (live)

Until now the per-table LRU block cache existed but was **dead**: the scan
called the null-cache decode path, so every query re-read and re-`zstd`-
decompressed every column block it touched — even back-to-back runs of the
same query. DuckDB, by contrast, runs ClickBench fully warm: with no
`memory_limit` set it sizes its buffer pool to ~80% of RAM (~49 GB here),
so after the first touch the entire 5M dataset is resident and every query
is decompress-free. thinDB's "best-of-3" was therefore **warm-vs-cold**
against DuckDB — re-decompressing on every rep.

This pass wires the scan through the cache and turns it into a proper
pinning buffer pool (`--cache-size`, default 256 MiB; run at **8 GiB**):

- **Pin/unpin (in-use accounting).** `acquire`/`insertPinned` bump a refcount;
  eviction walks the LRU tail and frees only **unpinned** blocks. A block a
  reader still holds is never freed, so the pool is correct for datasets
  **larger than the cache** — when the whole tail is pinned it temporarily
  exceeds budget rather than free an in-use block. Pins are held only across
  one block decode and dropped via `defer`, so a query that errors mid-decode
  can't leak a pin and wedge a block.
- **Concurrency.** A spinlock guards the O(1) metadata (map / LRU links / byte
  counter / pin counts), never held across decompress or decode — fine for
  the thread-per-connection server.
- **Coherence is free.** Segments are immutable and segment IDs come from a
  monotonic never-reused counter, so a cached block `(segment_id, rg, col)` is
  valid for the life of its segment and keys never collide with a future
  segment. Compaction's retired segments are simply never looked up again and
  age out via LRU; the memtable is never cached. No invalidation logic.

Effect is exactly where the cost is decompress-bound — the wide-projection /
string-scan queries — and absent where it's CPU- or GROUP-BY-bound. Cold
(rep 1, populates the pool) vs warm (reps 2-3, cache hit), same process:

| Query | cold | warm | warm Δ |
|---|---:|---:|---:|
| Q23 `SELECT * … LIMIT 10` | 2176 ms | **1289 ms** | −41% |
| Q20 `COUNT(*) WHERE URL LIKE` | 766 ms | **515 ms** | −33% |
| Q22 `MIN(URL),MIN(Title),COUNT DISTINCT` | 1242 ms | **962 ms** | −23% |
| Q24 `SearchPhrase ORDER BY … LIMIT` | 409 ms | **312 ms** | −24% |
| Q28 `REGEXP_REPLACE` (CPU-bound) | 2823 ms | 2709 ms | ~flat |

Suite total best-of-3 **24.8 s → ~21.1 s (−15%)**, 43/43, all of it in the
decode-bound queries. The remaining levers stay algorithmic: late
materialization for Q23 (#273), spill sort (#240), parallelism (#144).

## High-cardinality GROUP BY — hash routing (live)

A hash GROUP BY is one O(n) pass; the sort path is O(n log n). Hash wins on
every high-card query we measured — by 2× when the keys actually compress:

| Query | groups | sort | hash | |
|---|--:|--:|--:|---|
| Q35 `ClientIP, −1, −2, −3` | 730 K | 874 ms | **400 ms** | 2.2× (6.8:1 compression) |
| Q39 `… CASE …, URL GROUP BY` | — | 798 ms | **242 ms** | 3.3× |
| Q18 `UserID, minute, SearchPhrase` | 2.85 M | 997 ms | **894 ms** | |
| Q32 `WatchID, ClientIP` | 5.0 M | 1290 ms | **1128 ms** | near-unique, only log-factor |

These were routing to **sort** for two avoidable reasons, both fixed:

1. **State compaction.** The per-group accumulator `AccState` was 48 B (sized
   by its widest *rare* variant). Moving `min/max_large` to an inline `align(8)`
   struct and lazily boxing `group_concat` shrank it to **32 B** with no hot-path
   change (`count`/`sum`/`avg`/`min`/`max`/`distinct` all stay inline). That
   raised the routing threshold from 4.26M → 5.26M groups — enough that Q32's 5M
   now fits.
2. **Estimate fix (clamp + unknown-as-ceiling).** The router estimated groups as
   the *product of per-key NDVs* and bailed to sort on any `unknown` key. The
   product assumes independence and explodes when keys are correlated
   (`WatchID × ClientIP = 5M × 730K = 3.6e12` vs the real 5M); and computed keys
   (`minute(…)`, `ClientIP−k`, `CASE …`) are always `unknown`. Fix: the combined
   group count can never exceed the input row count, so clamp the estimate to
   `upper_rows`, and treat `unknown` keys as that same ceiling instead of giving
   up. Provably memory-safe (the ceiling is a true upper bound, so if the
   estimate fits, the real table fits). This is what unlocked Q18/Q35/Q39 — their
   computed keys no longer force a sort.

Deferred: functional-dependency tightening (a key that's a function of another
key adds no groups) — gives a tighter estimate but changes no routing decision
at this scale, since the row-count clamp already routes every case correctly.

## Metadata-only MIN/MAX (live)

`MIN(c)`/`MAX(c)` over a bare table (no GROUP BY, no WHERE) is answered by
folding the manifest's per-segment column min/max — no scan, no decode:

| Query | before | after | |
|---|--:|--:|---|
| Q06 `MIN/MAX(EventDate)` | 162 ms | **0.4 ms** | ~400×; **beats** DuckDB's 1 ms |

A new `MinMaxStats` leaf operator (src/exec/agg_stats.zig) is substituted at
plan time when the pattern matches and the columns carry exact stats. It falls
back to the normal scan+aggregate for: float/double (no stats) and string
(16-byte-prefix, approximate) columns; a populated memtable (unflushed rows);
or any tombstone (a deleted row could have been the extreme).

**Nullable columns supported.** The segment writer's `computeStats` now skips
NULL slots (previously it folded the placeholder value at null positions,
polluting the extreme), and an all-null row group stores an inverted
`min > max` "no values" sentinel that the fold ignores. So `MIN/MAX` over a
nullable column is both correct (NULLs excluded, per SQL semantics) and
served from stats. Verified end-to-end by re-importing the 5M dataset under
the null-aware writer (43/43, Q06 value unchanged).

## Per-column reads (live) — the ~160 ms scan floor, removed

Profiling the worst-vs-DuckDB queries showed a **column-count-independent ~160 ms
floor** on the cheap ones (Q01 1 col, Q02 2 cols, Q03 1 col — all ~165 ms). The
cause: `readSegment` read the **entire segment file** (~647 MB across all 105
columns) into memory per query; column pruning skipped *decoding* unused columns
but their bytes were still *read*. (A bulk-`memcpy` decode rewrite was a no-op in
ReleaseFast — LLVM already lowered the per-element `readInt` loop to a memcpy —
which confirmed decode was never the bottleneck.)

Fix (segment format v8): the footer now stores each column block's file offset
per row group, so the reader `pread`s only the columns a query needs instead of
the whole file. `has_nulls` is schema-derived, so a buffer-pool hit touches no
I/O at all. Result on the scan-bound queries:

| Query | before | after | |
|---|--:|--:|--:|
| Q19 `UserID = const` | 157 ms | **2.5 ms** | 63× |
| Q02 `SUM/COUNT/AVG` | 163 ms | **2.7 ms** | 60× |
| Q03 `AVG(UserID)` | 164 ms | **4.7 ms** | 35× |
| Q01 `COUNT WHERE` | 165 ms | **6.6 ms** | 25× |
| Q07 `… GROUP BY` | 169 ms | **9.5 ms** | 18× |

These now match or beat DuckDB-1t. The win generalizes (Q10 190→29, Q29 215→63,
Q08 462→305), dropping the suite **18.0 → 13.6 s**. Only `SELECT *` (Q23) is
unchanged — it reads every column, so there's nothing to prune (its lever is
late materialization, #273).

## LIKE fast path (live) — compiled segments + memchr-seeded substring

`LIKE` was a per-row recursive-backtracking matcher. It's now compiled once per
batch into literal segments split on `%` (the common `%lit%` / `lit%` / `%lit` /
exact / `%a%b%` shapes), matched via a SIMD memchr-seeded substring scan
(`indexOfScalarPos` on the first byte + `eql` for the rest) — no per-call
skip-table setup. Patterns with `_` keep the backtracker. General, no
query-specific casing. (A first cut using `std.mem.indexOfPos` *regressed* —
it builds a Boyer-Moore table per call; the memchr seed avoids that. Lesson
re-confirmed: measure, A/B controlled, keep the winner.)

Controlled best-of-5, same data, back-to-back vs the backtracker:

| Query | backtracker | memchr | |
|---|--:|--:|--:|
| Q22 `Title LIKE … AND URL NOT LIKE …` | 718 ms | **412 ms** | −43% |
| Q20 `COUNT WHERE URL LIKE` | 303 ms | **188 ms** | −38% |
| Q21 `… MIN(URL) WHERE URL LIKE` | 324 ms | **204 ms** | −37% |
| Q23 `SELECT * WHERE URL LIKE` | 944 ms | **863 ms** | −9% |

Suite **13.6 → 12.1 s**, 43/43.

## Flat group-accumulator state (live) — cache-conscious hash aggregate

Profiling Q32 (`GROUP BY WatchID, ClientIP` over the full 5M rows, no filter)
showed the **scan at ~4 ms and the aggregate at ~1010 ms** — essentially all the
time. WatchID is near-unique, so ~5M groups form: the new-group path runs ~5M
times and the top-k emit pass then visits all ~5M of them.

The hash map previously stored a **separate `[]AccState` slice per group**, so
both the per-group accumulator init and every emit-pass read chased a pointer to
a scattered arena allocation — a cache miss per group in hash order. The map now
stores a **`u32` group id**, and all accumulators live in one contiguous
`gstate` array indexed by `gid`. New groups append sequentially; emit walks
`gid` 0..N sequentially (bypassing the map). General — every `GROUP BY`
benefits, no per-query casing.

Controlled best-of-5, same data, back-to-back vs slice-per-group:

| Query | slice/group | flat state | |
|---|--:|--:|--:|
| Q17 `GROUP BY UserID, SearchPhrase` | 508 ms | **396 ms** | −22% |
| Q32 `GROUP BY WatchID, ClientIP` (5M groups) | 1033 ms | **926 ms** | −10% |
| Q18 `GROUP BY UserID, minute, SearchPhrase` | 763 ms | **736 ms** | −4% |

Suite **12.1 → 11.4 s**, 43/43. (An integer-packed-key variant was tried first
and measured *neutral* — using Q17/Q18 as no-change controls, Q32's apparent
move was entirely machine noise — so it was reverted. The cost was the slice
indirection, not key serialization.)

## Mask-guided conjunction (live) — short-circuit AND across rows

The Filter evaluated every conjunct of an `AND` over **all** rows, then ANDed the
masks. For Q22 (`Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND
SearchPhrase <> ''`) that meant *two* full 5M-row LIKE scans even though the
first conjunct already eliminates almost everything. The `AND` is now
mask-guided: each conjunct after the first is evaluated only on rows still alive
in the running mask (the active set is threaded into the leaf evaluators, and
the LIKE matcher skips inactive rows via `and` short-circuit). General — any
`AND` of a selective predicate with an expensive one benefits; no per-query
casing. (Cost-based reordering of the conjuncts is a further refinement; the
mask-guided evaluation already captures the win when the selective predicate is
written first, as ClickBench writes it.)

Best-of-5, vs full-mask AND:

| Query | full-mask | mask-guided | DuckDB-1t | |
|---|--:|--:|--:|--:|
| Q22 `Title LIKE … AND URL NOT LIKE …` | 412 ms | **218 ms** | 289 ms | **win** |

Suite **11.4 → 11.3 s**, 43/43.

## Single-row accumulate fast path (live) — hash-agg inner loop

`updateState` is built around contiguous-range SIMD reductions, but the hash
aggregate calls it once per row (`row..row+1`) — paying a SIMD kernel call +
setup per value, millions of times. COUNT and SUM now take a direct scalar
update (`updateStateRow`); MIN/MAX/AVG/stddev/distinct/percentile/group_concat
still defer to `updateState` so their semantics have one definition. General to
every grouped COUNT/SUM; no per-query casing.

Controlled best-of-5 vs per-row `updateState`: Q32 919→864 ms (−6%, the SUM
path). The suite total moved more (11.3→10.9 s) because Q30 issues **90 SUM
aggregates** (≈450M per-value updates) — exactly the case this removes setup
from. COUNT-only queries (Q17/Q18) are unchanged, as expected. 43/43.

## Late materialization (live) — fetch wide columns only for survivors

Q23 (`SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10`)
matches only **422 of 5M rows** and keeps **10**, yet the naive
`TopN → Filter → Scan(all 105 cols)` plan decoded every column for every row
(profile: Scan ~506 ms). The planner now recognizes
`Limit{ [Project] [OrderBy] Filter{ Scan(single base table) } }` where the
output is wider than the columns the filter + ORDER BY touch, and rewrites it to
a `LateScan`: the inner `Scan(probe cols + a hidden __rowloc) → Filter → TopN`
decodes only the probe columns plus each row's packed physical location; then
the wide output columns are fetched for just the ≤k survivors (re-decoding the
few row groups they live in — the row-group cache absorbs repeats — and reading
memtable-resident survivors from the pinned snapshot). The `__rowloc` rides
through tombstone compaction as a normal column, so locations stay correct after
deletes. General to any `SELECT <wide> … WHERE <pred> [ORDER BY …] LIMIT n`;
conservative shape match bails to the normal plan otherwise.

Best-of-5: **Q23 806 → 231 ms (−71%), under DuckDB-1t's 290 ms.** Suite
10.9 → 10.1 s, 43/43. (Q25/Q26/Q27 — `SELECT SearchPhrase … LIMIT` — correctly
do *not* late-materialize: their output isn't wider than the probe set.)

## Prefetch + integer-key group table (live) — cache-conscious GROUP BY

Profiling the agg-bound queries showed the cost is **not** emit/traversal (35–48
ms) or building the aggregates (small), but the **hash probe/insert — 60–75% of
the aggregate**, i.e. ~5M *random cache-missing* writes into a multi-million-
entry table. A faster table alone is neutral (it doesn't reduce the miss count;
an integer-key `AutoHashMap` A/B'd as neutral earlier). Two levers fix it:

1. **Software-prefetch probe pipeline** — `accumulateBatch` now runs in two
   phases per batch: compute every row's hash (+ packed key), then probe with a
   look-ahead `@prefetch` of the bucket `D=12` rows ahead. This overlaps the
   outstanding misses instead of serializing them. (Custom open-addressing
   tables in `group_table.zig`; the table is grown before a batch so slot
   addresses stay stable across the look-ahead.)
2. **Integer-key fast path** — when all group cols are fixed-width ints summing
   to ≤128 bits, the compound key packs into a `u128` (inline in the slot,
   single-compare hits, integer hash) — no per-row byte serialization (deletes
   Q32's ~100 ms key-build) and no key dup.

Both general (no per-query casing); the int path is chosen purely by column
types. Best-of-5 vs the StringHashMap path:

| Query | before | after | DuckDB-1t | |
|---|--:|--:|--:|--:|
| Q32 `GROUP BY WatchID, ClientIP` (5M groups) | 884 ms | **418 ms** | 370 | 1.13× |
| Q18 `GROUP BY UserID, minute, SearchPhrase` | 646 ms | **372 ms** | 361 | ~tied |
| Q17 `GROUP BY UserID, SearchPhrase` | 374 ms | **250 ms** | 201 | 1.24× |

Every GROUP BY benefits: suite **10.1 → 7.8 s**, 43/43.

Follow-up: the byte path's phase (a) previously dup'd **every** row's compound key
into the query arena — but ~71% of those (existing-group rows) were wasted and
never freed until query end. Keys are now serialized into one reused per-batch
blob (offsets in a spans list, slices resolved once it's built); only *new*
groups copy their key into `gkeys`. Q17 249→227 ms, Q18 366→360 ms (now ≤
DuckDB-1t's 361), and far less arena churn for every compound GROUP BY.

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
