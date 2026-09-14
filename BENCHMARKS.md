# Benchmarks

Snapshot of `zig build bench -Doptimize=ReleaseFast` results, captured for reference.

**Hardware:** AMD Ryzen 9 9900X (12C / 24T), 64 GB RAM, NVMe SSD, Windows 11.
**Zig:** 0.16. **Build:** ReleaseFast (the `bench` / `clickbench` steps now force it — see `build.zig`). **Workload:** 1,000,000 rows for the core benches unless noted.

To regenerate: `zig build bench` (always built ReleaseFast regardless of `-Doptimize`).

## Ordinary SQL against StarRocks (2026-09-11)

The [Sierra diagnosis](bench/ordinary_sql_diagnosis.md) records explicit DOP 12/16
comparisons, separate operator profiles, result validation and socket controls.
The main findings are unnecessary empty/selective lookup work, repeated lookup
construction and a confirmed delayed-ACK/Nagle response tail. It includes the
full timing ranges and separates reproducible costs from unstable rankings.

The [implementation results](bench/ordinary_sql_diagnosis.md#engine-implementation-2026-09-12-utc)
verify qualified scan pruning, empty fused-probe short-circuiting and POSIX MySQL
TCP_NODELAY. At DOP 12, median empty-plan latency drops from 805 to 68 ms,
crossplans from 1,304 to 478 ms, and no-estimates from 489 to 410 ms. DOP 16
confirms the large plan-query gains; cross-query timing variation and remaining
lookup/window costs are documented. The final source passes 1,501 tests and the
ReleaseFast benchmark suite, with all nine final SQL fingerprints preserved.

The [fresh September 12 sweep](bench/ordinary_sql_sweep_20260912.md) reruns all nine
cases against StarRocks at both DOPs using the application harness. ThinDB has the
lower median in five cases at each DOP. No-estimates remains about 2× slower;
base, child and detail cross remain 15–25% slower. All values are validated,
including the known DATE/DATETIME normalization for detail-cross. The report
retains full ranges and links to all 180 measured samples.

The [no-estimates follow-up](bench/ordinary_sql_diagnosis.md#no-estimates-follow-up-and-column-pruning-fix-2026-09-12-utc)
fixes the remaining wide lookup scans through query-block column pruning.
Unchanged no-estimates SQL now measures 141/143 ms at DOP 12/16 versus
StarRocks at 214/212 ms; base-simple improves to 157/151 ms. All nine cases
retain their fingerprints at both DOPs. The final build passes 1,503 tests
(five existing skips) and the ReleaseFast benchmark suite.

The [full five-method Sierra/AirDNA sweep](bench/rollforward_five_arm_sweep_20260912.md)
reruns all 30 variants at matched DOP 12, with fresh StarRocks timings and five
measurements per cell. Ordinary SQL has a lower median in all 30 cases;
UDF + regions is lowest in 27. All 120 ThinDB fingerprints match the archive,
and all 60 keyed/unkeyed pairs match. The report records AirDNA's live-versus-
snapshot differences, full ranges, source counts and every sample.

## Full local ClickBench (2026-09-12)

The [43-query DOP-12 run](bench/clickbench/CLICKBENCH_DOP12_20260912.md) completes
the full 99,997,497-row dataset on the local Ryzen 9900X. Three executions per
query use a fresh server per query, with hot time taken from the faster of
runs two and three. With an 8 GiB cache and 16 GiB query/shared budgets, the
sum of hot times is 19.296 seconds; peak working set is 24.571 GiB.

The closest common AMD x86 leaderboard tier, c6a.4xlarge, reports ClickHouse
18.150 s, DuckDB 26.252 s, StarRocks 44.467 s and Umbra 8.097 s. These are
cross-hardware references, not DOP-matched rankings: the published machines
have eight physical cores and 32 GiB RAM. The full report includes source
links, every query, the leaderboard-style geometric ratios and configuration
differences. Q28/Q29 use octet_length to preserve the canonical byte-length
semantics. No engine source was changed for this run.

## SQL window regions (2026-09-10)

`zig build bench-regions` compares ordinary and explicitly keyed SQL over
1,000,000 rows at DOP 12. Each arm receives one warmup and five alternating
measured runs. The harness requires actual region engagement through the
downstream window and matching aggregate totals. It also runs within
`zig build bench`; the following medians come from that full local run:

| Pipeline | Input | Ordinary SQL | Keyed SQL | Speedup |
|---|---|---:|---:|---:|
| LAG chain | Scan | 63.07 ms | 54.69 ms | 1.15x |
| LAG chain | UNION ALL | 65.10 ms | 36.71 ms | 1.77x |
| LAG default, running SUM, ordered LAST_VALUE | Scan | 98.49 ms | 66.98 ms | 1.47x |
| LAG default, running SUM, ordered LAST_VALUE | UNION ALL | 101.41 ms | 49.76 ms | 2.04x |

The separate focused run measured 1.37–1.69x across these cases. These are
synthetic SQL measurements, not Sierra/AirDNA rollforward results. See
[the implementation handoff](docs/plans/REGION_ELIGIBILITY_PLAN.md) for
coverage, frame semantics, fallback behavior, and validation.

---

## Core operations (1 M rows)

| Operation | Time | Throughput | ns/row |
|---|---:|---:|---:|
| insert memtable | 42 ms | 24 M rows/s | 42 |
| insert + flush | 90 ms | 11 M rows/s | 90 |
| sustained insert (1000 × 1k → 100 segs) | 210 ms | 4.8 M rows/s | 210 |
| scan flushed | 18 ms | 56 M rows/s | 18 |
| scan cold (cache populating) | 18 ms | 55 M rows/s | 18 |
| scan warm (cache hits) | 16 ms | 64 M rows/s | 16 |
| filter `qty > 50` (non-order-key, 49% match) | 22 ms | 44 M rows/s | 22 |
| filter `id < 50k` (order-key, narrow, 5% match) | 6 ms | **166 M rows/s** | 6 |
| filter `id >= N/2` (order-key, 50% match) | 16 ms | 62 M rows/s | 16 |
| aggregate count + sum + min + max | 19 ms | 53 M rows/s | 19 |
| aggregate stddev_pop + var_pop (Welford) | 29 ms | 35 M rows/s | 29 |
| aggregate count_distinct (~8 unique) | 28 ms | 35 M rows/s | 28 |
| aggregate percentile_cont(0.5) [exact] | 31 ms | 33 M rows/s | 31 |
| aggregate group_concat (~8 groups) | 45 ms | 22 M rows/s | 45 |
| groupBy tag (8 groups), count + sum | 41 ms | 24 M rows/s | 41 |

**Notes on the post-baseline aggregates:**
- `stddev_pop` / `var_pop` use Welford's algorithm — numerically stable, ~1.5× the cost of plain sum.
- `count_distinct` at 8-unique saturates the hash set quickly; cost is dominated by hashing every row's encoded value.
- `percentile_cont(0.5)` is **exact**: O(N) memory for the value buffer + a final sort. Roughly 1.6× the count+sum+min+max baseline.
- `group_concat` cost scales with output bytes — at ~125k rows/group × short tag values, the per-row buffer-append dominates.

## Flush internals (1 M rows)

| Phase | Time | Notes |
|---|---:|---|
| sort | 12 ms | pdqsort |
| zstd compress (level 3) | 14 ms (alone) | 20.7 MB → 3.7 MB, **5.66× ratio** |
| segment write to disk | 1–4 ms typical | NVMe ~3 GB/s for segment-sized writes |
| total flush | 36 ms | 581 MB/s raw throughput |

## TCP transport (in-process Connection vs above)

| Operation | TCP | In-process | TCP overhead |
|---|---:|---:|---:|
| scan | 23 ms | 16 ms | ~40% |
| insert (memtable) | 44 ms | 49 ms | none (effectively parallel) |
| insert + flush | 82 ms | 88 ms | none |

## Durability

| Mode | 1 M rows insert + flush | Throughput |
|---|---:|---:|
| `sync=.none` | 99 ms | 10.1 M rows/s |
| `sync=.per_flush` | 98 ms | 10.2 M rows/s |
| sustained 100 flushes, sync=.none | 85 ms / 100k | 1.2 M rows/s |
| sustained 100 flushes, sync=.per_flush | 153 ms / 100k | 0.7 M rows/s |
| insert 1 M with WAL | 115 ms | 8.7 M rows/s |
| 1000 × 1k inserts with WAL | 99 ms | 10.1 M rows/s |

## WAL group commit (concurrent writers)

| Threads | Total rows | Time | fsyncs | inserts/fsync |
|---:|---:|---:|---:|---:|
| 1 | 250 | 73 ms | 250 | 1.00 |
| 2 | 500 | 75 ms | 261 | 1.92 |
| 4 | 1000 | 97 ms | 296 | 3.38 |
| 8 | 2000 | 152 ms | 452 | 4.42 |

Leader-follower coalescing amortizes fsync cost ~4–5× at 8 threads.

## Compaction

| Scenario | Result |
|---|---|
| 200 segs, no compact | ingest 226 ms, scan 17 ms (12 M rows/s) |
| 5 segs, with compact | ingest 565 ms, scan 4.6 ms (**44 M rows/s**) |
| tombstone-pressure compact | delete 50k in 8 ms, compact in 15 ms |

Compaction reclaims **~3.6×** scan speed (12 → 44 M rows/s). Compaction is
build-aside: the merge runs lock-free and only the manifest swap + old-file
delete takes the table's exclusive lock, so it's safe to run against live
scans (the server runs it as a background sweep).

---

## Joins

All sizes use unique `bigint` keys [0..N) on both sides → inner equi-join emits exactly N output rows.

### Bigint key, sorted on join key (order_key = join_key)

| Shape | Hash | SMJ | Winner |
|---|---:|---:|---|
| 1k × 1k | 0.4 ms | 0.5 ms | ≈ tied (sub-ms variance) |
| 1k × 1M (dim × fact) | **29 ms** | 38 ms | hash 1.3× |
| 100k × 100k | 14 ms | 13 ms | tied |
| **1M × 1M** | 319 ms | **83 ms** | **smj 3.8×** |

### Other key types, 100k × 100k (sorted)

| Key | Hash | SMJ |
|---|---:|---:|
| string (varchar 16) | 20 ms | 16 ms |
| uuid (u128) | 16 ms | 12 ms |
| compound (bigint, bigint) | 17 ms | 12 ms |

### Unsorted input (order_key ≠ join_key) — SMJ pays real sort cost

| Shape | Hash | SMJ | SMJ slowdown vs sorted |
|---|---:|---:|---:|
| bigint 100k unsorted | 19 ms | 31 ms | 2.4× (vs 13 ms sorted) |
| **bigint 1M unsorted** | 336 ms | 280 ms | **3.4×** (vs 83 ms sorted) |
| string 100k unsorted | 24 ms | 43 ms | 2.7× (vs 16 ms sorted) |
| uuid 100k unsorted | 18 ms | 47 ms | 3.9× (vs 12 ms sorted) |

**Key insight:** pdqsort's already-sorted fast-path saves ~2–3× on SMJ. The merge-only fast-path (skip sort when stats prove pre-sorted) now skips that work entirely when both inputs are pre-sorted on the join keys.

### Range and mixed-predicate joins (100k × 100k unless noted)

| Shape | Hash | SMJ | Sweep | NLJ |
|---|---:|---:|---:|---:|
| equi + 1 range | 16 ms | 16 ms | — | — |
| equi + BETWEEN (2 ranges) | 16 ms | 15 ms | — | — |
| LEFT OUTER + range | 24 ms | 17 ms | — | — |
| pure range, 1k × 1k (624k pairs) | — | — | **8 ms (82 M/s)** | 10 ms (60 M/s) |
| pure range, 5k × 5k (15.6M pairs) | — | — | **105 ms (149 M/s)** | 263 ms (59 M/s) |
| pure range, 10k × 10k (62.5M pairs) | — | — | **432 ms (145 M/s)** | — |

Range overhead is small (~5-15%) on top of plain equi-joins — the per-pair check fits inside the Cartesian emit loop. Sweep is **~2.5× faster than NLJ** on pure-range joins because it advances both sides via merge-style cursors instead of nested loops. After marking the row-emit helpers in `cell_io.zig` as `inline`, sustained throughput jumped from ~90 to ~145 M rows/s — the per-row type-switch in `appendOneFromView` now fully inlines across the module boundary, and the no-mask code path in `emitMatchedRow` is hoisted into a tight branch-free loop.

### Skew detection & opaque predicates

| Variant | Time | Notes |
|---|---:|---|
| hash 100k × 100k, no detection | 17 ms | `skew_ratio_threshold = 0.0` |
| hash 100k × 100k, detection on (ratio=0.9) | 16–18 ms | within noise of no-detection |
| opaque NLJ 100k × 1k (fact × dim) | 787 ms | 0.85 M out/s, 666k pairs survived |

Detection cost is essentially zero after routing the Misra-Gries detector through the join's arena (was 50% when using GPA: uniform keys cycle counters constantly, churning malloc/free per sampled observation). Default `skew_ratio_threshold = 0.3` keeps detection on out-of-the-box; auto-route to SMJ fires when ratio AND absolute (≥20k bucket) both clear.

Opaque-predicate NLJ at 100k × 1k (realistic fact × dim shape) emits ~6.6 matches per left row through the callback. Output rate is ~0.8 M/s vs ~50 M/s for hard-coded range — the indirect call dominates when the loop body is otherwise tiny.

---

## Window functions (1 M rows)

Sort-based partitioning. Most benches use `PARTITION BY grp_lo` (100 partitions × 10k rows each) over a BIGINT value column. Output is materialized (drained) in every run.

### Per-function cost

| Function | Time | Throughput | ns/row | Notes |
|---|---:|---:|---:|---|
| `row_number()` | 199 ms | 5.0 M rows/s | 199 | sort + 1 counter per row |
| `rank()` (with ties) | 127 ms | 7.9 M rows/s | 127 | order-key compare on each transition |
| `dense_rank()` (with ties) | 129 ms | 7.8 M rows/s | 129 | same shape as `rank()` |
| `lag(qty)` | 199 ms | 5.0 M rows/s | 199 | direct index access in perm space |
| `first_value(qty)` | 198 ms | 5.0 M rows/s | 198 | one read, copy across partition |
| `sum(qty)` running (default frame) | 198 ms | 5.1 M rows/s | 198 | **prefix fast path** — single forward sweep |
| **`sum(qty)` whole partition** | **79 ms** | **12.7 M rows/s** | **79** | **broadcast fast path** — accumulate once, copy |
| `sum(qty) ROWS 10 PRECEDING` | 208 ms | 4.8 M rows/s | 208 | naive O(N×W); sliding-deque is future work |
| `avg(qty)` running | 200 ms | 5.0 M rows/s | 200 | running sum + count |
| `min+max` (both calls, shared spec) | 213 ms | 4.7 M rows/s | 213 | one sort serves both functions |
| `ntile(10)` | 198 ms | 5.1 M rows/s | 198 | position-based bucket assignment |
| `percent_rank()` | 131 ms | 7.7 M rows/s | 131 | same shape as `rank()` + division |
| `cume_dist()` | 129 ms | 7.7 M rows/s | 129 | peer-group counter |

### Spec sharing (verifies parser dedup + operator single-sort path)

| Shape | Time | Throughput | Note |
|---|---:|---:|---|
| 2 calls / **same** spec (1 sort) | 209 ms | 4.8 M rows/s | ≈ cost of 1 call — dedup works |
| 2 calls / **different** specs (2 sorts) | 322 ms | 3.1 M rows/s | ~1.5× the single-spec cost |
| 4 calls / **same** spec (1 sort) | 233 ms | 4.3 M rows/s | extra function cost only, not extra sort |

### Partition cardinality sweep

| Partitioning | Partitions | Rows/partition | Time | Throughput |
|---|---:|---:|---:|---:|
| no PARTITION BY | 1 | 1,000,000 | 45 ms | **22.1 M rows/s** |
| `PARTITION BY grp_lo` | 100 | 10,000 | 200 ms | 5.0 M rows/s |
| `PARTITION BY grp_hi` | 10,000 | 100 | 160 ms | 6.2 M rows/s |

**Reading the numbers:**
- Sort dominates: the `partition=1` case has no partition-boundary scan, only one sort + a linear sweep → **4× faster** than the 100-partition case. The bulk of per-function time is in the pdqsort over 1M rows.
- **The whole-partition broadcast fast path is the biggest win in the operator**: at 12.7 M rows/s it's 2.5× faster than the running-aggregate path. The algorithmic recognition pays off whenever the user writes `OVER (PARTITION BY x)` (no ORDER BY) — common pattern for "running total over all of partition X".
- **Spec dedup is verifiable**: 2 calls / same spec runs at 209 ms (essentially the single-call cost of 199 ms + a touch of per-call evaluation). 2 calls / different specs runs at 322 ms — two full sorts.
- Ranking functions (`rank`, `dense_rank`, `percent_rank`, `cume_dist`) are faster than `row_number` because they pay no per-row writes when the sort key doesn't change between consecutive rows. With many ties they reuse the prior rank.

**Headroom:** the naive sliding-frame path (`ROWS 10 PRECEDING`) shows we're an algorithmic improvement away from O(N) per-row aggregates. Combined with #145 SIMD, the perf pass should pull the per-function cost down 3–5×.

---

## ClickBench

End-to-end results for the 43-query ClickBench workload (5M-row `hits`
table) live in **[`bench/clickbench/RESULTS.md`](bench/clickbench/RESULTS.md)** —
43/43 pass, `COUNT(*)` 3 ms, median ~520 ms.

## How does this compare?

Cross-system join/scan benchmarks vary wildly with hardware, schema, and methodology — these are **order-of-magnitude** comparisons drawn from public sources and my own past measurements, not apples-to-apples.

### Scan throughput

| System | Bigint column scan |
|---|---:|
| **thinDB (warm)** | **68 M rows/s** |
| DuckDB | ~100–500 M rows/s (vectorized, similar) |
| ClickHouse | 100s of M rows/s (SIMD-heavy aggregate paths) |
| Polars | 50–200 M rows/s |
| Pandas | 5–20 M rows/s (Python overhead dominates) |

thinDB sits in the same order of magnitude as DuckDB / Polars for raw scan. Headroom remains via explicit SIMD in decode (today the @Vector kernels are limited to a few hot operators).

### Joins (1M × 1M, single bigint key)

| System | Hash join | Sort-merge |
|---|---:|---:|
| **thinDB** | **348 ms** | **89 ms** (pre-sorted) / 291 ms (unsorted) |
| DuckDB | 50–150 ms (parallel hash) | n/a (uses hash) |
| ClickHouse | 100–300 ms (depending on settings) | n/a |
| Polars | 100–300 ms | n/a |
| Pandas merge | 1,000–10,000 ms | n/a |

DuckDB beats us on hash join because it parallelizes across cores; we're single-threaded. Pre-sorted SMJ is competitive with DuckDB's parallel hash on this hardware. Vs Pandas: ~10–30× faster across the board.

### Aggregation (1M rows, global count + sum + min + max)

| System | Time |
|---|---:|
| **thinDB** | **18 ms (55 M rows/s)** |
| DuckDB | 10–30 ms |
| Polars | 15–40 ms |
| Pandas | 100–300 ms |

Competitive with vectorized analytical DBs; ~10× faster than Pandas.

### Window functions (1M rows, `ROW_NUMBER() OVER (PARTITION BY x ORDER BY y)`)

| System | Throughput |
|---|---:|
| PostgreSQL 16 | 3–6 M rows/s |
| **thinDB** | **5 M rows/s** |
| StarRocks | 15–25 M rows/s |
| DuckDB | 20–50 M rows/s |
| ClickHouse | 30–80 M rows/s |

Order-of-magnitude only; competitor numbers drawn from public benchmarks and vary widely by hardware/methodology. Bottom line: we're in PostgreSQL's neighborhood, **~3–5× behind the vectorized analytics engines**. The gap is mostly the absence of SIMD inner loops + a sliding-deque algorithm for framed aggregates, which #145 will close most of.

### What we don't do (yet)

Honest list of things competitors do that we don't:
- **Parallel execution.** We're single-threaded. DuckDB / ClickHouse / Polars all parallelize across cores. For 1M × 1M hash joins specifically, a 4-core hash join would likely close the gap.
- **Adaptive vector widths.** We have some `@Vector(N, T)` use but it's not pervasive in decode/filter loops.
- **GPU offload.** ClickHouse/Polars don't either, but Spark/Modin do.
- **Distributed.** Not in scope.

### What we do well

- **Cold-cache scan ≈ warm-cache scan** (17 vs 15 ms): zstd decode is fast and the LRU cache mostly serves repeated row groups, but cold reads still hit good IO throughput.
- **Order-key pruning** (177 M rows/s on narrow filter): hits the manifest-stats segment-skip + row-group-skip paths.
- **SMJ pre-sorted fast path on large symmetric joins** (3.9× over hash at 1M × 1M): the manifest-v4 stats let `.auto` route correctly without explicit hints.
- **Range-sweep on pure-range joins** (~90 M rows/s output, ~2× over NLJ): cursor-style merge replaces nested loops when both sides are sortable on the range key.
- **Compaction win for scan** (~5× speedup): segment count matters; the compactor pays off quickly.

**Bottom line:** thinDB's single-thread performance is in the same league as DuckDB/Polars on the operations we cover. The biggest gap is multi-core parallelism, which is intentional for v1 (single-node, single-writer-thread per table). v2+ can revisit.

---

## Reproducing

For MySQL wire benchmarks, `bench/mysql_packet_drain.cjs` exports
`drainQuery(callbackConnection, sql, values)`. It accepts a mysql2 callback
connection and fully consumes the response without decoding row values or
constructing JavaScript result rows. It returns the final result's row count,
column count and payload bytes, plus counts for every result set. Server
execution, serialization, socket transfer and packet framing remain measured.

The helper uses mysql2 3.16.0 internals. Its end-to-end checks cover empty and
multiple result sets, NULLs, large packets, SQL errors and connection reuse
against thinDB and StarRocks. Recheck those behaviors when changing drivers.
Keep correctness comparisons separate from packet-discard timings.

When benchmarking engines on a shared production host, size aggregate memory
headroom for all resident processes. The three-bucket rollforward comparison
uses separate engine phases and a bounded temporary thinDB instance; see
`docs/plans/REGION_ELIGIBILITY_PLAN.md` for results and the incident that led
to that procedure.

```
zig build bench -Doptimize=ReleaseFast
```

Output is to stdout; this file captures the current state. Re-run and update on perf-affecting changes (per CLAUDE.md guidance: track baseline numbers in PR descriptions).

To run a subset: bench bodies live in `bench/main.zig`, `bench/join_bench.zig`, `bench/compact_bench.zig`, `bench/durability_bench.zig`, `bench/tcp_bench.zig`, `bench/window_bench.zig`, `bench/materialize_bench.zig`. Comment out the ones you don't need from `bench/main.zig`'s `pub fn main()`.

## 2026-09-13: shared GROUP BY allocation

The parallel grouping core bounds initial reservation to 8,192 groups per
bucket, then forecasts larger reservations from observed joint-key density,
including the source-row counts represented by weighted runs. It retains
geometric state growth when estimates are too low. The SQL and operator order
are unchanged. At 256 buckets this limits initial hash storage to 64 MiB;
Q18 previously initialized 2 GiB from its input-row-count hint.

Local ClickBench on the Ryzen 9 9900X, Windows, DOP 12: 99,997,497 rows,
16 GiB source cache, 16 GiB query/shared budgets, region pool disabled,
compaction disabled. Baseline is commit `05bdcff`. Each query/build cell used
one warmup, five measured executions, and a separate value capture over
loopback MySQL packet drain. Builds alternated order between queries.

| Query | Baseline median ms | New median ms | Change | Baseline / new peak GiB |
|---|---:|---:|---:|---:|
| Q17 | 589.0 | 551.3 | -6.4% | 5.36 / 4.36 |
| Q18 | 574.1 | 536.2 | -6.6% | 5.24 / 4.42 |
| Q19 | 953.4 | 990.2 | +3.9% | 8.15 / 8.10 |
| Q29 | 5230.9 | 5251.7 | +0.4% | 23.66 / 24.12 |
| Q32 | 195.8 | 195.2 | -0.3% | 3.77 / 3.77 |

The 42-query sum of medians is essentially flat: 13.885 s before and
13.870 s after. The sum of minima from executions 2/3 is 13.400 s before
and 13.572 s after. Q19's measured ranges overlap (940–961 vs 948–1012 ms);
retain its slower median rather than claiming a universal speedup. Q29
remains variable and its string/regex work is a separate target. Q42 was run
but excluded from these historical aggregates because its zero/nonzero result
inconsistency was unresolved during that sweep. The completion fix below does
not retroactively validate those samples.

A separate Q18 profile retains 24,070,560 groups and halves final hash storage
from 2 GiB to 1 GiB. Preparation before workers falls from 80.8 to 8.8 ms;
the worker/final phase rises from 481.0 to 503.0 ms, yielding 561.8 to 511.9 ms
inside the grouping core. These are instrumented runs, separate from the
timings above. The new allocation counter covers table/state-array setup,
not string payload ownership or other operators' allocations.

Verification: `zig build test test-v2` reports 1,531 passed and five existing
skips; `zig build bench` completes with its exact-value checks. All 41
deterministic comparable query fingerprints match. Q18 has unordered LIMIT,
so its selected subset can vary; all 120 captured group counts match a
separate filtered aggregation on the baseline. Fixtures cover correlated,
unique and changing distributions, weighted rows, stale hints, workspace
reuse, geometric growth, and allocation-failure cleanup.

All samples, binaries, profiles, SQL, validation receipts and the complete
comparison are retained locally under
`.bench-data/group-allocation-20260912/RESULTS.md`; final raw samples are in
`bounded-sweep/result.json` there. Production port 13310 was untouched.

## 2026-09-13: parallel GROUP BY completion correctness

The shared staged grouping scheduler could collect final candidates while a
peer still held rows in an unpublished partial buffer. Independently sampled
empty queues and active-job counters did not establish pipeline completion.
Q42's large OFFSET exposed this as an intermittent zero-row result instead of
the expected ten rows.

The existing published-row counter now retains each row's unfinished-work
credit through staging and partial buffers until successful aggregation.
Final collection requires every scan producer to have closed and no unfinished
rows to remain. Weighted partials count once for this counter while preserving
their source-row aggregate weights. Failed folds retain their credits until
abort; aborted workers skip collection. Partial publication also preserves
ownership and releases its lock on allocation failure. This is a shared engine
fix; the SQL, operator order, and allocation improvements above are unchanged.

Before the fix, four of 144 Q42 executions returned zero rows. After the fix,
all 1,600 executions returned ten valid rows across 80 fresh private services
at max DOP 12, 4, and 1, alternating 8/16 GiB cache. Every service also matched
the complete grouped result and a diagnostic page with explicit tie breakers
against a serial reference: 102,676 qualifying source rows and 10,948 groups.
A deterministic delayed-partial regression fails before the fix and passes
after it. Fixtures also cover weighted rows, empty input, producer closure,
large offsets, repeated SQL executions, and publication allocation failures.

The full 43-query ClickBench sweep uses the same local hardware, dataset,
DOP 12, 16 GiB cache/budgets, and one-warmup/five-measurement protocol above.
All 41 deterministic archived fingerprints match. Q18's unordered LIMIT
subset matches a separate serial aggregation; Q42 matches the serial reference
with allowance for SQL ordering ties. Its median is **16.174 ms**, its minimum
of executions 2/3 is **16.353 ms**, and every execution returns ten rows.

The new **43-query** sum is **14.199 s in medians** and **13.998 s in hot
minima**. Comparing the same 42 queries excluding historical Q42, the previous
allocation build sums to 13.870 s and the fixed build to 14.183 s in medians
(+2.3%). Q29 accounts for 0.219 s of the 0.313 s difference. These are separate
sweeps, so the difference includes possible cache, scheduling, and host
variation; this correctness change does not establish a speedup.

A separate Q42 operator profile confirms seven actual workers under the DOP
12 cap, 34,857 weighted staged rows, all 10,948 groups, and 10,010 candidates
before OFFSET. Its final unfinished-row count is zero. Instrumented times
are excluded from the benchmark scores.

`zig build test test-v2 -j3` reports **1,535 passed and five existing skips**.
Release and profiling builds succeed. The native suite passes its exact-value
checks in a fresh scratch directory. Its first workspace run hit `AccessDenied`
during sustained flush; the same binary's full isolated rerun passed, and both
logs are retained without attributing an unconfirmed cause to the first error.
Reproduction scripts, the before/after
regression logs, every stress sample, full query comparisons, binary hashes,
and profiles are retained in
`.bench-data/q42-correctness-20260913/RESULTS.md` and its linked artifacts.
All private SQL services use port 7881 and `.clickbench-db`.

The subsequent full sweep uses one excluded warmup followed by exactly three
timed warm executions per query. All 43 queries pass their result checks.
Their arithmetic means sum to **14.046 s**; the sums of the first, second,
and third warm measurements are 14.039, 14.292, and 13.807 s. Q42 averages
13.109 ms and returns ten rows throughout. These means use all three samples
and are a different metric from the hot minima and medians above. Full
precision samples and validation receipts are retained under
`.bench-data/clickbench-warm3-20260913/RESULTS.md`.
