# Benchmarks

Snapshot of `zig build bench -Doptimize=ReleaseFast` results, captured for reference.

**Hardware:** AMD Ryzen 9 9900X (12C / 24T), 64 GB RAM, NVMe SSD, Windows 11.
**Zig:** 0.16. **Build:** ReleaseFast (the `bench` / `clickbench` steps now force it — see `build.zig`). **Workload:** 1,000,000 rows for the core benches unless noted.

To regenerate: `zig build bench` (always built ReleaseFast regardless of `-Doptimize`).

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

```
zig build bench -Doptimize=ReleaseFast
```

Output is to stdout; this file captures the current state. Re-run and update on perf-affecting changes (per CLAUDE.md guidance: track baseline numbers in PR descriptions).

To run a subset: bench bodies live in `bench/main.zig`, `bench/join_bench.zig`, `bench/compact_bench.zig`, `bench/durability_bench.zig`, `bench/tcp_bench.zig`, `bench/window_bench.zig`, `bench/materialize_bench.zig`. Comment out the ones you don't need from `bench/main.zig`'s `pub fn main()`.
