# Benchmarks

Snapshot of `zig build bench -Doptimize=ReleaseFast` results, captured for reference.

**Hardware:** AMD Ryzen 9 9900X (12C / 24T), 64 GB RAM, NVMe SSD, Windows 11.
**Zig:** 0.16. **Build:** ReleaseFast. **Workload:** 1,000,000 rows for the core benches unless noted.

To regenerate: `zig build bench -Doptimize=ReleaseFast`.

---

## Core operations (1 M rows)

| Operation | Time | Throughput | ns/row |
|---|---:|---:|---:|
| insert memtable | 49 ms | 20 M rows/s | 49 |
| insert + flush | 88 ms | 11 M rows/s | 88 |
| sustained insert (1000 × 1k → 100 segs) | 179 ms | 5.6 M rows/s | 179 |
| scan flushed | 16 ms | 62 M rows/s | 16 |
| scan cold (cache populating) | 17 ms | 58 M rows/s | 17 |
| scan warm (cache hits) | 15 ms | 68 M rows/s | 15 |
| filter `qty > 50` (non-order-key, 49% match) | 23 ms | 44 M rows/s | 23 |
| filter `id < 50k` (order-key, narrow, 5% match) | 6 ms | **177 M rows/s** | 6 |
| filter `id >= N/2` (order-key, 50% match) | 17 ms | 60 M rows/s | 17 |
| aggregate count + sum + min + max | 18 ms | 55 M rows/s | 18 |
| aggregate stddev_pop + var_pop (Welford) | 28 ms | 36 M rows/s | 28 |
| aggregate count_distinct (~8 unique) | 28 ms | 36 M rows/s | 28 |
| aggregate percentile_cont(0.5) [exact] | 29 ms | 34 M rows/s | 29 |
| aggregate group_concat (~8 groups) | 43 ms | 23 M rows/s | 43 |
| groupBy tag (8 groups), count + sum | 40 ms | 25 M rows/s | 40 |

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
| 200 segs, no compact | ingest 164 ms, scan 18 ms (11 M rows/s) |
| 5 segs, with compact | ingest 532 ms, scan 3.6 ms (**55 M rows/s**) |
| tombstone-pressure compact | delete 50k in 6 ms, compact in 10 ms |

Compaction reclaims **~5×** scan speed (11 → 55 M rows/s).

---

## Joins

All sizes use unique `bigint` keys [0..N) on both sides → inner equi-join emits exactly N output rows.

### Bigint key, sorted on join key (order_key = join_key)

| Shape | Hash | SMJ | Winner |
|---|---:|---:|---|
| 1k × 1k | 2.6 ms | 3.2 ms | ≈ tied (sub-ms variance) |
| 1k × 1M (dim × fact) | **30 ms** | 40 ms | hash 1.3× |
| 100k × 100k | 15 ms | 14 ms | tied |
| **1M × 1M** | 348 ms | **89 ms** | **smj 3.9×** |

### Other key types, 100k × 100k (sorted)

| Key | Hash | SMJ |
|---|---:|---:|
| string (varchar 16) | 27 ms | 17 ms |
| uuid (u128) | 25 ms | 13 ms |
| compound (bigint, bigint) | 25 ms | 14 ms |

### Unsorted input (order_key ≠ join_key) — SMJ pays real sort cost

| Shape | Hash | SMJ | SMJ slowdown vs sorted |
|---|---:|---:|---:|
| bigint 100k unsorted | 21 ms | 31 ms | 2.2× (vs 14 ms sorted) |
| **bigint 1M unsorted** | 387 ms | 291 ms | **3.3×** (vs 89 ms sorted) |
| string 100k unsorted | 29 ms | 47 ms | 2.8× (vs 17 ms sorted) |
| uuid 100k unsorted | 21 ms | 40 ms | 3.1× (vs 13 ms sorted) |

**Key insight:** pdqsort's already-sorted fast-path saves ~2–3× on SMJ. The merge-only fast-path (skip sort when stats prove pre-sorted) now skips that work entirely when both inputs are pre-sorted on the join keys.

### Range and mixed-predicate joins (100k × 100k unless noted)

| Shape | Hash | SMJ | Sweep | NLJ |
|---|---:|---:|---:|---:|
| equi + 1 range | 21 ms | 17 ms | — | — |
| equi + BETWEEN (2 ranges) | 21 ms | 15 ms | — | — |
| LEFT OUTER + range | 24 ms | 19 ms | — | — |
| pure range, 1k × 1k (624k pairs) | — | — | **7 ms (85 M/s)** | 14 ms (43 M/s) |
| pure range, 5k × 5k (15.6M pairs) | — | — | **180 ms (87 M/s)** | 324 ms (48 M/s) |
| pure range, 10k × 10k (62.5M pairs) | — | — | 693 ms (90 M/s) | — |

Range overhead is small (~5-15%) on top of plain equi-joins — the per-pair check fits inside the Cartesian emit loop. Sweep is **~2× faster than NLJ** on pure-range joins because it advances both sides via merge-style cursors instead of nested loops. Output rate stays ~90 M rows/s sustained.

### Skew detection & opaque predicates

| Variant | Time | Notes |
|---|---:|---|
| hash 100k × 100k, no detection | 17 ms | `skew_ratio_threshold = 0.0` |
| hash 100k × 100k, detection on (ratio=0.9) | 16–18 ms | within noise of no-detection |
| opaque NLJ 100k × 1k (fact × dim) | 880 ms | 0.76 M out/s, 666k pairs survived |

Detection cost is essentially zero after routing the Misra-Gries detector through the join's arena (was 50% when using GPA: uniform keys cycle counters constantly, churning malloc/free per sampled observation). Default `skew_ratio_threshold = 0.3` keeps detection on out-of-the-box; auto-route to SMJ fires when ratio AND absolute (≥20k bucket) both clear.

Opaque-predicate NLJ at 100k × 1k (realistic fact × dim shape) emits ~6.6 matches per left row through the callback. Output rate is ~0.8 M/s vs ~50 M/s for hard-coded range — the indirect call dominates when the loop body is otherwise tiny.

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

To run a subset: bench bodies live in `bench/main.zig`, `bench/join_bench.zig`, `bench/compact_bench.zig`, `bench/durability_bench.zig`, `bench/tcp_bench.zig`. Comment out the ones you don't need from `bench/main.zig`'s `pub fn main()`.
