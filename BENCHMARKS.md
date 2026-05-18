# Benchmarks

Snapshot of `zig build bench -Doptimize=ReleaseFast` results, captured for reference.

**Hardware:** AMD Ryzen 9 9900X (12C / 24T), 64 GB RAM, NVMe SSD, Windows 11.
**Zig:** 0.16. **Build:** ReleaseFast. **Workload:** 1,000,000 rows for the core benches unless noted.

To regenerate: `zig build bench -Doptimize=ReleaseFast`.

---

## Core operations (1 M rows)

| Operation | Time | Throughput | ns/row |
|---|---:|---:|---:|
| insert memtable | 48 ms | 21 M rows/s | 48 |
| insert + flush | 85 ms | 12 M rows/s | 85 |
| sustained insert (1000 × 1k → 100 segs) | 179 ms | 5.6 M rows/s | 179 |
| scan flushed | 19 ms | 52 M rows/s | 19 |
| scan cold (cache populating) | 18 ms | 56 M rows/s | 18 |
| scan warm (cache hits) | 15 ms | 67 M rows/s | 15 |
| filter `qty > 50` (non-order-key, 49% match) | 23 ms | 43 M rows/s | 23 |
| filter `id < 50k` (order-key, narrow, 5% match) | 6 ms | **165 M rows/s** | 6 |
| filter `id >= N/2` (order-key, 50% match) | 16 ms | 64 M rows/s | 16 |
| aggregate count + sum + min + max | 18 ms | 55 M rows/s | 18 |
| groupBy tag (8 groups), count + sum | 32 ms | 31 M rows/s | 32 |

## Flush internals (1 M rows)

| Phase | Time | Notes |
|---|---:|---|
| sort | 12 ms | pdqsort |
| zstd compress (level 3) | 17 ms (alone) | 20.7 MB → 3.7 MB, **5.66× ratio** |
| segment write to disk | 1–4 ms typical | NVMe ~3 GB/s for segment-sized writes |
| total flush | 37 ms | 558 MB/s raw throughput |

## TCP transport (in-process Connection vs above)

| Operation | TCP | In-process | TCP overhead |
|---|---:|---:|---:|
| scan | 22 ms | 19 ms | ~16% |
| insert (memtable) | 43 ms | 48 ms | none (effectively parallel) |
| insert + flush | 81 ms | 85 ms | none |

## Durability

| Mode | 1 M rows insert + flush | Throughput |
|---|---:|---:|
| `sync=.none` | 87 ms | 11.5 M rows/s |
| `sync=.per_flush` | 88 ms | 11.4 M rows/s |
| sustained 100 flushes, sync=.none | 78 ms / 100k | 1.3 M rows/s |
| sustained 100 flushes, sync=.per_flush | 147 ms / 100k | 0.7 M rows/s |
| insert 1 M with WAL | 104 ms | 9.6 M rows/s |
| 1000 × 1k inserts with WAL | 84 ms | 12 M rows/s |

## WAL group commit (concurrent writers)

| Threads | Total rows | Time | fsyncs | inserts/fsync |
|---:|---:|---:|---:|---:|
| 1 | 250 | 67 ms | 250 | 1.00 |
| 2 | 500 | 72 ms | 255 | 1.96 |
| 4 | 1000 | 87 ms | 281 | 3.56 |
| 8 | 2000 | 127 ms | 370 | 5.41 |

Leader-follower coalescing amortizes fsync cost ~5× at 8 threads.

## Compaction

| Scenario | Result |
|---|---|
| 200 segs, no compact | ingest 156 ms, scan 17 ms (12 M rows/s) |
| 5 segs, with compact | ingest 516 ms, scan 3.5 ms (**57 M rows/s**) |
| tombstone-pressure compact | delete 50k in 7 ms, compact in 9 ms |

Compaction reclaims **4.7×** scan speed (12 → 57 M rows/s).

---

## Joins

All sizes use unique `bigint` keys [0..N) on both sides → inner equi-join emits exactly N output rows.

### Bigint key, sorted on join key (order_key = join_key)

| Shape | Hash | SMJ | Winner |
|---|---:|---:|---|
| 1k × 1k | 2.6 ms | **0.5 ms** | smj 5× |
| 1k × 1M (dim × fact) | **29 ms** | 45 ms | hash 1.5× |
| 100k × 100k | 15 ms | 15 ms | tied |
| **1M × 1M** | 302 ms | **91 ms** | **smj 3.3×** |

### Other key types, 100k × 100k (sorted)

| Key | Hash | SMJ |
|---|---:|---:|
| string (varchar 16) | 20 ms | 19 ms |
| uuid (u128) | 14 ms | 16 ms |
| compound (bigint, bigint) | 16 ms | 16 ms |

### Unsorted input (order_key ≠ join_key) — SMJ pays real sort cost

| Shape | Hash | SMJ | SMJ slowdown vs sorted |
|---|---:|---:|---:|
| bigint 100k unsorted | 19 ms | 31 ms | 2.0× (vs 15 ms sorted) |
| **bigint 1M unsorted** | 331 ms | 276 ms | **3.0×** (vs 91 ms sorted) |
| string 100k unsorted | 23 ms | 42 ms | 2.2× (vs 19 ms sorted) |
| uuid 100k unsorted | 17 ms | 39 ms | 2.4× (vs 16 ms sorted) |

**Key insight:** pdqsort's already-sorted fast-path saves ~2–3× on SMJ. The merge-only fast-path (skip sort when stats prove pre-sorted) now skips that work entirely when both inputs are pre-sorted on the join keys.

### Range and mixed-predicate joins (100k × 100k unless noted)

| Shape | Hash | SMJ | NLJ |
|---|---:|---:|---:|
| equi + 1 range | 17 ms | 15 ms | — |
| equi + BETWEEN (2 ranges) | 18 ms | 15 ms | — |
| LEFT OUTER + range | 23 ms | 16 ms | — |
| pure range, 1k × 1k (624k pairs) | — | — | 12 ms |
| pure range, 5k × 5k (15.6M pairs) | — | — | 300 ms |

Range overhead is small (~5-15%) on top of plain equi-joins — the per-pair check fits inside the Cartesian emit loop. NLJ on pure range produces ~52 M output rows/sec regardless of N; total time scales as O(N×M) since every pair must be evaluated.

---

## How does this compare?

Cross-system join/scan benchmarks vary wildly with hardware, schema, and methodology — these are **order-of-magnitude** comparisons drawn from public sources and my own past measurements, not apples-to-apples.

### Scan throughput

| System | Bigint column scan |
|---|---:|
| **thinDB (warm)** | **67 M rows/s** |
| DuckDB | ~100–500 M rows/s (vectorized, similar) |
| ClickHouse | 100s of M rows/s (SIMD-heavy aggregate paths) |
| Polars | 50–200 M rows/s |
| Pandas | 5–20 M rows/s (Python overhead dominates) |

thinDB sits in the same order of magnitude as DuckDB / Polars for raw scan. Headroom remains via explicit SIMD in decode (today the @Vector kernels are limited to a few hot operators).

### Joins (1M × 1M, single bigint key)

| System | Hash join | Sort-merge |
|---|---:|---:|
| **thinDB** | **302 ms** | **91 ms** (pre-sorted) / 276 ms (unsorted) |
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

- **Cold-cache scan ≈ warm-cache scan** (18 vs 15 ms): zstd decode is fast and the LRU cache mostly serves repeated row groups, but cold reads still hit good IO throughput.
- **Order-key pruning** (165 M rows/s on narrow filter): hits the manifest-stats segment-skip + row-group-skip paths.
- **SMJ pre-sorted fast path on large symmetric joins** (3.3× over hash at 1M × 1M): the manifest-v4 stats let `.auto` route correctly without explicit hints.
- **Compaction win for scan** (4.7× speedup): segment count matters; the compactor pays off quickly.

**Bottom line:** thinDB's single-thread performance is in the same league as DuckDB/Polars on the operations we cover. The biggest gap is multi-core parallelism, which is intentional for v1 (single-node, single-writer-thread per table). v2+ can revisit.

---

## Reproducing

```
zig build bench -Doptimize=ReleaseFast
```

Output is to stdout; this file captures the current state. Re-run and update on perf-affecting changes (per CLAUDE.md guidance: track baseline numbers in PR descriptions).

To run a subset: bench bodies live in `bench/main.zig`, `bench/join_bench.zig`, `bench/compact_bench.zig`, `bench/durability_bench.zig`, `bench/tcp_bench.zig`. Comment out the ones you don't need from `bench/main.zig`'s `pub fn main()`.
