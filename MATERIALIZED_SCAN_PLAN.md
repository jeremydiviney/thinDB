# Parallel scan over materialized CTE buffers

Goal: run the existing V2 **table** query-handler apparatus (cardinality-driven
routing, DOP selection, aggregate lanes/workers, silo grid, global parallel
reduce) over a **materialized CTE buffer**, not just over base tables — so that
a CTE that reads a *prior* CTE's output parallelizes the same way a table scan
does, instead of falling back to the serial `MatScan` + serial `routeGroupBy`.

## Why it's possible

A materialized stage (`exec/mat_stage.zig`) is **immutable after `ensureRun()`**
and is already stored as a list of `chunk_rows = 65_536` chunks — deliberately
sized "for striping like segment row-group tiles." Reads are zero-copy
`ColumnView` (`[]const`). So N workers can claim disjoint chunk ranges via the
same atomic steal cursor and read their stripe lock-free — structurally the same
as claiming row-group tiles from a table.

## The cut line (verified)

`ParallelScan.create` (parallel_scan.zig:198-291) couples to `*Table` at exactly
six points; everything above the worker leaf is source-agnostic.

| Concern | Table today | Buffer equivalent |
|---|---|---|
| Enumerate work units | `manifest.segments[].row_group_count` | `result.chunks.len` |
| Weight units | `byteAwareBounds` (footer bytes) | `chunk.rows` |
| Ranged leaf | `Scan.allocWithProjectionLoc` + `setRange` | new `ChunkRangeScan.create(stage, lo, hi)` |
| Snapshot/lock | `ddl_lock` + `captureSnapshot` | none (frozen post-`ensureRun`; run-once barrier) |
| Surviving units (DOP) | `survivingWorkUnits()` (zonemap prune) | chunk count (no prune, v1) |
| Memory budget | `table.query_memory_budget` | inherited query accountant |

Reused unchanged: `next_chunk.fetchAdd` steal loop, core leasing,
`effectiveThreads`, `stealLoop`/`roundSteal`, `drainWorker` merge,
`tryFuseFilter/Compute/Aggregate/Probe`, the global-reduce `Lane`s, the silo grid.

## Design — a `ScanSource` seam

1. **`ScanSource`** interface consumed by `ParallelScan` instead of `*Table`:
   `unitCount`, `unitWeight(i)`, `pin/unpin`, `makeLeaf(range, needed, acct)`.
   - `TableSource` — wraps today's logic verbatim (pure refactor).
   - `ChunkSource` — wraps a `*Stage`.
2. **Worker leaf** generalized from `[]*Scan` to `[]WorkerLeaf` (the methods
   ParallelScan actually calls: `setRange`, `next`, `survivingWorkUnits`,
   `tryFuseFilter`, `outputSchema`, `accountant`, `deinit`).
   - `Scan` (segment) — unchanged.
   - `ChunkRangeScan` (new) — ranged zero-copy `MatScan` with fused-filter support.
3. **Handlers** (`buildGroupTopN`, `buildScanSelect`, `buildGlobalAggregate`,
   silo grid, global reducer) take a `ScanSource`. In `cte_stages.zig
   buildGenericBlock`, a single-materialized-stage input routes through the SAME
   handlers with a `ChunkSource`.

The routing/cardinality apparatus is already buffer-ready: `MatScan.stats()`
surfaces exact `stats_upper_rows` + capped `col_stats`; `AdaptiveGroupBy` already
defers to realized counts. `effectiveThreads` clamps to 1 for a 1-chunk buffer,
so tiny CTEs stay serial with no overhead.

**Skipped for buffers (v1):** `tryMetaAggStats` and zonemap top-N late-mat
(exploit on-disk segment stats a buffer lacks) — fall through to the scan path.
Future: chunk-level min/max at `appendBatch` re-enables pruning + meta-agg.

## Correctness / concurrency / memory

- **Run-once barrier:** `ChunkSource.pin()` calls `stage.ensureRun()` before
  workers spawn. Producer drain stays serial in v1; the consumer parallelizes.
- **Lock-free reads:** immutable post-drain; disjoint chunk ranges; no locks.
- **Refcount single-threaded:** the parallel consumer is ONE `use` on the stage
  (one `uses_total++`, one `releaseUse` when all workers finish).
- **Accounting:** input already reserved (`.materialize`) by producer; consumer
  reserves only its own working memory via the injected accountant.

## Phasing

- **Phase 1** — extract `ScanSource`/`WorkerLeaf`, refactor table path onto
  `TableSource`. ZERO behavior change. Gate: full suite + ClickBench identical.
- **Phase 2** — `ChunkSource` + `ChunkRangeScan` leaf + `ensureRun` barrier.
- **Phase 3** — wire handlers in `cte_stages` to use `ChunkSource` for
  single-stage-input blocks; serial fallback for uncovered shapes.
- **Phase 4** — validate (suite + StarRocks parity) + measure (wayroll
  `customer_agg_by_month` + tail parallelize; no ClickBench regression).

## Open decisions

1. v1 = eager-materialize-then-parallel-consume (recommended) vs streaming
   parallel fusion across the CTE boundary (later lever).
2. Worker-leaf interface (chosen) vs synthetic-Table.
3. Chunk-level zonemaps — defer or fold into Phase 2.

## Expectation

Realistic wayroll win: a parallel factor on the serial aggregate (~0.30s) + the
serial tail (~0.75s), not 12× on the whole 2.0s — producer drains stay serial in
v1. The architectural prize is every CTE-derived aggregate/join/window getting
the full apparatus; bigger payoff on wider/heavier CTE workloads.
