# Range-separability — concept-proof findings (branch: separable-exec)

Date: 2026-06-15. Goal: validate range-based query separability (shard a query by
a leading prefix of the table order key, run shards in parallel, merge) before
building the engine layer.

## Setup
- DB `clickbench_fsst__public` (100M rows). Order key = **(CounterID, EventDate,
  UserID, EventTime, WatchID)**.
- Target query **Q27**: `SELECT CounterID, AVG(length(URL)) l, COUNT(*) c FROM hits
  WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC
  LIMIT 25`. The ONLY ClickBench query that groups by the leading order key
  (CounterID), so the one true range-separable case.
- CounterID: 6506 distinct, largest single group 8.5M rows (8.5%) — no dominant
  group, so 12 contiguous ranges balance to ~8.3M each (skew ceiling ~11.7×).
- Experiment driver: `tests/bun/clickbench/_sep_experiment.mjs`,
  `_shard_ceiling.mjs`, `_conc_probe.mjs` (client-side proxy, no engine changes).

## Findings

### 1. Range partitioning is CORRECT
Splitting Q27 into N CounterID ranges and merging (concat → re-sort by l → top 25)
reproduces the monolithic result exactly (MATCH). HAVING is per-group so it
applies locally; only the final ORDER BY l LIMIT 25 needs a merge.

### 2. The server executes ONE query at a time (global lock)
Each connection runs on its own thread (server.zig:154), yet concurrent queries
scale **perfectly linearly**: 1 query 7.3s, 2 concurrent 14.7s (2×), 4 concurrent
31.1s (4×). A global lock is held for the whole query execution. **Consequence:
"fire up 12 concurrent connections" cannot parallelize.** The feature MUST be
*intra-query* worker fan-out, not concurrent connections.
- NOTE: intra-query workers DO parallelize (the silo scales ~8× within one
  query), so the feature is still viable as intra-query fan-out — the global lock
  is at query granularity, not in the scan/cache hot path.
- SEPARATE finding worth its own look: the server can only serve one query at a
  time across all connections — a real throughput limit for a server.

### 3. Range partitioning does NOT reduce total work
Sum of the 12 per-shard DOP1 times ≈ 7665ms ≈ the monolithic DOP1 time (7421ms).
Each shard's range predicate exactly covers its own data — there is no over-scan
for zonemaps to prune (the whole table is needed across all shards), and CounterID
is low-cardinality (6506) so sorted-stream doesn't beat the silo's hash. So the
only available lever is parallelism — which the silo already provides.

### 4. The value ceiling vs the existing silo — MARGINAL on Q27
- Silo (current engine), DOP12 monolithic: **920ms** (= 8.07× scaling over the
  7421ms serial cost — already good).
- Per-shard DOP1 costs (balanced ~8.3M shard ≈ 600–614ms). With *optimal*
  balancing the slowest shard ≈ the largest single group (8.5M) ≈ **~620ms**.
- So range-sep's theoretical parallel-12 ceiling ≈ **620ms vs silo 920ms ≈ 1.48×**
  — BEFORE subtracting fan-out overhead (12× query compile + 12× segment open +
  merge + coordination). If that overhead exceeds ~300ms the win evaporates.
- My quick greedy split overshot (one 15M shard → 1585ms ceiling → 0.58×, i.e.
  LOSES); proper skew-aware balancing (close a shard before a big group overshoots)
  is required even to reach the 620ms ceiling.

## Strategic read
On single-node ClickBench, range-separability is **marginal**: the silo already
parallelizes a single GROUP BY ~8×, partitioning doesn't reduce work, and only
**1 of 43** queries even fits the order-key-prefix construct — for a best-case
~1.5× that overhead may erase.

Where it WOULD pay off (none represented in ClickBench):
- Complex multi-CTE / join / window pipelines the silo does NOT parallelize
  end-to-end (the GLOBAL/tree-scoped contract).
- Huge-cardinality GROUP BY where range-split enables sorted-stream and avoids
  hash-table spill.
- Distributed execution (no shared lock).

## Recommendation
Do NOT build the engine layer purely for ClickBench — the evidence says it won't
clearly beat the silo on the one query that fits. Options to weigh:
1. Re-aim the concept proof at a **complex pipeline** (multi-CTE + join) where the
   engine doesn't already parallelize, and measure there before building.
2. Investigate the **global per-query execution lock** (finding #2) — letting the
   server run queries concurrently may be a bigger, broader win than separability.
3. Shelve range-separability as "marginal on single-node, revisit for
   spill/distributed."
