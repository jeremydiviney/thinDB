# Keyed Pipeline Regions — design

Status: DESIGN (2026-07-16). Evidence base: the rf_custom probe
(`bench/rf_custom.zig`, commits 2e20ebe → 70681ef) — the full 15-CTE wayroll
rollforward hand-built on these mechanisms runs the identical kernels at
**0.72s warm vs the engine's 5.3s (7.4×)**, value-verified against the
engine's own output at four checkpoints. Predecessor: SEPARABLE BY / AUTO-SEP
(#119/#163) — right contract, wrong runtime (see §6).

## 1. The concept

A **region** is a maximal plan subtree in which one partition key set `K`
is preserved end to end: every operator either partitions by `⊇ K`, is
row-wise, consumes a small broadcast input, or unions arms that all carry
`K`. Inside a region, rows of different `K`-groups never interact — so the
engine may partition ONCE at the region entry and run the entire subtree
shard-locally, with **no stage materialization and no repartitioning**
between the region's operators. The region's output is one ordinary Stage;
everything outside the region is unchanged.

Compiled shape:

    RegionExec(K):
      E0  broadcasts: build small inputs once (dims, lookup TVFs)
      E1  exchange:   parallel fused-filter scan of the source, workers
                      scatter rows into H ≫ T hash-shard buckets by wyhash(K)
      E2  shards:     T threads work-steal shards (largest first); per shard,
                      run the compiled region program start-to-finish
      E3  combine:    emit as a Stage (concat), or feed the exit operator
                      (re-aggregate / downstream consumer) via stage adoption

## 2. Where each mechanism comes from (all measured in rf_custom)

| Mechanism | Measured effect | Engine graft point |
|---|---|---|
| Exchange scatter at the scan (batch → per-shard index lists → per-(shard,column) typed appends) | scan+scatter ≈ plain scan + ~100ms; replaces N per-slice rescans | ParallelScan worker construction + a scatter sink |
| H ≫ T hash shards + whale-first work stealing | whale shard (8× mean) absorbed, no straggler tail | region scheduler policy (trivial) |
| Ordering contract fused into the ONE consolidation | killed a full-input rematerialize pass | sort refs over exchange buckets; gather emits groups contiguous & ordered |
| Zero inter-stage materialization inside the region | the core ~4× | the region program IS the stage list, run in-place per shard |
| Pooled region buffers (clear, never free; sized by prior run/stats) | fixed Windows allocator drift AND cut steady stages 660→440ms | region slab pool, same discipline as the block cache |
| Worker-lifetime kernel scratch | removed ~10M per-group store inits | per-worker scratch slot (worker_state/worker_arena already exist for TVFs) |
| Contiguous partition views for per-group operators | group copies become appendSlice/appendRange | groups are ranges of the shard buffer by construction |
| Normalized key-prefix ordering (u64) | string compares mostly skipped | window/samplesort normalized keys already exist — reuse |
| Broadcast inputs built once per query/worker | rate table build once per worker | TVF broadcast_inputs + worker_state (exists) |

## 3. Planner half

- **Detection**: `partition_keys.zig` (shipped, e6fc649) already computes
  per-node maximal key sets bottom-up and reports maximal K-subtrees.
  Region = maximal subtree with `K ⊇ region key`, entry at the K-carrying
  source scan(s) or an existing stage, exit where K coarsens or an
  unsupported op appears.
- **Activation**: reuse the SEPARABLE BY surface as the explicit trusted
  contract (rename/alias `REGION BY (cols)`), plus auto-marking from the
  detector behind `THINDB_REGION=1` (same pattern as auto-sep Phase 2, new
  runtime). Explicit first; auto flips default only after the value harness
  and no-regression gates.
- **Region program compile — ONCE, not per shard** (auto-sep failure #4):
  lower the subtree's ops into a compact per-shard program over resolved
  column indices:
    - row-wise compute/filter → existing expr/predicate eval over shard views
    - keyed GROUP BY → in-shard aggregate (groups are contiguous ranges;
      no global hash table)
    - same-key window (row_number/lag/last_value class) → in-group argsort +
      linear pass
    - partitioned TVF → per-group kernel call (SDK Partition/Writer over
      range views — proven verbatim in rf_custom)
    - small-side join → broadcast hash map probe
    - UNION ALL of K-arms → append into the same shards
  Decline list (region ends / not eligible): big-big joins, DISTINCT-heavy
  windows, aggregates not keyed by ⊇ K, sorts on non-K orders, spill-needing
  shapes (v1), correlated subqueries.

## 4. Executor half

New `src/exec/region_exec.zig`:
- E1 reuses the ParallelScan recipe (snapshot, per-chunk fused-filter Scans,
  byte-aware bounds) with a scatter sink instead of drainWorker. Port
  `scatterColumn`/`gatherColumn`/`appendStoreRange` from rf_custom.
- E2 shard loop: per-thread state = scratch store sets + kernel worker_state
  + arena; shard buffers from the region pool; phase tick counters feed
  `--profile-ops` as `[region]` lines.
- E3: `MaterializedResult.adoptSlices` (shipped in M1) is the graft — region
  output becomes a normal Stage with per-shard chunks; downstream MatScan /
  ParallelScan / chunk-skip machinery works unchanged. Exit-side global
  aggregates use the existing partial-combine routes.
- Memory: accountant charges the region working set once (input buckets +
  ≤T in-flight shard buffers + output); shard buffers recycle through the
  pool as shards complete. Natural spill unit = a shard (defer to v2).

## 5. Correctness contract

- NULL keys hash to one shard (empty-bytes sentinel) — matches engine
  NULL-group semantics.
- In-shard sort dialect: ASC NULLS FIRST etc. — one comparator module shared
  with the engine sort so ties resolve identically where the SQL determines
  them. Where the SQL does NOT determine a pick (MAX_BY over fully tied sort
  keys — see rf_custom finding), results are implementation-defined exactly
  as they already are between engine routes; document, don't chase.
- Gate every phase on: per-query value harness vs mono (the ladder-cap
  pattern), full suite green, and STRUCTURALLY INERT on queries with no
  region (recognizer returns null → zero cost).

## 6. Why this succeeds where SEPARABLE BY/AUTO-SEP didn't

| Auto-sep failure (measured) | Region answer |
|---|---|
| Static range slices → whale slice held 51% of rows | H ≫ T hash shards + size-ordered stealing |
| Each slice re-scanned + re-filtered the input (12× boundary reads) | scatter once at the scan |
| Per-stage slicing kept every materialize barrier | zero stages inside the region |
| dop1 slices ran the generic serial operator chain | the region program is the fused path; there is no generic fallback inside |
| Per-slice plan recompile | compile once, run per shard |

## 7. Phasing

- **P0 (prove in-engine)**: region_exec runtime + a hard-coded recognizer for
  the rollforward shape in cte_stages, behind `THINDB_REGION=1`. Target:
  engine-served p15 ≈ 1.3–2s (vs 5.3 today; hand ceiling 0.72). Realistic
  loss vs hand: generic agg accumulators and expr eval vs bespoke structs.
- **P1 (generalize)**: program compiler from IR for the full §3 vocabulary;
  auto-detection marks regions; targets: AirDNA agg/cross, expanded-metrics.
- **P2 (productize)**: REGION BY syntax unification with SEPARABLE BY,
  default-on above a cost threshold, pool sizing from stats, spill story.

Expected end state: every same-key CTE chain gets the ~4× structural win +
pooling automatically; the rollforward class lands ~1.5s engine-served
without hand code.
