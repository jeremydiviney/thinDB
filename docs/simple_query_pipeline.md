# Simple Query Pipeline

This is the first target for a new physical execution layer. It covers one
query block with no joins, no window functions, and no correlated subqueries.
The parser and IR stay as they are; this layer is selected after IR compile
when a block matches one of these shapes.

## Scope

Included operators:

- table scan or materialized input scan
- filter
- compute/projection
- group by
- aggregate functions already supported by the engine
- having
- order by
- limit/offset/top-N

Excluded for this phase:

- joins
- windows
- recursive or correlated subqueries
- multi-use CTE sharing decisions
- set operations
- inserts/updates/deletes

CTEs and subquery blocks can still feed this pipeline after materialization.
Each materialized block is treated as a new source.

## Core Decision

Do not build one large operator tree with every branch active in the hot path.
Build a small family of tight physical pipeline shapes and choose one per query
block.

The first production shape should be flexible enough to cover:

```text
scan -> optional filter -> optional compute/project -> optional group
     -> optional having -> optional order/topN/limit -> output
```

The implementation should still specialize internally so that the common paths
do not pay for unused stages.

## Shape Taxonomy

ClickBench query mapping for these shapes is tracked in
`docs/engine_v2_clickbench_shapes.md`.

### 1. Scan / Filter / Project

```text
scan -> filter? -> compute? -> project? -> output
```

Use when there is no group by and no order/limit that can be fused.

Execution:

- workers claim scan ranges
- scan reads only needed columns
- filter is fused into scan when accepted by `Scan.tryFuseFilter`
- compute is fused when row-local and accepted by the scan path
- projection controls emitted columns, not decoded columns

Output can be streamed batches.

### 2. Scan / Filter / Top-N

```text
scan -> filter? -> per-worker topN -> final topN merge -> late/project output
```

Use when there is `ORDER BY ... LIMIT/OFFSET` without group by.

Execution:

- use `zonemapTopN` when leading order key can prune row groups
- otherwise use per-worker bounded top-N heaps
- final merge sorts only worker candidates
- if requested output columns are wider than predicate/order columns, use late
  materialization where possible

Important cases:

- `LIMIT n` without order can stop once enough rows have been emitted if no
  blocking operator exists.
- `ORDER BY ... LIMIT n` still scans all qualifying rows unless zonemap pruning
  can prove row groups cannot beat the current threshold.

### 3. Scan / Filter / Group

```text
scan -> filter? -> route rows to group buckets -> group -> output groups
```

Use when there is group by but no order/top-N requirement.

Execution:

- scan workers route rows into local bucket buffers
- published buffers go to shared group buckets
- group workers lease group buckets, with one worker owning a bucket at a time
- bucket count, chunk rows, scan tile size, and lease count are runtime-tuned
  from estimates and/or defaults

For unordered `GROUP BY ... LIMIT n`, use the existing `emit_limit` semantic:
emit any `n` groups without global order.

### 4. Scan / Filter / Group / Top-N

```text
scan -> filter? -> route rows -> group buckets -> per-worker/local topN
     -> final topN merge
```

This is the current harness shape and should be the first integration target.

Execution:

- filters should be fused into scan when possible
- route rows by group-key hash into local buffers
- publish full local buffers into group-bucket queues
- group workers lease hot buckets and drain them
- each worker computes local top-N candidates from owned/finalized buckets
- final merge sorts only a small candidate set

The harness showed:

- a shared group table per bucket is viable
- group bucket locking is not the bottleneck
- scanning the ring to choose hot buckets can be worth it for high-cardinality
  shapes even when scheduler CPU looks high
- lock contention is usually tiny compared with scan, route, aggregate, and
  top-N CPU

### 5. Materialized Input / Group / Top-N

```text
materialized source -> route/group/topN
```

Use after a blocking CTE/subquery block or after future join/window outputs.

The group layer should not care whether input came from storage scan or a
materialized batch source. It needs:

- column accessors for group keys and aggregate inputs
- row count/cardinality stats when available
- batch iteration
- optional known ordering/partitioning metadata

## Having

`HAVING` applies after aggregate state finalization.

Implementation:

- for grouped queries without top-N, filter finalized group rows before output
- for grouped top-N queries, having must run before top-N candidate selection
- if having references only aggregate outputs, it can be evaluated directly on
  aggregate state without materializing full rows

Ordering:

```text
group finalize -> having -> topN/order/limit
```

## Order By and Top-N

Order handling should be split into three cases:

1. no order, no limit: stream/materialize normally
2. order without limit: full blocking sort
3. order with limit/offset: bounded top-N

For group-by queries, top-N should run over finalized groups, not raw rows.
For non-grouped queries, top-N should run in scan workers where possible.

Final exact ordering still happens at the end for the small candidate set.

## Runtime Parameters

These are physical parameters, not parser concepts:

- `dop`
- `scan_tile_rgs`
- `chunk_rows`
- `bucket_count`
- `group_lease_buckets`
- `route_block_rows`
- local bucket reserve
- top-N candidate multiplier for offset/ties

Initial defaults can come from the harness. The long-term plan is to select
them from:

- input row estimate
- filter selectivity estimate
- group key cardinality estimate
- aggregate count/state size
- order/top-N presence
- memory budget
- runtime feedback

## Pipeline Input Contract

A simple physical block should accept either:

- a table scan source
- a materialized batch source

Required source capabilities:

- schema
- estimated row count
- column stats/cardinality when available
- scan/batch iterator
- optional sort state
- optional pruning support
- optional projection pushdown

Table scan sources add:

- segment/row-group pruning
- zonemap access
- physical row locations for late scan
- cache-backed column decode

Materialized sources add:

- known row count
- already decoded column vectors
- no storage zonemap pruning

## Planner Selection

The existing compiler should remain the fallback. Add a gate that recognizes a
simple query block after IR rewrites have fused limit/topK hints.

Reject the new path if the block contains:

- join
- window
- set union
- correlated subquery
- unsupported aggregate
- unsupported expression in group key/order/having
- output type requiring a currently missing materialization path

When accepted, build a `SimplePipelineSpec` from the IR:

```text
source
filter predicate?
derived columns?
projection/output columns
group keys
aggregate specs
having predicate?
order specs
limit/offset
runtime parameter hints
```

## Implementation Stages

### Stage 1: Spec Extraction

Add a compile-time recognizer that walks one IR block and emits a
`SimplePipelineSpec`, but still executes through the old engine.

Deliverable:

- explain/debug output showing whether the block is accepted and which shape it
  maps to
- no behavior change

### Stage 2: Productionize Group Top-N Shape

Move the useful parts of `bench/clientip_pipeline.zig` into a new exec module.

Target shape:

```text
scan -> filter -> group -> having? -> topN/order -> output
```

Deliverable:

- support generic column bindings instead of hard-coded ClickBench queries
- use existing `Scan` decode/cache/pruning mechanisms
- keep current engine as fallback

### Stage 3: Non-Grouped Top-N Shape

Implement:

```text
scan -> filter -> topN -> output
```

Include zonemap top-N and late materialization support.

### Stage 4: Materialized Source Shape

Let materialized CTE/subquery blocks feed:

```text
materialized -> group/topN
materialized -> filter/project/topN
```

This makes blocking boundaries compatible with the same physical kernels.

## Open Questions

- How much generic expression support is needed in the first fast path?
- Should group-key packing be fully generic immediately, or start with fixed
  integer/string/narrow-key cases?
- Should `HAVING` be compiled to aggregate-state predicates first, with row
  predicates as fallback?
- How much of the current `Query` vtable should be reused versus adding a new
  physical-block API?
- Should runtime parameter selection live in the planner gate or in the
  physical pipeline constructor?

## First Integration Target

Start with this shape:

```text
Scan(table)
  -> optional fused Filter
  -> GroupBy(group_cols, aggs)
  -> optional Having
  -> optional OrderBy + Limit top-N
```

This gives the highest leverage because it maps directly to the harness work
and is common in analytical queries.
