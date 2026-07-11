# V2 Grouped Query Engine Plan

This is the working plan for the new grouped-query V2 handler.

## Supported Logical Shape

The handler targets a single grouped query block:

```sql
SELECT <group fields>, <aggregate outputs>
FROM <table>
WHERE <optional filter>
GROUP BY <group fields>
ORDER BY <optional output keys>
LIMIT/OFFSET <optional>
```

No joins, subqueries, CTE composition, windows, or group-by expressions in this pass.

## Projection Rules

- The SQL parser/IR layer remains responsible for semantic validation.
- V2 should assume a valid grouped query block and should not duplicate SQL legality checks.
- For this pass, selected outputs are plain group-by columns and aggregate outputs only.
- Selected expressions over group keys are unsupported for now, even if derivable.

## Group Keys

- Plain group-by columns only for this pass.
- If fixed-width group columns fit in `<= 128` bits, pack them into the smallest key width that fits:
  - `u32`, `u64`, `u96`, or `u128`.
- If the logical group key exceeds `128` bits, hash the complete key to `u128`.
- String keys are hash inputs.
- Hash collisions are assumed not to happen for now.
- No collision checks in this pass.
- For hashed/string keys, store a representative row reference per group and late materialize original group fields during final emission.

## Filters

- Reuse the existing scan path and fused-filter machinery:
  - `Scan.allocWithProjectionLoc`
  - `Scan.addPrune`
  - `Scan.tryFuseFilter`
  - `Scan.next`
- If a filter cannot be fused, evaluate it row-by-row inside the V2 scan layer using existing predicate evaluation where practical.
- Unsupported predicate forms should return a clear unsupported-filter error.

## Aggregates

- Aggregate state must be generic, not hardcoded to `count/refresh_sum/width_sum`.
- Reuse existing aggregate planning/type rules where possible, especially output type derivation.
- First generic pass supports fixed-state numeric aggregates:
  - `COUNT(*)`
  - `COUNT(col)`
  - `SUM`
  - `AVG`
  - `MIN`
  - `MAX`
- Aggregate inputs are fixed-width numeric columns for now.
- More complex aggregate state such as distinct, percentile, group concat, strings, decimals, and UDAFs is deferred.
- Aggregate execution should use SoA state arrays per aggregate where practical.
- `ORDER BY` compares finalized aggregate values, not internal state.

## Ordering And Limit

- `ORDER BY` may reference any output column:
  - group fields
  - aggregate outputs
- Multiple order keys and mixed `ASC`/`DESC` are supported.
- `ORDER BY` without `LIMIT` sorts all grouped rows single-threaded for now.
- `ORDER BY ... LIMIT n OFFSET m` keeps the first `n + m` sorted candidates, then drops `m`.
- `LIMIT/OFFSET` without `ORDER BY` is supported and non-deterministic/internal-order.
- No `ORDER BY` and no `LIMIT` emits all grouped rows in internal order.
- Final ordering/top-N/limit/offset may materialize the full grouped result in memory for this pass.

## Execution Strategy

- Build a cleaner generic grouped engine path, but keep the performance ideas and tuned defaults from the current harness work:
  - staged scan -> staging -> group flow
  - dynamic scheduler behavior
  - CPU pinning where already used
  - `dop = 12` default
  - `raw_chunk_rows = 8192`
  - `raw_group_chunk_rows = 8192`
  - `raw_batch_chunks = 12`
  - current bucket defaults unless explicitly retuned
- The final order/materialization layer is single-threaded for now.

## Fallback Policy

- V1 is going away.
- Unsupported V2 shapes should return a clear error.
- Do not silently route unsupported queries to V1.

