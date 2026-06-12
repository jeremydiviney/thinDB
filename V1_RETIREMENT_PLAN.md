# V1 SELECT-Engine Retirement Plan

Survey date: 2026-06-12, branch `engineV2`. Goal: `src/exec/engine_v2.zig` + `v2_*` silos own ALL
record-producing (SELECT) execution; the legacy compile path in `src/net/local.zig` keeps only
non-record-producing statements.

Engine selection today: `engine_v2.zig:144` `v2Enabled()` = `THINDB_ENGINE_V1` unset;
`local.zig:2307-2316` routes SELECTs to `cte_stages.compileStaged` (structure nodes) or
`engine_v2.tryCompile`, and falls to legacy `compileOp` when `tryCompile` returns null.
Policy is "V2-or-error" for matched shapes — most silo declines surface as
`UnsupportedQueryShape`, NOT legacy; only the enumerated lanes below actually reach legacy code.

---

## 1. KEEP (non-record-producing legacy; permanent)

All dispatched from `compileOp` (`local.zig:3385`) after `engine_v2.isSelectQuery`
(`engine_v2.zig:171-202`) returns false on the root op tag:

| Statement kind | IR op | Compile entry |
|---|---|---|
| DDL (CREATE/DROP/ALTER/USE/...) | `.ddl` | `compileDdl` (local.zig:3681) |
| SHOW | `.show` | `compileShow` (local.zig:3682) |
| INSERT VALUES | `.insert` | `compileInsert` (local.zig:3683, body 4084) |
| Multi-statement batch | `.batch` | rejected at compileOp:3688; wire layers iterate |
| COPY | `.copy` | rejected at compileOp:3690; PG dispatcher (`net/pg/copy.zig`) |
| CREATE TABLE AS | `.create_table_as` | `compileCreateTableAs` (local.zig:3979) — drains a SELECT *source* via `compileOp(ctx, op.source)` at :3982 (see ABSORB A12) |
| INSERT ... SELECT | `.insert_select` | `compileInsertSelect` (local.zig:4048) — source via `compileOp` at :4053 (ABSORB A12) |
| SET @var | `.set_var` | `compileSetVar` (local.zig:3705/3782) |
| DELETE | `.delete_op` | `compileDelete` (local.zig:3745) |
| UPDATE | `.update_op` | `compileUpdate` (local.zig:3761) |
| EXPLAIN | `.explain` | compileOp:3709-3738 — inner compiled via legacy `compileOp(ctx, e.inner)` at :3714 (RISK R2) |

Also KEEP: `subquery_resolve.zig` as a *pass* (pre-compile rewrite is policy-compliant), and the
shared exec operators (Sort/TopN/Filter/Compute/Project/Join/Window/SetUnion/Materialize/
AliasRename/ParallelScan/zonemapTopN/lateScan/MetaAggStats) — these are engine-neutral, used by V2.

---

## 2. ABSORB — every lane where a SELECT still enters legacy machinery

### Theme A: wide accumulators (NeedsWideAccumulator → tryCompile null → legacy compileOp)

The sentinel is caught at `engine_v2.zig:96` and mapped to a legacy fallback; in staged plans
`cte_stages.zig:228` maps it to `UnsupportedQueryShape` instead (RISK R1).

- **A1. Grouped SUM/AVG over 64-bit int.** Gate: `v2_shape_group_topn.zig:910-913`
  (`physicalTypeFor(input_type) == .i64` in validateShape; raiser comment engine_v2.zig:722-724).
  Missing: i128 (or split hi/lo u64) state slots in the silo grid + lowcard handler
  (lowcard has no equivalent — it never sees these; silo raises first). Size: **M**. Order: 1st —
  it is the only *error-typed* fallback, and absorbing it un-breaks the staged-CTE shape gap too.
- **A2. Nullable group keys.** Gate: `engine_v2.zig:627-629` in buildGroupTopN (`if nullable →
  return error.NeedsWideAccumulator`). Redundant inner gates: `v2_shape_group_topn.zig:819`
  ("nullable group key") and `v2_lowcard_group.zig:188` decline to null. Missing: validity bit in
  the packed/hashed key (legacy uses NULL-tagged byte keys). Size: **M**. Order: 2nd.
- **A3. Derived group keys reading nullable columns.** Gate: `engine_v2.zig:630-634`
  (`exprReadsNullable`, incl. CASE-without-ELSE, engine_v2.zig:326-347). Falls out of A2 (the
  Compute output carries nullability). Size: **S** once A2 lands. Order: 2nd (with A2).

### Theme B: legacy-only aggregates (tryCompile pre-gate → legacy compileOp)

- **B1. UDF aggregates.** Gate: `engine_v2.zig:92` `hasLegacyOnlyAggregate` (:101-122, `.udf`);
  belt-and-suspenders `hasUdfAgg` checks at engine_v2.zig:617/841. Legacy lane:
  `compileOp` :3558-3561 → `Query.udfGroupBy` (exec/exec.zig:574). Missing: variable-state
  (init/accumulate/finalize via registry) slots in the grouped cores, or a V2-owned generic
  grouped operator (see D-theme). Size: **M**.
- **B2. GROUP_CONCAT.** Same gate (`engine_v2.zig:107` `.group_concat`); legacy hash Aggregate
  holds growing string state. Missing: per-group variable-length string state. Size: **M**.

### Theme C: legacy leaf sources (tryCompile null → legacy compileOp)

- **C1. FROM-less SELECT (single_row).** Gate: `engine_v2.zig:89` `legacyLeaf` (:126-142);
  classify recognizes `.single_row` (:273) but `compileSelectBlock` has no builder. Legacy:
  `SingleRowSource.create` (compileOp:3708). Missing: trivial V2 single-row leaf + the
  Compute/Project decorators above it. Size: **S**.
- **C2. CSV/JSON file scans.** Same gate (`.file_scan`); legacy: compileOp:3432-3455
  (`exec.fileScan` + alias + pruning). Missing: a V2 `file_scan` source builder (serial is fine —
  not a perf shape; reuse `exec.fileScan` as the leaf). Size: **S/M** (decorator plumbing).
- **C3. pg_catalog virtual tables (incl. JOINs across them).** Gate: `local.zig:2304`
  `referencesPgCatalog` (:2323-2345) forces `legacy_only` → whole plan on compileOp; leaf built at
  compileOp:3391-3399 (`pgcat.build`). Missing: pg_catalog as a V2 materialized-input leaf
  (`SourceKind.materialized` already exists) usable by cte_stages joins. Size: **M**. Metadata-only,
  zero perf relevance — absorb late.

### Theme D: declined silo shapes (today V2-or-ERROR, not legacy — must be absorbed before the
legacy hash Aggregate can be deleted, and several are user-visible gaps)

Dispatch: `engine_v2.zig:725-751` — lowcard (`v2_lowcard_group.zig:169`) then silo grid
(`v2_shape_group_topn.zig:187` tryBuild → `validateShape` :804); both-null →
`error.UnsupportedQueryShape` (engine_v2.zig:751). Decline inventory:

- lowcard (`v2_lowcard_group.zig`, all `return null`/`declineFree`): key count >MAX_KEYS or 0
  (:170), agg count 0/>MAX_AGGS (:171), HAVING (:174), derived (:175), unknown key column (:184),
  nullable key (:188), unpackable key type (:194), packed key >64 bits (:199), COUNT col missing
  (:208), SUM/AVG/MIN/MAX non-numeric (:217), COUNT(DISTINCT) non-int/>64-bit (:236), other agg
  funcs (:248), distinct-over-coded-key (:258), ORDER BY non-output (:272), probe/fuse failure
  (:289-298), uncodeable string key (:304), unknown NDV (:325), over GATE_GROUPS/state-bytes (:332)
  → all fall to the silo, so only silo declines are terminal.
- silo grid (`v2_shape_group_topn.zig` traceDecline): group-key count (:805), aggregate count
  (:806/:859), group key column/type (:815/:825), nullable key (:819), nullable agg input (:865),
  count input (:870/:879), agg column/type (:883-884), string-agg slot count (:886-887),
  **A1 wide accumulator (:910-913)**, agg state count (:914/:937), avg output type (:917),
  COUNT(DISTINCT) non-integer (:934), width >64 (:935), distinct slot count (:936), other agg
  funcs — `stddev_pop/samp`, `var_pop/samp`, `percentile` (:952), ORDER BY non-output key (:957),
  ORDER BY string aggregate (:963), unsupported HAVING form (:967-970).
- matchGroupTopN nulls (engine_v2.zig:349-467): double Project/Limit/OrderBy, two Computes
  (:441), two WHERE filters (:447), non-scan source (:453), Project naming a non-group-output
  column (:412-419) → error.

Absorb items:
- **D1. Grouped stddev/variance (fixed extra state: sum, sum², n).** Size: **S/M**.
- **D2. Grouped percentile + COUNT(DISTINCT) over strings/floats/>64-bit (variable state /
  tiered sets — grouped distinct tiers exist for ints; extend width tiers).** Size: **M/L**.
- **D3. A generic V2 grouped fallback** for everything validateShape declines structurally
  (multi-Compute, exotic HAVING, ORDER BY string-MIN/MAX, >MAX_KEYS/AGGS). This is the single
  biggest-ticket item: a V2-owned NULL-aware, variable-state hash aggregate (effectively a
  rewritten `exec.Aggregate`) that also serves staged blocks (E2) and B1/B2. Size: **L**.
  Order: after A/B so it ships with validity + variable-state from day one.

### Theme E: router-level absorption — joins / CTEs / windows / UNION / subqueries

`classifySimplePipeline` (engine_v2.zig:205-285) matches only linear single-source blocks; structure
nodes are detected by `cte_stages.needsStaging` (cte_stages.zig:39-52) and compiled by
`compileStaged`. That path is V2-owned at the top but calls back into legacy machinery:

- **E1. Two-phase join aggregate combine.** `cte_stages.zig:313` →
  `local.routeJoinPartialGroupBy` (local.zig:2581-2632): fused partials + legacy hash
  `groupByTopK` combine. Absorb = re-home routing + combine onto the V2 grouped core (D3) or an
  exec-layer helper owned by neither router. Size: **M**. (RISK R4.)
- **E2. Stage/join/window/union-backed GROUP BY.** `cte_stages.zig:318-326` →
  `local.routeGroupBy` (local.zig:2549-2561): the legacy trio — `routeStreamGroupBy`
  (:2420-2452 → `streamGroupBy`/SortedAggregate), `routeRadixGroupBy` (:2468-2541 →
  RadixAggregate), hash `groupByTopK` (exec.zig:538 → `aggregate.zig` Aggregate). Absorb = D3
  operator + a V2-side strategy router over `stats()`. Size: **L** (rides D3).
- **E3. Subquery inner queries compile on legacy `compileOp`.** `subquery_resolve.zig:206, 224,
  264, 584, 668, 795, 924` (scalar / EXISTS / IN / correlated set/range/scalar rewrites). These are
  full SELECTs executed at compile time on legacy operators. Absorb = call the V2 entry
  (`engine_v2.tryCompile` / `compileStaged` via a thin shim) instead of `compileOp`; needs a
  CompileCtx→CompileInput adapter. Size: **M**.
- **E4. `buildServerQuerySession` legacy IR dispatcher.** `local.zig:1854-2030` — a second,
  fully-legacy SELECT compiler with two live callers: embedded `PlanBuilder.compile`
  (`api/plan.zig:216`) and the IR-over-TCP transport (`net/tcp_server.zig:385`, exported via
  `root.zig:82-98`). Bypasses engine_v2 entirely. Absorb = route both through
  `compileWithSession` (Session-aware) and delete the dispatcher. Size: **M** (mostly plumbing +
  ownership semantics of CompileCtx for the embedded API).
- **E5. EXPLAIN inner plan.** compileOp:3714 compiles the inner via legacy compileOp, so EXPLAIN
  prints legacy plans for queries that EXECUTE on V2. Absorb = compile inner through the same
  V2-first dispatch as compileWithSession. Size: **S**. (RISK R2.)
- Note: plain CTE/join/window/UNION *structure* is already V2-routed (compileStaged); the
  generic-block operators (Join, Window, SetUnion, MatScan, Sort, Filter, Compute, Project in
  `buildGenericBlock`, cte_stages.zig:259-380) are shared exec operators, not legacy — no absorption
  needed beyond E1/E2. `local.compileSelectProject` / `local.complementColumns` /
  `local.windowInputNames` (called at cte_stages.zig:282/287/343) should move to a neutral module
  so cte_stages stops importing `local`.

### Theme F: misc legacy-only lanes

- **F1. `tryMinMaxStats`** (local.zig:2370-2391, used at compileOp:3522-3531) — legacy metadata
  MIN/MAX lane. V2's `tryMetaAggStats` (engine_v2.zig:889-920) is a superset → dead after router
  deletion; nothing to absorb. Size: **0** (DELETE-AFTER).
- **F2. Env-gated experimental routes** — `routeLeaseGroupBy` (local.zig:2640, `THINDB_LEASE_AGG`)
  and `routeParallelGroupBy` (local.zig:2714, `THINDB_PARALLEL_AGG`), plus
  `ParallelScan.tryLeaseGroupBy` (parallel_scan.zig:514). Off by default; decide keep-as-exec-tool
  (RadixLeaseAggregate is engine-neutral) or delete with the router. Size: **S** (decision).
- **A12 (cross-ref).** CTAS / INSERT-SELECT *sources* (KEEP table) should compile their SELECT
  source through the V2 entry once E3's adapter exists. Size: **S**.

Suggested absorption order: A1 → A2/A3 → D1 → B2/B1 → D2 → D3 → E2/E1 → E3/A12 → E4/E5 → C1/C2 → C3 → F2.

---

## 3. DELETE-AFTER (dead once ABSORB completes)

- Legacy SELECT router branches in `compileOp` (local.zig:3385): `.scan/.file_scan/.alias/.limit/
  .select/.exclude/.filter/.order_by/.group_by/.compute/.join/.materialize/.window/.set_union/
  .single_row` arms (:3387-3708) — compileOp shrinks to the statement table in KEEP.
- `buildServerQuery` / `buildServerQuerySession` (local.zig:1854-2030) after E4.
- GROUP BY routing + hash machinery: `routeGroupBy`/`routeStreamGroupBy`/`routeRadixGroupBy`/
  `routeLeaseGroupBy`/`routeParallelGroupBy`/`routeJoinPartialGroupBy`/`perGroupTableBytes`/
  `groupKeysSortedPrefix`/`groupKeysCardUnderLimit` (local.zig:2353-2790) after D3/E1/E2.
- Legacy aggregate operators in `exec/`: `Aggregate` (hash, byte-key + coded-key + top-k emit,
  `aggregate.zig`), `SortedAggregate` (streamGroupBy), `udfGroupBy` operator, `RadixAggregate`,
  `RadixLeaseAggregate` (unless kept as the high-card V2 strategy), and Query methods
  `groupByTopK`/`streamGroupBy`/`udfGroupBy`/`radixGroupBy`/`leaseGroupBy` (exec.zig:538-572,
  555-563). NOTE: deletable only after E2 — the V2 staged path calls them today.
- Coded-GROUP-BY glue specific to the legacy hash path: compileOp :3582-3638 (Phase 4.2
  single-string-key + multi-key coded), `setCodedKey`/`configureCodedKeys` on Aggregate.
- `tryMinMaxStats` (local.zig:2370) and legacy `MinMaxStats` op if unused elsewhere (F1).
- `legacyLeaf` + `hasLegacyOnlyAggregate` gates and the `error.NeedsWideAccumulator` sentinel
  (engine_v2.zig:89-98, 101-142; v2_shape_group_topn.zig:912; cte_stages.zig:228 mapping).
- `v2Enabled()` / `THINDB_ENGINE_V1` env pin (engine_v2.zig:80-82, 144-146) and the `getenv`
  extern; `referencesPgCatalog` legacy_only gate (local.zig:2304) after C3.
- `-Dtest-engine-v1` build option + env injection (build.zig:98-108) and the V1-pinned suite
  wiring; `tests/integration/sql_test.zig:2617` THINDB_ENGINE_V1 branch.
- Legacy late-mat duplicates in local.zig (`lateMatBaseTable`/`lateMatShape`/`buildLateMat`,
  compileOp:3461-3494 and buildServerQuerySession:1889-1921) — V2 has its own
  (`tryScanSelectLateMat`, engine_v2.zig:1097).

---

## 4. RISKS

- **R1. Staged-CTE wide-accumulator mapping.** `cte_stages.zig:227-229` deliberately maps
  `NeedsWideAccumulator → UnsupportedQueryShape` (staged plans have no legacy fallback). Absorbing
  A1 must delete the sentinel everywhere *at once*; an intermediate state where the silo still
  raises it but tryCompile no longer catches it would leak an internal error type to callers.
- **R2. EXPLAIN prints legacy plans.** Inner of `.explain` compiles via compileOp (local.zig:3714).
  Tests/users comparing EXPLAIN output will see plan text change when E5 lands — stage it with
  fixture updates; EXPLAIN must keep working for shapes V2 errors on until full absorption.
- **R3. Perf regressions on legacy-fast shapes.** The legacy hash Aggregate carries tuned fast
  paths (count-in-slot single-int-key COUNT(*) — routeRadixGroupBy:2499 declines radix *because*
  hash wins; mask-guided AND / Q22-class filter shapes; coded GROUP BY through WHERE). D3/E2 must
  re-bench Q10/Q11/Q22-class and the full warm sweep before deleting; ClickBench warm ≈15.8s on
  `clickbench_fsst` is the floor.
- **R4. Two-phase join aggregates depend on legacy Aggregate.** `routeJoinPartialGroupBy` combine
  is a legacy `groupByTopK` (local.zig:2631) and the J1-J7 join wins were measured with it.
  Replacing the combine with the V2 core must hold the join-bench numbers (J5 85ms, J1 168ms).
- **R5. Subquery resolution executes at compile time** (E3): swapping `compileOp` for the V2 entry
  changes allocator/ownership flow (CompileCtx vs CompileInput, node_arena lifetimes) — the
  correlated-rewrite paths build synthetic IR (`buildRewrittenInner`) whose shapes V2 must accept
  (grouped lookups with multi-key projections); any V2-or-error gap here turns a working subquery
  into an error. Audit shapes first.
- **R6. Embedded API surface** (E4): `PlanBuilder.compile` and `serveTcp` are public
  (`root.zig:82-98`); rerouting them changes Session defaults and error sets — semver-visible for
  embedded users.
- **R7. NULL semantics parity.** A2/A3 + D3 re-implement the NULL-tagged grouping the legacy path
  owns today; the #77-#82 NULL campaign tests (3VL, GROUP-BY-NULL-keys, empty-agg→NULL) are the
  acceptance gate — run both pins until the pin is deleted.
- **R8. `force_group_by` knob** (`--force-group-by sort|hash|radix`, read at local.zig:2429/2475):
  user-visible control over legacy routing; D3/E2 must either honor it or deprecate it explicitly.

---

## 5. Phased campaign

- **Phase 1 — Wide state + NULL keys (A1, A2, A3).** i128 SUM/AVG slots in the silo grid;
  validity-tagged packed/hashed keys (or NULL-id reservation in dicts). Delete the
  NeedsWideAccumulator sentinel + the cte_stages mapping.
  Gate: full 659-test suites green under BOTH pins; bun 145; value-verify grouped-SUM/NULL-key
  queries vs `hits_full_v2.duckdb`; warm sweep ≈15.8s on clickbench_fsst (no regression).
- **Phase 2 — Aggregate-function completeness (D1, B2, D2, B1).** stddev/var fixed state;
  group_concat + UDAF variable-state slots; percentile; string/float/wide COUNT(DISTINCT) tiers.
  Drop `hasLegacyOnlyAggregate`.
  Gate: both-pin suites; targeted DuckDB value checks for each new agg; bench unchanged.
- **Phase 3 — Generic V2 grouped core + staged-block group-by (D3, E2, E1).** NULL-aware
  variable-state hash aggregate owned by exec-V2; V2 strategy router (stream/lowcard/silo/
  radix-class/generic) over `stats()`; cte_stages stops calling `local.routeGroupBy` /
  `routeJoinPartialGroupBy`; decide F2 (lease/parallel-agg experiments) and R8 (force_group_by).
  Gate: suites both pins; bun 145; FULL ClickBench warm + join bench J1-J7 + window W1/W2 at
  current numbers; value-verify all 43 vs hits_full_v2.duckdb.
- **Phase 4 — Router consolidation (E3, A12, E4, E5).** Subquery inners, CTAS/INSERT-SELECT
  sources, PlanBuilder/tcp_server, EXPLAIN inner all compile through the V2-first dispatch.
  Gate: suites both pins (subquery/CTAS/EXPLAIN fixtures updated deliberately); bun 145.
- **Phase 5 — Leaves (C1, C2, C3).** single_row + file_scan V2 builders; pg_catalog as a
  materialized V2 leaf; drop `legacyLeaf` and `referencesPgCatalog`'s legacy_only.
  Gate: suites + integration_client (pg_catalog-heavy) both pins; psql/mysql smoke vs
  thindb-server.
- **Phase 6 — Delete (everything in §3).** Remove compileOp SELECT arms, buildServerQuerySession,
  legacy aggregate operators + Query methods, THINDB_ENGINE_V1 / v2Enabled, -Dtest-engine-v1 and
  the V1-pinned suite variant (single suite remains).
  Gate: 659-test suites green (single pin now); bun 145; ClickBench warm ≈15.8s on
  clickbench_fsst; full value-verify vs hits_full_v2.duckdb; grep proves no
  `THINDB_ENGINE_V1`/`groupByTopK`/`streamGroupBy` references remain.

---

## 6. Phase-3 implementation blueprint (settled 2026-06-12, after Phases 1-2 shipped)

Direction (owner-confirmed): NO serial fallback operator and NO rewrite of the silo. The
staged-block GROUP BY (E2) is a SOURCE SEAM on the existing silo grid — same worker
scheduler, same staging/route/bucket-fold/emit, same packed/128-bit-hash key system. The
table fast path must end Phase 3 byte-identical (zero diffs inside its functions).

### The seam
- `runGridScanBurst` (group_topn_harness_core.zig) is the only source-aware step: claim a
  scan tile → drive batches → `appendBatchRawChunks`. Everything downstream is already
  source-agnostic (`appendBatchRawChunks` takes a `thindb.Batch`).
- Staged source = materialized batch list + atomic claim counter. Reuse the existing claim
  arithmetic by setting the claim space to the batch count (tile = batch index range).

### Structure: duplicate the thin setup, share the heavy core
- `runSiloGrid` is ~400 lines of table-coupled setup (snapshot, seg_start, stats_scan,
  RG-based worker sizing, per-worker Scan creation, filter fusing). Do NOT thread
  `if (staged)` through it. Add `runSiloGridStaged(allocator, batches, schema, cpus, cfg)`
  duplicating only the setup it needs (worker sizing from row count; no snapshot/scans;
  same PipeShared/buckets/queues construction, same worker spawn/merge/emit/teardown calls).
  Convergence/factoring can come later once stable; duplication keeps the fast path provably
  untouched.
- Worker loop: staged jobs replace the tile claim with a batch-index claim; the scheduler's
  `next_scan_rg/total_scan_rgs/active_scan_jobs` accounting is reused verbatim with
  total = batch count.

### Shape layer (v2_shape_group_topn.zig)
- Staged entry `tryBuildStaged(allocator, up_schema, request)`: validateShape already
  resolves types/nullability from a schema slice (the Compute probe path) — make the table
  param optional in `resolveColumnType/Nullable` and the few `table.schema` readers.
- Hashed-key (string/wide-key) emit: the table path late-materializes key values via each
  group's __rowloc through LateScan. Staged sources have no table rows — define the staged
  rowloc as pack(batch_idx, row_idx) and late-materialize from the MATERIALIZED batch list
  (kept alive until emit). Packed-key emit needs nothing (lossless decode).
- The materialized input must be memory-accounted (larger-than-RAM discipline): account the
  drained batches against the query accountant; release on error.

### Routing
- New V2 operator (StagedGroupTopN or a GroupTopNPipeline source variant): drain the
  upstream `exec.Query` (its own operators keep their internal parallelism) into owned
  batches → run the staged silo → emit. Hook: cte_stages.zig buildGenericBlock `.group_by`
  arm replaces `local.routeGroupBy`; `local.routeJoinPartialGroupBy` (E1) re-homes after.
- lowcard handler stays TABLE-ONLY (its gates are storage-proven NDV + dict codes; staged
  inputs have neither). Staged GROUP BY always rides the silo grid. Scan-only staged shapes
  already run as generic blocks — no change.
- Capability backlog rides along free once the seam exists: the silo's structural declines
  (>MAX_KEYS/AGGS, exotic HAVING, ORDER BY string agg) and wide/string COUNT(DISTINCT)
  remain separate lanes (V2-or-error today, not legacy dependencies).

### Gates
- Table-path functions diff-clean (git diff scoped to the table lanes), warm sweep ≈15.9s,
  join bench J1-J7 and window W1/W2 at current numbers (windows feed staged GROUP BY).
