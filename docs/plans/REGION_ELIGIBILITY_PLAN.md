# Keyed Regions — eligibility round (hand-off)

### Coverage and cold preparation follow-up (2026-09-09)

Active branch: `region-coverage-and-cold-start`, based on released v0.1.89.
The owner requested continued implementation until the Sierra/AirDNA
rollforward matrix uses keyed execution correctly wherever its partitions
permit it. Diagnosis and all read-only production replay samples are in
`.bench-data/region-diagnosis-report.md` and
`.bench-data/region-diagnosis-local/production-results/`.

Priorities for this round:

1. Compile region broadcast branches with normal CTE sharing and staging;
   cold expanded preparation currently re-executes shared branches.
2. Retain several bounded region programs and independent structural
   boundary hints. Rebuild data-dependent state after source changes;
   never bypass version checks to obtain a cache hit.
3. Validate and remove the remaining application exclusions for the
   fifteen matrix shapes, including explicit deterministic SQL selection
   wherever arbitrary aggregate picks prevent stable detail comparisons.
4. Measure both cold and interleaved warm queries, assert actual region
   provenance, and preserve valid boundaries where parent/division
   regrouping changes the partition key. Region engagement alone is not
   evidence that the expensive part was accelerated.

Completed and validated locally on 2026-09-09:

- Broadcast inputs now use ordinary CTE staging and the shared typed bulk
  copier. The latter fixes rejection of widened `SUM(BIGINT)` payloads after
  expensive preparation, allowing expanded-cross's outer lookup joins into
  the main region.
- The per-database cache retains up to 32 programs under a combined retained
  byte budget, with idle LRU eviction and pinned live borrowers. Independent
  boundary hints rebuild fresh inputs after source changes; data-version and
  kernel checks still guard all cached programs.
- Explicit unfiltered scan/window regions are supported. Broadcast joins
  preserve full-width composite keys and pinned strings. Duplicate build
  keys decline the one-match broadcast path, preserving join multiplicity
  through ordinary staging above a valid inner region.
- Computed output replacement now follows ordinary SQL compilation for
  qualified aliases, simultaneous expressions, and subsequent projections,
  including invalidation of replaced literal-pinned facts.
- The companion Wayroll worktree enables all ordinary rollforward variants
  in Zig mode. Hooks remain excluded. Cross-division ranking runs before
  replacing the source division, and zero-sum exchange rates use an explicit
  ranked selection in place of `ANY_VALUE`.

The original 30 Sierra/AirDNA cases plus 12 combined-option cases pass.
All 84 keyed SELECTs engaged a region. Keyed and unkeyed UDF values and wire
column names/types match exactly across all 42 configurations. Plain SQL is
exact in 39 configurations; the remaining differences affect only the two
exchange-rate columns, at most `6.661338147750939e-16`. Full row multiplicities
and all other fields match. Interleaved cache reuse and changed-lookup
rebuilding also pass. These checks validate the listed configurations, not
every possible SQL or hook pipeline.

Final validation: `zig build test test-v2 -j2` passes 1,491 tests (five
skipped); `zig build bench -j2` passes. The three companion application suites
pass 58 tests, and changed files introduce no additional ESLint violations.
Final ordinary-SQL scan/window and UNION/window benchmarks show 1.53x and
2.48x speedups with exact totals.

Local warm samples improve AirDNA expanded-simple from 3,242 ms unkeyed UDF
to 337 ms keyed, and expanded-cross from 3,592 ms to 247 ms. These are single
samples with profiling, not production medians. Compound variants can still
pay substantial preparation/staged-operation costs, and large detail output
still pays sorting, serialization, and transfer costs.

The full local matrix, measurement method, limitations, and raw artifact
locations are in `.bench-data/region-coverage-results-report.md`; exact
counts and binary/source hashes are in `region-coverage-summary.json` in
the same directory. Final replay folders are
`region-coverage-verified-results/` and
`region-coverage-verified-extra-results/`. Validation logs use the
`region-coverage-green-` prefix.

Production remained a read-only diagnostic reference. The candidate ran
only against the separate local bench database on port 13311 and was stopped
after validation. This round has not been deployed to production.

### Earlier eligibility implementation (historical hand-off)

Branch `region-eligibility`, preceding the follow-up above. Predecessor
design: [REGION_PLAN.md](./REGION_PLAN.md). Owner's direction: broaden keyed
regions for general SQL constructs while preserving functionality. Wayroll
is a validation workload; engine eligibility is the primary objective.

### First engine implementation

- W3: entry computes retain their source columns and use collision-free
  physical output names. Initial compilation and cached execution share the
  same projection and renaming recipe. Replaced columns no longer inherit
  stale literal facts from scan filters.
- W4: boundary search follows joins' left inputs, aliases, windows, and
  primary TVF inputs. Unsupported outer operations remain staged while a
  supported inner CTE executes as a region. The search limit is 256 nodes.
- W5: `LAG` with literal/substituted nonnegative offsets, no default or a
  NULL default, and a partition matching the region ranges. Each call sorts
  its own in-range permutation, supporting multiple columns and descending
  orders. Mixed `LAG`/`ROW_NUMBER` calls are supported. `LAST_VALUE` remains
  on its existing separate path.
- General projection expansion is shared with ordinary SQL compilation,
  including stars, aliases, and replacement rules. The physical row locator
  stays out of SQL-visible projections.
- Correctness fixes found during validation: release the temporary source
  after a failed entry-compute build; widen SQL `SUM(BIGINT)` to `LARGEINT`
  with checked i128 accumulation; use the shared SQL comparison semantics
  for floating-point window order.
- Repeated declarations remember the successful inner boundary. The cache
  fingerprints the original IR, every source table's data version, and TVF
  identities before failed outer candidates can rewrite shared IR. Reuse
  still validates the key contract and fresh scans; source changes return
  to normal boundary search. This avoids repeatedly draining outer joins
  just to reject the same candidates.
- Integration checks compare column names/types and values, assert that the
  intended output column is produced by a region, and repeat queries to
  exercise cached execution. `zig build bench-regions` compares ordinary
  and keyed SQL window chains on generated data with exact aggregate checks.

W2 now omits the estimates dependency chain and UNION when estimates are
disabled. W1's fallback-policy change, W6's currency modes, and W7's frame
aggregates remain follow-up work. Ordinary UNION ALL ingress is implemented
as described below. Application exclusions still
require exactness checks for every newly enabled shape combination.

### Ordinary SQL UNION ALL ingress (2026-09-09)

- A general `UNION ALL` stream can enter a region, followed by compatible
  windows, aggregates, and other existing regional operations. No UDF is
  required. The specialized shared-base union/TVF append path is retained.
- Branches and the entry's projection/compute/filter chain use the ordinary
  staged SQL compiler. This preserves positional aliases, numeric widening,
  nested unions, duplicate rows, and shared/explicitly materialized CTEs.
- One source stream feeds the scatter; branch operators retain ordinary SQL
  parallelism. Branch operations are not fused into the regional program.
  Parallelizing ingress itself remains a potential performance follow-up.
- When fewer input streams than workers are available, bucket sorting moves
  to the parallel shard phase. Buffers are reserved after ingress joins, so
  shard workers never allocate concurrently from a producer's private arena.
  Stable arrival-order tie breaks are unchanged.
- Cached executions rebuild that source. Source versions and output schema
  are validated; no literal-pinned fact from one arm is applied to the union.
- Removed an obsolete compiler rejection for grouping exactly by the range
  keys. The existing runtime supports zero extra subgroup keys.
- New integration cases compare column names/types and values, require the
  intended downstream regional output, repeat cached executions, and cover
  overlapping branches, NULLs, empty inputs, aliases/casts, entry expression
  order, shared CTEs, changes in either source, DDL, and invalid input shapes.

Validation: `zig build test test-v2 -j2` passes 1,483 tests (five skipped),
including the new union cases at DOP 4. ReleaseFast `dist` and the full
`zig build bench -j2` suite pass. One benchmark attempt hit a transient
Windows `AccessDenied` during its unrelated durability flush; retrying the
same binary completed successfully.

Final local measurements (ReleaseFast, one million rows, DOP 12, five
alternating runs after warmup, parse through teardown):

| Ordinary SQL source | Staged median | Keyed median | Speedup |
|---|---:|---:|---:|
| Filtered scan followed by windows | 69.30 ms | 42.74 ms | 1.62x |
| Two UNION ALL branches followed by windows | 61.72 ms | 25.41 ms | 2.43x |

Both return exact totals `494505000` and `484515000`; neither uses UDFs.
The first union implementation took 88.35 ms versus 69.80 ms staged in a
separate run. Profiling found about 67 ms in serial bucket sorting; moving
that work to shard workers produced the final result above. These are local
synthetic comparisons, not production measurements. Logs:
`%TEMP%/thindb-region-union-final-green-tests.log` and
`%TEMP%/thindb-region-union-final-bench-retry.log`.

Wayroll validation used the candidate ReleaseFast binary in
`.bench-data/region-union-build/bin` and only the separate bench listener on
13311. All 14 estimates-off combinations and four estimates-on plan
combinations matched plain SQL, unkeyed UDF, and keyed execution exactly
(one warmup and one measured run per arm, rate folding disabled). Saved
child/detail/expanded replay retained its prior results: 11/12 exact against
unkeyed UDF, 10/12 against plain SQL, 12/12 exact cached repeats, no errors.
The existing AirDNA exchange-rate differences are unchanged; no new amount
or other-column differences appeared. The AirDNA cross-plan lookup test
returned the same 15,140 rows in all five runs and retained a cache hit after
recreating its 5,988-row temporary table. Artifacts:

- `.bench-data/region-union-replay-results.json`
- `.bench-data/region-noest-union-matrix.log`
- `.bench-data/region-estimates-union-matrix.log`
- `.bench-data/region-crossplans-union-final.json`

No application gate was widened in this batch. The candidate has not been
deployed to production.

Separate issue found while constructing the cache fixture: ordinary SQL
`UPDATE right_arm SET amount = NULL WHERE id = 1001` on a flushed INT column
panics in `api/update.zig:computeNewRows` -> `transform.appendMaskedStringy`
(the NULL expression arrives as a string view). This runs outside regional
execution. The regression appends new rows to change its fixture; the
UPDATE issue remains a follow-up. Trace:
`%TEMP%/thindb-region-union-final-tests.log` from the initial scheduling check.

### Earlier local measurements

Local measurements on 2026-09-08 (ReleaseFast, 1,000,000 generated rows,
DOP 12, five alternating measured runs after warmup, parse through teardown):

| Run context | Staged median | Keyed median | Speedup |
|---|---:|---:|---:|
| Standalone region benchmark | 67.04 ms | 32.90 ms | 2.04x |
| Region benchmark after the full bench suite | 64.41 ms | 41.83 ms | 1.54x |
| Final full bench suite after boundary-cache change | 70.82 ms | 47.00 ms | 1.51x |
| Full bench after estimates-off and exact integer-key changes | 64.40 ms | 46.62 ms | 1.38x |
| Full bench after passthrough views and temporary lookup caching | 71.56 ms | 50.29 ms | 1.42x |

All five runs returned exact totals `494505000` and `484515000`. The full-suite
run additionally asserts that the region produces the second window's
output, rather than merely finding a smaller accelerated input stage. These
are local synthetic results, not production/Wayroll speedup claims.

### Local fixture diagnosis and repair (2026-09-08)

The initial saved-dump replay failed in the **unkeyed UDF arm**, before the
keyed arm was attempted. It was not a failure of the plain inline-SQL
reference or of production. The first dump used in that replay was
estimates-off (`21-13-04-516Z`), not detail. The existing baseline binary
returned the same `Unknown column` against the bench copy.

Tracing each CTE isolated the first error to `with_month_diff`: its SELECT
requests `otherMrrAmount` and `originalOtherMrrAmount`, but the bench copy's
persisted `rf_currency_convert` predates those outputs. A `SELECT *` probe
confirmed that its output schema omitted both. Wayroll's current source
includes them in `src/thindb-functions/rf_currency_convert.zig`; the caller
lists them in `src/workers/rollforwardProcess/helpers.ts` around line 1763.

Read-only schema/function checks compared the existing **local production
CDC listener on 13310** with the separate bench server on 13311. These were
not checks against the remote `starrocks1` server.

- All six base/lookup table schemas checked match exactly: invoices (34
  columns), division, currency rates, external plans, estimate date map,
  and number sequence.
- The bench `report_customer_revenue_rollforward` had 28 columns versus
  production's 30: the same two other-MRR fields are missing. Wayroll's
  `database/migrations/1787443200000-addOtherMrrAmountFields.ts` defines
  both as `INT NOT NULL DEFAULT 0`; its table seed SQL also includes them.
- The bench copy had no `report_customer_revenue_rollforward_child` table;
  production has it with 30 columns. After function repair, the child dump
  fails specifically at `customer_monthly_totals_pre`, which reads it.
- All nine production `rf_*` function definitions match the current Wayroll
  worktree after normalizing line endings and the SQL CREATE wrapper. The
  bench copy had six stale Zig functions; its three SQL functions matched.
  Besides the added fields, old partitioned execution declarations predated
  the current `.either` declarations used for region execution.

First repaired the bench function registry, using the existing Wayroll
command `tsx src/scripts/thindbSyncFunctions.ts 127.0.0.1 13311`. All nine
bench definitions now match source. The app normally performs this
reconciliation during connection-pool initialization; direct mysql2 dump
replay bypassed it. That initialization writes function definitions and
`_fn_versions`, so use a raw read-only client for production validation.

Post-repair checks used saved CRM project 1000049, hash shard `[a,b)`, with
column names and every returned value compared after sorting rows (no
float tolerances or masked columns):

| Saved shape | Rows | Result |
|---|---:|---|
| estimates off (`21-13-04`) | 2,292 | Plain SQL = unkeyed UDF; forced keyed still returns `RegionUnsupportedConstruct` (W2's known UNION blocker). |
| detail (`21-13-08`) | 5,179 | Plain SQL = unkeyed UDF = forced keyed UDF on the new bench binary. |
| expanded cross (`21-13-11` / `21-13-12`) | 2,997 | Plain SQL = unkeyed UDF = forced keyed UDF on the new bench binary. |
| child cross (`21-13-17`) | 2,997 | Plain SQL = unkeyed UDF on the existing local production listener, using read-only SELECT/SET statements. After child-table restoration, the new bench binary also matches with forced keyed execution. |

Restored the child table into the bench copy from one read-only source SELECT
covering the `[a,b)` hash slice across all projects: 586,739 rows (158,259
Homebot and 428,480 AirDNA). Its complete 30-column schema came from
production's `SHOW CREATE TABLE`; both missing parent-report columns were
added locally with the migration's defaults. Every child row and column
matched the captured snapshot, including after the bench server restarted.
The ordered snapshot SHA-256 is
`e1738da002f7dced1d6f2066adbd69dfe935be3276b3f8e80bebf9bb6f5be0f0`.
The child fixture covers only that hash slice; existing invoice/parent-report
data retains its earlier snapshot date. No production data, schema,
function, or server process was changed.

Fresh `rfFullMatrix.ts` runs covered child, detail, and expanded modes, both
simple and cross-division, for CRM and AirDNA. A separate raw replay forced
KEYED BY and compared every column without the harness's cross-division
rate folding, both on the first keyed run and its repeat:

- All six CRM shapes and AirDNA's child/expanded shapes matched plain SQL
  and unkeyed UDFs exactly.
- AirDNA detail simple: 202,414 rows; keyed and unkeyed UDFs matched exactly.
  Both differ from plain SQL on two `exchangeRate` cells and their two
  `lastExchangeRate` successors, by floating-point rounding only. All other
  columns matched.
- AirDNA detail cross: 212,348 rows; keyed versus unkeyed UDFs differs on
  13 rows, confined to `exchangeRate` (9 cells) and `lastExchangeRate` (8).
  These include 0-versus-1 selections, beyond the accepted last-ulp exception.
  The generator uses `ANY_VALUE(exchangeRate)` when the cross-division sum
  is zero (`helpers.ts` around line 2942). A subsequent per-row investigation
  confirmed identical `cross_division_fields` inputs in a different arrival
  order: every input amount in all nine differing current-rate groups is
  zero. `ANY_VALUE` chooses a different non-NULL rate, and LAG propagates
  those choices. All amounts and other columns matched. Keep cross-detail
  excluded while this is unresolved.
- Expanded cross matched values but repeated keyed runs were slower:
  CRM about 1.86 s versus 0.51 s unkeyed; AirDNA about 12.89 s versus 3.09 s.
  The trace showed two expensive failed outer-boundary attempts before a
  cached inner boundary, whose regional execution itself took only 15 ms
  in the CRM probe. The generic declaration-to-boundary cache shortcut now
  removes both failed attempts on unchanged repeats. After one warmup,
  three repeat runs measured CRM 713.7/774.2/732.8 ms (732.8 ms median) and
  AirDNA 3682.1/3670.5/3452.8 ms (3670.5 ms median). These compare against
  the earlier single repeat samples, not an interleaved controlled A/B.
  All repeated result hashes matched and all warm traces show zero failed
  outer candidates plus one pooled region-cache hit. First compilation
  remains expensive; the shortcut addresses repeat-query work.

Diagnostic scripts, snapshots, saved SQL pairs, and full column-level
results are under `.bench-data/region-*` (ignored local artifacts). Preserve
SQL dumps before rerunning the app harness: its debug writer randomly prunes
the directory to 20 files. Running from an isolated working directory with
`tsx --tsconfig <wayroll-worktree>/tsconfig.json <absolute-script-path>`
keeps that retention policy away from previous captures. These runs are
local correctness/diagnostic checks, not the final production benchmark.
The complete 12-shape replay was repeated on the final binary: 11/12 shapes
match unkeyed UDFs exactly, 10/12 match both references exactly, and every
keyed result matches its own cached repeat. The two AirDNA detail findings
above are unchanged. The temporary bench server was stopped after validation.
The final ReleaseFast binary is under `.bench-data/region-verified-build`.
Data changes anywhere in the declaration invalidate the shortcut, so live
CDC may reduce its hit rate; production still needs a separate measurement.

A Debug-host diagnostic replay crashed in the persisted `rf_estimates`
DLL's arena allocation path. Its stack is captured in
`.bench-data/region-debug-dll-crash.log`. The cause is not established and
no fix is claimed here; the Wayroll matrix and performance measurements
above use ReleaseFast, as required by this plan.

Final local checks: `zig build test test-v2 -j2` passed (1,473 tests passed,
5 skipped), the ReleaseFast build passed, `zig build bench -j2` completed,
and formatting/diff checks passed. The bench target also needed its missing
`build_options` import and stale snapshot/segment-writer calls brought up to
date before the full suite could run.

### Estimates-off continuation (2026-09-08)

- Wayroll `buildPreRollforwardCTE` now makes `invoice_base_with_estimates`
  a direct projection when `includeEstimates === false`. Its dependencies
  omit the unused estimates chain; latest/average FX conversion remains.
  Ten new generator cases plus the existing customer-tag and region-key
  tests pass (18 tests across three suites).
- The generic region collector accepts consecutive entry projections,
  retaining later projections and intervening computes in execution order.
- An unordered passthrough TVF declaring `.either` execution can defer the
  initial range choice to a later partition-sensitive operation. Existing
  dispatch checks still enforce legal per-range or whole-shard execution.
  This addresses the estimates-off currency-to-customer-months transition
  without matching UDF names or report shapes.
- The new two-project regression exposed a pre-existing consolidation bug:
  BIGINT normalization discarded the low bit, so 100 and 101 shared a key.
  Normalized keys now retain all 64 bits and a separate NULL discriminator.
  Boundary coverage includes adjacent negative/positive values, both i64
  extremes, NULL, and a UDF-to-window pipeline with cached execution.
  `RowKey` grows from 24 to 32 bytes; measure this cost in future large sorts.
- A separate pre-existing schema limitation was observed: a VARCHAR carried
  through an aligned TVF retains its source type in a region, while ordinary
  TVF output uses its declared STRING type. The new granularity fixture
  explicitly casts its input to the declared STRING type; it does not claim
  to fix that metadata discrepancy. Wayroll's tested carry columns are TEXT.
  The subsequent continuation below fixes this limitation.

The proposed zero-revenue rate rule is tested but **not applied**. Replacing
only cross-division `ANY_VALUE(exchangeRate)` with
`MAX_BY(exchangeRate, groupOrderNumber)` aligns the rate with the existing
currency-metadata rank and makes keyed/unkeyed UDF cross-detail identical.
On the AirDNA slice it changes rate fields in eight of 212,348 rows (six
current-rate and five lagged-rate cells), with no amount or other-field
changes. CRM remains identical. User preference is pending because this
changes existing displayed values. Plain-SQL versus UDF weighted-rate
rounding differences remain separately unresolved.

Current verification: `zig build test test-v2 -j2` passed (1,476 passed,
five skipped); ReleaseFast dist passed. Current binary:
`.bench-data/region-final-build/bin/thindb-server.exe`.

The new binary replays the previous 12 child/detail/expanded cases with the
same outcomes: 11/12 match unkeyed UDFs, 10/12 match plain SQL, all cached
repeats are exact, and no new differences were introduced.

Estimates-off application validation covers CRM and AirDNA, hash `[a,b)`,
across base simple/cross, expanded simple, quarterly/annual intervals, and
simple/cross plan filters. All 14 cases match plain SQL exactly with rate
folding disabled. Every keyed-arm capture contains the declaration and no
estimates TVF. CRM simple plan filtering returns zero rows in this fixture;
AirDNA's corresponding case has 14,530 rows, and crossplans has 15,140.

The base saved-query comparison also checks old/new plain SQL, old/new
unkeyed UDF SQL, keyed SQL, and its repeat: all six arms agree. Traces show
currency conversion, customer-month collapse, and downstream calculation
TVFs executing in the same region, rather than only the entry scan.

Controlled local base-report timings (five rotating measured runs after
warmup, same SQL, raw mysql2 query completion, every result verified):

| Project | Rows | Plain SQL median | Unkeyed UDF median | Keyed UDF median | UDF speedup |
|---|---:|---:|---:|---:|---:|
| CRM 1000049 | 2,292 | 156.98 ms | 123.64 ms | 22.23 ms | 5.56x |
| AirDNA 1000073 | 14,530 | 376.29 ms | 449.80 ms | 134.82 ms | 3.34x |

These use the local fixture, not production. Captures/results:
`.bench-data/region-noest-keyed-matrix-*`, `region-noest-final-comparison.log`,
`region-noest-bench-results.json`, and `region-final-replay-results.json`.

The Wayroll gate now permits estimates-off combinations within the existing
aggregate/non-child/invoice-date/no-hook exclusions, except cross-division
plan filtering. A five-run rotating three-arm check of that combination
returned exact results but measured AirDNA plain SQL 800 ms, unkeyed UDF
1,082 ms, and keyed UDF 1,358 ms medians. CRM's two-row result was faster
keyed (132 versus 156 ms), but that fixture is too small to outweigh the
AirDNA regression. The exclusion is shape-based, with no project IDs.
Final end-to-end runs confirm the excluded shape emits no KEYED declaration
and remains value-exact. This is a performance follow-up, not an eligibility
failure. Results: `.bench-data/region-noest-crossplans-ab.log` and
`region-noest-crossplans-final-gate.log`.

Roll out the engine fixes before the Wayroll gate change; nothing has been
deployed. The temporary 13311 fixture server is stopped. Production's 13310
listener remains PID 127376 and was not restarted or changed.

The full `zig build bench -j2` retry passed. The first attempt stopped with
`AccessDenied` during a durability-benchmark flush; the cause is not
established, and no storage fix is claimed. The complete retry included the
region benchmark above. Logs are `%TEMP%/thindb-region-final-bench.log` and
`%TEMP%/thindb-region-final-bench-retry.log`; complete tests/build logs use the
same `thindb-region-final-` prefix. Final formatting and diff checks pass.

### Passthrough metadata and temporary lookup cache continuation

Aligned TVF passthroughs now capture their input bindings before computed
outputs can shadow those names. When the declared output uses a different
compatible string-family type, a `view_cols` operation appends a typed view
over the same bytes and NULL bitmap. It does not copy strings or alter the
source column. Regression coverage includes all STRING/VARCHAR/CHAR/JSON
view combinations, NULL/empty/UTF-8 data, borrowed-buffer identity, rejection
of numeric reinterpretation, per-range and whole-shard TVFs, downstream
windows, output aliases that shadow inputs, and cached repetitions.

The cross-plan slowdown was reproduced with saved SQL including its actual
DROP/CREATE/INSERT temporary-table prefix. Reusing the same temporary table
gave 313-321 ms runs; recreating it gave 577-741 ms and invalidated the region
cache. The latter rebuilds both the program and roughly 1.7-1.9 GiB of pooled
working state. Baseline traces: `.bench-data/region-crossplans-baseline*`.

Temporary tables with no disk segments, at most 16,384 rows, and at most 4 MiB
of column data now have a content-based version. Under the table lock it
fingerprints column names/types/nullability, uniqueness, order keys, and all
ordered values and validity bits. Identical replacements may reuse compiled
lookups and pooled state across sessions. Changed contents or schemas
invalidate them. Larger/spilled temporary tables are uncacheable; allocation
addresses cannot safely identify recreated tables. Integration cases cover
same-count replacements, NULL-to-zero changes, type changes, changed order,
multiple sessions, and flushed temporary tables, with the lookup output
required to come from the region.

The real AirDNA lookup has 5,988 plan IDs. An initial 4,096-row bound excluded
it; the final 16,384-row / 4 MiB bound is exercised by a 6,002-row integration
fixture. On the final replay, DROP/CREATE with unchanged values now reports a
pooled cache hit. These are content comparisons, not allocator identities.

Five rotating application runs after warmup (including plan-table recreation,
all output fields compared, no rate folding) now give:

| Project / estimates-off crossplans | Rows | Plain SQL median | Unkeyed UDF median | Keyed UDF median |
|---|---:|---:|---:|---:|
| CRM 1000049 | 2 | 165 ms | 179 ms | 71 ms |
| AirDNA 1000073 | 15,140 | 957 ms | 1,138 ms | 447 ms |

AirDNA is 2.55x faster than the matched unkeyed UDF arm. The earlier keyed
1,358 ms measurement was a separate run; use the rotating comparison above
for the current performance claim. The estimates-off crossplans exclusion
has been removed from Wayroll. Existing hook/detail/child/alternate-FX and
expanded-cross exclusions remain. The proposed zero-revenue rate rule is
still unapplied.

Current ReleaseFast binary: `.bench-data/region-content-build/bin/thindb-server.exe`.
Full tests: 1,479 passed, five skipped (`thindb-region-content-final-tests.log`
under `%TEMP%`). Traces/results: `.bench-data/region-crossplans-content-final*`
and `region-noest-crossplans-content-ab.log`.

Final application checks on this binary: all 14 estimates-off cases and four
estimates-enabled plan-filter cases match every field exactly. Each keyed
capture includes the declaration. The previous 12 child/detail/expanded
replays retain the same known outcomes (11/12 exact against unkeyed UDFs,
10/12 against plain SQL, 12/12 exact repeats, no new errors/differences).
Artifacts: `region-noest-content-matrix.log`, `region-estimates-content-plans.log`,
and `region-content-replay-results.json` under `.bench-data`.

The owned bench listener has been stopped; production 13310 remains PID
127376. No production deployment, schema/function write, or restart occurred.

The complete `zig build bench -j2` suite passes on this build, including the
million-row exact window comparison (71.56 ms staged / 50.29 ms keyed).
Log: `%TEMP%/thindb-region-content-bench.log`. Final application checks:
three Jest suites / 18 tests passed (`.bench-data/region-wayroll-content-tests.log`).
Formatting and diff checks pass. No commits or deployments were made.

## 0. Ground rules for whoever picks this up

- General engine work only. Never special-case a benchmark or a wayroll query
  shape; every recognizer extension must be a legitimate SQL construct.
- Correctness bar: value-exact against the plain-SQL thinDB arm (and the
  StarRocks arm where it runs) via the wayroll harness `RF_COMPARE=1`. The
  known, accepted exception is a last-ulp `exchangeRate` float difference in
  detail mode (summation order inside `rf_customer_months`); every derived
  amount must still be identical.
- Build the server with `-Doptimize=ReleaseFast`. Run `zig build test` and
  `zig build test-v2` before each commit. Never build or test while a bench
  is running.
- Do not run AirDNA (project 1000073) detail/expanded variants at full data on
  starrocks1; the 1/16 hash shard (`RF_HASH=a`) is fine. `/data/starrocks` is
  never touched.
- Merge, don't rebase. Never `git add -A`; check `git diff --cached
  --name-only` before committing. `src/net/ir_dump.zig` is an untracked local
  file: never commit or delete it. The wayroll branch `rollforward-thindb-local`
  gets commits, never a PR.

## 1. How the region path is engaged today

- wayroll's generator (`src/workers/rollforwardProcess/helpers.ts`, around
  line 3392) decides `regionEligible` and, when true, sets
  `regionKeys: ['customerNumberLC']` on the final CTE block. `CTEBuilder`
  (`src/helpers/CTEBuilder/index.ts:398`, `regionKeyDeclaration`) then emits
  `WITH KEYED BY (customerNumberLC) invoice_base AS (...)`. Off switch:
  `THINDB_KEYED_REGIONS=0`.
- The current gate (verbatim):
  `cteHooks.length === 0 && doAgg && !childCustomer && fxMode === INVOICE_DATE
  && !(expandedRollforward && isCrossDivision)`.
  Every exclusion exists because a declaration over an unsupported shape is a
  hard query error.
- thinDB side: `src/net/cte_stages.zig:96` calls
  `region_rollforward.compileDeclared` with `try`; the recognizer returns
  `error.RegionUnsupportedConstruct` (`src/net/region_rollforward.zig:167-168`)
  when no boundary matches a supported shape, and
  `error.RegionKeyContractViolation` (lines 164-165) when no boundary even
  satisfies the key contract. Both surface to the client as query errors.
- Diagnostics: start the server with `THINDB_REGION_TRACE=1` (per-boundary
  declines, side-join declines, phase timings on stderr) and
  `THINDB_REGION_STEPS=1` (prints every dispatched step, bottom-up).
- Boundary search (`compileDeclared`, lines 95-168): starting at the topmost
  CTE carrying `region_keys`, walk down through `materialize / select /
  exclude / filter / group_by / compute / limit / order_by` nodes; each
  `materialize` node is a candidate boundary, verified with
  `verifyKeyContract` (line 175), hashed, cache-checked, then `buildRegion`.
  Any other node kind (`join`, `window`, `table_fn`, `union`) ends the walk
  (`else => break`, line 160).
- Inside `buildRegion`: `collectPipeline` (line 1704) flattens the left spine
  into `Step`s (line 1676: select, exclude, compute, alias_name, filt,
  group_by, window, table_fn, join, union_tvf); anything else is
  `RegionNoMatch` with no trace line. The entry schema is built at lines
  2002-2030 (scan output ++ entry-derived computes via
  `region.computeOutputSchema`, line 2019). Steps dispatch bottom-up through
  `dispatchStep` (line 2292); `dispatchWindow` (line 2573) accepts only
  all-`ROW_NUMBER` or all-`LAST_VALUE` windows; `dispatchJoin` (line 2585)
  tries the co-partitioned side join (`trySideJoin`, line 3120), then a
  drained small side (<= 1M rows) as a hash/keyed probe.
- The executor (`src/exec/region_exec.zig`) already has a `.lag` op
  (spec at line 999: `{ name, src, offset }`; schema at 1154; state at 1412;
  run at 1714: per range, `offset` leading NULLs then the source shifted) that
  the recognizer never produces.

## 2. Findings: one blocker per excluded shape

Method: take the Zig-arm SQL dumps the harness writes under `ctedebug/` when
`NODE_ENV=development`, force the declaration with

    sed -E '0,/^WITH /s//WITH KEYED BY (customerNumberLC) /' dump.sql > dump_keyed.sql

run each against a trace-enabled local mirror, and read the `[region]` lines.
For the child shape, single constructs were injected into the (engaging) base
dump one at a time to isolate the culprit. Timings are the local mirror on the
deployed binary (v0.1.81, main add919a), medians, CRM = project 1000049,
AirDNA = 1000073, hash shard `a`.

| Shape | Decline point | Blocker | Evidence |
|---|---|---|---|
| child simple / cross | entry schema build, right after `compile scan_build` (no `entry_schema` mark) | An entry-derived column that reuses a scan column's name in invoice_base: `CASE ... END AS customerNumber`, `IF(...) AS customerName`, `IF(...) AS customerEmail`. `computeOutputSchema` fails on the duplicate name. | Injecting only `LOWER(customerEmail) AS customerEmail` into the base dump declines; injecting only the CASE route key (`CASE WHEN originalCustomerNumber IS NOT NULL THEN LOWER(originalCustomerNumber) ELSE LOWER(customerNumber) END AS customerNumberLC`) engages; injecting only the inline `report_customer_revenue_rollforward_child` totals scan engages; the NULL-arm parent `IF`s under new names engage. |
| detail simple / cross | `dispatch declined on step 13/29 'window'` | `LAG(externalPlanId, @comparisonMonths)` and `LAG(planName, ...)` `OVER (PARTITION BY projectId, divisionId, customerNumberLC ORDER BY month)` in `rollforward_with_last_plan_and_division`. In agg mode column pruning deletes these windows (their outputs are unreferenced), which is why base passes. | Same windows exist in the base dump; base engages, detail declines at the window step. |
| expanded cross | `dispatch declined on step 4/61 'join' (right: cs, type: left, on: 4)` at both tried boundaries | `LEFT JOIN cross_division_with_latest_fields_cs cs` (a LAST_VALUE-windowed CTE over `cross_division_agg_cs`, itself built on the parent-keyed `rf_expanded_calc` pass, which partitions by projectId, divisionId). Not scan-shaped, so no side join; and the boundary walk stops at the join node, so the boundaries under it (the same block expanded simple runs in-region) are never tried. | Side declines: `no ON pair binds route`, `right subtree not scan-shaped`, `route name 'customerNumberLC' unresolved`. Only two boundaries attempted (steps 4/61 and 3/60). |
| noest (estimates off) | pipeline collection (`declared block declined` with no walk mark) | The estimates CTE chain and its UNION ALL are still emitted, guarded by `where: '1 = @includeEstimates'` (helpers.ts:1485), a dead arm the recognizer can't see through (plain UNION ALL is not a Step). | SQL diff vs base: only the guard; the `estimatesArm` TVF at helpers.ts:1547 is skipped when estimates are off, so the inline chain is what remains. |
| fx latest / average | pipeline collection | Four plain UNION ALLs from the SQL currency handler / pass-through pairs (`converted_amount_adjusted_*`, `converted_invoice_normalized_*`, `converted_invoice_target_currency_*`, `post_converted_invoice`, `with_plan_data`) because `currencyArm` (helpers.ts:1744) only emits `rf_currency_convert` for `INVOICE_DATE`. | `grep -c 'UNION ALL'` = 4 in the fx dumps vs 0 non-TVF unions in base. |
| hooks, non-LTM (Aplos net, TTM, project 1000053) | none | The blanket `cteHooks.length === 0` exclusion. | Forced declaration engaged: 24.7 ms vs 131 ms unkeyed Zig vs 306 ms SQL; sorted output of 379 rows byte-identical to the unkeyed run. |
| hooks, LTM (VM MRR1 1000052, KidKare MRR1 1000050) | pipeline collection | Three plain UNION ALLs plus window kinds the region lacks: `AVG(...) ROWS BETWEEN 11 PRECEDING AND CURRENT ROW`, running `SUM(...) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, partition-wide `MIN/MAX/COUNT`, `LAG`, mixed with `ROW_NUMBER`; all partitioned by projectId, divisionId, customerNumberLC. | Window census of the VM MRR1 dump. |

Local medians (ms), SQL arm / Zig arm today / the in-region sibling shape:

| Shape | CRM | AirDNA |
|---|---|---|
| base simple (in-region) | 144 / 21 | 386 / 121 |
| base cross (in-region) | 200 / 28 | 571 / 143 |
| expanded simple (in-region) | 387 / 28 | 1712 / 176 |
| child simple | 139 / 121 / 21 | 395 / 431 / 121 |
| child cross | 211 / 189 / 28 | 549 / 699 / 143 |
| expanded cross | 521 / 440 / 28 | 2333 / 2706 / 176 |
| noest simple | 138 / 123 / 21 | 393 / 441 / 121 |
| fx latest | 136 / 94 / 21 | 406 / 449 / 121 |
| fx average | 140 / 92 / 21 | 417 / 439 / 121 |
| detail simple | 187 / 156 / n.a. | 1603 / 1509 / n.a. |
| detail cross | 281 / 218 / n.a. | 2013 / 2153 / n.a. |

Side effect worth knowing: outside the region, `TableFnExec`
(`src/exec/table_fn.zig`, worker state lifetime around lines 1219-1322)
rebuilds `rf_currency_convert`'s 1M-row rate lookup per worker per query
(58% of CPU on the box for child simple). The region pool retains worker
state across queries, so moving a shape in-region removes that cost too. If
any shape must stay outside, building broadcast-derived TVF state once per
query (shared read-only) is the fallback lever.

## 3. Work items, in order

Each item is independently shippable; verify with the harness after each one
(section 4) before moving on.

### W1. Soft decline in the engine + declare everywhere in wayroll

- thinDB: in `cte_stages.zig` around line 96, catch
  `error.RegionUnsupportedConstruct` from `compileDeclared`, print the
  existing `[region] KEYED BY block did not compile ...` line, and continue
  with the staged path as if no declaration were present. Keep
  `error.RegionKeyContractViolation` hard (a declared key missing from every
  partition is a real mistake). Add a switch to make unsupported shapes hard
  again for tests/CI (e.g. `THINDB_REGION_STRICT=1`, or a session `SET`),
  and use it in the existing region tests so coverage doesn't silently
  degrade. Update the comment at the call site ("The declaration is a hard
  contract") and DESIGN.md if it states the hard-error policy.
- Owner sign-off needed: REGION_PLAN.md's contract says unsupported
  constructs are compile errors, never a silent fall-back. The proposal keeps
  the log line (not silent) and keeps contract violations hard.
- wayroll (`helpers.ts:3392-3400`): drop the shape gate; always set
  `regionKeys: ['customerNumberLC']`. Keep `THINDB_KEYED_REGIONS=0` as the
  off switch. Update `src/helpers/CTEBuilder/regionKeys.test.ts` if it
  encodes the gate.
- Effect: non-LTM hooks (Aplos class) land in-region immediately; every later
  item flows without a generator change; the trace shows what to add next.

### W2. wayroll: skip the estimates chain when estimates are off

- `helpers.ts` around 1485 (`where: '1 = @includeEstimates'`) and the
  `invoiceBaseWithEstimates` union: when `params.includeEstimates === false`,
  do not emit `last_month_invoices ... estimates_final` nor the UNION ALL;
  `invoice_base_with_estimates` becomes a plain projection of the base.
- Engine alternative (general, optional): constant-fold a UNION ALL arm whose
  filter is a literal `false` after SET substitution before recognition.
  The generator fix is simpler and also removes dead work from the SQL arm.
- Verify: `RF_ONLY="noest"` for both projects, `RF_COMPARE=1`.

### W3. thinDB: entry-derived columns that shadow scan columns

- `region_rollforward.zig` lines 2002-2030. When an entry-derived name equals
  a scan column name, `computeOutputSchema` (line 2019) fails and the whole
  block declines. Fix options, pick the simplest that keeps every downstream
  name resolving: (a) derive under a hidden name (`__entry_shadow_<name>`),
  then hide the scan column and alias the derived one, reusing the existing
  exclude / alias_name step machinery in the frame builder (`b.fb`); or
  (b) drop shadowed scan columns from `scan_schema` before building the entry
  schema when nothing else references them. Mind `rowloc_entry` (last scan
  column) and the pinned-literal loop that follows.
- Reproduce before/after with the injection probe: replace
  `customerEmail AS customerEmail,` in a base dump with
  `LOWER(customerEmail) AS customerEmail,` (declines today).
- Verify: `RF_ONLY="child"` both projects; the child re-key CASE on the
  route key already engages, so no scatter change is needed.

### W4. thinDB: boundary walk descends through joins

- `compileDeclared` lines 95-168: add `.join => |j| cur = j.left` to the
  walk (the left input is the block spine; the right input is the side).
  Consider `.window => |w| cur = w.upstream` and `.table_fn` likewise only if
  a real shape needs it; expanded cross needs the join case only.
- Effect: the `cs` join stays in the staged path; the region compiles the
  block under `final_rollforward_<hash> p`, the same block expanded simple
  already runs in-region. The `e` side (`final_rollforward_<hash2>`, the
  parent-keyed `rf_expanded_calc` pass) is also above the new boundary.
- Verify: `RF_ONLY="expanded cross"` both projects; expect the trace to show
  `region engaged` and no `dispatch declined ... 'join'`.

### W5. thinDB: LAG windows

- `dispatchWindow` (line 2573): add an all-`LAG` case (mirroring
  `pushRanksFromWindow` at 4063 / `pushFillLast` at 4085) that pushes one
  `.lag` op per call. Requirements to check at recognition: `PARTITION BY`
  contains the declared keys (the contract already guarantees it); `ORDER BY`
  equals the region's range order (month ascending, matching `order_specs`);
  the offset is a literal after SET substitution (`@comparisonMonths` -> 1);
  no default-value argument, or map a literal default onto the NULL fill.
- Verify: `RF_ONLY="detail"` both projects. AirDNA detail runs will report
  `zig=DIFF` of a handful of rows: confirm with `RF_DIFF_CHARS=6000` that
  every difference is the known last-ulp `exchangeRate` and nothing else.

### W6. wayroll kernel: fx latest / average through `rf_currency_convert`

- `helpers.ts:1744` gates the TVF to `INVOICE_DATE`. Extend
  `src/thindb-functions/rf_currency_convert.zig` (broadcast inputs at
  `broadcast_inputs = &.{1,2}`; `rf_usd_rates.sql` supplies the daily USD
  rate table) with a mode argument: latest = one rate per currency (about
  183 rows), average = one rate per (currency, month) (tens of thousands of
  rows). Emit the TVF for those modes so the four handler UNION ALLs
  disappear from the SQL.
- The SQL handler chain (`converted_amount_adjusted_*` ...) is the value
  reference; keep the StarRocks emission unchanged.
- Verify: `RF_ONLY="fx"` both projects, plus `RF_SR=1` for a third reference.

### W7. thinDB: LTM hook constructs (long tail)

- Ordinary `UNION ALL` ingress is now supported; see the implementation
  notes above. Further branch fusion or parallel scatter requires measurement.
- Frame window aggregates: `AVG` over `ROWS BETWEEN n PRECEDING AND CURRENT
  ROW`, running `SUM`, partition-wide `MIN`/`MAX`/`COUNT`. These need new
  region ops in `region_exec.zig` (per-range sliding window over the sorted
  range, same shape as `.lag`).
- Only after W1-W6; measure the MRR1 hooks (`RF_ONLY` in CASES mode) before
  and after.

## 4. Verification recipe

- Production: the owner authorizes read-only benchmarks against the existing
  server on port 13310. Connect to that listener; do not start another server,
  restart/stop it, or open its data directory from another process. Check
  each harness mode for setup writes before using it under this authorization.
- Local testing: use the separate `.wayroll-bench-db` copy, DB
  `wayroll_prod__public`, MySQL port 13311. Ensure the copy is not already
  open in another process, then start the ReleaseFast server with
  `--data-dir .wayroll-bench-db --mysql-port 13311 --pg-port 0 --native-port 0
  --max-dop 12 --cache-size 6G --memory-budget 32G --query-memory-budget
  25769803776`, with `THINDB_REGION_TRACE=1` in the environment while
  developing.
- Harness (wayroll worktree `C:/development/wayroll-api/.wt-thindb`, branch
  `rollforward-thindb-local`):

      RF_HASH=a RF_ITER=3 RF_COMPARE=1 RF_HOST=127.0.0.1 RF_PORT=13311
      NODE_ENV=development NODE_OPTIONS=--max-old-space-size=8192
      RF_PROJECT=1000049 RF_ONLY="child"
      node_modules/.bin/tsx src/scripts/rfFullMatrix.ts

  (one command line; on Windows set the variables first, then run tsx).
  Output lines: `OK <variant> rows=N sql=min/median zig=min/median zig=MATCH`.
  Without `RF_PROJECT` the harness runs the CASES list (hook projects:
  `hooks: Aplos net (TTM fires)`, `hooks: VM MRR1 trailing`, `hooks: KidKare
  MRR1`). `NODE_ENV=development` writes each arm's SQL to
  `ctedebug/cte-debug-<ts>-final_result.sql` (Zig arm = contains `TABLE(rf_`,
  keyed = contains `KEYED BY`).
- Direct probes: any dump can be replayed with a small mysql2 script (run the
  `SET` lines, then the `WITH`), and forced keyed with the `sed` above. For
  value checks, sort the JSON rows and `cmp` the keyed vs unkeyed outputs.
- Box (starrocks1, ssh alias, `sudo -n` works): thindb service on port
  13310, conf `/etc/thindb/thindb.conf`, binary v0.1.81. The harness copy
  lives in `~/wayroll-bench`; passwords come from the conf on the box and are
  never printed. A 3-arm run adds `RF_SR=1`.
- Before trusting a regression: check box load and stray processes, confirm
  the listener pid is the binary you built, and compare on the same machine.

## 5. Open questions for the owner

- Soft-decline policy (W1) versus the "never a silent fall-back" contract in
  REGION_PLAN.md section 5. Proposed: logged fall-back by default, strict
  switch for tests.
- AirDNA crossplans loses locally while already in-region (SQL 832 vs Zig
  919 ms median): a region-path performance question, not eligibility.
- Box amplification: region steady state is about 63 ms on the box vs 21 ms
  locally for the same binary and data; 68% user-space spin at three
  addresses, hypothesis: memtable rows appended by live CDC. Not part of this
  round.
- PR #35 (decoded block cache) merged 2026-09-07 (51b4541). It adds about
  14 GB peak working set on the 100M ClickBench table and is not on the box
  yet (deployed v0.1.81 predates it).
