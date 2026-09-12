# Ordinary SQL diagnosis: Sierra, September 11–12, 2026 UTC

The largest actionable costs are unnecessary lookup work and repeated lookup
construction. The empty/selective plan cases scan the entire stored rollforward
table. No-estimates builds two currency lookups even though that conversion
branch produces no rows. Cross queries repeatedly build the same external-plan
lookup. A separate socket experiment confirms a delayed-ACK/Nagle response tail.

The findings below describe the original baseline. The subsequent
[engine implementation and before/after verification](#engine-implementation-2026-09-12-utc)
are recorded at the end of this report. Production services remain unchanged.

## Method and retained evidence

The main matrix contains nine cases, explicit DOP 12 followed by DOP 16, one
warmup, five measured executions, and one separate fingerprint execution per
engine/case/DOP: 180 measured queries, 36 warmups, 36 fingerprint queries.
Engine order alternates between case blocks and reverses in the second DOP pass.
Every ThinDB block starts a fresh isolated service; it stops before StarRocks
runs. Profiling is a separate matrix with one warmup and one retained profile
per engine/case/DOP. No profiling stage-forcing environment variables are used.

The archived Wayroll `b5359d9e` generator, Sierra project `1000049`, division
`1000142`, original per-case dates, USD target and hash range `[a,d)` are retained.
Ordinary SQL and all parameters are saved. UDF switches and keyed declarations
are disabled. The final MySQL response is consumed without decoding rows during
timings. Application module loading and connection setup are recorded separately;
per-call SQL generation and plan-ID discovery remain in the end-to-end samples.

ThinDB uses the existing private snapshot
`/home/ubuntu/wayroll-bench/region-matrix-0258e6d/data` on loopback port **13311**.
The ReleaseFast Zig 0.16 Linux binary is engine commit `d29f5f1`, SHA-256
`87b120dbe6be6b4f4e1967dca46b4a7031f0474ff959348689e807c78eb17796`.
The checkout at `e4aba81` adds documentation after that engine commit.
Limits remain 2 GiB block cache, 4 GiB region pool, 16 GiB query/shared budgets,
20 GiB service ceiling, no swap and no compaction. The startup reserve changed
from 10 to 8 GiB after a headroom interruption; execution limits did not change.
An initial completed base-simple sample set with a connection-cleanup error is
archived under `incomplete-cleanup-*` and excluded. Other completed blocks were
retained on resume.

StarRocks 4.0.10 reads its live `wayroll` database on loopback port 9030. These
engines do not share a transactional snapshot. Each benchmark connection verifies
`pipeline_dop=12/16`, `parallel_fragment_exec_instance_num=1`,
`enable_runtime_adaptive_dop=false`, `enable_query_cache=false`, and disabled
profiling during timings. No global StarRocks setting changes are made.
The host is a Ryzen 9 5950X, 16 physical / 32 logical CPUs, with live production
and CDC workloads.

All raw evidence is retained locally under `.bench-data/ordinary-sql-diagnosis/`
and remotely under `/home/ubuntu/wayroll-bench/ordinary-sql-diagnosis/`:

- `samples.csv`, `samples.json`, `summary.json`: every accepted sample and comparisons.
- `timing-*-*/`: exact SQL/parameters, session settings, logs, memory receipts.
- `profile-*-*/`: separate ThinDB operator/MySQL traces and StarRocks
  `EXPLAIN ANALYZE` plus raw profiles; warm and subsequent profiles are retained.
- `profiles.json`: parsed stage costs, row counts and effective scan/pipeline DOP.
- `materialization-*/`: controlled CTE perturbations, outside the main benchmark.
- `transport-{off,on}-*/`, `packet-gaps.json`: socket readbacks and packet traces.
- `detail-value-check.json`, compressed decoded results: separate value validation.
- `health-*.json`, `tests-verified.log`: service/CDC receipts and local tests.

Early `service.json` files include inherited archive fields such as its old
three-iteration setting. The current `result.json` samples and this protocol are
authoritative for execution counts; later receipts nest the old build metadata.

## Matched baseline

Median milliseconds; parentheses retain the full range of the five measurements.
These shared-host samples show substantial scheduling variation. In particular,
the second pass is not evidence that increasing DOP intrinsically accelerates
ThinDB relative to StarRocks.

| Case | DOP | StarRocks median (range), ms | ThinDB median (range), ms |
|---|---:|---:|---:|
| base simple | 12 | 463 (445–472) | 362 (350–375) |
| base simple | 16 | 3582 (3163–5264) | 743 (610–811) |
| noest simple | 12 | 221 (211–226) | 475 (470–549) |
| noest simple | 16 | 2299 (856–2930) | 631 (530–665) |
| plans simple | 12 | 697 (678–739) | 779 (775–889) |
| plans simple | 16 | 1553 (1419–3001) | 1399 (1078–1935) |
| crossplans | 12 | 775 (762–977) | 1262 (1199–1296) |
| crossplans | 16 | 1823 (1103–1936) | 1987 (1619–2039) |
| base cross | 12 | 552 (539–893) | 843 (790–947) |
| base cross | 16 | 743 (600–1071) | 947 (863–1015) |
| child cross | 12 | 635 (567–971) | 985 (819–1016) |
| child cross | 16 | 728 (639–933) | 908 (893–1021) |
| detail cross | 12 | 2302 (948–2876) | 1199 (1096–1364) |
| detail cross | 16 | 748 (652–855) | 977 (901–1120) |
| expanded simple | 12 | 6988 (4464–7230) | 1405 (1132–1492) |
| expanded simple | 16 | 2229 (2166–2620) | 1298 (1169–1317) |
| interval quarter | 12 | 1421 (678–5064) | 511 (434–635) |
| interval quarter | 16 | 541 (451–554) | 441 (393–546) |

The raw StarRocks profiles locate an important confounder. Base-simple's DOP-16
profile has 2328 ms execution wall and 2285 ms peak scheduling time, versus
259/256 ms at DOP 12. Reported cumulative operator time is similar, 213 versus
236 ms, with zero spill bytes. These counters overlap; they are not additive.
They support scheduling delay as an explanation for the apparent reversals,
without proving whether host competition or StarRocks scheduling policy caused it.

The three uncertain cases received another 60 uninstrumented measurements, this
time running DOP **16 before 12**, again with one warmup, five measurements and
separate fingerprints. These are retained under `confirmation/`, not pooled into
the first matrix:

| Case | DOP | StarRocks median (range), ms | ThinDB median (range), ms |
|---|---:|---:|---:|
| noest simple | 16 | 1885 (402–2959) | 682 (608–820) |
| noest simple | 12 | 257 (225–562) | 690 (614–812) |
| detail cross | 16 | 1708 (782–4733) | 1126 (1025–1401) |
| detail cross | 12 | 1755 (1505–3235) | 1259 (1236–1395) |
| interval quarter | 16 | 3545 (1041–3750) | 615 (452–785) |
| interval quarter | 12 | 948 (714–1453) | 463 (441–529) |

| Original apparent loss | Classification supported by these runs |
|---|---|
| No estimates | Reproducible execution/preparation loss at DOP 12, plus a smaller transport cost. DOP-16 cross-engine ranking is scheduling-dominated. |
| Crossplans | Selective-workload execution cost: broad totals scan and repeated lookup builds for three rows. Loss persists at both matched DOPs; output delivery is negligible. |
| Detail cross | Measurable extra window/lookup/serialization work, but the original cross-engine loss is not stable. It reverses in both confirmation blocks; treat the ranking as measurement/scheduling variation. |
| Base cross | Reproducible execution cost at both matched DOPs; repeated lookup builds and the cross window chain dominate, with a smaller transport tail. |
| Child cross | Same reproducible execution pattern as base-cross at both matched DOPs. |
| Plans simple | Empty-workload preparation cost is directly reproduced. The cross-engine margin varies because StarRocks setup/scheduling also varies. |
| Interval quarter | Original 7 ms margin is measurement noise at this resolution. ThinDB wins both baseline and confirmation medians; no persistent loss is established. |

Base-simple and expanded-simple remain useful controls, with lower ThinDB medians
in both passes. The very large StarRocks timings are retained rather than promoted
as evidence of an engine improvement.

## Dominant stages and parallelism

The following are the second, separate DOP-12 profiling executions. ThinDB
`[cte] execute` subtracts nested upstream stage execution and is an exclusive
stage wall time. Indented per-operator `[cte]` lines are inclusive and must not
be added. The `[self]` operator table is another view of the same work, not extra
time. `compile.staged_total` includes eagerly executed stages; it is not pure
compiler CPU time. Profile costs are attribution evidence, not replacement
benchmark timings.

| Case / stage | Output rows | Exclusive stage ms | Additional evidence |
|---|---:|---:|---|
| noest: normalized-currency window | 27,485 | 179.93 | Fact scan still returns the original invoice rows |
| noest: pre-records window | 27,485 | 275.93 | Two 1,018,207-row FX builds; both joined conversion outputs are empty |
| plans simple: customer monthly totals | 14,239 | 888.16 | Invoice input is already empty; all 576 stored-rollforward row groups are scanned |
| crossplans: customer monthly totals | 30,119 | 727.69 | Two invoice rows ultimately yield three result rows |
| crossplans: cross-division ranked | 4 | 168.28 | Window sorting totals only 0.003 ms; lookup preparation dominates |
| base cross: pre-records window | 64,192 | 102.62 | Includes an external-plan lookup |
| base cross: cross-division ranked | 31,558 | 174.66 | Repeated external-plan lookups |
| base cross: second last-amount window | 30,119 | 61.68 | Additional window stage after cross-division processing |
| child cross: cross-division ranked | 31,558 | 179.80 | Same major physical stages as base cross |
| detail cross: cross-division ranked | 31,558 | 233.96 | Same lookup-heavy shared stage |
| detail cross: latest-fields window | 30,119 | 111.06 | Additional detail-specific window work |

The two plan cases decode 37,453,423 stored-rollforward rows with **zero row-group
pruning**. Plans-simple spends 954 ms in staged compilation and only 11 ms in
the subsequent result pull. Crossplans spends 770 ms in staged compilation and
408 ms in the subsequent pull. Its final serialization is 0.004 ms. This is
execution performed during preparation, not time spent sending three rows.

At DOP 16, the same totals stages cost 1003 ms (plans-simple) and 905 ms
(crossplans), with the same output cardinalities. Both plan-ID discovery queries
return exactly `['']`; every run asserts this. They remain empty/selective tests.

Base/child cross each scan the 77,989-row `external_plan` lookup **seven times**;
base-simple scans it four times. The external-plan scans use two workers, limited
by two row groups. Hash-table construction traces separately report eight build
workers. The invoice scans report requested DOP 12/16 but effective DOP **5**.
Stored-rollforward scans and the no-estimates FX scans use the requested 12/16.
Stage-buffer scans also have smaller worker counts. A server-wide DOP setting
therefore does not describe the parallelism of every expensive operation.

The StarRocks profiles provide a useful physical contrast. Plans-simple's two
stored-rollforward scans (nodes 137 and 132) each read **71,434 raw rows**, apply
five pushed predicates and emit 14,239 rows; their node times are 4.60 and 5.65 ms.
Crossplans' corresponding scan (node 93) reads **318,504 raw rows**, applies four
pushed predicates and emits 61,677 rows in 7.63 ms before aggregation. ThinDB's
37.45-million-row scans therefore represent a directly measured pruning gap,
in addition to work that an empty consumer could avoid altogether.

StarRocks' other leading DOP-12 node costs include no-estimates' invoice scan
(14.79 ms, 27,485 rows), crossplans' lookup join node 83 (21.84 ms, two rows),
and the cross gap-fill nested-loop join (27.54/28.19 ms for base/child,
68.42 ms for detail, 2,685 output rows each). Detail's result sink is 46.03 ms.
These are StarRocks' node-time metrics; they must not be added to ThinDB's
exclusive stage times or treated as identical wall-clock accounting.

StarRocks raw profiles independently confirm pipeline DOP 12 or 16, alongside
single-driver pipelines, with one fragment instance. For example base-cross has
112 pipelines at the requested DOP and 51 at DOP 1 in each profile. These counts
do not mean all drivers run simultaneously. The original default-DOP inference
is consistent with the installed version's
[half-the-reported-CPU-count calculation](https://raw.githubusercontent.com/StarRocks/starrocks/4.0.10/fe/fe-core/src/main/java/com/starrocks/system/BackendResourceStat.java).

The ordinary window parallel-sort threshold is 65,536 rows
(`src/exec/window.zig`). The important 64,192-row and 30–32K-row windows take
the serial path. DOP-12 window sorting totals are 71 ms for base-cross, 72 ms
for child-cross and 88 ms for detail-cross. These are material costs, but they
do not explain the nearly identical 168/175 ms ranked-stage costs at four versus
31,558 rows: repeated lookup builds are the first target there.

## Setup, transport, and materialization controls

Ordinary non-plan application work is roughly 10–26 ms in the first pass.
Plan discovery adds a separate query; at DOP 12 its measured medians are
10/40 ms for StarRocks/ThinDB plans-simple and 21/51 ms for crossplans.
Temporary-table setup cannot be inferred from ThinDB's setup ACK timestamps,
because the multi-statement response buffers them until execution finishes.
Separate profiling calls measure temporary-table setup at approximately
2 ms on ThinDB and 233–247 ms on StarRocks at DOP 12. Thus temporary-table setup
does not explain the ThinDB losses. These split-call measurements are excluded
from the ordinary-SQL timing table.

The transport experiment keeps the binary and SQL fixed, interposes libc
`accept4` only in the isolated service, and reads back accepted sockets'
`TCP_NODELAY` values. Default is 0; the experimental setting changes it to 1.
Packet captures cover port 13311 only. No production socket is modified.

| Case | First-row-to-completion median, NODELAY off | NODELAY on |
|---|---:|---:|
| base simple | 42.50 ms | 12.56 ms |
| noest simple | 42.82 ms | 10.63 ms |
| detail cross | 72.88 ms | 45.07 ms |
| crossplans | 0.07 ms | 0.08 ms |

With NODELAY off, packet traces show 40–42 ms gaps followed by the held server
segment within roughly 8–10 microseconds of the client's ACK. Those gaps disappear
with NODELAY on in the three large-result controls. The three-row control has no
corresponding gap. Final-row-to-completion itself is about 0.05–0.11 ms: the stall
is before the last row packet, not an expensive final EOF operation. Server
serialization is also independently measured: approximately 2–4 ms for aggregate
cross results, versus 26–34 ms for detail-cross.

The socket experiment establishes a transport cost of about 28–32 ms in these
response tails. Overall query medians were slower in the later NODELAY-on blocks
because execution timings changed; no end-to-end speedup is claimed from those
successive blocks. It cannot explain the hundreds of milliseconds before output.

Controlled `invoice_base AS MATERIALIZED` runs alternate order with the original
SQL and, where supported, `NOT MATERIALIZED`. Base-simple is unchanged within
noise (348.5 versus 349.5 ms unprofiled). No-estimates measured 566.7 versus
491.2 ms unprofiled, but separate profiled controls converge to 442.3 versus
446.0 ms. Both FX builds remain with forced materialization. This rejects a
simple “add a shared invoice buffer” explanation. These variants are diagnostic
perturbations, not benchmark-winning rewrites. The base `NOT MATERIALIZED`
variant fails with `Unknown column` on the archived binary; its incomplete attempt
is retained under `rejected-base-not-materialized-0` and excluded. Every successful
variant has the original duplicate-sensitive result fingerprint.

## Ranked general engine proposals

1. **Defer shared-stage and lookup preparation until a consumer needs rows.**
   Preserve compile-time schema validation, but allow empty probe input to avoid
   running `customer_monthly_totals`. The ordinary join's existing empty-probe
   shortcut cannot recover work already executed by eager stage construction.
   In parallel, investigate predicate propagation into this table scan: reading
   37.45 million rows for 14–30K retained rows is a concrete scan/pruning target.
   Inspect `collectStages`, eager stage scans and filtered-stage compilation in
   `src/net/cte_stages.zig` before changing execution policy.
2. **Preserve empty-input avoidance across alternate compiled join paths.**
   No-estimates has fewer CTEs and windows, yet builds both million-row currency
   tables for a conversion branch with zero output. Inspect the probe preparation,
   fusion and empty-batch/peek contracts in `src/exec/join.zig` and staged scans.
   The exact eligibility condition still needs a focused implementation-level
   test; the unnecessary work and its location are measured.
3. **Reuse compatible lookup scans/builds within a query.**
   External-plan input is scanned four times in base-simple and seven times in
   the shared cross pipeline. Any sharing must include snapshot, filter, projection,
   key type/coercion, collation and residual semantics in its identity. This should
   be structural reuse, not recognition of these table names or query strings.
4. **Enable TCP_NODELAY on accepted MySQL sockets.**
   This is a small, independently demonstrated transport improvement. Retest small
   responses, streamed large results and CDC ingestion before changing the default.
5. **Tune ordinary window materialization and sorting after lookup costs.**
   Measure narrower live-column sets, reuse of proven ordering, and thresholds
   around 32K–64K rows. The current cross input lies just below the parallel window
   threshold. Do not assume increasing global DOP fixes serial stage chains.

The new table-driven integration fixture in `tests/integration/sql_test.zig`
covers constant and runtime-empty joins, empty global COUNT, left-join NULL
extension, a selective join, duplicate-sensitive shared window CTEs under
default/forced/inlined materialization, and same-spec LAG/running SUM. Data is
explicitly flushed and allocations use `std.testing.allocator`. These establish
semantic acceptance cases for the proposals; they are not claims that the
optimizations have been implemented. `zig build test` passed: 1,496 tests passed,
five existing skips. The diagnostic C interposer builds with
`gcc -Wall -Wextra -Werror -shared -fPIC`.

## Result preservation and cleanup

All **32** ThinDB fingerprint checks across the main matrix, transport controls
and confirmation matrix match the archived ordinary-SQL results. All successful
materialization variants match their corresponding original results. Main-matrix
fingerprints are stable across DOPs in both engines.

Sixteen of the eighteen main cross-engine raw fingerprints match. The two
detail-cross fingerprints differ because `month` is returned as DATE text by
ThinDB and midnight DATETIME text by StarRocks. Separate decoded comparisons
match all 30,119 rows and 54 columns at both DOPs. The confirmation run additionally
captures every field as text to avoid numeric precision loss: after normalizing
only DATE to midnight for the DATE/DATETIME column pair, the duplicate-sensitive
row multisets match exactly, without numeric rounding. See
`confirmation/exact-value-check.json`. Other MySQL metadata types also differ
(integer widths, NULL literals and float/double); this establishes value
equivalence for the workload, not identical cross-engine result metadata.

Exploratory fixture construction also exposed existing qualified-key handling
after an empty LEFT JOIN and bare-UNION ORDER BY behavior. Exact observations are
retained in `.bench-data/ordinary-sql-diagnosis/exploratory-limitations.md`.
The successful fixtures use distinct lookup column names and an explicit outer
sort to isolate the intended semantic checks. No engine compatibility changes
are bundled with this diagnosis.

The 50 main/profile/transport/confirmation candidate memory-event receipts have
zero limit/OOM events. The maximum recorded candidate peak is **4.974 GiB**;
the four successful materialization services also have peak-memory receipts.
Final production checks show ThinDB PID **2579063**, restart count **0**, StarRocks
BE PID **848762**, and CDC **RUNNING** with checkpoint 39922 completed. The
diagnostic unit is inactive with PID 0 and port 13311 is free. Production on
port 13310 was not restarted, replaced or opened from another process.

## Reproduction

Use the scripts alongside this report on the benchmark host. The Python runner
accepts the existing archive, a new output directory, private snapshot path and
`timing` / `profile` / `transport-off` / `transport-on`; supply the StarRocks
credential on stdin. It validates the binary checksum, snapshot ownership, free
port, headroom and candidate identity before starting/stopping its own service.
`DIAG_CASES` (pipe-separated labels) and `DIAG_DOPS` select confirmation blocks.
Build `ordinary_sql_nodelay.c` into `ordinary_sql_nodelay.so` beside the runner
for the transport modes. The report generator accepts the results and archive
directories and checks complete sample counts and fingerprints.

StarRocks profiles follow the official
[EXPLAIN ANALYZE](https://docs.starrocks.io/docs/best_practices/query_tuning/query_profile_text_based_analysis/)
and [get_query_profile](https://docs.starrocks.io/docs/sql-reference/sql-functions/utility-functions/get_query_profile/)
interfaces. Profiles are retained separately because collection changes timing.

## Engine implementation (2026-09-12 UTC)

Three general engine fixes are now implemented and verified. This does not close
every execution gap identified above; the remaining work is ranked below.

- **Qualified pruning:** Scan uses the same column resolver as row evaluation.
  Alias, projection and compute wrappers map hints to the correct source slot.
  Filter fusion checks resolved slots so a qualified reference cannot bypass a
  replaced or renamed value. Limits, TopN and windows stop hints; aggregates
  forward only uncapped group-key predicates. This also fixes existing unsafe
  pruning across those boundaries.
- **Empty fused probes:** A pure filter over an existing stage checks for a
  surviving row before fused hash joins prepare their lookup inputs. An empty
  probe short-circuits a chain of non-FULL joins. Nonempty inputs retain fused
  parallel probing. The check reuses existing stage views and adds no CTE
  materialization, SQL rewrite, join reordering or query-specific recognizer.
- **MySQL transport:** Both accepted-connection paths enable TCP_NODELAY on
  supported POSIX systems. The Linux accepted-socket test reads back value 1.
  Windows remains a no-op because Zig 0.16's public socket interface does not
  expose the required option setter for its AFD handles.

### Matched before/after timings

The comparison replays the archived ordinary SQL and parameters against the old
and changed ThinDB binaries on the same private snapshot, port 13311. Sierra,
dates, three hash buckets, cache sizes, memory budgets and SQL semantics stay
fixed. There are no UDF calls or keyed-region declarations in these queries.
Each binary/case/DOP block starts a fresh service, warms once, measures five
times, then fingerprints separately. Binary order alternates between cases.
The 36 main blocks supply 180 timing samples; reversed-order confirmation adds
20. Operator/MySQL profiles are separate runs and never replace these timings.

These are final-SQL wire times: they include the archived variable/temp-table
prefix and server compilation, execution and response delivery. Statement ACK
timestamps retain the setup boundaries. Application module/lookup setup is
excluded by replaying its archived SQL and parameter output; the earlier
cross-engine application measurements remain above. Starting the snapshot also
reloads its registered function libraries, outside the measured warm samples.
StarRocks was not retimed in this implementation pass, so this table establishes
ThinDB before/after effects rather than a new cross-engine ranking.

| Case | DOP 12 before → after, ms | DOP 16 before → after, ms |
|---|---:|---:|
| base-simple | 360.5 → 327.0 | 365.5 → 356.5 |
| noest-simple | 488.6 → 410.2 | 510.4 → 379.0 |
| plans-simple | 805.1 → 68.3 | 949.3 → 77.6 |
| crossplans | 1304.0 → 478.3 | 1312.0 → 505.3 |
| base-cross | 706.5 → 654.5 | 706.6 → 888.6 |
| child-cross | 682.3 → 648.8 | 978.5 → 934.9 |
| detail-cross | 849.5 → 758.9 | 1107.1 → 1062.3 |
| expanded-simple | 1215.8 → 942.5 | 1401.6 → 1207.8 |
| interval-quarter | 517.5 → 345.5 | 552.0 → 437.0 |

The plan cases improve by roughly **91–92%** (empty simple) and **61–63%**
(three-row crossplans). No-estimates improves by **16–26%** in the main medians,
with substantial variation in its DOP 12 candidate samples. The full sample
lists below retain that variation.

Base-cross's apparent DOP 16 regression in the main matrix did not reproduce at
that magnitude in two reversed-order pairs. Their before/after medians were
**886.8/899.3 ms** and **864.6/836.2 ms**, with overlapping sample ranges. Separate
DOP 16 profiles were **870.2/817.1 ms**. Window sort time was nearly unchanged
(83.63/84.96 ms), and the candidate's empty-probe check cost 0.056 ms. This supports
measurement variation rather than a reproducible 182 ms execution regression;
it does not establish a consistent base-cross execution speedup at DOP 16.

### Operator evidence

The following DOP 12 CTE times are the profiler's **exclusive stage execution**
times, excluding upstream CTE stage wall time. They are not summed with inclusive
operator/worker timers or substituted for uninstrumented query times.

| Stage | Output rows | Before → after exclusive ms | Structural evidence |
|---|---:|---:|---|
| plans-simple: customer_monthly_totals | 14,239 | 930.81 → 22.82 | 576 → 11 row groups; 37,453,423 → 720,896 decoded rows |
| crossplans: customer_monthly_totals | 30,119 | 835.91 → 29.94 | Same scan reduction; 61,677 rows survive before grouping |
| noest-simple: rollforward_pre_records_temp/window | 27,485 | 324.48 → 76.82 | Two 1,018,207-row currency builds disappear |
| base-cross: cross_division_ranked/window | 31,558 | 238.10 → 214.63 | Repeated lookup work remains |

The monthly scan's configured DOP is 12; effective scan parallelism changes from
12 workers to 6 because only eleven row groups survive. The invoice scan still
reads nine groups with effective parallelism five. Each removed currency scan
previously used twelve effective workers, and its FastTable build used eight
threads. The two baseline currency build phases took 80.8 and 114.3 ms, with
zero output rows; those build-phase times include their child scan/materialize
work. The candidate's 50.270 ms empty-probe check includes 50.15 ms spent producing
the required existing normalized-currency stage, leaving about 0.12 ms for the
check itself at the profiler's rounding precision.

At DOP 12, median first-row-to-completion time changes from 42.5 to 11.6 ms for
base-simple, 42.4 to 13.4 ms for no-estimates, 43.2 to 11.4 ms for base-cross, and
73.2 to 38.9 ms for detail-cross. Other substantial result sets show the same
roughly 30 ms tail reduction at both DOPs. The three-row crossplans tail stays
about 0.1 ms. This matches the earlier socket/packet experiment and the accepted
socket option readback; response serialization and packet processing still cost
time after the first row.

The current classifications are: no-estimates has confirmed avoidable execution
and transport cost; the plan cases have confirmed scan cost on deliberately
empty/selective workloads; base/child/detail cross retain execution and delivery
cost with unstable rankings in noisy blocks. The original 1.4% interval-quarter
loss remains measurement noise, although its transport tail is now fixed.

### Remaining implementation work, in order

1. **Share repeated lookup scans, materialization and build data.** The changed
   base-cross query still reads 77,989 external-plan rows seven times, at effective
   DOP 2. Individual scan drains take 22–51 ms. Hash preparation/insertion itself
   is only about 1–2 ms per lookup, so a hash-table-only cache would miss much of
   the cost. Existing stage-backed shared builds are the mechanism to extend.
   The crossplans rank stage still takes 191.30 ms for four rows, while all its
   window sorts total only 0.004 ms: lookup preparation remains the priority.
2. **Defer remaining eager lookup stages behind empty probes.** Empty-plan
   execution still builds customer_monthly_totals after invoice_base returned
   zero rows; it now costs 22.82 ms rather than 930.81 ms. The new short-circuit
   covers pure filters over existing stage views, not every eager compilation
   path or transformed input.
3. **Reduce wide cross-query copies and window work.** Base-cross still has a
   214.63 ms rank stage over 31,558 rows and a 59.44 ms following stage over 30,119
   rows. Its nine window sorts total 77.37 ms and these windows remain below the
   existing parallel-sort threshold. Preserve frame semantics and ordering while
   investigating column lifetimes, source-order reuse and parallel thresholds.

### Verification, artifacts and reproduction

The final source passes **1,501 tests, with five existing skips**. New fixtures
check actual segment pruning for qualified names; renamed/replaced columns;
sibling aliases; limits, windows and aggregate output boundaries; and a fused
two-join chain with empty, NULL and nonempty probes. They assert that unused build
stages never execute and that a valid surviving row still probes both lookups.
The prior ordinary-SQL fixtures continue to cover selective joins, shared CTEs,
explicit materialization modes, LAG and running windows.

`zig build bench -Doptimize=ReleaseFast` also completes. Representative local
one-million-row results: warm scan 13.85 ms, eight-group aggregate 26.83 ms, and
two window calls sharing a sort 181.72 ms. These are retained smoke/regression
observations, not a matched native-benchmark speedup claim. The Linux socket
option test passes separately, and all 113 client integration tests pass.

All **50** main/profile/confirmation fingerprint runs match the archived ThinDB
results. The final review build additionally passes all **nine** SQL fingerprints.
The timed Linux ReleaseFast binary is `737729376ca0f4f1ce0723b9a7fbe4011648cd52450676c8ae5479da2ade3f81`.
The final review binary is `7453ed688cd07bf22920b99a17c7c5e59cd718c1c2e8e9dccee2d24c62c1977e`; its only engine-source
delta is four internal identifier renames to match repository style, verified
against the retained timed patch before the final fingerprint pass.

Artifacts are in `.bench-data/ordinary-sql-implementation/`: `results.json`
contains all 50 runs, samples, profiles, build/service receipts and memory events;
`review-smoke/` contains the final build's nine result checks;
`final-tests.log` and `native-bench.log` retain verification;
`engine.patch` and `review-build/engine.patch` identify the measured source;
`final-health.json`, `final-unit.txt` and `final-port.txt` record cleanup.
The 51 final service receipts have no memory-limit/OOM events; the largest
recorded benchmark peak is **3.588 GiB** under the unchanged 20 GiB cap.

The reusable drivers are `bench/ordinary_sql_implementation.py` and
`bench/ordinary_sql_replay.cjs`, alongside the existing diagnosis helpers.
On the benchmark host they were installed as `compare.py` and `replay.cjs`
under `/home/ubuntu/wayroll-bench/ordinary-sql-implementation`. Place each binary
and its checksum metadata in `baseline/` or `candidate/`, then run
`python3 compare.py timing 12` and `python3 compare.py timing 16`.
`python3 compare.py profile 12 noest-simple plans-simple crossplans base-cross`
collects separate profiles. The helper validates snapshot ownership, free port,
binary identity and headroom and stops only its own verified benchmark process.

At 2026-09-12T01:24:18.304992+00:00, production ThinDB remained PID 2579063,
restart count 0, active; StarRocks BE remained PID
848762, and CDC was RUNNING. The benchmark service is inactive
with PID 0 and port 13311 is free. Production port 13310 and its data directory
were not restarted, replaced or opened by this work.

### Every main timing sample (milliseconds)

| Case | DOP | Before samples, ms | After samples, ms |
|---|---:|---|---|
| base-simple | 12 | 366.0, 359.6, 359.6, 360.5, 363.6 | 330.8, 327.0, 332.2, 325.2, 326.1 |
| base-simple | 16 | 369.6, 369.7, 365.5, 365.5, 363.6 | 339.8, 334.3, 356.5, 385.5, 451.0 |
| noest-simple | 12 | 567.1, 502.5, 488.6, 478.5, 465.7 | 465.3, 331.6, 348.8, 630.6, 410.2 |
| noest-simple | 16 | 595.6, 540.9, 510.4, 505.4, 502.7 | 401.8, 379.0, 421.8, 323.8, 322.1 |
| plans-simple | 12 | 805.1, 803.2, 895.1, 955.2, 799.2 | 69.6, 70.1, 68.3, 68.2, 68.0 |
| plans-simple | 16 | 957.2, 949.3, 937.0, 1032.5, 878.9 | 77.6, 78.2, 80.0, 76.2, 75.5 |
| crossplans | 12 | 1340.4, 1423.0, 1290.4, 1304.0, 1257.5 | 487.3, 482.7, 478.3, 474.8, 465.2 |
| crossplans | 16 | 1248.3, 1300.3, 1326.4, 1442.6, 1312.0 | 505.3, 495.6, 516.7, 592.2, 496.7 |
| base-cross | 12 | 719.9, 699.4, 694.6, 706.7, 706.5 | 691.1, 648.7, 653.2, 654.5, 670.1 |
| base-cross | 16 | 727.9, 705.2, 702.5, 706.6, 734.7 | 883.2, 1089.3, 910.4, 866.6, 888.6 |
| child-cross | 12 | 686.1, 682.3, 746.6, 659.4, 658.6 | 665.6, 644.2, 733.2, 648.8, 627.4 |
| child-cross | 16 | 923.5, 978.5, 1002.5, 882.6, 989.8 | 871.5, 934.9, 807.1, 1003.9, 1073.2 |
| detail-cross | 12 | 860.9, 849.5, 903.6, 838.7, 819.5 | 812.3, 849.7, 744.8, 744.4, 758.9 |
| detail-cross | 16 | 1031.9, 1107.1, 1217.2, 933.4, 1189.8 | 1030.6, 1062.3, 1387.9, 995.2, 1231.0 |
| expanded-simple | 12 | 1215.8, 1158.9, 1154.1, 1452.7, 1252.5 | 942.5, 985.4, 1173.0, 930.4, 931.1 |
| expanded-simple | 16 | 1404.5, 1401.6, 1411.4, 1341.3, 1255.5 | 1275.8, 1346.0, 1126.3, 1197.5, 1207.8 |
| interval-quarter | 12 | 437.6, 387.8, 530.5, 604.6, 517.5 | 388.5, 440.8, 344.7, 344.1, 345.5 |
| interval-quarter | 16 | 552.0, 604.3, 523.4, 513.8, 710.4 | 445.1, 486.0, 405.1, 437.0, 369.9 |

Confirmation base-cross DOP 16, in execution order:

| Build | Five samples, ms |
|---|---|
| Candidate | 978.9, 1056.4, 899.3, 879.5, 816.9 |
| Baseline | 886.8, 895.9, 809.6, 986.6, 872.6 |
| Baseline | 1094.2, 831.7, 929.7, 864.4, 864.6 |
| Candidate | 862.1, 771.1, 836.2, 820.2, 977.2 |

## No-estimates follow-up and column-pruning fix (2026-09-12 UTC)

The approximately 2× loss in the fresh sweep was reproducible unnecessary
execution work. The new engine build reduces no-estimates from about 435 ms to
142 ms with unchanged SQL and results. StarRocks measures about 213 ms in the
new comparison. This section supersedes the earlier no-estimates diagnosis;
the nine-case sweep remains a record of its earlier build.

### Why removing estimates exposed such a large gap

Both variants scan 27,485 invoice rows. Base adds only two estimate rows,
producing 27,487 normalized rows versus 27,485 without estimates. Removing
estimates therefore removes substantial SQL structure but little row work.

Separate StarRocks DOP 12 profiles show planner time falling from 208 ms for
base to 48 ms without estimates (transformer 64→13 ms; optimizer 119→26 ms).
Execution falls only from 155.663 to 126.247 ms. These instrumented figures
explain why StarRocks improves much more than ThinDB when estimates disappear;
they are not substitutes for uninstrumented timing samples.

ThinDB still built four `external_plan` lookups, each reading 77,989 rows and
carrying an 11-column source schema. Only four or five source columns are
needed by each lookup. The unused columns include the wide `planJson` payload.
The existing whole-query projection collector disabled pruning on a wildcard
in another CTE, so these columns were decoded, gathered and copied repeatedly.
Only 20 lookup rows belong to Sierra; this fix changes column width, not that
row count or the join's filtering semantics.

Diagnostic SQL variants at DOP 12 used one warmup, five alternating-order
measurements and a separate fingerprint run. Median SQL packet-drain times:

| Variant | Base, ms | No estimates, ms |
|---|---:|---:|
| Original | 395.6 | 377.6 |
| Explicit narrow lookup subqueries | 210.6 | 178.4 |
| Explicit project filter on lookups | 140.0 | 123.5 |
| Narrow lookup plus project filter | 147.2 | 131.2 |

All fingerprints match. These variants are diagnostic only. Derived subqueries
also enable some existing stage sharing, so their gain cannot be attributed
entirely to column width. The final engine experiment below keeps the original
SQL, original stage count and four separate lookup builds, isolating the benefit
of automatic column pruning. No benchmark application query was rewritten.

### General engine change and operator evidence

`local.join_leaf_input_names` collects the references on a scan's ancestor path
within its query block. `cte_stages` preserves that block context through nested
joins and supplies the resulting names to existing scan projection. Join keys,
range predicates, residual filters, computed expressions, grouping and sorting
inputs are retained, including parser-generated join-key computations. Sibling
CTE bodies do not disable this analysis. Wildcards on the relevant path and
unsupported column-scope boundaries conservatively retain all columns.

Separate before/after no-estimates profiles at DOP 12:

| Observation | Before | After |
|---|---:|---:|
| Four lookup build phases, including input scans | 234.7 ms | 18.4 ms |
| Individual lookup build times | 44.6 / 73.2 / 58.4 / 58.5 ms | 4.0 / 4.6 / 4.3 / 5.5 ms |
| Lookup rows per build | 77,989 | 77,989 |
| Lookup effective scan workers | 2 each | 2 each |
| Invoice survivors / effective scan workers | 27,485 / 5 | 27,485 / 5 |
| Join operator exclusive time | 100.90 ms | 10.30 ms |
| Scan operator exclusive time | 70.32 ms | 31.32 ms |
| ParallelScan operator exclusive time | 93.55 ms | 11.05 ms |
| Window sorting, six calls | 25.528 ms | 26.912 ms |
| Pre-records window stage, excluding upstream stages | 65.94 ms | 23.53 ms |
| Materialized stages | 10 | 10 |

Build phases include descendant work; do not add them to exclusive operator
times. Exclusive operator counters include worker activity and are not an
additional wall-time decomposition. The unchanged row counts, stage count,
lookup count and effective workers rule out those explanations for this gain.
StarRocks' original no-estimates profile records 64 pipelines at DOP 12 and 27
at DOP 1; setting session DOP does not mean every operator runs with 12 workers.

The new ThinDB profile reports `compile.staged_total=96.353 ms`, which includes
eager stage execution; it is not pure compiler overhead. Table-block construction
is 6.942 ms. Subsequent result pulls take 39.093 ms and row encoding/writing
1.848 ms. The uninstrumented first-row-to-completion median is about 2.1 ms,
so the old 43 ms transport tail does not explain the remaining work. The fused
probe's 43.1 ms includes producing its approximately 40.8 ms source stage.

### Unchanged application benchmark

Median application-to-result elapsed time, one warmup and five measured runs
per engine/case/DOP, with separate fingerprints. Earlier ThinDB values come
from the fresh sweep; StarRocks was rerun alongside the new candidate.

| Case | DOP | Earlier ThinDB, ms | New ThinDB median (range), ms | New StarRocks median (range), ms |
|---|---:|---:|---:|---:|
| No estimates | 12 | 435.8 | **141.5 (133.6–149.6)** | 214.4 (209.4–225.1) |
| No estimates | 16 | 433.9 | **142.6 (138.0–159.2)** | 211.6 (209.8–213.2) |
| Base simple | 12 | 391.8 | **156.9 (151.4–165.5)** | 427.0 (411.1–434.0) |
| Base simple | 16 | 330.9 | **151.4 (147.2–161.1)** | 443.2 (421.2–458.7) |

No-estimates is approximately 3.1× faster than the earlier ThinDB build and
33–34% lower in elapsed time than StarRocks in the new comparison. ThinDB now
also reflects the small expected base/no-estimates difference.

The existing harness preserves Sierra, dates, three hash buckets, USD, ordinary
SQL, cache and memory settings. Engine order alternates by case and reverses at
DOP 16. StarRocks session DOP is explicit with cache and adaptive DOP disabled.
The candidate runs only on the private snapshot at port 13311.

### Validation, remaining work and receipts

All 18 candidate fingerprints across the nine cases and both DOPs match the
archived ThinDB results. All eight new timed-block fingerprints also match the
earlier sweep and each other across engines. No numerical tolerance or query
rewrite was introduced. Other cases received correctness verification here,
not a new five-run performance sweep.

Two integration fixtures cover nested lookup scans behind a wildcard CTE,
unused payload removal, NULL results, serial/parallel execution, wildcard
preservation, WHERE and ON predicates, grouping, FULL joins and renamed
subquery outputs. Final `zig build test`: **1,503 passed, five existing skips**,
including all 113 client tests. `zig build bench -Doptimize=ReleaseFast` passes.
An earlier full run hit the existing Windows compactor race's `AccessDenied`;
the complete final rerun passes. Intermediate diagnostic/test failures are
retained with their logs.

Remaining priorities are invoice scan/gather work (about 30 ms wall time with
five effective workers), window sorting (about 27 ms), and grouping/materialized
buffer work. Reusing the remaining narrow lookups or propagating provable join
constraints may help, but the four lookup builds now total only 18.4 ms.
Neither follow-up is required to eliminate this reproduced loss.

Candidate SHA256: `affae8694249dd49e3b1aeaab26f8f51cf4f5688ecfb21b5d7dd607df4932ee6`.
Baseline SHA256: `7453ed688cd07bf22920b99a17c7c5e59cd718c1c2e8e9dccee2d24c62c1977e`.
Both are Zig 0.16.0 ReleaseFast x86_64-linux-gnu. Source patches, build metadata,
all samples, diagnostic SQL, operator profiles, fingerprints and test/bench
logs are in `.bench-data/ordinary-sql-noest-20260912/`; the remote campaign is
`/home/ubuntu/wayroll-bench/ordinary-sql-noest-20260912/`.

The six new timed/profile services and both nine-case verification services
record zero memory-limit/OOM events. At 2026-09-12T19:01:45 UTC production
ThinDB remains PID 2579063, restart count 0; StarRocks BE remains PID 848762,
and CDC is RUNNING. The benchmark service is stopped with PID 0 and port 13311
free. Production was not deployed, restarted or opened by a second engine.
