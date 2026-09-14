# Keyed Regions — eligibility round (hand-off)

### Sierra full five-arm sweep after fusion fix (2026-09-11)

Reran all 15 Sierra variants and all five arms using engine d29f5f1.
StarRocks timings are fresh. Clients ran locally on starrocks1: ThinDB
used the private snapshot on 13311; StarRocks read live data on 9030.
Hash buckets a/b/c, archived b5359d9e generator and per-case dates, DOP 12
for ThinDB, cache/pool 2/4 GiB, shared/query budgets 16 GiB and service
ceiling 20 GiB. Each ThinDB arm used a fresh service, one warmup, three
measured queries and one untimed fingerprint query. Final row packets were
discarded without Node value decoding. Startup/teardown/validation are
outside timing; application query generation/setup remains inside.

Median milliseconds:

| Variant | StarRocks SQL | ThinDB SQL | SQL + regions | Zig UDF | UDF + regions |
|---|---:|---:|---:|---:|---:|
| base simple | 485 | 360 | 214 | 218 | 112 |
| base cross | 532 | 713 | 553 | 387 | 125 |
| expanded simple | 1,721 | 1,007 | 240 | 441 | 112 |
| expanded cross | 1,986 | 1,762 | 1,943 | 886 | 163 |
| child simple | 478 | 368 | 220 | 201 | 113 |
| child cross | 530 | 679 | 531 | 408 | 182 |
| interval quarter | 451 | 458 | 266 | 216 | 159 |
| interval annual | 446 | 377 | 230 | 208 | 121 |
| fx latest | 591 | 368 | 220 | 151 | 140 |
| fx average | 599 | 360 | 226 | 159 | 183 |
| noest simple | 222 | 477 | 317 | 199 | 114 |
| plans simple | 674 | 767 | 91 | 124 | 99 |
| crossplans | 755 | 1,199 | 1,098 | 185 | 136 |
| detail simple | 468 | 448 | 247 | 229 | 114 |
| detail cross | 624 | 1,030 | 848 | 506 | 215 |

Sums of the fifteen medians (seconds): SR 10.56; SQL 10.37; SQL + regions
7.24; UDF 4.51; UDF + regions 2.09. SQL + regions is faster than ordinary
SQL in 14/15 variants, versus 4/15 in the preceding DOP 12 sweep. Its total
fell from 19.46 to 7.24 seconds; ordinary SQL also fell from 12.34 to 10.37
seconds. These are successive full-suite runs on a shared host; the separate
matched saved-SQL controls isolate the preparation defect more directly.

Expanded cross remains the SQL-region exception: 1,943 versus 1,762 ms.
UDF + regions beats SR in all 15 cases and is fastest in 13/15. The other
winners are unkeyed UDF for FX average (159 versus 183 ms), and SQL +
regions for plans simple (91 versus 99 ms for UDF + regions).

All 300 accepted warmup/timing queries and 60 validation queries completed
without query errors. All 30 keyed/unkeyed fingerprint pairs match, all
60 ThinDB fingerprints match the preceding DOP 12 run, and result counts
agree across all five arms. All keyed arms engaged regions at DOP 12;
unkeyed arms did not. Saved SQL verifies the UDF/declaration switches for
each arm. Expensive rejected fusion attempts: zero across the entire sweep.

Startup headroom checks interrupted the run after seven variants; completed
cases were retained and an incomplete annual-case attempt was archived and
excluded before rerunning that case. The unused-memory startup requirement
was reduced from 12 to 10 GiB, based on the preceding Sierra sweep peak of
4.72 GiB. Service/query/cache/DOP limits stayed unchanged. The final 60
accepted services peaked at 4.98 GiB with zero memory-limit/OOM events.
Production PID 2579063 and zero restarts were unchanged; CDC stayed RUNNING.
The candidate stopped and 13311 was released. No production deployment.

Full table, comparison CSV, raw SQL/traces, fingerprint and memory audits,
health checks and build provenance: `.bench-data/five-arm-d29f5f1-sierra-dop12/`
(mirrored under `/home/ubuntu/wayroll-bench/five-arm-d29f5f1-sierra-dop12`).
Portable bundle: `sierra-results.tgz`. `benchmark-report.md`, `timings.csv`,
`before-after.csv` and `matrix.json` contain the accepted results.

### Reject unsupported SQL fusion before preparation (2026-09-11)

The general fix checks branch join eligibility during collection, before
preparing sources or draining lookup inputs. Collection and dispatch share
one predicate for supported join kinds, range conditions and residual ON
predicates. The rollforward gap-fill branch contains a range join; previously
the fused attempt prepared earlier joins before reaching that rejection.
Known-empty branch filters also retain ordinary staging and its pruning.
Supported branches still fuse, and compatible regions around unsupported
branches remain available. No query, table or UDF names select these rules.

A bounded per-database rejection cache additionally avoids repeating
data-dependent fusion failures when inputs are unchanged. Its fingerprint
includes table/data/schema identity, declared keys, CTE sharing, session and
compile context, and immutable scalar kernel identity. Volatile calls and
unversionable inputs are excluded. Data/schema changes retry; actual join
input compilation/execution errors preserve their error identity. The first
data-dependent proof after a change still costs work. This cache alone did
not resolve the first pilot; the early structural check removes the measured
rollforward failure even on the first execution.

Matched 7b99d27/candidate controls used DOP 12, the same private snapshot on
starrocks1 port 13311, saved SQL and hash buckets a/b/c. Each arm had one
warmup, three measured raw-packet-discard queries and one untimed fingerprint.
Fresh services used cache/pool 2/4 GiB, shared/query budgets 16 GiB, service
ceiling 20 GiB; build order alternated. Headroom checks paused between arms
when necessary. These are six saved-SQL controls, not a fresh full five-arm
application matrix; they exclude application generation/setup. Shared-host
load can still affect elapsed time. Median milliseconds:

| Case | Regressed SQL + regions | Fixed SQL + regions | Speedup |
|---|---:|---:|---:|
| Sierra base simple | 809 | 251 | 3.23x |
| Sierra expanded simple | 1,036 | 283 | 3.67x |
| Sierra expanded cross | 3,205 | 2,346 | 1.37x |
| AirDNA base simple | 2,508 | 1,467 | 1.71x |
| AirDNA expanded simple | 2,511 | 1,209 | 2.08x |
| AirDNA expanded cross | 11,086 | 9,649 | 1.15x |

Each regressed query attempted and rejected one fused union; every fixed
query had zero expensive failed union attempts, including the warmup.
Every arm retained one successful region. Rejection-cache hits were zero in
all six fixed controls, confirming that the structural check accounts for
this recovery. All six result fingerprints match each other and the earlier
ordinary-SQL reference, including row/column counts. All 48 warmup/timing
queries and 12 validation queries completed. Peak service memory was
17.64 GiB with zero memory-limit/OOM events. Production PID 2579063
and restart count remained unchanged; CDC stayed RUNNING. The diagnostic
service stopped and port 13311 was released. No production deployment.

Validation: `zig build test -j1 --summary all` passed 1,495 tests, with five
known skips. Coverage includes range/residual join fallback, empty/nonempty
branches, changed lookup data, ALTER/DROP/recreate, CTE sharing and cache
fingerprint context/kernel/volatility changes. `zig build bench` passed during
implementation; final `zig build bench-regions` passed exact-value checks.
Generic shared-window branch cases retained 1.59-2.13x speedups over ordinary
SQL. A high-cardinality grouped-reduction microbenchmark still ran at 0.81x;
regional execution is not universally faster. The full application five-arm
sweep remains the next comparison after this fix is integrated.

Evidence: `.bench-data/sql-region-rejection-fix/`, mirrored under
`/home/ubuntu/wayroll-bench/sql-region-rejection-fix`. Full receipts, traces,
health/memory audits and summaries are in `control-results.tgz`; exact binary
hashes are in `full-metadata.json`. Candidate Linux ReleaseFast SHA-256:
`87b120dbe6be6b4f4e1967dca46b4a7031f0474ff959348689e807c78eb17796`.

### Confirm SQL regional preparation regression (2026-09-11)

Controlled comparison of parent 062b8c7 (same engine as d4b66ca) against
7b99d27 confirms that the shared-branch fusion change introduced a broad
SQL regional regression. Both binaries used DOP 12, the same private
snapshot and saved SQL/parameters, hash buckets a/b/c, cache/pool 2/4 GiB,
query/shared budgets 16 GiB and service ceiling 20 GiB. Each arm received
a fresh service, one warmup, three measured queries and one fingerprint
query. These controls exclude application SQL generation/setup and are
separate from the full application benchmark. Build order alternated.

Median milliseconds:

| Case | Parent SQL | Current SQL | Parent SQL + regions | Current SQL + regions |
|---|---:|---:|---:|---:|
| Sierra base simple | 337 | 346 | 204 | 791 |
| Sierra expanded simple | 925 | 961 | 221 | 786 |
| Sierra expanded cross | 1,699 | 1,663 | 1,817 | 2,883 |
| AirDNA base simple | 1,827 | 1,757 | 1,653 | 2,667 |
| AirDNA expanded simple | 7,458 | 6,946 | 1,777 | 2,894 |
| AirDNA expanded cross | 12,238 | 15,812 | 12,301 | 13,468 |

The last case's ordinary-SQL control shifted materially on the shared host;
do not attribute its entire difference to regional compilation. The other
five cases hold ordinary SQL within about 7%, while regions regress 59-288%.

For Sierra base-simple, the successful region still takes 114/115 ms on
parent/current. The extra ~587 ms is outside that execution. Current
traces show a failed sql_union attempt while preparing the staged input,
followed by the same cached smaller region. dispatchJoin can compile and
drain join inputs during speculative branch construction; a later decline
discards that work before the fallback builds its ordinary input. This
repeats even on warmed queries. All six current controls show one failed
union attempt per timed query; parent controls show none.

Priority: make failed shared-branch preparation cheap and avoid repeating
it in cached/unsupported staged inputs, while preserving supported fusion
and invalidation for changed data, schemas, kernels and CTE sharing. Keep
the fix structural and general. Recheck the saved controls, generic cache
invalidation/fallback tests, then the full application matrix. No engine
fix was applied during this diagnosis.

All four arm fingerprints match in all six cases. The 96 warmup/timing and
24 untimed validation queries succeeded; 24 services peaked at 17.07 GiB
with no memory-limit/OOM events. The diagnostic service stopped and 13311
was released. Production PID 2579063 and restart count stayed unchanged;
CDC remained RUNNING. No deployment occurred.

Evidence: `.bench-data/sql-region-regression-7b99d27/`, mirrored under
`/home/ubuntu/wayroll-bench/sql-region-regression-7b99d27`. The parent
checkout is `.bench-data/sql-region-parent-control` (detached 062b8c7).

### DOP 12 rerun of all thinDB arms (2026-09-11)

Reran all four thinDB arms for both companies and all 15 variants on
starrocks1, using the same 7b99d27 ReleaseFast binary, private snapshot on
13311, archived query generator/dates, and hash buckets a/b/c. Only DOP
changed from 4 to 12: block cache 2 GiB, region pool 4 GiB, shared/query
budgets 16 GiB, service ceiling 20 GiB. Every arm received a fresh service,
one warmup, three measured queries, and one untimed fingerprint query.
Timed final row packets were drained without Node value decoding.
StarRocks was not rerun; its preceding results are retained in the matrix.

Sums of the fifteen query medians, seconds, shown as DOP 4 -> DOP 12:

| Dataset | SQL | SQL + regions | UDF | UDF + regions |
|---|---:|---:|---:|---:|
| Sierra | 11.21 -> 12.34 | 16.62 -> 19.46 | 3.95 -> 5.14 | 1.58 -> 2.16 |
| AirDNA | 54.81 -> 44.40 | 57.72 -> 49.00 | 42.95 -> 34.64 | 15.22 -> 12.24 |

DOP 12 reduced AirDNA totals by 15-20%, but increased Sierra totals by
10-37%. SQL+regions still lost to ordinary SQL in 23/30 cases. UDF+regions
remained fastest in 29/30 cases; Sierra FX-average favored unkeyed UDF.
All keyed arms engaged regions, all timed keyed traces used the requested
DOP, and none used the new fused SQL union opcode. AirDNA expanded-cross
UDF+regions improved from 985 to 674 ms; its shard phase roughly halved,
while scan/scatter was nearly unchanged. Sierra's smaller scans paid more
overhead at DOP 12. Trace comparisons are saved with the reports.

All 480 new warmup/timing queries and 120 fingerprint queries succeeded.
All 60 keyed/unkeyed fingerprint pairs matched at DOP 12. Cross-DOP raw
fingerprints matched in 116/120 arms. Both SQL arms in the two AirDNA detail
variants differed across DOP; the UDF arms matched. A separate untimed
rerun of the identical saved SQL reproduced all four SQL fingerprints.
Comparing every row isolated differences to exchangeRate/lastExchangeRate:
94 detail-simple rows (maximum absolute difference 3e-16) and 288 detail-cross
rows (7e-16). All other fields, including monetary amounts, matched exactly.
This is not bitwise cross-DOP equivalence; no normalization was applied to
the recorded fingerprints.

The 120 benchmark services peaked at 17.23 GiB with zero memory-limit/OOM
events. The benchmark and subsequent comparison services were stopped;
13311 was released. Production retained PID 2579063 and zero restarts,
and CDC remained RUNNING. No engine changes or deployment occurred.

Full matrix, DOP comparison CSV, stage traces, validation details and health
receipts: `.bench-data/five-arm-7b99d27-dop12/`, mirrored under
`/home/ubuntu/wayroll-bench/five-arm-7b99d27-dop12` on starrocks1.
The separate row-comparison harness and compressed packet captures are in
`/home/ubuntu/wayroll-bench/dop-detail-check-7b99d27`.

### Full five-arm rollforward rerun (2026-09-11)

Engine commit `7b99d27`, Linux ReleaseFast, ran on starrocks1 localhost
against the private snapshot on port 13311. StarRocks used live production
data on 9030. Both companies ran all 15 variants with hash buckets a/b/c,
the archived Wayroll b5359d9e generator, and the prior per-case dates.
The production thinDB service on 13310 was not deployed or restarted.

Final procedure: DOP 4, 2 GiB data cache, 4 GiB retained-region budget,
16 GiB query/shared budgets, and a 20 GiB service ceiling. Each thinDB arm
started a fresh isolated service, warmed once, ran three timed queries, then
ran one untimed fingerprint query. Startup, shutdown and fingerprint work
are excluded from timings. Final row packets are discarded without Node
value decoding. These settings differ from the preceding DOP 16 production
run; this is not a controlled historical before/after comparison.

All 30 variants completed with no query errors in the accepted results;
60/60 keyed/unkeyed fingerprint pairs matched. All 60 keyed case/arm
combinations engaged an existing region, but **none used the new SQL
`union_all` opcode**. SQL+regions was slower than ordinary SQL in 24/30
cases. Traces show failed shared-fork attempts before falling back to a
smaller existing region; avoid paying that preparation work repeatedly
before pursuing more general branch coverage. UDF+regions had the lowest
median in 29/30 cases (Sierra FX average favored unkeyed UDF slightly).

Sums of the fifteen case medians, seconds:

| Dataset | SR SQL | thinDB SQL | SQL + regions | UDF | UDF + regions |
|---|---:|---:|---:|---:|---:|
| Sierra | 10.53 | 11.21 | 16.62 | 3.95 | 1.58 |
| AirDNA | 49.89 | 54.81 | 57.72 | 42.95 | 15.22 |

Sierra source counts matched. AirDNA's live StarRocks data had slightly more
rows/customers than the frozen copy; detail output differed by about 300
rows. The fingerprint checks establish keyed/unkeyed equivalence within
thinDB, not cross-engine value equivalence.

An initial attempt with an 8 GiB query budget and retained service state
across variants hit the budget and then the isolated cgroup OOM limit. It
and the pilot were excluded, and the thinDB phase was rerun. A service-job
cancellation interrupted the last case; its partial samples were excluded
and all four arms of that case were rerun with unchanged settings. The
restart helper now checks the actual service PID. All 120 accepted service
instances have memory receipts: peak 16.55 GiB, no limit/OOM events.
The final candidate was stopped and port 13311 released. Production remained
at PID 2579063 with zero automatic restarts; CDC remained RUNNING.

Full tables, CSV, source counts, validation and raw traces are in
`.bench-data/five-arm-7b99d27/benchmark-report.md`, `timings.csv`,
`matrix.json` and the sibling result directories. Matching evidence is on
starrocks1 under `/home/ubuntu/wayroll-bench/five-arm-7b99d27`.

### Fuse shared SQL branches within one region (2026-09-10)

Branch: `region-shared-sql-branches`, based on `062b8c7`. This implements
the shared-input approach below. The independent-region discovery
experiment at `62eedfe` remains excluded. No production deployment or
production benchmark was performed in this round.

A common CTE input is retained once inside a keyed region. Compatible
UNION ALL branches run over borrowed frame snapshots, then concatenate
within each declared-key partition. Nested branches follow the same rule.
Supported branch steps include filters, projections, expressions, aliases,
SQL windows, and existing eligible joins. Ordinary UNION type planning is
shared with regional execution. Filters preserve empty range positions;
left-before-right concatenation preserves tied window ordering. Key
provenance survives nested unions only for value-identical output slots.

This is structural SQL support with no table, query-text or UDF-name
recognizers. Forced materialization, external CTE consumers, changed keys,
incompatible windows and unsupported branch operators retain ordinary
staging. A declined fusion retries the original staged-ingress path at the
same boundary. Cache reuse revalidates the sharing recipe and table
versions. Existing restrictions still apply: branch GROUP BY/TVFs,
unfiltered co-partitioned sides and duplicate-key broadcast joins are not
newly enabled. Supported grouped reductions can follow the combined frame.

Validation covers two/three branches, different window frames and order
specs, ties, positional numeric widening, duplicates, NULL keys/payloads,
empty branches/ranges, typed Boolean/UUID movement, dimension joins,
multiplying LEFT joins, filtering INNER joins, cached source updates and
changed external sharing. The benchmark's plan inspection checks live
producers separately because a final scalar aggregate may already have
destroyed its input operator during compilation.
The final full test run passed 1,490 tests with five existing skips;
formatting and whitespace checks passed.

Local Windows in-process measurements, ReleaseFast, 1M input rows, DOP 12,
one warmup per arm and five rotating measurements; medians in milliseconds.
Every execution checks exact aggregate totals against ordinary SQL, and
separate plan checks require the expected fused UNION op count. The
materialized arm explicitly retains the common CTE with `AS MATERIALIZED`;
this compares execution modes in the candidate, not different commits.
There is no MySQL transfer or Node decoding in these measurements.

`zig build bench-regions` uses the C allocator, matching the ReleaseFast
server's process allocator on this platform:

| SQL shape | Partitions | Ordinary SQL | Keyed materialized base | Keyed fused branches | Fused speedup vs materialized |
|---|---:|---:|---:|---:|---:|
| Shared window + overlapping filters | 100 | 103.85 | 93.93 | 50.54 | 1.86x |
| Three branches + independent windows | 100 | 130.46 | 97.11 | 54.91 | 1.77x |
| Filtered projections + grouped reduction | 100 | 20.89 | 21.09 | 18.08 | 1.17x |
| Shared window + overlapping filters | 10,000 | 85.42 | 100.83 | 49.75 | 2.03x |
| Three branches + independent windows | 10,000 | 126.66 | 108.21 | 53.08 | 2.04x |
| Filtered projections + grouped reduction | 10,000 | 20.90 | 25.26 | 23.71 | 1.07x |

The full `zig build bench` suite also passed. It explicitly uses
`DebugAllocator` even in ReleaseFast and ran the same cases with these
results; do not blend the two harnesses into one baseline:

| SQL shape | Partitions | Ordinary SQL | Keyed materialized base | Keyed fused branches |
|---|---:|---:|---:|---:|
| Shared window + overlapping filters | 100 | 86.69 | 80.68 | 55.03 |
| Three branches + independent windows | 100 | 125.58 | 108.75 | 60.67 |
| Filtered projections + grouped reduction | 100 | 22.92 | 26.16 | 27.74 |
| Shared window + overlapping filters | 10,000 | 81.16 | 100.62 | 79.57 |
| Three branches + independent windows | 10,000 | 105.86 | 107.00 | 72.57 |
| Filtered projections + grouped reduction | 10,000 | 25.87 | 32.65 | 40.02 |

Window-heavy forks benefit in both harnesses. Small reductions can lose to
ordinary SQL and can also regress against staged keyed execution; fusion
still pays filtering, copying and exchange costs. No universal speedup or
new production rollforward result is claimed.

Evidence: `.bench-data/shared-branch-bench.log`,
`.bench-data/shared-branch-full-bench.log`, and
`.bench-data/shared-branch-final-tests.log`. Next: compare this branch with
the correctness baseline on the private Linux FX snapshot, then use the
remaining rejection traces to prioritize further general operator support.

### UNION experiment and prerequisite correctness fixes (2026-09-10)

Current branch: `region-payload-and-cache-correctness`, engine commit
`d4b66ca`. The general UNION-branch discovery experiment is preserved on
`keyed-sql-union-coverage` at `62eedfe`; it is excluded from the current
branch because the additional region boundaries regress the full-SQL FX
workload. No changes from this round were deployed to production.

The experiment exposed three general engine defects, now repaired:

- Region column movement omitted Boolean and UUID payloads. Typed movement
  now covers every stored payload type, preserving NULL validity and memory
  accounting.
- Right join payloads could overwrite same-named left columns. Table/CTE
  qualifiers and explicit scan aliases now remain distinct. Selected right
  keys preserve their values and NULLs, including an empty build side.
- Deleting only persisted rows did not invalidate cached lookup results.
  Table versions now include tombstone generation and cache UID, covering
  segment-only deletes and table recreation.

Programs that fold entirely to emission also decline regional execution:
there is no shard-local work to amortize the exchange and consolidation.
The existing mixed-width join-key output-type issue below remains separate;
the new namespace regression uses matching key types and different payload
types to isolate the binding defect.

The fixes passed the full test suite (1,481 passed, five existing skips),
`zig build bench`, and a Linux ReleaseFast distribution build. Snapshot
validation passed all 24 checks comparing repeated raw-row fingerprints and
row counts for SQL, SQL+regions and UDF+regions across Sierra/AirDNA monthly,
FX latest and FX average. The experiment separately passed those checks and
comparisons through all 39 Sierra CTEs after its correctness repairs.

Median milliseconds, one warmup and five samples, three hash buckets a/b/c,
on starrocks1 localhost against the private snapshot on port 13311. DOP 4,
2 GiB block cache, 4 GiB retained-region budget, 20 GiB cgroup maximum; final
packets discarded without Node value decoding. The baseline was refreshed
immediately before the fixes run; the experiment ran earlier on the same
shared host. Small differences are not established performance changes.

| Dataset / variant | SQL+regions baseline | Fixes | UNION experiment | UDF+regions baseline | Fixes | UNION experiment |
|---|---:|---:|---:|---:|---:|---:|
| Sierra monthly | 213 | 211 | 185 | 95 | 83 | 83 |
| Sierra FX latest | 233 | 220 | 403 | 112 | 118 | 170 |
| Sierra FX average | 210 | 231 | 399 | 121 | 134 | 138 |
| AirDNA monthly | 1,980 | 2,033 | 2,269 | 645 | 685 | 657 |
| AirDNA FX latest | 2,017 | 1,982 | 3,786 | 1,169 | 1,214 | 1,288 |
| AirDNA FX average | 1,952 | 2,041 | 3,500 | 1,268 | 1,186 | 1,195 |

Next: preserve a shared input once and fuse compatible split/UNION/rejoin
work within a region. Merely discovering additional independent regions
adds materialization, repeated preparation, and exchanges. Start with a
generic window/union/dimension-join regression requiring one exchange across
the compatible fork, while preserving union casts, duplicates, NULLs,
shared/forced CTE semantics and window ties. Then retry FX. Final aggregation,
global sort and MySQL output remain later targets.

Evidence: `.bench-data/keyed-union-coverage/final/report.md` and the matching
directory on starrocks1 under `/home/ubuntu/wayroll-bench/`. The final candidate
peaked at 11.06 GiB with no pressure/OOM event and was stopped. Production
remained at PID 2579063, zero automatic restarts; CDC stayed RUNNING with
37,413 completed checkpoints. This is a targeted comparison, not a new
full five-arm or StarRocks sweep.

### Preserve routed keys across frame replacements (2026-09-10)

Engine commit `bdec8da` fixes the physical route-key identity lost when a
TVF or aggregation replaces the regional frame. Replacing TVFs now carry
the route through their existing `ordered_output` partition-value contract;
matching output names alone are insufficient. GROUP BY carries it through
an unchanged group column and remaps constant-column indices to the new
frame. Co-partitioned joins resolve the route against physical columns,
rather than the SQL-visible alias map. No Wayroll-specific recognition or
function changes were added. Unmarked TVFs and computed key replacements
retain ordinary fallback.

The new regression failed before the fix. It requires four windows, two
row-generating TVFs, an aggregation, and a co-partitioned join in one region,
then compares all output values with ordinary execution. It covers DOP 1
and 4, NULL keys and values, and cached runs. Separate negative cases verify
that a TVF merging customer keys, and a computed key replacement after
aggregation, cannot inherit the old route.

Validation: full `zig build test -j2` passed (826 integration, 113
client/server, 533 unit, and 5 config tests; 5 existing unit skips).
`zig build bench -j2` passed, including exact-result regional window checks.
Local regional/ordinary window speedups were 1.12x (LAG scan), 1.46x (LAG
UNION ALL), 1.21x (mixed scan), and 1.58x (mixed UNION ALL).

Follow-up `54578b3` also invalidates entry-filter constants when a replacement
does not prove their values survive: TVFs retain only partition-key literals,
and aggregates retain only group-column literals. The new regression changed
a non-partition project column from 100 to 101; the regional join incorrectly
returned the lookup for 100 before the guard. TVF and aggregate replacements
now match the ordinary join values, including cached runs. The final full
test run passed 827 integration, 113 client/server, 533 unit, and 5 config
tests (1,478 total; 5 existing skips).

A separate mixed-width join schema issue surfaced during this check:
ordinary execution widens a selected INT key joined to BIGINT, while the
regional join retains INT. The constant-provenance regression uses matching
BIGINT keys to isolate the value bug. Mixed-width join output-type parity
remains a follow-up alongside the remaining SQL GROUP BY coverage limits.

The server follow-up exposed a separate memory/configuration constraint.
The default retained-region cache has its own 8 GiB budget, independent of
the block cache and active query budget. One benchmark attempt reached its
33 GiB OS ceiling; a subsequent attempt with a 6 GiB block cache was killed
inside its 32 GiB cgroup. Production thinDB, StarRocks, and CDC stayed healthy.
These attempts are archived and excluded from final timing results.
An overly small 2 GiB region-cache budget evicted the compiled cross pipeline
and lost its warm-run benefit. The follow-up configuration uses a 6 GiB block
cache, `THINDB_REGION_POOL_MB=4096`, DOP 16, a 24 GiB shared query budget,
and a 16 GiB per-query budget, retaining 12 GiB initial host headroom.
Each arm's warmup and three measurements run consecutively: interleaving
different SQL/UDF programs can churn this smaller cache and measure repeated
compilation instead. Fresh candidates separate the datasets. Memory-aware
region-cache sizing/accounting remains a follow-up concern for deployment;
this repair does not change those budgets automatically.

The full five-arm sweep at `bdec8da` completed all 30 dataset/variant cases
with one warmup and three measured samples per arm, on starrocks1 via
localhost, retaining the three hash buckets a/b/c and prior case dates.
Final result packets were discarded without Node value decoding. All 60
SQL/UDF keyed-versus-unkeyed wire fingerprints matched, and all four thinDB
arms returned matching row counts in every case. Eight source-count checks
match the preceding snapshot exactly. The full timing phase peaked at
22.77 GiB with zero memory-ceiling-pressure or OOM events.

Totals (sum of the fifteen medians, seconds):

| Dataset | SR SQL | thinDB SQL | SQL + regions | UDF | UDF + regions |
|---|---:|---:|---:|---:|---:|
| Sierra | 10.47 | 12.76 | 8.90 | 6.41 | 3.12 |
| AirDNA | 47.21 | 45.00 | 35.30 | 35.84 | 12.21 |

After the stale-constant guard, final head `54578b3` was compared with
`6a216c4` on the six affected UDF+regions cases using matched 6 GiB data / 4 GiB
region-cache settings. These focused controls are separate from the matrix
above. Median milliseconds, three measured runs after warmup:

| Dataset | Variant | Before | Final | Speedup |
|---|---|---:|---:|---:|
| Sierra | base cross | 310 | 153 | 2.03x |
| Sierra | expanded cross | 871 | 177 | 4.92x |
| Sierra | detail cross | 304 | 196 | 1.55x |
| AirDNA | base cross | 1,472 | 580 | 2.54x |
| AirDNA | expanded cross | 9,241 | 604 | 15.29x |
| AirDNA | detail cross | 2,540 | 1,460 | 1.74x |

All six final-head fingerprints match both their unkeyed controls and the
earlier matrix outputs. Regional operation sequences are unchanged by the
constant guard. Both datasets' UDF cross programs extend from the regressed
13 operations to 28 (base), 31 (expanded), and 25 (detail), including later
windows, aggregation, and UDF stages. SQL cross programs still stop at the
later GROUP BY boundary (17–18 operations). Shared-host load and cache
behavior explain why focused-control medians differ from matrix medians;
neither matched timing phase recorded memory pressure.

Final Linux ReleaseFast SHA256:
`dc47d31b5b83591cfbe84d94028678b5c70d1f4633637647296762da30ac885b`.
Both candidates were stopped. Production thinDB PID 256623, StarRocks BE PID
848762, and the original running CDC job remained healthy. No production
binary was replaced. Full matrix, CSV, raw queries/traces, fingerprints,
failed-attempt archives, and matched controls are in ignored
`.bench-data/five-arm-bdec8da/`, `.bench-data/five-arm-54578b3/`, and
`.bench-data/five-arm-routebase-6a216c4/`. The combined report is
`.bench-data/five-arm-bdec8da/report/benchmark-report.md`.

### Five-arm server sweep: SQL gains and UDF coverage regression (2026-09-10)

The complete Sierra/AirDNA sweep ran on starrocks1 with engine `6a216c4`
(Linux ReleaseFast, SHA256
`ddc30341a5aabbb519bf92555e377f47ca4305dc1a4e2d5fb93461d2d5930e8c`)
and Wayroll query generation `b5359d9e`. All five arms used the prior query
dates and three customer-hash buckets `[a,d)`, with one warmup and three
measured runs. Clients ran on localhost and discarded final row packets
without value decoding. StarRocks completed before the candidate started.

Totals below are sums of the fifteen case medians:

| Dataset | SR SQL | thinDB SQL | SQL + regions | UDF | UDF + regions |
|---|---:|---:|---:|---:|---:|
| Sierra | 11.01 s | 9.48 s | 6.61 s | 4.44 s | 3.08 s |
| AirDNA | 47.26 s | 46.00 s | 38.13 s | 39.08 s | 27.53 s |

All 150 dataset/variant/arm combinations completed without query errors.
All 240 timed keyed executions engaged a region. All four thinDB modes
returned matching row counts in every variant. A separate untimed pass
computed order-independent row-packet fingerprints: all 30 SQL keyed/unkeyed
pairs and all 30 UDF keyed/unkeyed pairs matched. This validates paired wire
outputs; it is not a value comparison with StarRocks live data. Sierra source
counts match the copy, while AirDNA live StarRocks has about 0.02% more rows.

**The new build regresses some UDF cross-division pipelines despite region
engagement.** AirDNA UDF-plus-regions base-cross increased from 493 to 1,453 ms;
expanded-cross from 600 to 11,833 ms. Sierra expanded-cross increased from
203 to 821 ms. The prior AirDNA base-cross region contained 29 operations,
including ranking, aggregation, and later UDF stages. The new one emits
after 13 operations: a subsequent window declines and the remaining work
stages ordinarily. SQL windows now enter regions, but the SQL path also hits
later GROUP BY coverage limits. Engagement alone is not full pipeline coverage.

The likely UDF blocker is physical route-key identity after a frame-replacing
TVF: `pushReplaceTvf` generates new physical names and resets the frame;
`dispatchWindow` compares its partition columns with the earlier `route_name`.
Next priority is preserving **proven** routed-key identity across these
transitions, with a regression test that requires the later window and UDF
stages inside the region. Do not remove the guard without a value-preservation
proof. Then address the SQL GROUP BY declines and repeat the five-arm sweep.
This diagnosis is supported by source and traces; no fix was applied during
this benchmark run.

The candidate retained DOP 16, an 8 GiB cache, 24 GiB shared query budget, and
16 GiB per-query budget. Its OS ceiling was 34 GiB to preserve 12 GiB initial
headroom; peak cgroup usage was 32.74 GiB with zero ceiling-pressure/OOM events.
The temporary server was stopped after validation. Production thinDB, StarRocks
BE, and CDC remained running with their original PIDs/job ID.

The complete timing matrix, exact CSV medians, raw samples, SQL captures,
traces, fingerprints, source counts, and health metadata remain in ignored
`.bench-data/five-arm-6a216c4/`. The report is
`report/benchmark-report.md`; the CSV is `report/benchmark-results.csv`.

### Shared SQL windows and ordinary fallback (2026-09-10)

The follow-up implementation replaces the SQL window whitelist with a
regional `window` operation backed by `exec/window.zig`. Ordinary and keyed
execution now share the evaluator for all sixteen existing window function
kinds: ranking/distribution, LAG/LEAD, FIRST/LAST/NTH_VALUE, and aggregates.
Partition and order references, arguments, defaults, output types, and aliases
are resolved once. Each worker borrows its complete shard input and reuses
output stores. Regional admission checks that every partition retains the
actual routed key; compatible partitions may have different extra columns
and sort orders. TVF passthrough views preserve route-key identity.

The initial LAG-with-default probe failed before the change. It and the
multi-column LAST_VALUE probe now execute inside regions with value parity. Broader tests
cover mixed specs, ascending/descending orders, NULL keys/values, string
results, default columns, large offsets, cached execution, and source changes.

Shared evaluation exposed existing frame bugs: FIRST_VALUE ignored its frame,
and RANGE/GROUPS were evaluated as physical ROWS. Frame-aware value functions
now respect frame bounds, including IGNORE NULLS and empty frames. RANGE
includes peers and supports integer offsets over one numeric order column
(including scaled decimals); GROUPS advances by peer groups. Prefix aggregate
frames retain a linear evaluation path with peer broadcasting. The old
whole-partition FIRST_VALUE tests now specify that frame explicitly, and new
tests assert expected values separately from keyed/ordinary parity. Temporal
RANGE offsets and EXCLUDE remain unsupported. Semantics reference:
[PostgreSQL window functions](https://www.postgresql.org/docs/current/functions-window.html).

As requested, KEYED BY now permits ordinary fallback instead of making lack
of regional coverage a query error. This supersedes the older hard-decline
contract below. Incompatible window/group partitions and global sort/limit
boundaries can become staged ingress for a later region. Earlier compatible
CTEs within that ingress can independently use regions. A regression test
asserts exactly two regions around a global window and compares every value,
including cached runs and changed input. Invalid SQL still reports its normal
semantic error; fallback tests explicitly verify that no region engaged.
Declared keys are retained in cache entries and compared before reuse.

The focused local ReleaseFast benchmark uses 1,000,000 synthetic rows, DOP 12,
one warmup and five alternating measured runs per arm. Every keyed execution
must include the downstream window in its region; aggregate totals must match.
Medians from `.bench-data/sql-window-bench.log`:

| SQL pipeline | Ingress | Ordinary | Keyed | Speedup |
|---|---|---:|---:|---:|
| LAG chain | Scan | 63.40 ms | 46.20 ms | 1.37x |
| LAG chain | UNION ALL | 61.52 ms | 38.32 ms | 1.61x |
| LAG default + running SUM + ordered LAST_VALUE | Scan | 95.93 ms | 56.91 ms | 1.69x |
| LAG default + running SUM + ordered LAST_VALUE | UNION ALL | 94.67 ms | 59.49 ms | 1.59x |

Validation: `zig build test -j2` completed with exit 0: 823 integration tests,
113 client/server tests, and 533 unit tests passed; five existing unit tests
were skipped. After helper naming cleanup, the focused window/region suite
was rerun. `zig build bench-regions -j2` and `zig build bench -j2` both completed
with exit 0. In the full general benchmark run, the four keyed comparisons
measured 1.15x, 1.77x, 1.47x, and 2.04x respectively; retain both runs rather
than treating the small synthetic timing differences as a stable gain.
Logs are `.bench-data/sql-window-full-test-final.log`,
`sql-window-final-targeted.log`, and `sql-window-full-bench.log`.

These are local synthetic comparisons, not new Sierra/AirDNA results. The
next application-level measurement must replay the full SQL rollforward arm,
verify values and regional coverage, and retain the bounded, separate-phase
server procedure documented below. A mixed-spec window operator with an
incompatible partition currently stages as a unit; adjacent compatible CTEs
can still use regions.

### Plain SQL region eligibility and UDF attribution (2026-09-10)

Checkpoint commit `516c454` preserves the earlier benchmark documentation and
the reusable packet-discard helper. The following experiment uses the same
`0258e6d` engine and `b5359d9e` Wayroll application; no engine changes were made.

Adding `KEYED BY (customerNumberLC)` to the outer full-SQL `WITH`, with all
SQL/Zig UDF switches disabled, was rejected with `RegionUnsupportedConstruct`
in all 30 Sierra/AirDNA configurations. The requested SQL-plus-regions column
is therefore **unsupported**, with no valid timing. The benchmark captures
verify the declaration was inserted and no `rf_*` functions were present.

The traces show boundary search declining SQL windows. The shared currency
normalization uses `LAST_VALUE(originalCurrency)` ordered by both
`invoiceDate` and `originalCurrency`; the regional `pushFillLast` path accepts
only one ascending order column. Later windows also exceed current coverage:
`LAG` with a non-NULL default, partition-wide `MIN`/`MAX`, and cumulative
`SUM`. See `dispatchWindow`, `push_lag`, `pushFillLast`, and `checkRangeOrder`
in `src/net/region_rollforward.zig`. The final date/type aggregate belongs
outside the customer region; its key-contract decline alone is expected.
The complete SQL pipeline fails because no supported inner boundary is found.
This does not mean ordinary SQL fundamentally cannot use keyed regions.

An additional Zig-without-regions control measures the regional contribution
to the existing UDF-shaped pipeline. All 15 variants per dataset used three
hash buckets `[a,d)`, one warmup, and three measured runs, with result packets
consumed without decoding values. Totals are sums of per-case medians:

| Dataset | thinDB SQL | Zig only | Zig + regions | SQL / Zig only | Zig only / Zig + regions |
|---|---:|---:|---:|---:|---:|
| Sierra | 10.03 s | 4.89 s | 2.13 s | 2.05x | 2.30x |
| AirDNA | 39.13 s | 33.50 s | 10.22 s | 1.17x | 3.28x |

AirDNA expanded-cross illustrates the difference: SQL took 9,593 ms,
Zig only 10,139 ms, and Zig plus regions 600 ms. These ratios measure regions
on the UDF-shaped queries; they cannot substitute for the still-unavailable
SQL-to-keyed-SQL comparison or predict the speed of generic SQL windows.

All 120 keyed Zig executions engaged a region; plain SQL and Zig-only did
not. Successful thinDB arms returned matching row counts. Captured Zig
queries and parameters match after removing the keyed declaration and
consistently normalizing generated CTE names; the SQL arms match under the
same check. Value-level correctness was not rechecked in this timing run.

Measurements ran via localhost on starrocks1, using the isolated thinDB data
copy on 13311. The temporary instance retained the previous memory limits,
recorded zero memory-limit/OOM events, and was stopped before StarRocks
benchmarking. Production thinDB and CDC were left running. Raw queries,
traces and phase results are in ignored `.bench-data/sql-keyed-results-thin/`
and `sql-keyed-results-sr/`; the combined report and CSV are in
`.bench-data/sql-keyed-results/`.

Next engine work: support general ordered `LAST_VALUE`, non-NULL `LAG`
defaults, and aggregate windows in regions, with full value parity tests.
Then rerun the explicit plain-SQL arm and inspect which expensive stages
actually execute inside regions before attributing the remaining UDF benefit.

### Server benchmark checkpoint (2026-09-10)

Engine changes are committed as `0258e6d`; companion Wayroll changes as
`b5359d9e` on its local `rollforward-thindb-local` branch. The Linux candidate
was benchmarked on starrocks1 against a separate production-data copy on
13311. The production thinDB listener on 13310 remains on v0.1.89.

The benchmark now consumes MySQL row packets without constructing Node rows.
The reusable decoder bypass is `bench/mysql_packet_drain.cjs`. With identical
AirDNA detail queries, alternating decoded/discard controls changed Zig
simple from 2,198 to 506 ms and cross from 2,545 to 585 ms. All result bytes
were still transmitted. The adapter passed protocol checks on both engines.

The three-bucket range `[a,d)` selects a, b and c together. It increases
AirDNA invoice input by 2.99x and Sierra input by 2.56x simple / 2.96x cross.
Matched one/three-bucket runs cover all 15 variants on both datasets, with
one warmup and three measured runs per arm and no row-value decoding.
All 120 three-bucket Zig queries engaged regions; thinDB SQL/Zig row counts
matched. Summed case medians (three buckets):

| Dataset | StarRocks SQL | thinDB SQL | thinDB Zig + regions | Zig vs SR, one to three buckets |
|---|---:|---:|---:|---:|
| Sierra | 12.11 s | 11.39 s | 2.39 s | 5.11x to 5.06x |
| AirDNA | 53.45 s | 50.11 s | 13.20 s | 4.41x to 4.05x |

An initial cohosted attempt exhausted RAM and the OS killed StarRocks BE at
06:38:20 UTC. The extra thinDB instance was retaining about 28 GiB alongside
production services. The benchmark client and candidate were stopped, the
existing BE startup script restored StarRocks, and backend readiness, an empty
blacklist and a table query were verified. Production thinDB and CDC stayed
running. That attempt is excluded from the results.

Final measurements ran StarRocks with the temporary thinDB instance stopped,
then thinDB SQL/Zig with no StarRocks benchmark queries running. Both thinDB
sizes used DOP 16, cache 8 GiB, shared query budget 24 GiB, per-query budget
16 GiB and a 36 GiB OS ceiling. No ceiling-pressure or OOM event occurred in
the final runs. The temporary instance has been stopped. Future three-bucket
work must retain separate phases and a bounded temporary instance.

Full matrices, raw samples, source counts and procedure remain in the ignored
`.bench-data/region-scale-results/`, `region-scale-baseline/`,
`region-drain-results/` and `region-scale-runbook.md`. Deployment-specific
scripts, credentials, copied databases and customer results stay outside git.

The next comparison at this checkpoint was full SQL with an explicit keyed
declaration while all SQL/Zig UDF switches remained off, to isolate regional
and UDF contributions. Its findings appear above. Actual region coverage
matters alongside timings: engagement alone can represent a small inner
boundary rather than the expensive portion of the query.

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
