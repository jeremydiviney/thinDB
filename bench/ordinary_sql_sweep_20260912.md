# Ordinary SQL benchmark sweep — September 12, 2026 UTC

Later on September 12, the [no-estimates follow-up](ordinary_sql_diagnosis.md#no-estimates-follow-up-and-column-pruning-fix-2026-09-12-utc)
fixed wide lookup scans. No-estimates now measures 141/143 ms at DOP 12/16
versus a fresh StarRocks comparison of 214/212 ms. The full sweep below is
retained as the earlier build's baseline; only base-simple and no-estimates
received new five-run timings in that follow-up.

Fresh comparisons of the verified implementation build with StarRocks. Values below are median elapsed milliseconds; **bold** marks the lower median in each DOP pair. These are measured rankings, not statistical significance claims.

| Case | Rows | SR DOP 12 | ThinDB DOP 12 | SR DOP 16 | ThinDB DOP 16 |
|---|---:|---:|---:|---:|---:|
| Base simple | 3,539 | 482 | **392** | 455 | **331** |
| No estimates | 3,538 | **217** | 436 | **216** | 434 |
| Plans simple | 0 | 691 | **134** | 863 | **132** |
| Crossplans | 3 | 759 | **478** | 911 | **484** |
| Base cross | 4,781 | **513** | 642 | **540** | 637 |
| Child cross | 4,781 | **558** | 654 | **549** | 663 |
| Detail cross | 30,119 | **653** | 751 | **635** | 789 |
| Expanded simple | 3,539 | 1,579 | **924** | 1,673 | **914** |
| Interval quarter | 5,056 | 443 | **329** | 446 | **323** |

ThinDB has the lower median in 5 of nine cases at DOP 12 and 5 of nine at DOP 16. Full ranges follow below.

Each engine/case/DOP block has one warmup, five measured executions and one separate fingerprint execution: 180 timing samples, 36 warmups and 36 fingerprints. Engine order alternates by case and reverses in the second DOP pass. Timings use the archived application harness, including its per-call lookups, SQL generation, temporary/session setup, server work and result packet drain; one-time module and connection setup are recorded separately. Rows are not decoded in timing runs. No operator profiling is enabled.

Both DOPs are explicit. StarRocks sessions verify pipeline_dop, one fragment instance, disabled adaptive DOP, disabled query cache and disabled profiling. ThinDB uses its existing 2 GiB cache, 4 GiB region pool, 16 GiB query/shared budgets and a 20 GiB service cap, with compaction disabled. Every ThinDB block starts a fresh service on port 13311 over the private snapshot. Sierra, the archived dates, USD and hash range [a,d) are preserved. SQL has no UDF calls or keyed-region declarations.

Plans-simple intentionally returns zero rows and crossplans three: Sierra still has only the empty-string plan ID. Treat these as empty/selective workloads.

All 18 ThinDB fingerprints match the archived results, and both engines preserve fingerprints across DOPs. Sixteen of eighteen raw cross-engine fingerprints match. Detail-cross differs only in the known DATE versus midnight DATETIME representation; separate exact comparisons verify every value in all 30,119 rows and 54 columns at both DOPs, normalizing that date representation alone and retaining duplicate counts without numeric rounding.

ThinDB build: 7453ed688cd07bf22920b99a17c7c5e59cd718c1c2e8e9dccee2d24c62c1977e, Zig 0.16.0, ReleaseFast, x86_64-linux-gnu. The working engine/test patch was checked against the verified build before launch.

All 18 ThinDB service receipts have zero memory-limit/OOM events; peak observed memory is 1.543 GiB. Production ThinDB PID/restart count before and after: 2579063/0 and 2579063/0. StarRocks BE PID remains 848762; CDC is RUNNING. Health receipt: 2026-09-12T09:19:42.715521+00:00.

Raw results, every sample, exact value checks, session settings, build metadata and health/memory receipts are retained in .bench-data/ordinary-sql-sweep-20260912/. The results/samples.csv file contains all timing observations. Reproduction uses bench/ordinary_sql_diagnosis.py and bench/ordinary_sql_report.cjs with the new campaign archive. Production port 13310 and its data directory were not modified.

## All median/range comparisons

| Case | DOP | StarRocks median (range), ms | ThinDB median (range), ms | Thin/SR |
|---|---:|---:|---:|---:|
| base simple | 12 | 482 (474–529) | 392 (369–480) | 0.81 |
| base simple | 16 | 455 (441–480) | 331 (328–338) | 0.73 |
| noest simple | 12 | 217 (207–244) | 436 (370–449) | 2.00 |
| noest simple | 16 | 216 (205–236) | 434 (382–499) | 2.01 |
| plans simple | 12 | 691 (672–703) | 134 (115–173) | 0.19 |
| plans simple | 16 | 863 (842–1129) | 132 (120–200) | 0.15 |
| crossplans | 12 | 759 (744–785) | 478 (454–532) | 0.63 |
| crossplans | 16 | 911 (819–1098) | 484 (478–563) | 0.53 |
| base cross | 12 | 513 (511–651) | 642 (641–664) | 1.25 |
| base cross | 16 | 540 (519–547) | 637 (636–675) | 1.18 |
| child cross | 12 | 558 (540–565) | 654 (646–663) | 1.17 |
| child cross | 16 | 549 (537–561) | 663 (658–672) | 1.21 |
| detail cross | 12 | 653 (622–667) | 751 (749–806) | 1.15 |
| detail cross | 16 | 635 (611–651) | 789 (764–799) | 1.24 |
| expanded simple | 12 | 1579 (1537–1824) | 924 (909–927) | 0.58 |
| expanded simple | 16 | 1673 (1644–1781) | 914 (901–938) | 0.55 |
| interval quarter | 12 | 443 (426–484) | 329 (324–334) | 0.74 |
| interval quarter | 16 | 446 (441–479) | 323 (318–329) | 0.72 |
