# Expanded-metrics query on thinDB vs StarRocks — findings (2026-07-04)

## Source & transform
- Source: `C:\development\wayroll-api\ctedebug\cte-debug-2026-07-04T08-33-32-076Z-insert_to_output_table.sql`
  (181 KB, 5194 lines) — a StarRocks temp-table chain.
- Structure: `external_plan_ids_temp` (VALUES) + 13 `SET @var` + **7** `CREATE TEMPORARY TABLE … AS WITH … SELECT`
  (rollforward_pre_records, customer_monthly_totals, primary_pipeline_mat1, rollforward_pre_records_exp,
  expanded_metrics_pipline_mat1, expanded_metrics_mat, joined_expanded_mat) + final INSERT with
  translated_plan_ids/rollforward_filtered_result. ~130 nested CTEs total, **dual pipeline** (base + `_exp`),
  cross-division parent/child rollups + cross-sell/churn metrics. Much bigger than the prior 39-CTE rollforward.
- Transform: `testSQL/_transform_expanded.mjs` — drops DROP/SET/CREATE-temp/INSERT-target scaffolding,
  converts each CTAS + external_plan_ids_temp into CTEs, inlines @vars, swaps project **1000051 → 1000073**
  (thinDB only has 1000073/1000054 loaded) and repopulates the plan filter with a real AirDNA plan.
- Output: `testSQL/expanded_1000073_1plan.sql` (cross-division, 1 plan — the faithful test).
  `testSQL/expanded_1000073_div.sql` (extra single-division filter — a CONFOUND, do not use: it breaks
  cross-division semantics since one division has nothing to cross-sell against).

## Result: it RUNS on thinDB
- thinDB executes the full ~130-CTE dual-pipeline query **end-to-end, no crash, no unsupported shape**:
  **255,968 rows × 52 cols in ~33 s** (per-query budget raised: `--query-memory-budget 26000000000`).
- StarRocks needs its `query_mem_limit` raised to ~34 GB (OOMs at the default 16 GB) and takes **~69 s**.
  → As one monolithic CTE, thinDB is ~2× faster here AND fits where StarRocks needs tuning. (The temp-table
  decomposition exists precisely because the monolithic form is too big for one fragment on either engine.)

## 2026-07-05 update: emit fix → 14–16 s monolithic (~4.5× faster than StarRocks)
- Profiling showed 72% of wall (15.4 s) inside the two big GroupTopNPipeline calls — and inside THOSE,
  99.4% was `late.materializeInto`: the hashed-key emit late-materialized 3.37M group rowrefs in
  group-hash (random) order, so LateScan's per-rowgroup run batching degenerated to 3.37M single-row
  block borrows. Fix (8f5581c): sort rowrefs ascending before materializing, feed the key-gather the
  inverse permutation. late_mat 15 723 ms → 61 ms; **monolithic wall 33 s → 14–16 s**, all fingerprint
  SUMs bit-identical. This fix applies engine-wide to hashed-key GROUP BY emit (Q29-class shapes).
- `SEPARABLE BY (customerNumberLC)`: correct (all 8 fingerprint SUMs bit-identical) at ~28–30 s —
  **slower than monolithic now**. Post-fix the mono plan is already fully parallel at DOP 12; slicing
  adds routing/recompute overhead without reducing total compute. SEPARABLE's winning regime remains
  window/DRAM-bound tails (the 39-CTE rollforward: ratio 0.68).
- Pre-partition UAF crash FIXED (76c89d9): group-by-rooted candidates compiled against the dead
  compile-phase arena. `THINDB_SEP_PREPART_NONSIMPLE=1` now routes aggregate/window-rooted candidates
  crash-free (values exact), but candidates serially re-inline shared prefixes → loses wall-clock;
  default stays gated to simple table chains. Follow-up: let candidates read earlier candidates'
  routed buffers.
- New mono profile is balanced: Join 34.5% (2.6 s), Compute 18%, GroupTopN 16% — no single-lever
  target remains; the ~3 s × 6 sequential 341K-row window stages are the next structural candidate.

## Bug found + fixed: missing division sentinels (DATA gap, not an engine bug)
- thinDB initially returned **0 rows**. Bisected to `rollforward_with_plan_and_division_aba7f3c5`:
  `INNER JOIN division ON division.id = r.divisionId` with cross-division rows carrying `divisionId = -2`.
- thinDB's loaded `division` table was **missing the `-2` ("All Division") and `-1` ("Null Division")
  sentinel rows** that StarRocks has → the INNER JOIN dropped every cross-division row. thinDB's SQL is
  correct; its data was incomplete. Inserted both rows (memtable, this session) → query produces rows.
  **Permanent fix: reload thinDB's `division` with the negative-id sentinels.**

## Value comparison (cross-division, 1 plan; SUMs over rollforward_filtered_result)
| metric | thinDB | StarRocks | Δ |
|---|---|---|---|
| rows | 255968 | 256587 | −0.24% |
| SUM(amount) | 754,349,824 | 756,766,108 | −0.32% |
| SUM(originalAmount) | 755,980,404 | 758,380,038 | −0.32% |
| SUM(diffAmount) | 41,276,749 | 43,005,906 | −4.0% |
| SUM(crossSellAmount) | 2,116,865 | 2,136,387 | −0.9% |
| SUM(crossChurnAmount) | −1,251,714 | −3,614,407 | **2.9×** |
| SUM(childUpAmount) | 837,814 | 845,282 | −0.9% |
| SUM(childDownAmount) | −655,888 | −666,611 | −1.6% |
| SUM(lastAmount) | 712,926,703 | 713,615,425 | −0.1% |

Close but **not bit-exact**. Most metrics within 0.1–0.3%; the standout is **crossChurnAmount (2.9×)**.
All output rows are `divisionId = -2` (fully cross-division), so the gap is inside the cross-division
aggregation.

## Open items (next session)
1. **crossChurn 2.9× discrepancy** — the clear signal. crossSell nearly matches while crossChurn is 3× off,
   pointing to a specific bug in the cross-churn computation path (sign / CASE / filter) in
   `cross_division_agg_exp` / `with_recalculated_cross_sells`. Bisect those CTEs vs SR.
2. **~0.24% row gap** — 619 rows handled differently. Possibly linked to the `external_plan` data gap
   (thinDB 18143 vs SR 18278 for the project) — it's LEFT-joined so shouldn't drop rows, but worth ruling out.
3. **Permanent data fixes**: reload division sentinels (-2/-1) and the missing external_plan rows.

## Harness
- `testSQL/_transform_expanded.mjs` (DIV env = optional single-division shrink; PLANARG = comma plans)
- `testSQL/_probe_expanded.mjs` — lifts expanded_metrics_pipline_mat1's inner CTEs to top-level for COUNT bisect
- `/tmp/_fp2.mjs` — fingerprint (COUNT + column SUMs), arg `thindb`|`sr`
- StarRocks: $SR_HOST:9030, db wayroll, `SET query_mem_limit = 34000000000` before the query.
