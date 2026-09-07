# Keyed Regions — eligibility round (hand-off)

Status: PLAN (2026-09-07). Research complete, no code changed yet. Predecessor
design: [REGION_PLAN.md](./REGION_PLAN.md). Owner's direction: get more (ideally
all) rollforward shapes flowing through the keyed-region path.

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
  && includeEstimates && !(expandedRollforward && isCrossDivision)`.
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

- Plain `UNION ALL` of arms that all carry the keys (today only the
  base-with-TVF form `union_tvf` is a Step; see `unionTvfArm`, line 1799).
- Frame window aggregates: `AVG` over `ROWS BETWEEN n PRECEDING AND CURRENT
  ROW`, running `SUM`, partition-wide `MIN`/`MAX`/`COUNT`. These need new
  region ops in `region_exec.zig` (per-range sliding window over the sorted
  range, same shape as `.lag`).
- Only after W1-W6; measure the MRR1 hooks (`RF_ONLY` in CASES mode) before
  and after.

## 4. Verification recipe

- Local mirror: data dir `.wayroll-prod-db`, DB `wayroll_prod__public`, no
  password, MySQL port 13310. Start the ReleaseFast server with
  `--data-dir .wayroll-prod-db --mysql-port 13310 --pg-port 0 --native-port 0
  --max-dop 12 --cache-size 6G --memory-budget 32G --query-memory-budget
  25769803776`, with `THINDB_REGION_TRACE=1` in the environment while
  developing.
- Harness (wayroll worktree `C:/development/wayroll-api/.wt-thindb`, branch
  `rollforward-thindb-local`):

      RF_HASH=a RF_ITER=3 RF_COMPARE=1 RF_HOST=127.0.0.1 RF_PORT=13310
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
- PR #35 (decoded block cache) is open and awaiting a decision; it adds about
  14 GB peak working set on the 100M ClickBench table.
