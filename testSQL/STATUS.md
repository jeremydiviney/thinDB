# Wayroll rollforward query — status

## ✅ 2026-06-19: ENTIRE QUERY RUNS END-TO-END

`testSQL/rollforward_1000054_flat_agg.txt` (39-CTE flat stack + final SELECT)
executes against the local `wayroll__public` DB in ~3s, returning 120 rows,
no errors. Probe all green: `py testSQL/_probe_flat_agg.py`.

Engine/dialect gaps closed this session (each its own commit, full suite green):
1. **Temporal string-literal coercion** — `DATE_ADD('2026-05-01', INTERVAL …)`,
   `LAST_DAY('…')` accept a string literal where a date is wanted (compute
   special-cases literals only; column footgun protection kept).
2. **`@var` as a join-ON constant** — `r.col = @var` / `r.col = f(@var)` no longer
   trips SqlOnNonEquiUnsupported.
3. **`lit op @var` and `@var op …`** WHERE guards (`1 = @includeEstimates`,
   `@comparisonMonths > 1`).
4. **UNION reconciles VARCHAR/CHAR with STRING** arms (shared StringView repr).
5. **`@var` args inside window calls** resolve (`LAG(x, @comparisonMonths, 0)`).
6. **CASE/IF reconciles VARCHAR/CHAR branches with STRING**.

NEXT (optional): validate result values against the StarRocks source
(`mcp Wayroll execute-query` / `get-customer-rollforward`) — running ≠ verified.

Everything below is older historical context, much of it superseded.

---

# Wayroll rollforward query — overnight status

Goal: get actable (projectId **1000054**) data local in a thinDB `wayroll` DB and
run `rollforward_1000054_full_inline_cte.sql` against it, building CTE-by-CTE.

## DONE

### 1. Data loaded into local thinDB `wayroll` ✅ (primary goal)
- thinDB server: `./zig-out/bin/thindb-server.exe --data-dir .wayroll-db --mysql-port 7881 --max-dop 12 --query-memory-budget 8589934592`
  (separate port/data-dir from the clickbench server on 7880, runs in background)
- DB name over the wire: **`wayroll__public`** (thinDB uses `<db>__<schema>`, default schema `public`).
- Source: StarRocks/Doris `wayroll` @ 64.20.36.26:9030 (creds via env `SR_PWD`, never committed).
- Loader: `tests/bun/wayroll_load.mjs` (bun + mysql2). Streams source, batched bulk INSERT.
  Run: `cd tests/bun && SR_PWD='...' bun run wayroll_load.mjs`

7 source tables (identified from the query's FROM/JOIN minus CTEs/temp tables):

| table | rows loaded | filter | order key (StarRocks PK) |
|---|--:|---|---|
| invoice_import_amortized | 809,973 | projectId=1000054 | (projectId, integrationConfigId, invoiceId, invoiceItemId, modelType, date) |
| currency_exchange_rate | 1,003,931 | whole table | (currencyTo, currencyBase, date) |
| report_customer_revenue_rollforward | 189,283 | projectId=1000054 | (projectId, revenueModel, divisionId, month, customerNumber) |
| number_sequence | 10,000 | whole table | (id) |
| estimate_date_map | 662 | whole table | (sourceMonth, sourceDay, targetMonth, leapYear, targetDay) |
| division | 6 | projectId=1000054 | (id) |
| external_plan | 6 | projectId=1000054 | (projectId, integrationConfigId, externalPlanId) |

Schemas saved: `testSQL/schema/_starrocks_schemas.txt`.

### 2. Division choice ✅
actable (1000054) has 6 divisions. Largest by invoice_import_amortized rows:
**divisionId 1000093 ("Hotel Effectiveness")** — 417,007 rows, 6,465 customers.
modelType is all `mrr`. Query's `divisionId IN (1)` was swapped to `IN (1000093)`.

### 3. Query transformed ✅
- `testSQL/_transform.py` produces `testSQL/rollforward_1000054_thindb.sql`:
  - Inlines all `@variables` (e.g. `@projectId`→1000054, `@curMonth`→'2026-05-01', `@modelType`→'mrr', division→1000093).
  - Removes `SET` / `DROP TEMPORARY TABLE` / `CREATE TEMPORARY TABLE` DDL.
  - Connects the 3 temp-table blocks + final block into ONE statement.
- `testSQL/_flatten_and_probe.py` parses that into a fully **flat** 38-CTE single WITH:
  `testSQL/rollforward_1000054_thindb_flat.sql` (this is the "one big CTE stack").
  It can also probe CTE-by-CTE: `py testSQL/_flatten_and_probe.py --probe`.

## IMPORTANT: rebuild from latest main — the UNION crash was already fixed

The CTE-bodied UNION crash (below) was observed on a STALE server binary built from
`fc919df`. After pulling latest `main` (`97830ce`) you MUST rebuild the server
(`zig build -Doptimize=ReleaseFast`) — the crash is GONE on `97830ce`. The minimal
repro `WITH u AS (SELECT id FROM division UNION ALL SELECT id FROM division) SELECT * FROM u`
now returns rows. Lesson: always rebuild the wayroll server after pulling.

## PROGRESS (on latest 97830ce): query runs through 10 of 38 CTEs

CTEs 1–10 pass (incl. the UNION at #9). New blocker = **CTE #11
`converted_invoice_normalized_handler`**: `ComputeNoSuchOverload` on **DECIMAL
columns** — `CAST(rate AS DOUBLE)`, `ROUND(rate)`, `COALESCE(rate,1)`, `rate/2` all
fail, though the same on numeric LITERALS works. Same root pattern as the VARCHAR
gap: the scalar overload resolver doesn't handle the sized column types
(`.varchar`, `.decimal`) — only canonical `.string`/`.double`/`.int`.
**Next workaround: map the loader's `decimal` → `DOUBLE` (like varchar→TEXT) and
reload, OR fix the engine overload resolution for decimal/varchar.**

(Older snapshot below, kept for the repro — but the UNION crash itself is fixed.)

## PROGRESS: query runs through 8 of 38 CTEs

After two fixes (TEXT columns + typed DATE literals), the probe gets through CTEs
1–8 cleanly, then the **server crashes** on CTE #9.

```
[ 1/38] OK  invoice_base
[ 2/38] OK  last_month_invoices
[ 3/38] OK  customer_3month_aggregates
[ 4/38] OK  last_month_invoices_with_adjusted_dates
[ 5/38] OK  estimate_candidates
[ 6/38] OK  invoice_estimate_sequence
[ 7/38] OK  estimates_final
[ 8/38] OK  estimates_transformed
[ 9/38] CRASH invoice_base_with_estimates  (Lost connection — server died)
```

CTE #9 is just:
```sql
invoice_base_with_estimates AS (
  SELECT * FROM invoice_base      -- 21 columns
  UNION ALL
  SELECT * FROM estimates_transformed
)
```
ROOT CAUSE FOUND (minimal, deterministic repro on a 6-row table):
```sql
WITH u AS (SELECT id FROM division UNION ALL SELECT id FROM division)
SELECT * FROM u;          -- crashes the thinDB server
```
It is **NOT** schema mismatch / NULL-vs-typed / scale (all ruled out):
- both arms fully evaluate alone (`COUNT(*)` fine);
- the *same* UNION as a TOP-LEVEL query works (`SELECT .. UNION ALL SELECT .. LIMIT 5`);
- a 1-row typed-vs-NULL union works.
The crash is purely structural: **a CTE whose body is a `UNION ALL`, when referenced,
hard-crashes the server.** That is the bug to fix in the engine (CTE-bodied set-op
materialization). No stderr trace emitted (hard crash). The query itself is valid
(runs on StarRocks).

### Fixes already applied to get this far
1. **TEXT not VARCHAR** for string columns (loader `mapType`) — see gap #1.
2. **Typed DATE literals**: `@curMonth`→`DATE '2026-05-01'`, `@currentDate`→`DATE '2026-05-15'`
   in `_transform.py` — see gap #2.

## RUNNING THE QUERY — how to resume

`py testSQL/_flatten_and_probe.py --probe` walks the 38 CTEs in dependency order,
running `WITH <prefix> SELECT * FROM <lastCTE> LIMIT 1` for each.

**First blocker was `LOWER(customerNumber)` → `ComputeNoSuchOverload`.** Root cause:
thinDB registers string scalar kernels (LOWER/UPPER/CONCAT/LENGTH/SUBSTRING) only for
the `.string` type. `VARCHAR(n)` DDL maps to a distinct `.varchar` type with **no
string-function overloads**; `TEXT`/`STRING` map to `.string`. The loader originally
used VARCHAR → string funcs failed on every character column.

**Fix applied:** loader now maps `varchar`/`char` → `TEXT`. Reloading with TEXT columns
(in progress / done by morning). Re-run the probe after reload to find the next blocker.

## thinDB GAPS found (candidates to fix in the engine)

1. **VARCHAR has no string-function overloads.** `LOWER/UPPER/CONCAT/LENGTH/SUBSTRING(varcharCol)`
   → `ComputeNoSuchOverload`, while the same on a `.string`/TEXT column (or a string literal)
   works. `VARCHAR(n)` (parse_ddl → `.varchar`) should resolve string kernels like `.string`
   does (alias `.varchar`→`.string` in overload resolution, or register kernels for `.varchar`).
   Worked around by loading character columns as TEXT.
2. **`DATE_ADD`/`DATE_SUB` won't coerce a string literal to date.** `DATE_ADD(CURDATE(), ...)`
   works but `DATE_ADD('2026-05-01', INTERVAL -1 MONTH)` → `ComputeNoSuchOverload`. Also
   `CAST('2026-05-01' AS DATE)` → `ComputeNoSuchOverload`. Only `DATE '2026-05-01'` (typed
   literal) works. MySQL/StarRocks auto-coerce string→date here. Worked around with typed
   DATE literals; ideally add string→date coercion for the temporal builtins (and fix
   `CAST(... AS DATE)` from a string).
3. **🔴 Selecting from a CTE whose body is a `UNION ALL` hard-crashes the server.** Minimal
   repro: `WITH u AS (SELECT id FROM division UNION ALL SELECT id FROM division) SELECT * FROM u;`
   The same UNION as a top-level query works. This is the current blocker (CTE #9).
   **Crash bug — highest priority.**
4. **`CREATE DATABASE IF NOT EXISTS` unsupported** (`SqlExpectedIdent`). `CREATE TABLE IF NOT
   EXISTS` *is* supported. Add `IF NOT EXISTS` to CREATE DATABASE (and check CREATE SCHEMA).
5. **`UNION ALL` of several `COUNT(*)` aggregate selects** → `UnsupportedQueryShape`
   (used a per-table count loop instead). Minor.

## Files in testSQL/
- `rollforward_1000054_full_inline_cte.sql` — original (StarRocks dialect, user-provided).
- `rollforward_1000054_thindb.sql` — transformed (nested-WITH, vars inlined).
- `rollforward_1000054_thindb_flat.sql` — fully flat 38-CTE stack.
- `_transform.py`, `_flatten_and_probe.py` — the transform + probe tooling.
- `schema/_starrocks_schemas.txt` — source DDLs.
- `_load.log`, `_run_full.*`, `_wayroll_srv.*` — run logs (scratch).

## NEXT (morning)
1. Confirm reload finished with TEXT columns (counts match table above).
2. `py testSQL/_flatten_and_probe.py --probe` → find next unsupported construct.
3. Fix/work around each (note as thinDB gaps), advancing through all 38 CTEs to the final SELECT.
