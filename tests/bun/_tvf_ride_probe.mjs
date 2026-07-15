// Diagnostic driver for the TVF ordering seam (task #177): runs the captured
// wayroll cross-rollforward SQL against the BENCH server (:13312) in two
// variants — inline chain vs rf_updown_chain TVF (textual surgery matching
// wayroll's zig emission) — so server-side THINDB_TRACE_OPSCAN /
// THINDB_TVF_TRACE / --profile-ops output attributes the difference.
// Usage: node _tvf_ride_probe.mjs [inline|tvf|both]
import { readFileSync } from "fs";
import mysql from "mysql2/promise";

const LOG = "C:/development/wayroll-api/slowCTELogs/slow-cte-2026-07-15T01-51-43-458Z.txt";
const PARAMS = {
  targetCurrency: "'USD'",
  childCustomer: "0",
  isCrossDivision: "1",
  revenueModel: "'wayroll-net-mrr'",
  currentDate: "'2026-07-14'",
  curMonth: "'2026-07-01'",
  isLeapYear: "0",
  projectId: "1000073",
  hashStart: "NULL",
  hashEnd: "NULL",
  modelType: "'mrr'",
  includeEstimates: "1",
  comparisonMonths: "1",
};

const KERNEL_COLS = ["projectId", "divisionId", "customerNumberLC", "month", "minDate", "amount", "originalAmount", "exchangeRate", "planId"];
const CARRY_COLS = [
  "customerNumber", "customerName", "customerEmail", "customerNumberHash", "parentCustomerNumber",
  "parentCustomerName", "date", "nonRecurringAmount", "originalNonRecurringAmount", "currency",
  "integrationConfigId", "hasAdjustment",
];
const EXPANDED = [
  "childAddedToParentCount", "childRemovedFromParentCount", "childAddedPlanCount", "childRemovedPlanCount",
  "childUpCount", "childDownCount", "childAddedToParentAmount", "childRemovedFromParentAmount",
  "childAddedPlanAmount", "childRemovedPlanAmount", "childUpAmount", "childDownAmount",
  "crossSellCount", "crossChurnCount", "crossSellAmount", "crossChurnAmount",
];
const OUT_COLS = [
  "projectId", "divisionId", "customerNumber", "customerNumberLC", "customerName", "customerEmail",
  "customerNumberHash", "parentCustomerNumber", "parentCustomerName", "date", "minDate", "month",
  "amount", "originalAmount", "nonRecurringAmount", "originalNonRecurringAmount", "currency",
  "integrationConfigId", "exchangeRate", "planId", "hasAdjustment",
  "lastAmount", "lastOriginalAmount", "lastPlanId", "lastExchangeRate",
  ...EXPANDED,
  "diffAmount", "fxChange", "customerStartDate", "upDown", "activeChange", "isActive",
];

function tvfBlock(uid, inputCte, hasExpanded) {
  const inputCols = [
    ...KERNEL_COLS,
    ...CARRY_COLS,
    ...EXPANDED.map(f => (hasExpanded ? `CAST(${f} AS BIGINT) AS ${f}` : `CAST(0 AS BIGINT) AS ${f}`)),
  ].join(", ");
  return `, rollforward_with_active_status_${uid} AS (\nSELECT\n  ${OUT_COLS.join(",\n  ")}\nFROM TABLE(rf_updown_chain((SELECT ${inputCols} FROM ${inputCte}), 1) PARTITION BY projectId, divisionId, customerNumberLC ORDER BY month)\n)\n`;
}

function surgery(sql, uid, inputCte, hasExpanded) {
  const start = sql.indexOf(`, rollforward_with_last_amounts_${uid} AS (`);
  const end = sql.indexOf(`, rollforward_with_plan_and_division_${uid} AS (`);
  if (start < 0 || end < 0) throw new Error(`anchors missing for ${uid}`);
  return sql.slice(0, start) + tvfBlock(uid, inputCte, hasExpanded) + sql.slice(end);
}

const raw = readFileSync(LOG, "utf8");
let full = raw.slice(raw.indexOf("Full SQL:") + "Full SQL:".length);
const epilogue = full.indexOf("\nParams:");
if (epilogue >= 0) full = full.slice(0, epilogue);
// The driver substitutes :name everywhere (namedPlaceholders), not just in
// the SET statements — mirror that.
for (const [k, v] of Object.entries(PARAMS)) {
  full = full.replace(new RegExp(`:${k}\\b`, "g"), v);
}
if (/[=(,\s]:[a-zA-Z]/.test(full)) throw new Error("unsubstituted params remain");

const inlineSql = full;
let tvfSql = surgery(full, "db8a08af", "rollforward_customer_records_with_gaps_filled", false);
tvfSql = surgery(tvfSql, "059d2fc6", "cross_division_with_latest_fields", true);

const mode = process.argv[2] ?? "both";
const c = await mysql.createConnection({ host: "127.0.0.1", port: 13312, user: "root", database: "wayroll_prod__public", multipleStatements: false });
async function runVariant(label, sql) {
  const t0 = Date.now();
  let rows = 0;
  const stmts = sql.split(";").map(s => s.trim()).filter(Boolean);
  for (const [i, stmt] of stmts.entries()) {
    try {
      const [r] = await c.query({ sql: stmt, timeout: 300000 });
      if (Array.isArray(r)) rows = r.length;
    } catch (e) {
      console.log(`${label}: stmt ${i}/${stmts.length} FAILED: ${(e.sqlMessage || e.code || e.message).slice(0, 200)}`);
      console.log(`  stmt head: ${stmt.slice(0, 160).replace(/\s+/g, " ")}`);
      return -1;
    }
  }
  console.log(`${label}: ${rows} rows in ${Date.now() - t0}ms`);
  return rows;
}
let a = -1;
let b = -1;
if (mode !== "tvf") a = await runVariant("inline", inlineSql);
if (mode !== "inline") b = await runVariant("tvf   ", tvfSql);
if (mode === "both") console.log(a === b ? "row counts MATCH" : `ROW COUNT MISMATCH ${a} vs ${b}`);
await c.end();
