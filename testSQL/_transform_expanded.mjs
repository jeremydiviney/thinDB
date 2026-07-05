// Transform a StarRocks temp-table-chain rollforward dump into one big CTE
// query runnable on thinDB. Drops DROP/SET/CREATE-temp/INSERT-target
// scaffolding, converts each `CREATE TEMPORARY TABLE X AS <body>;` into
// `X AS (<body>)`, converts external_plan_ids_temp into a VALUES-style CTE,
// inlines @vars, swaps project 1000051 -> 1000073 with real AirDNA plan ids.
//   bun _transform_expanded.mjs <src.sql> <out.sql>
import { readFileSync, writeFileSync } from "fs";
const [SRC, OUT, PLANARG] = process.argv.slice(2);
const PLANS = (PLANARG || "price_1Pv5ANGd5jE924o7k2YHiLZ4,market-monthly-usd-3995,market-monthly-usd-1995").split(",");

let txt = readFileSync(SRC, "utf8");

const VARS = {
  "@targetCurrency": "'USD'", "@childCustomer": "0", "@isCrossDivision": "TRUE",
  "@revenueModel": "'wayroll-net-mrr'", "@currentDate": "'2026-07-04'",
  "@curMonth": "'2026-07-01'", "@isLeapYear": "FALSE", "@projectId": "1000073",
  "@hashStart": "NULL", "@hashEnd": "NULL", "@modelType": "'mrr'",
  "@includeEstimates": "0", "@comparisonMonths": "1",
};
for (const name of Object.keys(VARS).sort((a, b) => b.length - a.length)) {
  txt = txt.split(name).join(VARS[name]);
}

const epCte = "external_plan_ids_temp AS (\n" +
  PLANS.map(p => `  SELECT '${p}' AS externalPlanId`).join("\n  UNION ALL\n") + "\n)";

// Each CTAS body's only ';' is its terminator, so [^;]* captures exactly it.
const ctas = /CREATE TEMPORARY TABLE (\w+)\s*(?:\([^)]*\)\s*)?(?:DISTRIBUTED[^\n]*\n)?(?:PROPERTIES\s*\([^)]*\)\s*)?AS\s*([^;]*);/g;
const members = [];
let m;
while ((m = ctas.exec(txt)) !== null) {
  const name = m[1];
  let body = m[2].replace(/\s+$/, "");
  if (name === "external_plan_ids_temp") continue;
  members.push(`${name} AS (\n${body}\n)`);
}

// final: INSERT INTO <target> (cols) WITH <members> SELECT ...
const fin = txt.match(/INSERT INTO \w+\s*\([^)]*\)\s*WITH\s+([\s\S]*)$/);
const finalWith = fin[1].trim();

let out = "WITH " + [epCte, ...members].join(",\n") + ",\n" + finalWith + "\n";
// Optional single-division shrink (env DIV): narrow the base scans so the
// monolithic-CTE peak memory fits. Applied identically on both engines, so
// the value comparison stays valid. Only the literal base-WHERE projectId
// filters match (join conditions are `x.projectId = y.projectId`).
if (process.env.DIV) {
  out = out.split("projectId = 1000073").join(`projectId = 1000073 AND divisionId = ${process.env.DIV}`);
}
// Optional SEPARABLE BY on the tail CTE (rollforward_filtered_result). Its
// ORDER BY line is unique, so we splice the clause right after it, inside
// the CTE body. Whole-chain per-customer slicing via closure detection.
if (process.env.SEP) {
  const anchor = "ORDER BY customerNumber, date, upDown";
  if (!out.includes(anchor)) throw new Error("SEP anchor not found");
  out = out.replace(anchor, anchor + `\n  SEPARABLE BY (${process.env.SEP})`);
}
writeFileSync(OUT, out);
console.log(`wrote ${OUT}: ${members.length} CTAS members + external CTE + final WITH`);
console.log("members:", members.map(x => x.split(" AS")[0]).join(", "));
