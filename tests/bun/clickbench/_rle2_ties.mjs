// Secondary check for the DIFF queries from _rle2_verify: for ORDER BY+LIMIT
// tie-prone queries, compare the SORT-KEY value multiset (tie order is
// engine-nondeterministic; the key multiset is not). Q03/Q05 printed raw.
import mysql from "mysql2/promise";
import { readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const DUCKDB = "C:/Users/jerem/AppData/Local/Microsoft/WinGet/Packages/DuckDB.cli_Microsoft.Winget.Source_8wekyb3d8bbwe/duckdb.exe";
const DUCK_DB = resolve(here, "../../../bench/clickbench/duckdb/hits_full_v2.duckdb");
const DB = process.env.THINDB_DB ?? "clickbench_rle__public";
const queries = readFileSync(resolve(here, "../../../bench/clickbench/queries.sql"), "utf8")
  .split("\n").map((l) => l.trim()).filter((l) => l.length > 0 && !l.startsWith("--"));

// query idx → column index of the ORDER BY key in the projection
// (-1 = the sort key is EventTime, not projected → compare via boundary SQL)
const SORT_COL = { 16: 2, 17: 2, 18: 3, 30: 2, 31: 2, 32: 2, 38: 1, 39: 5, 40: 2, 41: 2 };

function csvParse(text) {
  const rows = [];
  let row = [], field = "", inQ = false, i = 0;
  while (i < text.length) {
    const c = text[i];
    if (inQ) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i += 2; continue; }
        inQ = false; i++; continue;
      }
      field += c; i++; continue;
    }
    if (c === '"') { inQ = true; i++; continue; }
    if (c === ",") { row.push(field); field = ""; i++; continue; }
    if (c === "\r") { i++; continue; }
    if (c === "\n") { row.push(field); rows.push(row); row = []; field = ""; i++; continue; }
    field += c; i++;
  }
  if (field.length > 0 || row.length > 0) { row.push(field); rows.push(row); }
  return rows;
}

function duckRows(sql) {
  const script = `.mode csv\n.headers off\n${sql}\n`;
  const r = spawnSync(DUCKDB, [DUCK_DB, "-readonly"], { input: script, encoding: "utf8", maxBuffer: 1 << 28 });
  if (r.status !== 0) throw new Error(r.stderr);
  return csvParse(r.stdout);
}

const conn = await mysql.createConnection({
  host: "127.0.0.1", port: 7880, user: "root", password: "", database: DB,
  rowsAsArray: true, bigNumberStrings: true, supportBigNumbers: true, dateStrings: true,
});

for (const [idx, keyCol] of Object.entries(SORT_COL)) {
  const sql = queries[idx];
  const [rows] = await conn.query({ sql, timeout: 120000 });
  const thinKeys = rows.map((r) => String(r[keyCol]).replace(/\.0+$/, "")).sort();
  const duckKeys = duckRows(sql).map((r) => r[keyCol].replace(/\.0+$/, "")).sort();
  const same = thinKeys.length === duckKeys.length && thinKeys.every((x, i) => x === duckKeys[i]);
  console.log(`Q${idx} sort-key multiset ${same ? "MATCH (tie artifact)" : "MISMATCH"}  thin=[${thinKeys}] duck=[${duckKeys}]`);
}

// Q17 has NO ORDER BY (`GROUP BY UserID, SearchPhrase LIMIT 10`) — any 10
// groups are valid. Verify each group thinDB returned has the right count.
{
  const [rows] = await conn.query({ sql: queries[17], timeout: 120000 });
  let ok = 0;
  for (const r of rows) {
    const phrase = String(r[1]).replace(/'/g, "''");
    const vsql = `SELECT COUNT(*) FROM hits WHERE UserID = ${r[0]} AND SearchPhrase = '${phrase}';`;
    const want = duckRows(vsql)[0][0];
    if (String(want) === String(r[2])) ok++;
    else console.log(`Q17 group (${r[0]}, '${r[1]}') thin=${r[2]} duck=${want}`);
  }
  console.log(`Q17 per-group recount: ${ok}/${rows.length} groups exact (no ORDER BY — group choice is free)`);
}

// Q23/Q24 sort on EventTime: compare the top-10 EventTime multiset directly.
for (const probe of [
  { q: 23, where: "URL LIKE '%google%'" },
  { q: 24, where: "SearchPhrase LIKE '%google%'" },
]) {
  const bsql = `SELECT EventTime FROM hits WHERE ${probe.where} ORDER BY EventTime LIMIT 10;`;
  const [trows] = await conn.query({ sql: bsql, timeout: 120000 });
  const thin = trows.map((r) => String(r[0])).sort();
  const duck = duckRows(bsql).map((r) => r[0]).sort();
  const same = thin.length === duck.length && thin.every((x, i) => x === duck[i]);
  console.log(`Q${probe.q} top-10 EventTime multiset ${same ? "MATCH (tie artifact)" : "MISMATCH"}  thin=[${thin[0]}..${thin[9]}] duck=[${duck[0]}..${duck[9]}]`);
}
await conn.end();
