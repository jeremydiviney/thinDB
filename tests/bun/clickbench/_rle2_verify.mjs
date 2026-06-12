// One-off: verify thinDB (RLE phase-2 kernels) result VALUES against DuckDB
// ground truth for all 43 ClickBench queries. Canonicalization: per-value
// (NULL sentinel, float rounding to 6 sig figs, CRLF strip), rows joined with
// \x01, sorted → multiset compare. ORDER BY+LIMIT tie divergence is reported
// for manual inspection, not auto-excused.
import mysql from "mysql2/promise";
import { readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const DUCKDB = "C:/Users/jerem/AppData/Local/Microsoft/WinGet/Packages/DuckDB.cli_Microsoft.Winget.Source_8wekyb3d8bbwe/duckdb.exe";
const DUCK_DB = resolve(here, "../../../bench/clickbench/duckdb/hits_full_v2.duckdb");
const DB = process.env.THINDB_DB ?? "clickbench_rle__public";
const ONLY = process.env.ONLY ? new Set(process.env.ONLY.split(",").map(Number)) : null;

const queries = readFileSync(resolve(here, "../../../bench/clickbench/queries.sql"), "utf8")
  .split("\n").map((l) => l.trim()).filter((l) => l.length > 0 && !l.startsWith("--"));

const NULLS = "␀NULL␀";

function canonVal(v) {
  if (v === null || v === undefined) return NULLS;
  let s = String(v);
  s = s.replace(/\r/g, "");
  // round float-looking values (decimal point or exponent) to 6 sig figs;
  // pure integers (incl. 64-bit IDs) compare as exact strings. After rounding,
  // strip trailing fraction zeros so a whole result compares equal whether an
  // engine printed "1587", "1587.0", or "1.587e3". KNOWN GAP: a double an
  // engine renders as full integer digits ≥17 long (e.g. AVG(UserID) →
  // "2528953029789716000") stays exact-string and won't match the other
  // engine's exponent form — IDs need exact compare, so we don't round those.
  if (/^-?\d+\.\d+(e[+-]?\d+)?$/i.test(s) || /^-?\d+(\.\d+)?e[+-]?\d+$/i.test(s)) {
    const f = Number(s);
    if (Number.isFinite(f)) {
      s = f.toPrecision(6);
      if (s.includes(".") && !/e/i.test(s)) s = s.replace(/\.?0+$/, "");
    }
  } else if (/^-?\d+\.0*$/.test(s)) {
    s = s.replace(/\.0*$/, "");
  }
  return s;
}

function canonRows(rows) {
  return rows.map((r) => r.map(canonVal).join("\x01")).sort();
}

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

function duckRun(sql) {
  const script = `.mode csv\n.nullvalue ${NULLS}\n.headers off\n${sql}\n`;
  const r = spawnSync(DUCKDB, [DUCK_DB, "-readonly"], { input: script, encoding: "utf8", maxBuffer: 1 << 28 });
  if (r.status !== 0) return { err: (r.stderr || "exit " + r.status).slice(0, 200) };
  if (r.stdout.length === 0) return { rows: [] };
  return { rows: csvParse(r.stdout) };
}

const conn = await mysql.createConnection({
  host: "127.0.0.1", port: 7880, user: "root", password: "", database: DB,
  rowsAsArray: true, bigNumberStrings: true, supportBigNumbers: true, dateStrings: true,
});

let pass = 0, fail = 0, errs = 0;
for (let i = 0; i < queries.length; i++) {
  if (ONLY && !ONLY.has(i)) continue;
  const sql = queries[i];
  let thin;
  try {
    const [rows] = await conn.query({ sql, timeout: 120000 });
    thin = canonRows(rows);
  } catch (e) {
    console.log(`Q${String(i).padStart(2, "0")} THINDB-ERR ${e.code ?? e.message}`);
    errs++; continue;
  }
  const d = duckRun(sql);
  if (d.err) {
    console.log(`Q${String(i).padStart(2, "0")} DUCK-ERR ${d.err.replace(/\n/g, " ")}`);
    errs++; continue;
  }
  const duck = canonRows(d.rows);
  if (thin.length === duck.length && thin.every((x, k) => x === duck[k])) {
    console.log(`Q${String(i).padStart(2, "0")} OK     rows=${thin.length}`);
    pass++;
  } else {
    console.log(`Q${String(i).padStart(2, "0")} DIFF   thin=${thin.length} duck=${duck.length}`);
    const tset = new Set(thin), dset = new Set(duck);
    const onlyT = thin.filter((x) => !dset.has(x)).slice(0, 3);
    const onlyD = duck.filter((x) => !tset.has(x)).slice(0, 3);
    for (const r of onlyT) console.log(`    thin-only: ${r.replace(/\x01/g, " | ").slice(0, 220)}`);
    for (const r of onlyD) console.log(`    duck-only: ${r.replace(/\x01/g, " | ").slice(0, 220)}`);
    fail++;
  }
}
console.log(`\nVALUE CHECK: ${pass} ok, ${fail} diff, ${errs} err`);
await conn.end();
