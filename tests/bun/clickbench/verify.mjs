// Correctness probe: for each ClickBench query, EXPLAIN it (flag whether the
// RadixLeaseAggregate is in the plan) and hash its full result set (order-
// independent). Run once with THINDB_LEASE_AGG on and once off, then diff the
// emitted `idx hash` lines — any difference is a correctness regression.
import mysql from "mysql2/promise";
import { readFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";

const here = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.THINDB_MYSQL_PORT ?? "7880");
const DB = process.env.THINDB_DB ?? "clickbench__public";
const queries = readFileSync(resolve(here, "../../../bench/clickbench/queries.sql"), "utf8")
  .split("\n").map((l) => l.trim()).filter((l) => l.length > 0 && !l.startsWith("--"));

function hashRows(rows) {
  const lines = rows.map((r) => JSON.stringify(Object.values(r)));
  lines.sort();
  return createHash("md5").update(lines.join("\n")).digest("hex").slice(0, 12);
}

const conn = await mysql.createConnection({ host: "127.0.0.1", port: PORT, database: DB });
for (let i = 0; i < queries.length; i++) {
  const sql = queries[i];
  let lease = false;
  try {
    const [ex] = await conn.query({ sql: "EXPLAIN " + sql, timeout: 120000 });
    lease = JSON.stringify(ex).includes("RadixLeaseAggregate");
  } catch {}
  let tag = "ok", h = "-", n = 0;
  try {
    const [rows] = await conn.query({ sql, timeout: 120000 });
    n = Array.isArray(rows) ? rows.length : 0;
    h = hashRows(Array.isArray(rows) ? rows : []);
  } catch (e) {
    tag = "ERR:" + (e.code ?? e.message);
  }
  console.log(`Q${String(i + 1).padStart(2, "0")} ${lease ? "LEASE" : "     "} rows=${String(n).padStart(3)} ${h} ${tag}`);
}
await conn.end();
