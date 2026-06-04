import mysql from "mysql2/promise";
import { readFileSync, writeFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
const here = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT ?? "7880");
const DB = process.env.DB ?? "clickbench_full__public";
const OUT = process.env.OUT ?? "/tmp/res.json";
// GROUP BY queries that exercise count/sum/min/max combinable aggregates.
const IDXS = (process.env.IDXS ?? "7,12,14,15,30,31,33,34,36,38,40,41,42").split(",").map(Number);
const qs = readFileSync(resolve(here, "../../../bench/clickbench/queries.sql"), "utf8")
  .split("\n").map((l) => l.trim()).filter((l) => l.length > 0 && !l.startsWith("--"));
const c = await mysql.createConnection({ host: "127.0.0.1", port: PORT, user: "root", password: "", database: DB, rowsAsArray: true });
const out = {};
for (const idx of IDXS) {
  try {
    const [rows] = await c.query({ sql: qs[idx], timeout: 120000 });
    // canonicalize: stringify each row, sort, so partition/emit order doesn't matter
    const lines = rows.map((r) => r.map((v) => (v === null ? "∅" : String(v))).join("")).sort();
    out["Q" + idx] = { n: rows.length, h: lines.join("") };
  } catch (e) { out["Q" + idx] = { err: e.sqlMessage ?? e.message }; }
}
writeFileSync(OUT, JSON.stringify(Object.fromEntries(Object.entries(out).map(([k, v]) => [k, v.err ? v : { n: v.n, hash: hashStr(v.h) }])), null, 1));
console.log("wrote", OUT, Object.keys(out).length, "queries");
function hashStr(s) { let h = 0n; for (let i = 0; i < s.length; i++) { h = (h * 1099511628211n + BigInt(s.charCodeAt(i))) & 0xffffffffffffffffn; } return h.toString(16); }
await c.end();
