import mysql from "mysql2/promise";
import { readFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT ?? "7880");
const DB = process.env.DB ?? "clickbench_full__public";
const RUNS = Number(process.env.RUNS ?? "4");
const IDXS = (process.env.IDXS ?? "29,37,39").split(",").map(Number);

const qs = readFileSync(resolve(here, "../../../bench/clickbench/queries.sql"), "utf8")
  .split("\n").map((l) => l.trim()).filter((l) => l.length > 0 && !l.startsWith("--"));

for (const idx of IDXS) {
  const sql = qs[idx];
  const times = [];
  for (let i = 0; i < RUNS; i++) {
    const c = await mysql.createConnection({ host: "127.0.0.1", port: PORT, user: "root", password: "", database: DB });
    const t0 = performance.now();
    await c.query({ sql, timeout: 120000 });
    times.push(performance.now() - t0);
    await c.end();
  }
  times.sort((a, b) => a - b);
  console.log(`Q${idx}: MIN=${times[0].toFixed(1)}ms  runs=[${times.map((t) => t.toFixed(0)).join(",")}]  [${process.env.LABEL ?? ""}]`);
}
