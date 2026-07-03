import mysql from "mysql2/promise";
import { readFileSync } from "fs";

// Final SELECT ends in ORDER BY → LIMIT keeps the wire (and node's heap)
// tiny while both engines still evaluate the whole stack.
const q = readFileSync("testSQL/rollforward_template.sql", "utf8")
  .replaceAll("{{PROJECT}}", "1000073")
  .replaceAll("{{DIVISION}}", "1000339")
  .replace(/;\s*$/, "") + "\nLIMIT 100";

const RUNS = Number(process.env.RUNS || 5);

async function bench(name, cfg, runs) {
  const c = await mysql.createConnection(cfg);
  if (name === "StarRocks") await c.query("SET query_timeout=1200");
  let best = Infinity, rows = 0, all = [];
  try {
    await c.query({ sql: q, timeout: 600000 }); // warm
    for (let i = 0; i < runs; i++) {
      const s = performance.now();
      const [r] = await c.query({ sql: q, timeout: 600000 });
      const ms = performance.now() - s;
      all.push(ms); best = Math.min(best, ms); rows = r.length;
    }
  } catch (e) {
    console.log(`${name.padEnd(10)} ERR ${e.code || e.sqlMessage || e.message}`);
    await c.end();
    return null;
  }
  await c.end();
  console.log(`${name.padEnd(10)} best ${best.toFixed(0)}ms   runs [${all.map(x => x.toFixed(0)).join(", ")}]   rows=${rows}`);
  return best;
}

const t = await bench("thinDB", { host: "127.0.0.1", port: 7950, user: "root", password: "", database: "wayroll", rowsAsArray: true }, RUNS);
let s = null;
if (process.env.SR_PWD) {
  s = await bench("StarRocks", { host: "64.20.36.26", port: 9030, user: "root", password: process.env.SR_PWD, database: "wayroll", rowsAsArray: true }, Math.min(RUNS, 4));
}
if (t != null && s != null) console.log(`\nthinDB / StarRocks = ${(t / s).toFixed(2)}x`);
