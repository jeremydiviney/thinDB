import mysql from "mysql2/promise";

const PORT = Number(process.env.PORT ?? "7880");
const DB = process.env.DB ?? "clickbench_full__public";
const SQL = process.env.SQL ?? "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10";
const RUNS = Number(process.env.RUNS ?? "5");

const c = await mysql.createConnection({ host: "127.0.0.1", port: PORT, user: "root", password: "", database: DB });
const times = [];
for (let i = 0; i < RUNS; i++) {
  const t0 = performance.now();
  const [rows] = await c.query(SQL);
  const ms = performance.now() - t0;
  times.push(ms);
  process.stdout.write(`run ${i}: ${ms.toFixed(1)} ms  (rows=${rows.length})\n`);
}
times.sort((a, b) => a - b);
console.log(`MIN=${times[0].toFixed(1)} MED=${times[(RUNS / 2) | 0].toFixed(1)} ms  [${process.env.LABEL ?? ""}]`);
await c.end();
