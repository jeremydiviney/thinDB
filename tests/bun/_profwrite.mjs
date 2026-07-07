// Single-connection multi-row-INSERT writer for profiling the insert path.
import mysql from "mysql2/promise";
const N = Number(process.argv[2] ?? "200000");
const c = await mysql.createConnection({ host: "127.0.0.1", port: 13306, user: "root", password: "", database: "main" });
await c.query("DROP TABLE IF EXISTS pw");
await c.query("CREATE TABLE pw (id INT, a INT, b BIGINT, c STRING, d DOUBLE, e STRING, PRIMARY KEY(id))");
const words = ["alpha", "bravo", "charlie", "delta"];
const BATCH = 1000;
const t0 = Date.now();
for (let base = 1; base <= N; base += BATCH) {
  const rows = [];
  for (let i = base; i <= Math.min(base + BATCH - 1, N); i++) rows.push([i, i % 100000, i * 1000, words[i % 4] + i, i / 7, words[i % 4]]);
  await c.query("INSERT INTO pw (id,a,b,c,d,e) VALUES ?", [rows]);
}
const s = (Date.now() - t0) / 1000;
console.log(`wrote ${N} rows in ${s.toFixed(1)}s = ${Math.round(N / s)} rows/s`);
await c.end();
