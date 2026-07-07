// Isolate the two sides of the Flink pipeline:
//   read  = how fast MySQL streams 1M rows out (source ceiling)
//   write = how fast thinDB ingests 1M rows via 1000-row multi-row INSERTs
//           (what Flink's sink sends with rewriteBatchedStatements), 1 conn
//           and 2 conns (matches Flink parallelism 2).
import mysql from "mysql2/promise";

const N = 1_000_000;

// ---- READ: stream all rows out of MySQL, count them ----
{
  const c = await mysql.createConnection({ host: "127.0.0.1", port: 13307, user: "root", password: "root", database: "src" });
  const t0 = Date.now();
  let n = 0;
  await new Promise((resolve, reject) => {
    const q = c.connection.query("SELECT id,a,b,c,d,e FROM events");
    q.on("result", () => { n++; });
    q.on("end", resolve);
    q.on("error", reject);
  });
  const s = (Date.now() - t0) / 1000;
  console.log(`READ  MySQL: ${n} rows in ${s.toFixed(1)}s = ${Math.round(n / s)} rows/s`);
  await c.end();
}

// ---- WRITE: multi-row INSERT batches into thinDB ----
async function writeBench(conns) {
  const pool = [];
  for (let i = 0; i < conns; i++) pool.push(await mysql.createConnection({ host: "127.0.0.1", port: 13306, user: "root", password: "", database: "main" }));
  await pool[0].query("DROP TABLE IF EXISTS wbench");
  await pool[0].query("CREATE TABLE wbench (id INT, a INT, b BIGINT, c STRING, d DOUBLE, e STRING, PRIMARY KEY(id))");
  const words = ["alpha", "bravo", "charlie", "delta"];
  const BATCH = 1000;
  const batches = [];
  for (let base = 1; base <= N; base += BATCH) {
    const rows = [];
    for (let i = base; i <= Math.min(base + BATCH - 1, N); i++) rows.push([i, i % 100000, i * 1000, words[i % 4] + i, i / 7, words[i % 4]]);
    batches.push(rows);
  }
  const t0 = Date.now();
  let next = 0;
  await Promise.all(pool.map(async (conn) => {
    while (true) {
      const idx = next++;
      if (idx >= batches.length) break;
      await conn.query("INSERT INTO wbench (id,a,b,c,d,e) VALUES ?", [batches[idx]]);
    }
  }));
  const s = (Date.now() - t0) / 1000;
  console.log(`WRITE thinDB (${conns} conn): ${N} rows in ${s.toFixed(1)}s = ${Math.round(N / s)} rows/s`);
  for (const c of pool) await c.end();
}

await writeBench(1);
await writeBench(2);
