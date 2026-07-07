// Seed the MySQL source (:13307) with N rows of 5 random fields, and create the
// matching thinDB target table (:13306).
import mysql from "mysql2/promise";

const N = Number(process.argv[2] ?? "1000000");
const src = await mysql.createConnection({ host: "127.0.0.1", port: 13307, user: "root", password: "root", database: "src" });
const thin = await mysql.createConnection({ host: "127.0.0.1", port: 13306, user: "root", password: "", database: "main" });

await src.query("DROP TABLE IF EXISTS events");
await src.query("CREATE TABLE events (id INT PRIMARY KEY, a INT, b BIGINT, c VARCHAR(32), d DOUBLE, e VARCHAR(16))");
await thin.query("DROP TABLE IF EXISTS events");
await thin.query("CREATE TABLE events (id INT, a INT, b BIGINT, c STRING, d DOUBLE, e STRING, PRIMARY KEY(id))");

const words = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"];
const t0 = Date.now();
const BATCH = 2000;
for (let base = 1; base <= N; base += BATCH) {
  const rows = [];
  const hi = Math.min(base + BATCH - 1, N);
  for (let i = base; i <= hi; i++) {
    rows.push([i, (i * 2654435761) % 100000, i * 1000 + (i % 777), words[i % words.length] + "-" + (i % 9973), (i % 10000) / 7.0, words[(i * 3) % words.length]]);
  }
  await src.query("INSERT INTO events (id,a,b,c,d,e) VALUES ?", [rows]);
  if (base % 100000 === 1) process.stdout.write(`  ${hi}/${N}\r`);
}
const secs = (Date.now() - t0) / 1000;
const [cnt] = await src.query("SELECT COUNT(*) n FROM events");
console.log(`\nseeded ${cnt[0].n} rows into MySQL src.events in ${secs.toFixed(1)}s; thinDB main.events created (empty)`);
await src.end();
await thin.end();
