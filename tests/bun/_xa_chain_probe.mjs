// Regression probe for the production sink-poisoning bug: a multi-statement
// DML chain inside an XA branch (what Connector/J's DELETE batches look like
// in the Flink exactly-once sink's binlog phase). Broken server: staged-DML
// OKs lack SERVER_MORE_RESULTS_EXISTS + seq increment, so the client stops
// after the first OK and the connection desyncs. Healthy server: all OKs
// chain correctly and the connection stays usable through XA commit.
import mysql from "mysql2/promise";

const PORT = process.argv[2] ? Number(process.argv[2]) : 13399;
const c = await mysql.createConnection({
  host: "127.0.0.1", port: PORT, user: "root", multipleStatements: true,
});
let fail = false;
try {
  await c.query("CREATE DATABASE xaprobe").catch(() => {});
  await c.query("USE xaprobe__public");
  await c.query("DROP TABLE IF EXISTS t");
  await c.query("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
  await c.query("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)");

  await c.query("XA START 'probe1'");
  // The critical shape: chained staged DML. Must return one OK per statement,
  // all but the last flagged more-results.
  await c.query("DELETE FROM t WHERE id = 1; DELETE FROM t WHERE id = 2; UPDATE t SET v = 99 WHERE id = 3");
  await c.query("XA END 'probe1'");
  await c.query("XA PREPARE 'probe1'");
  await c.query("XA COMMIT 'probe1'");

  // Connection must still be healthy and the XA writes applied.
  const [rows] = await c.query("SELECT id, v FROM t ORDER BY id");
  const got = JSON.stringify(rows.map(r => [Number(r.id), Number(r.v)]));
  const want = JSON.stringify([[3, 99], [4, 40]]);
  if (got !== want) { console.log(`FAIL: rows ${got} != ${want}`); fail = true; }
  else console.log("xa chain + commit + verify: ok");

  // Mid-chain error inside XA: ERR must abort the chain, connection stays sane.
  await c.query("XA START 'probe2'");
  try {
    await c.query("DELETE FROM t WHERE id = 4; SELECT * FROM no_such_tbl; DELETE FROM t WHERE id = 3");
    console.log("FAIL: mid-chain error did not surface"); fail = true;
  } catch { /* expected */ }
  await c.query("XA END 'probe2'").catch(() => {});
  await c.query("XA ROLLBACK 'probe2'").catch(() => {});
  const [[alive]] = await c.query("SELECT COUNT(*) n FROM t");
  console.log("post-error connection healthy, count =", Number(alive.n));

  await c.query("DROP DATABASE xaprobe").catch(() => {});
} catch (e) {
  console.log("FAIL:", e.code, e.message?.slice(0, 100)); fail = true;
}
await c.end().catch(() => {});
console.log(fail ? "FAIL" : "PASS");
process.exit(fail ? 1 : 0);
