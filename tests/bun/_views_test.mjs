// Views + materialized views (manual REFRESH) over the mysql wire. Needs a
// server on :7955 with a WRITABLE scratch data dir:
//   ./zig-out/bin/thindb-server.exe --data-dir .views-db --mysql-port 7955 \
//     --pg-port 0 --native-port 0 --max-dop 4
import mysql from "mysql2/promise";
const c = await mysql.createConnection({ host: "127.0.0.1", port: 7955, user: "root", password: "", database: "main" });
let failed = 0;

const q = async (label, sql, expectErr = null) => {
  try {
    const [r] = await c.query({ sql, timeout: 60000 });
    if (expectErr) { console.log(`${label}: FAIL (expected ${expectErr}, got OK)`); failed++; }
    else console.log(`${label}: OK`);
    return r;
  } catch (e) {
    const msg = e.sqlMessage || e.code || e.message;
    if (expectErr && msg.includes(expectErr)) console.log(`${label}: OK (${msg})`);
    else { console.log(`${label}: FAIL (${msg})`); failed++; }
    return null;
  }
};

const rows = async (label, sql, want) => {
  const r = await q(label + "-run", sql);
  if (!r) { failed++; return; }
  const got = JSON.stringify(r.map((x) => Object.values(x)));
  if (got === JSON.stringify(want)) console.log(`${label}: OK`);
  else { console.log(`${label}: FAIL (want ${JSON.stringify(want)}, got ${got})`); failed++; }
};

await q("drop-t", "DROP TABLE IF EXISTS sales");
await q("create-t", "CREATE TABLE sales (id INT, region STRING, amt INT, PRIMARY KEY (id))");
await q("insert", "INSERT INTO sales (id, region, amt) VALUES (1,'east',10),(2,'west',20),(3,'east',30),(4,'west',5)");

// ---- Plain view ----
await q("drop-view-pre", "DROP VIEW IF EXISTS east_sales");
await q("create-view", "CREATE VIEW east_sales AS SELECT id, amt FROM sales WHERE region = 'east'");
await rows("view-query", "SELECT id, amt FROM east_sales ORDER BY id", [[1, 10], [3, 30]]);
// A view reflects base-table changes live.
await q("insert2", "INSERT INTO sales (id, region, amt) VALUES (5,'east',100)");
await rows("view-live", "SELECT id, amt FROM east_sales ORDER BY id", [[1, 10], [3, 30], [5, 100]]);
// View composes in a larger query (join / aggregate over it).
await rows("view-agg", "SELECT SUM(amt) FROM east_sales", [[140]]);
// OR REPLACE redefines.
await q("replace-view", "CREATE OR REPLACE VIEW east_sales AS SELECT id, amt FROM sales WHERE region = 'east' AND amt > 20");
await rows("view-replaced", "SELECT id FROM east_sales ORDER BY id", [[3], [5]]);
await q("dup-view", "CREATE VIEW east_sales AS SELECT id FROM sales", "exist");

// ---- Materialized view (manual refresh) ----
await q("drop-mv-pre", "DROP MATERIALIZED VIEW IF EXISTS region_totals");
await q("create-mv", "CREATE MATERIALIZED VIEW region_totals AS SELECT region, SUM(amt) AS total FROM sales GROUP BY region");
await rows("mv-query", "SELECT region, total FROM region_totals ORDER BY region", [["east", 140], ["west", 25]]);
// A materialized view is a SNAPSHOT — base changes are NOT visible until REFRESH.
await q("insert3", "INSERT INTO sales (id, region, amt) VALUES (6,'west',1000)");
await rows("mv-stale", "SELECT region, total FROM region_totals ORDER BY region", [["east", 140], ["west", 25]]);
await q("refresh", "REFRESH MATERIALIZED VIEW region_totals");
await rows("mv-refreshed", "SELECT region, total FROM region_totals ORDER BY region", [["east", 140], ["west", 1025]]);
// A materialized view scans like a table (it IS one) — filters/aggregates work.
await rows("mv-filter", "SELECT total FROM region_totals WHERE region = 'west'", [[1025]]);

// ---- Errors / drops ----
await q("refresh-plain", "REFRESH MATERIALIZED VIEW east_sales", "UnsupportedOp");
await q("drop-view", "DROP VIEW east_sales");
await q("view-gone", "SELECT * FROM east_sales", "not"); // dropped → no longer resolvable
await q("drop-mv", "DROP MATERIALIZED VIEW region_totals");
await q("mv-gone", "SELECT * FROM region_totals", "not");
await q("drop-missing", "DROP VIEW nope", "not");
await q("drop-missing-ok", "DROP VIEW IF EXISTS nope");

await q("cleanup", "DROP TABLE IF EXISTS sales");

console.log(failed === 0 ? "\nALL VIEW TESTS PASSED" : `\n${failed} VIEW TEST(S) FAILED`);
await c.end();
process.exit(failed === 0 ? 0 : 1);
