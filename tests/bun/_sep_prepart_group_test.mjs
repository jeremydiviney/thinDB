// Regression: a GROUP-BY-rooted multi-ref block as a pre-partition candidate
// crashed the server (use-after-free of the compile-phase arena: prePartition
// compiled candidates against the captured ctx.input whose allocator died at
// compile end; simple select chains masked it by never touching node_arena).
// Runs the shape both ways and compares row count + SUM. Needs the wayroll DB
// server on :7950 started with THINDB_SEP_PREPART_NONSIMPLE=1 to exercise the
// group-rooted candidate path (without it the block is still exercised as a
// per-slice private compile — also previously crashy under the dead arena).
import mysql from "mysql2/promise";
const base = `
WITH g AS ( SELECT customerNumberHash, month, SUM(amount) AS amt FROM report_customer_revenue_rollforward WHERE projectId = 1000073 GROUP BY customerNumberHash, month ),
a AS ( SELECT customerNumberHash, month, amt FROM g WHERE amt >= 0 ),
b AS ( SELECT customerNumberHash, month, amt FROM g WHERE amt < 0 ),
t AS ( SELECT customerNumberHash, month, amt FROM a UNION ALL SELECT customerNumberHash, month, amt FROM b ORDER BY customerNumberHash, month __SEP__ )
SELECT COUNT(*) AS n, SUM(amt) AS s FROM t`;
const c = await mysql.createConnection({ host:"127.0.0.1", port:7950, user:"root", password:"", database:"wayroll" });
const out = {};
for (const [label, sep] of [["nosep",""],["sep","SEPARABLE BY (customerNumberHash)"]]) {
  try { const [r] = await c.query({sql: base.replace("__SEP__", sep), timeout:180000});
    out[label] = `${r[0].n}|${Number(r[0].s).toFixed(2)}`;
    console.log(`${label}: ${out[label]}`); }
  catch(e){ console.log(`${label}: ERR`, e.sqlMessage||e.code||e.message); process.exit(1); }
}
await c.end();
if (out.nosep !== out.sep) { console.log("MISMATCH"); process.exit(1); }
console.log("MATCH");
