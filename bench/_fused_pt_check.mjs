import mysql from "mysql2/promise";
const c = await mysql.createConnection({ host:"127.0.0.1", port:7950, user:"root", password:"", database:"wayroll", rowsAsArray:true });
await c.query("DROP TABLE IF EXISTS _pt_dim").catch(()=>{});
// Unique single-key dim (date → rate): forces the FastTable + fused-probe
// lane, which with a LEFT join and unique keys now runs pass-through.
await c.query({ sql: "CREATE TABLE _pt_dim AS SELECT date, MAX(rate) AS rate FROM currency_exchange_rate WHERE currencyTo='EUR' GROUP BY date", timeout: 300000 });
const [dimN] = await c.query("SELECT COUNT(*) FROM _pt_dim");
console.log("dim rows:", Number(dimN[0][0]));

const base = "FROM invoice_import_amortized i {J} JOIN _pt_dim d ON d.date = i.invoiceDate WHERE i.projectId = 1000073";
const [l] = await c.query({ sql: `SELECT COUNT(*), COUNT(d.rate), SUM(d.rate) ${base.replace("{J}","LEFT")}`, timeout: 300000 });
const [inn] = await c.query({ sql: `SELECT COUNT(*), COUNT(d.rate), SUM(d.rate) ${base.replace("{J}","INNER")}`, timeout: 300000 });
const [probeN] = await c.query({ sql: "SELECT COUNT(*) FROM invoice_import_amortized WHERE projectId = 1000073", timeout: 300000 });

const leftTotal = Number(l[0][0]), leftMatched = Number(l[0][1]), leftSum = l[0][2];
const innTotal = Number(inn[0][0]), innMatched = Number(inn[0][1]), innSum = inn[0][2];
console.log(`LEFT  (pass-through): total=${leftTotal} matched=${leftMatched} sum=${leftSum}`);
console.log(`INNER (regular path): total=${innTotal} matched=${innMatched} sum=${innSum}`);
console.log(`probe rows: ${Number(probeN[0][0])}`);
const ok1 = leftTotal === Number(probeN[0][0]);
const ok2 = leftMatched === innTotal && innTotal === innMatched;
const ok3 = String(leftSum) === String(innSum);
console.log(`LEFT total == probe rows:        ${ok1 ? "✓" : "✗ FAIL"}`);
console.log(`LEFT matched == INNER total:     ${ok2 ? "✓" : "✗ FAIL"}`);
console.log(`SUM(rate) LEFT == INNER:         ${ok3 ? "✓" : "✗ FAIL"}`);
await c.query("DROP TABLE _pt_dim");
await c.end();
process.exit(ok1 && ok2 && ok3 ? 0 : 1);
