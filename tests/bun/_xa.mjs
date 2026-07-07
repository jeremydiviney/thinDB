import mysql from "mysql2/promise";
const c = await mysql.createConnection({ host:"127.0.0.1", port:7961, user:"root", password:"", database:"main" });
const q = async (s) => { try { await c.query(s); return "ok"; } catch(e){ return "ERR:"+(e.sqlMessage||e.message);} };
const count = async () => { const [r]=await c.query("SELECT COUNT(*) n FROM xt"); return r[0].n; };
console.log("create:", await q("CREATE TABLE xt (id INT, v INT, PRIMARY KEY(id))"));
// --- committed branch ---
console.log("start x1:", await q("XA START 'x1'"));
console.log("insert(staged):", await q("INSERT INTO xt (id,v) VALUES (1,10),(2,20)"));
console.log("count during branch (expect 0):", await count());
console.log("end x1:", await q("XA END 'x1'"));
console.log("prepare x1:", await q("XA PREPARE 'x1'"));
console.log("commit x1:", await q("XA COMMIT 'x1'"));
console.log("count after commit (expect 2):", await count());
console.log("commit x1 again (idempotent):", await q("XA COMMIT 'x1'"));
console.log("count still 2:", await count());
// --- rolled-back branch ---
console.log("start x2:", await q("XA START 'x2'"));
console.log("insert(staged):", await q("INSERT INTO xt (id,v) VALUES (3,30)"));
console.log("end x2:", await q("XA END 'x2'"));
console.log("prepare x2:", await q("XA PREPARE 'x2'"));
console.log("rollback x2:", await q("XA ROLLBACK 'x2'"));
console.log("count after rollback (expect 2):", await count());
await c.end();
