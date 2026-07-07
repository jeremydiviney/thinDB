import mysql from "mysql2/promise";
const phase = process.argv[2];
const c = await mysql.createConnection({ host: "127.0.0.1", port: 7962, user: "root", password: "", database: "main" });
const q = async (s) => { try { await c.query(s); return "ok"; } catch (e) { return "ERR:" + (e.sqlMessage || e.message); } };
const count = async () => { const [r] = await c.query("SELECT COUNT(*) n FROM xt"); return r[0].n; };

if (phase === "prepare") {
  console.log("create:", await q("CREATE TABLE xt (id INT, v INT, PRIMARY KEY(id))"));
  console.log("start:", await q("XA START 'dx1'"));
  console.log("insert:", await q("INSERT INTO xt (id,v) VALUES (7,700),(8,800)"));
  console.log("end:", await q("XA END 'dx1'"));
  console.log("prepare:", await q("XA PREPARE 'dx1'"));
} else if (phase === "commit") {
  console.log("count before commit (expect 0):", await count());
  console.log("commit after restart:", await q("XA COMMIT 'dx1'"));
  console.log("count after commit (expect 2):", await count());
}
await c.end();
