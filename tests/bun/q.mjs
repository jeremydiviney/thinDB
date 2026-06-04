import mysql from "mysql2/promise";
const conn = await mysql.createConnection({
  host: "127.0.0.1",
  port: Number(process.env.PORT ?? 7880),
  database: process.env.THINDB_DB ?? "clickbench_full__public",
});
try {
  const [rows] = await conn.query(process.env.Q);
  console.log(JSON.stringify(rows, null, 1).slice(0, 6000));
} catch (e) {
  console.log("ERROR:", e.code, e.message);
}
await conn.end();
