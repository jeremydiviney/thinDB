// #139 repro: hard-kill the client mid-bulk-INSERT and see if the server dies.
// Floods a PK table with 2000-row text INSERTs (same shape as _bulk_load.mjs /
// the Flink JDBC sink), then destroys the socket without protocol goodbye at a
// random moment while a batch is in flight. Loops until --rounds exhausted.
// Usage: node _disc_repro.mjs [port] [rounds]
import mysqlCb from "mysql2";
import mysql from "mysql2/promise";

const PORT = parseInt(process.argv[2] || "13399", 10);
const ROUNDS = parseInt(process.argv[3] || "10", 10);
const HOST = "127.0.0.1";

const admin = await mysql.createConnection({ host: HOST, port: PORT, user: "root", password: "" });
try { await admin.query("DROP TABLE d139"); } catch {}
await admin.query("CREATE TABLE d139 (id BIGINT NOT NULL, a INT, b STRING, PRIMARY KEY (id))");

function batchSql(base) {
  const vals = [];
  for (let i = 0; i < 2000; i++) vals.push(`(${base + i},${i % 97},'payload-${base + i}-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')`);
  return "INSERT INTO d139 (id,a,b) VALUES " + vals.join(",");
}

for (let round = 0; round < ROUNDS; round++) {
  await new Promise((resolve) => {
    const c = mysqlCb.createConnection({ host: HOST, port: PORT, user: "root", password: "" });
    let sent = 0;
    const base = round * 1_000_000;
    const pump = () => {
      // fire-and-forget: keep several batches in flight so the kill lands mid-execution
      for (let k = 0; k < 4; k++) c.query(batchSql(base + sent++ * 2000), () => {});
      // destroy the socket at a random delay while batches are executing
      setTimeout(() => {
        c.destroy(); // immediate RST-style teardown, no COM_QUIT
        resolve();
      }, 20 + Math.floor(Math.random() * 120));
    };
    c.connect(() => pump());
    c.on("error", () => {});
  });
  // is the server still there?
  try {
    const [[r]] = await admin.query("SELECT COUNT(*) n FROM d139");
    console.log(`round ${round}: server alive, rows=${r.n}`);
  } catch (e) {
    console.log(`round ${round}: SERVER UNREACHABLE: ${e.message}`);
    process.exit(1);
  }
}
// final write-path liveness check: can a fresh connection still insert + flush?
await admin.query("INSERT INTO d139 (id,a,b) VALUES (999999999,1,'post')");
const [[r]] = await admin.query("SELECT COUNT(*) n FROM d139");
console.log(`PASS: server survived ${ROUNDS} mid-batch disconnects, rows=${r.n}`);
await admin.end();
