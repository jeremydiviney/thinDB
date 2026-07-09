// #139 repro variant: kill the CLIENT PROCESS (taskkill /F) mid-bulk-upsert,
// matching the original incident (node _bulk_load.mjs was taskkilled while
// 2000-row batches were in flight against a many-segment PK table). Also kills
// one client mid-SELECT-stream so the server is writing to a dead socket.
// Parent = supervisor; child mode = flood worker.
// Usage: node _disc_repro2.mjs [port] [rounds]   (child: node _disc_repro2.mjs child <port>)
import mysql from "mysql2/promise";
import { spawn, execSync } from "child_process";
import { fileURLToPath } from "url";

const SELF = fileURLToPath(import.meta.url);

if (process.argv[2] === "child") {
  const port = parseInt(process.argv[3], 10);
  const c = await mysql.createConnection({ host: "127.0.0.1", port, user: "root", password: "" });
  // upsert flood: SAME key range every time → heavy upsert resolution + tombstones
  for (let i = 0; ; i++) {
    const base = (i % 5) * 2000;
    const vals = [];
    for (let k = 0; k < 2000; k++) vals.push(`(${base + k},${i},'p-${i}-${k}-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy')`);
    await c.query("INSERT INTO d139b (id,a,b) VALUES " + vals.join(","));
    if (i % 3 === 2) c.query("SELECT * FROM d139b").catch(() => {}); // streaming read in flight
  }
}

const PORT = parseInt(process.argv[2] || "13399", 10);
const ROUNDS = parseInt(process.argv[3] || "8", 10);
const admin = await mysql.createConnection({ host: "127.0.0.1", port: PORT, user: "root", password: "" });
try { await admin.query("DROP TABLE d139b"); } catch {}
await admin.query("CREATE TABLE d139b (id BIGINT NOT NULL, a INT, b STRING, PRIMARY KEY (id))");

for (let round = 0; round < ROUNDS; round++) {
  const child = spawn(process.execPath, [SELF, "child", String(PORT)], { stdio: "ignore" });
  await new Promise((r) => setTimeout(r, 400 + Math.floor(Math.random() * 800)));
  try { execSync(`taskkill /PID ${child.pid} /F`, { stdio: "ignore" }); } catch {}
  await new Promise((r) => setTimeout(r, 300));
  try {
    const [[r]] = await admin.query("SELECT COUNT(*) n FROM d139b");
    console.log(`round ${round}: server alive, rows=${r.n}`);
  } catch (e) {
    console.log(`round ${round}: SERVER UNREACHABLE: ${e.message}`);
    process.exit(1);
  }
}
await admin.query("INSERT INTO d139b (id,a,b) VALUES (77777777,1,'post')");
console.log("PASS: server survived process-kill mid-upsert x" + ROUNDS);
await admin.end();
