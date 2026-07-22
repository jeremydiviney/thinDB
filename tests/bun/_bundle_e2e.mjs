// Release-bundle smoke test: run thindb-server FROM AN EXTRACTED BUNDLE with a
// scrubbed PATH (no zig anywhere) and prove the bundled zig/ toolchain compiles
// a LANGUAGE zig function end-to-end.
//   bun tests/bun/_bundle_e2e.mjs <bundle-dir> [port]
import mysql from "mysql2/promise";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const bundle = process.argv[2];
const port = Number(process.argv[3] || 13399);
if (!bundle) { console.error("usage: bun _bundle_e2e.mjs <bundle-dir> [port]"); process.exit(2); }

const dataDir = mkdtempSync(join(tmpdir(), "thindb-e2e-"));
const exe = join(bundle, process.platform === "win32" ? "thindb-server.exe" : "thindb-server");
const scrubbedPath = process.platform === "win32" ? "C:\\Windows\\System32" : "/usr/bin:/bin";
const env = { ...process.env, PATH: scrubbedPath };
delete env.THINDB_ZIG_PATH;

const srv = spawn(exe, ["--data-dir", dataDir, "--mysql-port", String(port), "--pg-port", "0", "--native-port", "0", "--max-dop", "4"], { env, stdio: ["ignore", "pipe", "pipe"] });
let srvLog = "";
srv.stdout.on("data", (d) => (srvLog += d));
srv.stderr.on("data", (d) => (srvLog += d));

let c = null;
for (let i = 0; i < 50; i++) {
  await new Promise((r) => setTimeout(r, 200));
  try {
    c = await mysql.createConnection({ host: "127.0.0.1", port, user: "root", password: "", database: "main" });
    break;
  } catch { }
}
if (!c) { console.error("server never came up:\n" + srvLog); srv.kill(); process.exit(1); }

let failed = 0;
const q = async (label, sql, expectErr = null, tmo = 300000) => {
  try {
    const [r] = await c.query({ sql, timeout: tmo });
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

const src = `
const tdb = @import("tdb");
pub const spec = tdb.TableFnSpec{ .name = "zt_running", .execution = .partitioned };
pub const Input = struct { id: ?i64, g: ?i32, amt: ?i64 };
pub const Output = struct { id: ?i64, running: i64 };
pub fn process(ctx: *tdb.Ctx, p: tdb.Partition(Input), out: *tdb.Writer(Output)) !void {
    _ = ctx;
    const ids = p.col(.id);
    const amts = p.col(.amt);
    var running: i64 = 0;
    for (0..p.len) |i| {
        running += amts.get(i) orelse 0;
        try out.row(.{ .id = ids.get(i), .running = running });
    }
}
`;

await q("create-tbl", "CREATE TABLE zt (id BIGINT, g INT, amt BIGINT, PRIMARY KEY (id))");
await q("insert", "INSERT INTO zt (id, g, amt) VALUES (4,2,40),(1,1,10),(3,1,30),(2,1,20),(5,2,50)");
await q("create-fn", "CREATE FUNCTION zt_running LANGUAGE zig AS $$" + src + "$$");
const rows = await q("call", "SELECT id, running FROM TABLE(zt_running((SELECT id, g, amt FROM zt)) PARTITION BY g ORDER BY id) ORDER BY id");
const got = rows ? rows.map((r) => Number(r.running)).join(",") : "";
if (got !== "10,30,60,40,90") { console.log(`values: FAIL (${got})`); failed++; } else console.log("values: OK");

await c.end();
srv.kill();
await new Promise((r) => setTimeout(r, 500));
rmSync(dataDir, { recursive: true, force: true });
console.log(failed === 0 ? "BUNDLE-E2E PASS" : `BUNDLE-E2E FAIL (${failed})`);
process.exit(failed === 0 ? 0 : 1);
