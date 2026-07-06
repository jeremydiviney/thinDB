// LANGUAGE zig lifecycle regression: create/compile/call/dup/bad-source/
// drop over the mysql wire. Needs a server on :7951 with a WRITABLE
// scratch data dir (NOT the wayroll/clickbench DBs):
//   ./zig-out/bin/thindb-server.exe --data-dir .zigfn-db --mysql-port 7951 \
//     --pg-port 0 --native-port 0 --max-dop 4
// and the zig toolchain on PATH (exact server version).
import mysql from "mysql2/promise";
const c = await mysql.createConnection({ host: "127.0.0.1", port: 7951, user: "root", password: "", database: "main" });
let failed = 0;
const q = async (label, sql, expectErr = null, tmo = 180000) => {
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
await q("drop-pre", "DROP FUNCTION IF EXISTS zt_running");
await q("drop-tbl", "DROP TABLE IF EXISTS zt");
await q("create-tbl", "CREATE TABLE zt (id BIGINT, g INT, amt BIGINT, PRIMARY KEY (id))");
await q("insert", "INSERT INTO zt (id, g, amt) VALUES (4,2,40),(1,1,10),(3,1,30),(2,1,20),(5,2,50)");
await q("create-fn", "CREATE FUNCTION zt_running LANGUAGE zig AS $$" + src + "$$");
const rows = await q("call", "SELECT id, running FROM TABLE(zt_running((SELECT id, g, amt FROM zt)) PARTITION BY g ORDER BY id) ORDER BY id");
const got = rows ? rows.map(r => Number(r.running)).join(",") : "";
if (got !== "10,30,60,40,90") { console.log(`values: FAIL (${got})`); failed++; } else console.log("values: OK");
await q("dup", "CREATE FUNCTION zt_running LANGUAGE zig AS $$" + src + "$$", "FunctionAlreadyExists");
await q("bad-src", "CREATE FUNCTION zt_broken LANGUAGE zig AS $$ pub const x = ; $$", "FunctionInvalidDefinition");
await q("drop", "DROP FUNCTION zt_running");
await q("post-drop", "SELECT * FROM TABLE(zt_running((SELECT id, g, amt FROM zt)) PARTITION BY g)", "UnsupportedQueryShape");
await c.end();
console.log(failed === 0 ? "ALL PASS" : `${failed} FAILURES`);
process.exit(failed === 0 ? 0 : 1);
