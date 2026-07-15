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
const src2 = src.replace("running += amts.get(i) orelse 0;", "running += (amts.get(i) orelse 0) * 2;");
await q("replace", "CREATE OR REPLACE FUNCTION zt_running LANGUAGE zig AS $$" + src2 + "$$");
const rows2 = await q("call-replaced", "SELECT running FROM TABLE(zt_running((SELECT id, g, amt FROM zt)) PARTITION BY g ORDER BY id) ORDER BY running");
const got2 = rows2 ? rows2.map(r => Number(r.running)).join(",") : "";
if (got2 !== "20,60,80,120,180") { console.log(`replaced-values: FAIL (${got2})`); failed++; } else console.log("replaced-values: OK");
// P3: two-input LANGUAGE zig function over the wire (ABI v2 multi-table).
const src3 = `
const tdb = @import("tdb");
pub const spec = tdb.TableFnSpec{ .name = "zt_pair", .execution = .partitioned };
pub const Input = struct { g: ?i32, amt: ?i64 };
pub const Input2 = struct { g: ?i32, amt: ?i64 };
pub const Output = struct { g: ?i32, a_sum: i64, b_sum: i64 };
pub fn process(ctx: *tdb.Ctx, a: tdb.Partition(Input), b: tdb.Partition(Input2), out: *tdb.Writer(Output)) !void {
    _ = ctx;
    var sa: i64 = 0;
    var sb: i64 = 0;
    const aa = a.col(.amt);
    const ba = b.col(.amt);
    for (0..a.len) |i| sa += aa.get(i) orelse 0;
    for (0..b.len) |i| sb += ba.get(i) orelse 0;
    const g = if (a.len > 0) a.col(.g).get(0) else b.col(.g).get(0);
    try out.row(.{ .g = g, .a_sum = sa, .b_sum = sb });
}
`;
await q("multi-create", "CREATE OR REPLACE FUNCTION zt_pair LANGUAGE zig AS $$" + src3 + "$$");
const rows4 = await q("multi-call",
  "SELECT g, a_sum, b_sum FROM TABLE(zt_pair((SELECT g, amt FROM zt), (SELECT g, amt FROM zt WHERE g = 1)) PARTITION BY g) ORDER BY g");
const got4 = rows4 ? rows4.map(r => `${r.g}:${r.a_sum}/${r.b_sum}`).join(",") : "";
if (got4 !== "1:60/60,2:90/0") { console.log(`multi-values: FAIL (${got4})`); failed++; } else console.log("multi-values: OK");
await q("multi-drop", "DROP FUNCTION zt_pair");
// P4: scalar arguments.
const src4 = `
const tdb = @import("tdb");
pub const spec = tdb.TableFnSpec{ .name = "zt_scaled", .execution = .partitioned };
pub const Args = struct { mult: i64, label: []const u8 };
pub const Input = struct { g: ?i32, amt: ?i64 };
pub const Output = struct { g: ?i32, scaled: i64, tag: []const u8 };
pub fn process(ctx: *tdb.Ctx, args: Args, p: tdb.Partition(Input), out: *tdb.Writer(Output)) !void {
    _ = ctx;
    var sum: i64 = 0;
    const amts = p.col(.amt);
    for (0..p.len) |i| sum += amts.get(i) orelse 0;
    try out.row(.{ .g = p.col(.g).get(0), .scaled = sum * args.mult, .tag = args.label });
}
`;
await q("args-create", "CREATE OR REPLACE FUNCTION zt_scaled LANGUAGE zig AS $$" + src4 + "$$");
const rows5 = await q("args-call",
  "SELECT g, scaled, tag FROM TABLE(zt_scaled((SELECT g, amt FROM zt), 3, 'x2') PARTITION BY g) ORDER BY g");
const got5 = rows5 ? rows5.map(r => `${r.g}:${r.scaled}:${r.tag}`).join(",") : "";
if (got5 !== "1:180:x2,2:270:x2") { console.log(`args-values: FAIL (${got5})`); failed++; } else console.log("args-values: OK");
await q("args-badcount", "SELECT * FROM TABLE(zt_scaled((SELECT g, amt FROM zt), 3) PARTITION BY g)", "TableFnInputMismatch");
await q("args-drop", "DROP FUNCTION zt_scaled");
await q("drop", "DROP FUNCTION zt_running");
await q("post-drop", "SELECT * FROM TABLE(zt_running((SELECT id, g, amt FROM zt)) PARTITION BY g)", "UnsupportedQueryShape");
// DLL tier: register the library the compile tier just built, directly.
const { readdirSync } = await import("fs");
const scratch = "../../.zigfn-db/_zigfn_build/main_zt_running";
const { statSync } = await import("fs");
const dlls = readdirSync(scratch).filter(f => f.endsWith(".dll"))
  .sort((a, b) => statSync(scratch + "/" + a).mtimeMs - statSync(scratch + "/" + b).mtimeMs);
const dll = scratch + "/" + dlls[dlls.length - 1];
await q("dll-create", `CREATE FUNCTION zt_running LANGUAGE zig USING '${dll}'`);
const rows3 = await q("dll-call", "SELECT running FROM TABLE(zt_running((SELECT id, g, amt FROM zt)) PARTITION BY g ORDER BY id) ORDER BY running");
const got3 = rows3 ? rows3.map(r => Number(r.running)).join(",") : "";
if (got3 !== "20,60,80,120,180") { console.log(`dll-values: FAIL (${got3})`); failed++; } else console.log("dll-values: OK");
await q("dll-drop", "DROP FUNCTION zt_running");
// Pass-through tier: narrow kernel (Input) + carried columns (Carry) with
// operator-filled pass-through outputs; row_aligned enforced by the
// create-time validation subprocess.
const src_pass = `
const tdb = @import("tdb");
pub const spec = tdb.TableFnSpec{ .name = "zt_pass", .execution = .partitioned, .row_aligned = true };
pub const Input = struct { id: ?i64, g: ?i32, amt: ?i64 };
pub const Carry = struct { note: ?[]const u8 };
pub const passthrough = .{ "id", "g", "note" };
pub const Output = struct { id: ?i64, g: ?i32, note: ?[]const u8, running: i64 };
pub const Computed = struct { running: i64 };
pub fn process(ctx: *tdb.Ctx, p: tdb.Partition(Input), out: *tdb.Writer(Computed)) !void {
    _ = ctx;
    const amts = p.col(.amt);
    var running: i64 = 0;
    for (0..p.len) |i| {
        running += amts.get(i) orelse 0;
        try out.row(.{ .running = running });
    }
}
`;
await q("pass-create", "CREATE OR REPLACE FUNCTION zt_pass LANGUAGE zig AS $$" + src_pass + "$$");
const rows6 = await q("pass-call",
  "SELECT id, g, note, running FROM TABLE(zt_pass((SELECT id, g, amt, CASE WHEN g = 1 THEN 'one' END AS note FROM zt)) PARTITION BY g ORDER BY id) ORDER BY id");
const got6 = rows6 ? rows6.map(r => `${r.id}:${r.g}:${r.note}:${r.running}`).join(",") : "";
if (got6 !== "1:1:one:10,2:1:one:30,3:1:one:60,4:2:null:40,5:2:null:90") { console.log(`pass-values: FAIL (${got6})`); failed++; } else console.log("pass-values: OK");
await q("pass-drop", "DROP FUNCTION zt_pass");
const src_misaligned = src_pass
  .replace('.name = "zt_pass"', '.name = "zt_bad_pass"')
  .replace("for (0..p.len) |i| {", "for (0..p.len - 1) |i| {");
await q("pass-misaligned",
  "CREATE OR REPLACE FUNCTION zt_bad_pass LANGUAGE zig AS $$" + src_misaligned + "$$",
  "FunctionInvalidDefinition");
await c.end();
console.log(failed === 0 ? "ALL PASS" : `${failed} FAILURES`);
process.exit(failed === 0 ? 0 : 1);
