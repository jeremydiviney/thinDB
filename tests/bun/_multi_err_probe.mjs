// Raw-protocol probe: does a mid-chain statement error poison the connection?
// Sends `SET @a=1; SELECT * FROM no_such_table_x7; SET @b=2` as one
// multi-statement COM_QUERY. Protocol-correct: OK(more_results) then ERR then
// NOTHING (remaining statements aborted). Bug: a third packet (OK for stmt 3)
// arrives after the ERR — leftover bytes that desync the next command.
import net from "net";

const HOST = "127.0.0.1", PORT = process.argv[2] ? Number(process.argv[2]) : 13310;
const sock = net.connect(PORT, HOST);
let buf = Buffer.alloc(0);
const waiters = [];
function drain() {
  while (waiters.length && buf.length >= 4) {
    const len = buf.readUIntLE(0, 3);
    if (buf.length < 4 + len) break;
    const seq = buf[3];
    const payload = buf.subarray(4, 4 + len);
    buf = buf.subarray(4 + len);
    waiters.shift()({ seq, payload });
  }
}
sock.on("data", (d) => { buf = Buffer.concat([buf, d]); drain(); });
const readPacket = (ms = 4000) => new Promise((res, rej) => {
  waiters.push(res);
  setTimeout(() => {
    const i = waiters.indexOf(res);
    if (i >= 0) waiters.splice(i, 1);
    rej(new Error("timeout"));
  }, ms);
  drain();
});
function send(seq, payload) {
  const h = Buffer.alloc(4);
  h.writeUIntLE(payload.length, 0, 3);
  h[3] = seq;
  sock.write(Buffer.concat([h, payload]));
}
const describe = (p) =>
  p.payload[0] === 0xff ? `ERR(${p.payload.subarray(9).toString().slice(0, 60)})` :
  p.payload[0] === 0x00 ? `OK(status=0x${p.payload.readUInt16LE(3).toString(16)})` :
  `OTHER(header=0x${p.payload[0].toString(16)} len=${p.payload.length})`;

const hs = await readPacket();
const CLIENT_PROTOCOL_41 = 0x0200, CLIENT_SECURE_CONNECTION = 0x8000, CLIENT_PLUGIN_AUTH = 0x80000, CLIENT_DEPRECATE_EOF = 0x1000000;
const caps = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_DEPRECATE_EOF;
const user = Buffer.from("root\0");
const plugin = Buffer.from("mysql_native_password\0");
const resp = Buffer.alloc(4 + 4 + 1 + 23 + user.length + 1 + plugin.length);
let o = 0;
resp.writeUInt32LE(caps, o); o += 4;
resp.writeUInt32LE(1 << 24, o); o += 4;
resp[o] = 0x21; o += 1;
o += 23;
user.copy(resp, o); o += user.length;
resp[o] = 0; o += 1;
plugin.copy(resp, o);
send(1, resp);
const auth = await readPacket();
if (auth.payload[0] === 0xff) { console.log("AUTH FAILED"); process.exit(1); }

send(0, Buffer.from([0x1b, 0x00, 0x00]));
const so = await readPacket();
if (so.payload[0] === 0xff) { console.log("SET_OPTION rejected"); process.exit(1); }

console.log("sending chain: SET @a=1; SELECT * FROM no_such_table_x7; SET @b=2");
send(0, Buffer.concat([Buffer.from([0x03]), Buffer.from("SET @a=1; SELECT * FROM no_such_table_x7; SET @b=2")]));
const p1 = await readPacket();
console.log("packet 1:", describe(p1));
const p2 = await readPacket();
console.log("packet 2:", describe(p2));
let leftover = null;
try { leftover = await readPacket(2500); } catch { /* silence = correct */ }
if (leftover) console.log("packet 3 AFTER ERR (BUG — poisons connection):", describe(leftover));
else console.log("no packet after ERR — protocol-correct abort");

// Now issue a fresh single statement and see what the connection returns for it.
send(0, Buffer.concat([Buffer.from([0x03]), Buffer.from("SET @c=3")]));
let healthy = false;
try {
  const nxt = await readPacket(3000);
  console.log("next command's response:", describe(nxt), "seq=" + nxt.seq);
  healthy = nxt.payload[0] === 0x00 && nxt.seq === 1;
} catch {
  console.log("next command: NO RESPONSE (connection wedged)");
}
if (leftover || !healthy) { console.log("FAIL"); process.exit(1); }
console.log("PASS");
process.exit(0);
