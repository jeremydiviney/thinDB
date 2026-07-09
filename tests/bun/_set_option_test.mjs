// Raw-protocol regression test for COM_SET_OPTION (0x1B) + multi-statement
// COM_QUERY — replicates what Connector/J's executePreparedBatchAsMultiStatement
// does for the Flink JDBC sink's DELETE batches.
import net from "net";

const HOST = "127.0.0.1", PORT = 13310;
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
const readPacket = () => new Promise((res, rej) => {
  waiters.push(res);
  setTimeout(() => rej(new Error("timeout waiting for packet")), 8000);
  drain();
});
function send(seq, payload) {
  const h = Buffer.alloc(4);
  h.writeUIntLE(payload.length, 0, 3);
  h[3] = seq;
  sock.write(Buffer.concat([h, payload]));
}

// 1. handshake init from server
const hs = await readPacket();
console.log("handshake: protocol", hs.payload[0], "server", hs.payload.subarray(1, hs.payload.indexOf(0, 1)).toString());

// 2. HandshakeResponse41: root, empty password (empty auth response)
const CLIENT_PROTOCOL_41 = 0x0200, CLIENT_SECURE_CONNECTION = 0x8000, CLIENT_PLUGIN_AUTH = 0x80000, CLIENT_DEPRECATE_EOF = 0x1000000;
const caps = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_DEPRECATE_EOF;
const user = Buffer.from("root\0");
const plugin = Buffer.from("mysql_native_password\0");
const resp = Buffer.alloc(4 + 4 + 1 + 23 + user.length + 1 + plugin.length);
let o = 0;
resp.writeUInt32LE(caps, o); o += 4;
resp.writeUInt32LE(1 << 24, o); o += 4; // max packet
resp[o] = 0x21; o += 1; // charset utf8
o += 23; // zero filler
user.copy(resp, o); o += user.length;
resp[o] = 0; o += 1; // lenenc auth len = 0 (empty password)
plugin.copy(resp, o);
send(1, resp);
const auth = await readPacket();
if (auth.payload[0] === 0xff) { console.log("AUTH FAILED:", auth.payload.subarray(9).toString()); process.exit(1); }
console.log("auth ok (header 0x%s)", auth.payload[0].toString(16));

// 3. COM_SET_OPTION MULTI_STATEMENTS_ON (option = 0)
send(0, Buffer.from([0x1b, 0x00, 0x00]));
const so = await readPacket();
if (so.payload[0] === 0xff) {
  console.log("COM_SET_OPTION REJECTED:", so.payload.subarray(9).toString());
  process.exit(1);
}
console.log("COM_SET_OPTION ok, response header 0x%s len %d", so.payload[0].toString(16), so.payload.length);

// 4. multi-statement COM_QUERY — two OK-returning statements
send(0, Buffer.concat([Buffer.from([0x03]), Buffer.from("SET @a=1; SET @b=2")]));
const r1 = await readPacket();
const MORE = 0x0008;
if (r1.payload[0] === 0xff) { console.log("MULTI-STMT FAILED:", r1.payload.subarray(9).toString()); process.exit(1); }
// OK packet: [0x00][affected lenenc][insertid lenenc][status u16]...
const st1 = r1.payload.readUInt16LE(3);
console.log("stmt1 ok, status 0x%s more_results=%s", st1.toString(16), !!(st1 & MORE));
const r2 = await readPacket();
if (r2.payload[0] === 0xff) { console.log("MULTI-STMT #2 FAILED:", r2.payload.subarray(9).toString()); process.exit(1); }
const st2 = r2.payload.readUInt16LE(3);
console.log("stmt2 ok, status 0x%s more_results=%s", st2.toString(16), !!(st2 & MORE));

// 5. COM_SET_OPTION MULTI_STATEMENTS_OFF (option = 1) — Connector/J toggles back
send(0, Buffer.from([0x1b, 0x01, 0x00]));
const off = await readPacket();
if (off.payload[0] === 0xff) { console.log("SET_OPTION OFF REJECTED"); process.exit(1); }
console.log("COM_SET_OPTION OFF ok");

if (!(st1 & MORE) || (st2 & MORE)) { console.log("FAIL: MORE_RESULTS flag chain wrong"); process.exit(1); }
console.log("PASS");
process.exit(0);
