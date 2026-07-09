// Packet-logging TCP proxy for the MySQL wire: listens on 13311, forwards to
// 13310, prints every packet header (dir, seq, len, first byte) plus a text
// preview of client COM_QUERY payloads. Used to capture exactly what
// Connector/J sends for a failing multi-statement batch and what the server
// answers.
import net from "net";

const LISTEN = 13311, TARGET = 13310;
let connId = 0;

function packetScanner(tag, previewQueries) {
  let buf = Buffer.alloc(0);
  return (chunk) => {
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 4) {
      const len = buf.readUIntLE(0, 3);
      if (buf.length < 4 + len) break;
      const seq = buf[3];
      const head = len > 0 ? buf[4] : -1;
      let extra = "";
      if (previewQueries && head === 0x03) {
        extra = " q=" + JSON.stringify(buf.subarray(5, Math.min(4 + len, 120)).toString());
      } else if (head === 0xff) {
        extra = " ERR " + JSON.stringify(buf.subarray(13, Math.min(4 + len, 80)).toString());
      } else if (head === 0x00 && len >= 7) {
        extra = ` OK status=0x${buf.readUInt16LE(4 + 3).toString(16)}`;
      }
      if (len <= 16) extra += " hex=" + buf.subarray(4, 4 + len).toString("hex");
      console.log(`${tag} seq=${seq} len=${len} first=0x${head.toString(16)}${extra}`);
      buf = buf.subarray(4 + len);
    }
  };
}

net.createServer((cli) => {
  const id = ++connId;
  const srv = net.connect(TARGET, "127.0.0.1");
  const c2s = packetScanner(`[${id}] C>S`, true);
  const s2c = packetScanner(`[${id}] S>C`, false);
  cli.on("data", (d) => { c2s(d); srv.write(d); });
  srv.on("data", (d) => { s2c(d); cli.write(d); });
  cli.on("close", () => srv.destroy());
  srv.on("close", () => cli.destroy());
  cli.on("error", () => {});
  srv.on("error", () => {});
}).listen(LISTEN, "0.0.0.0", () => console.log(`mitm ${LISTEN} -> ${TARGET}`));
