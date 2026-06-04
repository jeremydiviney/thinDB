import mysql from "mysql2/promise";
import { createHash } from "node:crypto";
const c = await mysql.createConnection({host:"127.0.0.1",port:7880,user:"thindb",password:"",database:"clickbench__public",rowsAsArray:true});
const Q = {
  Q16: "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10",
  Q17: "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10",
  Q32: "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
  Q33: "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10",
};
const out = [];
for (const [name, sql] of Object.entries(Q)) {
  let best = 1e9, sum = "";
  for (let i = 0; i < 7; i++) {
    const t = performance.now();
    const [rows] = await c.query(sql);
    const ms = performance.now() - t;
    if (ms < best) best = ms;
    if (i === 0) sum = JSON.stringify(rows);
  }
  const cks = createHash("md5").update(sum).digest("hex").slice(0, 8);
  out.push(`${name} ${best.toFixed(1)}ms cks=${cks}`);
}
console.log("BATCH=" + (process.env.SB ?? "?") + "  " + out.join("  |  "));
await c.end();
