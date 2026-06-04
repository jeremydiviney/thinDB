import mysql from "mysql2/promise";
const c = await mysql.createConnection({host:"127.0.0.1",port:7880,user:"thindb",password:"",database:"clickbench__public",rowsAsArray:true});
const q = async (s) => (await c.query(s))[0];
// Q08: RegionID, COUNT(DISTINCT UserID) — 64-bit value → 96-bit tier. Known ref: region 229 → 171027.
const q08 = await q("SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 3");
console.log("Q08 top3:", q08.map(r=>r.join('=')).join('  '), "(exp top 229=171027)");
// Q13: SearchPhrase, COUNT(DISTINCT UserID)
const q13 = await q("SELECT SearchEngineID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY SearchEngineID ORDER BY u DESC LIMIT 1");
console.log("Q-distinct by SearchEngineID top:", q13[0].join('='));
// narrow-value tier: COUNT(DISTINCT AdvEngineID) [small int] grouped by RegionID — exercises a small tier
const narrow = await q("SELECT RegionID, COUNT(DISTINCT AdvEngineID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 1");
console.log("narrow-tier COUNT(DISTINCT AdvEngineID) by region top:", narrow[0].join('='));
// cross-check that one against a filtered global distinct for the same region
const region = Number(narrow[0][0]);
const ref = Number((await q(`SELECT COUNT(DISTINCT AdvEngineID) FROM hits WHERE RegionID = ${region}`))[0][0]);
console.log(`  region ${region}: grouped=${narrow[0][1]} filtered=${ref} match? ${Number(narrow[0][1])===ref}`);
await c.end();
