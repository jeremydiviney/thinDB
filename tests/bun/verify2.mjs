import mysql from "mysql2/promise";
const c = await mysql.createConnection({host:"127.0.0.1",port:7880,user:"thindb",password:"",database:"clickbench__public",rowsAsArray:true});
const q = async (sql) => (await c.query(sql))[0];
const region = (await q("SELECT RegionID, COUNT(*) AS c FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 1"))[0][0];
const grouped = (await q(`SELECT RegionID, COUNT(DISTINCT SearchPhrase) AS d FROM hits GROUP BY RegionID HAVING RegionID = ${region}`))[0][0][1];
const filtered = (await q(`SELECT COUNT(DISTINCT SearchPhrase) FROM hits WHERE RegionID = ${region}`))[0][0];
console.log(`Grouped string-distinct, region ${region}: grouped=${grouped} filtered=${filtered} match? ${grouped===filtered}`);
// also a region likely with NO empties skew sanity: smallest region
const r2 = (await q("SELECT RegionID, COUNT(*) AS c FROM hits GROUP BY RegionID ORDER BY c ASC LIMIT 1"))[0][0];
const g2 = (await q(`SELECT RegionID, COUNT(DISTINCT SearchPhrase) AS d FROM hits GROUP BY RegionID HAVING RegionID = ${r2}`))[0][0][1];
const f2 = (await q(`SELECT COUNT(DISTINCT SearchPhrase) FROM hits WHERE RegionID = ${r2}`))[0][0];
console.log(`Grouped string-distinct, region ${r2}: grouped=${g2} filtered=${f2} match? ${g2===f2}`);
await c.end();
