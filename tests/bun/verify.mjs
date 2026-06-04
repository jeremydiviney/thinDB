import mysql from "mysql2/promise";
const c = await mysql.createConnection({host:"127.0.0.1",port:7880,user:"thindb",password:"",database:"clickbench__public",rowsAsArray:true});
const q = async (sql) => (await c.query(sql))[0];
const v1 = (await q("SELECT COUNT(DISTINCT SearchPhrase) FROM hits"))[0][0];
const vEmpty = (await q("SELECT COUNT(DISTINCT SearchPhrase) FROM hits WHERE SearchPhrase = ''"))[0][0];
const vNon = (await q("SELECT COUNT(DISTINCT SearchPhrase) FROM hits WHERE SearchPhrase <> ''"))[0][0];
console.log("Q05 global COUNT(DISTINCT SearchPhrase):", v1, "(expected 610809)");
console.log("  WHERE SearchPhrase='' :", vEmpty, "(expected 1 — empty counted once)");
console.log("  WHERE SearchPhrase<>'':", vNon, "(expected 610808)");
console.log("  decomposition 1+nonEmpty =", vEmpty + vNon, "matches global?", (vEmpty+vNon)===v1);
// Grouped string-distinct path: pick the busiest region, compare grouped-with-HAVING vs filtered.
const region = (await q("SELECT RegionID FROM hits GROUP BY RegionID ORDER BY COUNT(*) DESC LIMIT 1"))[0][0];
const grouped = (await q(`SELECT RegionID, COUNT(DISTINCT SearchPhrase) AS d FROM hits GROUP BY RegionID HAVING RegionID = ${region}`))[0][0][1];
const filtered = (await q(`SELECT COUNT(DISTINCT SearchPhrase) FROM hits WHERE RegionID = ${region}`))[0][0];
console.log(`Grouped path region ${region}: grouped=${grouped} filtered=${filtered} match? ${grouped===filtered}`);
await c.end();
