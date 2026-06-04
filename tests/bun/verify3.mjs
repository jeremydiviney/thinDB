import mysql from "mysql2/promise";
const c = await mysql.createConnection({host:"127.0.0.1",port:7880,user:"thindb",password:"",database:"clickbench__public",rowsAsArray:true});
const q = async (sql) => (await c.query(sql))[0];
const rows = await q("SELECT RegionID, COUNT(DISTINCT SearchPhrase) AS d FROM hits GROUP BY RegionID");
const map = new Map(rows.map(r => [Number(r[0]), Number(r[1])]));
console.log("grouped rows:", rows.length);
for (const region of [229, 109120]) {
  const filtered = Number((await q(`SELECT COUNT(DISTINCT SearchPhrase) FROM hits WHERE RegionID = ${region}`))[0][0]);
  console.log(`region ${region}: grouped=${map.get(region)} filtered=${filtered} match? ${map.get(region)===filtered}`);
}
await c.end();
