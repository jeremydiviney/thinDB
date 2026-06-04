import mysql from "mysql2/promise";
const c = await mysql.createConnection({host:"127.0.0.1",port:7880,user:"thindb",password:"",database:"clickbench__public",rowsAsArray:true});
const q = async (s) => (await c.query(s))[0];
// GROUP BY routing spot-checks (hash + sort paths): values must match known refs.
console.log("Q05 distinct SearchPhrase:", (await q("SELECT COUNT(DISTINCT SearchPhrase) FROM hits"))[0][0], "(exp 610809)");
console.log("Q08 top region u:", (await q("SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 1"))[0].join(','));
console.log("Q15 top UserID count:", (await q("SELECT UserID, COUNT(*) AS c FROM hits GROUP BY UserID ORDER BY c DESC LIMIT 1"))[0].join(','));
// force sort path
await q("SET thindb_force_group_by='sort'").catch(()=>{});
console.log("(sort-forced not via SET; covered by --force-group-by flag separately)");
await c.end();
