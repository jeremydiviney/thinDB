import mysql from "mysql2/promise";
const c = await mysql.createConnection({host:"127.0.0.1",port:7880,user:"thindb",password:"",database:"clickbench__public",rowsAsArray:true});
async function timeq(sql, n=7){let best=1e9; for(let i=0;i<n;i++){const t=performance.now();await c.query(sql);const ms=performance.now()-t; if(ms<best)best=ms;} return best;}
const Q08   = "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10";
const MAXAGG= "SELECT RegionID, MAX(UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10"; // same scan+group+sort, no distinct set
const CNT   = "SELECT RegionID, COUNT(*) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10";   // group+sort, no UserID scan, no distinct
const Q04   = "SELECT COUNT(DISTINCT UserID) FROM hits";                                                // global distinct, 8-byte set, no gid pack
const q08=await timeq(Q08), mx=await timeq(MAXAGG), cnt=await timeq(CNT), q04=await timeq(Q04);
console.log("Q08 full  (grp + COUNT DISTINCT UserID):", q08.toFixed(1)+"ms");
console.log("MAX(UserID) (grp + scan UserID, no distinct):", mx.toFixed(1)+"ms");
console.log("COUNT(*)    (grp + sort, no UserID scan):", cnt.toFixed(1)+"ms");
console.log("Q04 global  COUNT(DISTINCT UserID) (8-byte set):", q04.toFixed(1)+"ms");
console.log("---");
console.log("combined-distinct insert overhead  = Q08 - MAX =", (q08-mx).toFixed(1)+"ms  (the (gid,UserID) packed-key set vs a trivial agg)");
console.log("UserID column scan cost            = MAX - COUNT(*) =", (mx-cnt).toFixed(1)+"ms");
console.log("group-by RegionID + sort + emit    = COUNT(*) =", cnt.toFixed(1)+"ms (3238 groups, cache-resident)");
await c.end();
