import mysql from "mysql2/promise";
const c = await mysql.createConnection({host:"127.0.0.1",port:7880,user:"thindb",password:"",database:"clickbench__public",rowsAsArray:true});
const Q08="SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10";
let best=1e9,all=[]; for(let i=0;i<11;i++){const t=performance.now();await c.query(Q08);const ms=performance.now()-t;all.push(ms);if(ms<best)best=ms;}
console.log("Q08 best-of-11:", best.toFixed(1)+"ms", " runs:", all.map(x=>x.toFixed(0)).join(","));
await c.end();
