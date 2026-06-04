import mysql from "mysql2/promise";
const c = await mysql.createConnection({host:"127.0.0.1",port:7880,user:"thindb",password:"",database:"clickbench__public",rowsAsArray:true});
const Q="SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10";
let best=1e9,all=[]; for(let i=0;i<11;i++){const t=performance.now();await c.query(Q);const ms=performance.now()-t;all.push(Math.round(ms));if(ms<best)best=ms;}
console.log("SB="+(process.env.SB??"?")+" Q32 best-of-11:",best.toFixed(1)+"ms  runs:",all.join(","));
await c.end();
