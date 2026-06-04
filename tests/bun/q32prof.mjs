import mysql from "mysql2/promise";
const c = await mysql.createConnection({host:"127.0.0.1",port:7880,user:"thindb",password:"",database:"clickbench__public",rowsAsArray:true});
const Q="SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10";
for(let i=0;i<5;i++){const t=performance.now();await c.query(Q);console.log("run",i,(performance.now()-t).toFixed(1)+"ms");}
await c.end();
