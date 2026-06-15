import mysql from "mysql2/promise";
const Q="SELECT CounterID, AVG(length(URL)) l, COUNT(*) c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*)>100000 ORDER BY l DESC LIMIT 25";
const mk=()=>mysql.createConnection({host:"127.0.0.1",port:7880,user:"root",password:"",database:"clickbench_fsst__public"});
const c1=await mk(),c2=await mk(),c3=await mk(),c4=await mk();
await c1.query(Q); // warm
let t=performance.now(); await c1.query(Q); console.log("1 query        :",(performance.now()-t).toFixed(0),"ms");
t=performance.now(); await Promise.all([c1.query(Q),c2.query(Q)]); console.log("2 concurrent   :",(performance.now()-t).toFixed(0),"ms");
t=performance.now(); await Promise.all([c1.query(Q),c2.query(Q),c3.query(Q),c4.query(Q)]); console.log("4 concurrent   :",(performance.now()-t).toFixed(0),"ms");
await c1.end();await c2.end();await c3.end();await c4.end();
