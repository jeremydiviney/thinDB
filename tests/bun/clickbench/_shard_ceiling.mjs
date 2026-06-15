import mysql from "mysql2/promise";
import { readFileSync } from "node:fs";
const N=12;
const dist=readFileSync("../../../bench/_counterid_dist.tsv","utf8").split("\n").map(l=>l.trim()).filter(Boolean)
  .map(l=>{const[a,b]=l.split("\t");return{cid:+a,c:+b};}).sort((a,b)=>a.cid-b.cid);
const total=dist.reduce((s,r)=>s+r.c,0), target=total/N;
const bounds=[dist[0].cid]; let acc=0;
for(let j=0;j<dist.length;j++){acc+=dist[j].c; if(acc>=target&&bounds.length<N&&j+1<dist.length){bounds.push(dist[j+1].cid);acc=0;}}
const ranges=bounds.map((lo,i)=>({lo,hi:i+1<bounds.length?bounds[i+1]:null}));
for(const r of ranges)r.rows=dist.filter(d=>d.cid>=r.lo&&(r.hi===null||d.cid<r.hi)).reduce((s,d)=>s+d.c,0);
console.log("shards:",ranges.length,"rows(M):",ranges.map(r=>(r.rows/1e6).toFixed(1)).join(" "));
const BASE="SELECT CounterID, AVG(length(URL)) l, COUNT(*) c FROM hits WHERE URL <> ''";
const TAIL="GROUP BY CounterID HAVING COUNT(*)>100000 ORDER BY l DESC LIMIT 25";
const c=await mysql.createConnection({host:"127.0.0.1",port:7880,user:"root",password:"",database:"clickbench_fsst__public"});
const times=[];
for(const r of ranges){let p=`CounterID >= ${r.lo}`; if(r.hi!==null)p+=` AND CounterID < ${r.hi}`;
  const sql=`${BASE} AND ${p} ${TAIL}`; await c.query(sql); // warm
  let t=performance.now(); await c.query(sql); times.push(performance.now()-t);}
times.sort((a,b)=>a-b);
const sum=times.reduce((s,x)=>s+x,0),max=Math.max(...times);
console.log("per-shard DOP1 ms:",times.map(x=>x.toFixed(0)).join(" "));
console.log(`sum=${sum.toFixed(0)}ms (total work)  max=${max.toFixed(0)}ms (parallel-12 ceiling)`);
console.log(`silo DOP12 monolithic baseline ~920ms  =>  range-sep ceiling ${max.toFixed(0)}ms = ${(920/max).toFixed(2)}x vs silo`);
await c.end();
