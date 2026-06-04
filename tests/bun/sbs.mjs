import { readFileSync } from "node:fs";
const thin = JSON.parse(readFileSync("cur.json","utf8")).outcomes;
const duck = JSON.parse(readFileSync("../../bench/clickbench/duckdb/_duck_best.json","utf8"));
const tm = new Map(thin.map(o=>[o.idx,o.ms]));
let tT=0,tD=0,win=0,loss=0,tie=0;
const rows=[];
for(let i=0;i<43;i++){const t=tm.get(i),d=duck[String(i)];tT+=t;tD+=d;
  const delta=t-d; let mark; if(delta<-0.5){mark='win ';win++;}else if(delta>0.5){mark='LOSS';loss++;}else{mark='tie ';tie++;}
  rows.push(`Q${String(i).padStart(2,'0')}  ${t.toFixed(1).padStart(8)}  ${String(d).padStart(6)}   ${(delta>=0?'+':'')+delta.toFixed(1).padStart(7)}  ${(t/d).toFixed(2)}x  ${mark}`);}
console.log("Qxx      thinDB    duck     delta   ratio");
console.log(rows.join("\n"));
console.log("─".repeat(52));
console.log(`TOTAL  ${tT.toFixed(0).padStart(7)}ms  ${tD}ms   thinDB ${(tD/tT).toFixed(2)}x faster   (win ${win} / tie ${tie} / loss ${loss})`);
const losses=thin.map(o=>({q:o.idx,t:o.ms,d:duck[String(o.idx)],x:o.ms-duck[String(o.idx)]})).filter(r=>r.x>0.5).sort((a,b)=>b.x-a.x);
console.log("Losses (thinDB slower):", losses.map(r=>`Q${r.q} +${r.x.toFixed(0)}ms (${(r.t/r.d).toFixed(2)}x)`).join(", "));
