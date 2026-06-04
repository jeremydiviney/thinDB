import { readFileSync } from "node:fs";
const thin = JSON.parse(readFileSync("rebench_emptystr.json","utf8"));
const prev = JSON.parse(readFileSync("rebench.json","utf8"));
const duck = JSON.parse(readFileSync("../../bench/clickbench/duckdb/_duck_best.json","utf8"));
const pmap = new Map(prev.outcomes.map(o=>[o.idx,o.ms]));
let tThin=0,tDuck=0;
const losses=[];
for (const o of thin.outcomes){ const d=duck[String(o.idx)]; tThin+=o.ms; tDuck+=d; if(o.ms-d>0.5) losses.push({q:o.idx,thin:o.ms,duck:d,delta:o.ms-d,ratio:o.ms/d}); }
losses.sort((a,b)=>b.delta-a.delta);
console.log("=== Remaining LOSSES vs DuckDB ===");
for(const r of losses) console.log(`Q${String(r.q).padStart(2,"0")}  ${r.thin.toFixed(1).padStart(7)}  duck ${String(r.duck).padStart(5)}  +${r.delta.toFixed(1)}  ${r.ratio.toFixed(2)}x`);
console.log(`\nTOTAL thinDB ${tThin.toFixed(0)}ms  duck ${tDuck}ms  (${(tDuck/tThin).toFixed(2)}x)`);
// biggest movers vs previous run
const movers=[];
for(const o of thin.outcomes){ const p=pmap.get(o.idx); if(p!=null && Math.abs(o.ms-p)>=4) movers.push({q:o.idx,prev:p,now:o.ms,d:o.ms-p}); }
movers.sort((a,b)=>a.d-b.d);
console.log("\n=== Movers vs prior run (|Δ|≥4ms) ===");
for(const m of movers) console.log(`Q${String(m.q).padStart(2,"0")}  ${m.prev.toFixed(1)} → ${m.now.toFixed(1)}  (${m.d>=0?"+":""}${m.d.toFixed(1)})`);
