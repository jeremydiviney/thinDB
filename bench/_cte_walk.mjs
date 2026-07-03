import mysql from "mysql2/promise";
import { readFileSync } from "fs";

// --- WITH-stack flattener (same helpers as _kn_nostage.mjs) ---
function rb(s){let d=1,i=0,inS=false,q="";while(i<s.length){const c=s[i];if(inS){if(c===q)inS=false;i++;continue;}if(c==="'"||c==='"'||c==="`"){inS=true;q=c;i++;continue;}if(c==="(")d++;else if(c===")"){d--;if(d===0)return{body:s.slice(0,i),rest:s.slice(i+1)};}i++;}throw 0;}
function pw(sql){let s=sql.trim();if(!/^WITH\b/i.test(s))return{ctes:[],final:s};s=s.slice(4);const ctes=[];while(true){s=s.replace(/^[\s,]+/,"");const m=/^([a-zA-Z_]\w*)\s+AS\b\s*(?:(?:NOT\s+)?MATERIALIZED\s+)?\(/i.exec(s);if(!m)break;const name=m[1];s=s.slice(m[0].length);const{body,rest}=rb(s);ctes.push({name,body:body.trim()});s=rest.replace(/^\s+/,"");if(s[0]===","){s=s.slice(1);continue;}break;}return{ctes,final:s.trim()};}
function flatten(sql){const{ctes,final}=pw(sql);const flat=[];for(const cte of ctes){if(/^\s*WITH\b/i.test(cte.body)){const sub=flatten(cte.body);for(const f of sub.flat)flat.push(f);flat.push({name:cte.name,body:sub.final});}else flat.push({name:cte.name,body:cte.body});}return{flat,final};}

const sql = readFileSync("testSQL/rollforward_template.sql","utf8")
  .replaceAll("{{PROJECT}}","1000073").replaceAll("{{DIVISION}}","1000339").replace(/;\s*$/,"");
const { flat, final } = flatten(sql);
const N = flat.length;

const K_FROM = Number(process.env.K_FROM || 1);
const K_TO   = Number(process.env.K_TO   || N);
const T_RUNS = Number(process.env.T_RUNS || 3);
const S_RUNS = Number(process.env.S_RUNS || 2);

function prefixDefs(K){
  const tgt = flat[K-1].name;
  const defs = flat.slice(0,K).map(c=>`${c.name} AS (\n${c.body}\n)`).join(",\n");
  return { defs, tgt };
}
async function prefixQuery(conn, K){
  // SELECT * ORDER BY first-3 LIMIT 100: forces every column to be computed
  // (no COUNT(*) projection pruning) while keeping the wire transfer tiny.
  const { defs, tgt } = prefixDefs(K);
  const [,fields] = await conn.query({ sql:`WITH ${defs}\nSELECT * FROM ${tgt} LIMIT 1`, timeout:120000 });
  const ord = fields.slice(0, Math.min(3, fields.length)).map(f=>`\`${f.name}\``).join(", ");
  return { q: `WITH ${defs}\nSELECT * FROM ${tgt} ORDER BY ${ord} LIMIT 100`, tgt };
}
// Real final SELECT over the whole stack; it ends in ORDER BY, so LIMIT 100
// keeps the wire tiny while both engines still evaluate everything.
const fullQuery = sql + "\nLIMIT 100";

const tdb = await mysql.createConnection({ host:"127.0.0.1", port:7950, user:"root", password:"", database:"wayroll", rowsAsArray:true });
let sr = null;
if (process.env.SR_PWD){
  sr = await mysql.createConnection({ host:"64.20.36.26", port:9030, user:"root", password:process.env.SR_PWD, database:"wayroll", rowsAsArray:true });
  await sr.query("SET query_timeout=1200");
}

async function best(conn, q, runs){
  let b=Infinity, n=null;
  try {
    await conn.query({sql:q,timeout:600000}); // warm
    for(let i=0;i<runs;i++){
      const s=performance.now();
      const [r]=await conn.query({sql:q,timeout:600000});
      b=Math.min(b,performance.now()-s);
      n = r.length===1 ? Number(r[0][0]) : r.length;
    }
    return {b,n};
  } catch(e){ return {b:null,n:null,err:e.code||e.sqlMessage||e.message}; }
}

console.log(`${N} CTEs in stack. Walking K=${K_FROM}..${K_TO} then FULL. (thinDB best-of-${T_RUNS}, SR best-of-${S_RUNS})\n`);
console.log(`  K  target                                    thinDB     Δt      SR      Δs    t/s   count`);
let pt=0, ps=0;
const rows=[];
for(let K=K_FROM; K<=K_TO; K++){
  const {q,tgt}=await prefixQuery(tdb,K);
  const t=await best(tdb,q,T_RUNS);
  const s=sr?await best(sr,q,S_RUNS):{b:null,n:null};
  const dt=t.b!=null?t.b-pt:null, ds=s.b!=null?s.b-ps:null;
  if(t.b!=null)pt=t.b; if(s.b!=null)ps=s.b;
  const f=(x,w=7)=>x==null?"—".padStart(w):x.toFixed(0).padStart(w);
  const ratio=(t.b!=null&&s.b!=null)?(t.b/s.b).toFixed(2).padStart(5):"    —";
  const cnt=t.n!=null?t.n:(t.err?("ERR "+t.err):"—");
  console.log(` ${String(K).padStart(2)}  ${tgt.padEnd(40)} ${f(t.b)} ${f(dt,6)} ${f(s.b)} ${f(ds,6)} ${ratio}   ${cnt}${s.err?"  SR-ERR:"+s.err:""}`);
  rows.push({K,tgt,t:t.b,s:s.b});
}
// full query (real final SELECT)
const tf=await best(tdb,fullQuery,T_RUNS);
const sf=sr?await best(sr,fullQuery,S_RUNS):{b:null};
const f=(x,w=7)=>x==null?"—".padStart(w):x.toFixed(0).padStart(w);
console.log(` --  ${"FULL (final SELECT)".padEnd(40)} ${f(tf.b)} ${f(tf.b!=null?tf.b-pt:null,6)} ${f(sf.b)} ${f(sf.b!=null?sf.b-ps:null,6)} ${(tf.b!=null&&sf.b!=null)?(tf.b/sf.b).toFixed(2).padStart(5):"    —"}   rows=${tf.n}${tf.err?" ERR "+tf.err:""}`);

await tdb.end(); if(sr)await sr.end();
