import mysql from "mysql2/promise";
import { readFileSync } from "fs";
function rb(s){let d=1,i=0,inS=false,q="";while(i<s.length){const c=s[i];if(inS){if(c===q)inS=false;i++;continue;}if(c==="'"||c==='"'||c==="`"){inS=true;q=c;i++;continue;}if(c==="(")d++;else if(c===")"){d--;if(d===0)return{body:s.slice(0,i),rest:s.slice(i+1)};}i++;}throw 0;}
function pw(sql){let s=sql.trim();if(!/^WITH\b/i.test(s))return{ctes:[],final:s};s=s.slice(4);const ctes=[];while(true){s=s.replace(/^[\s,]+/,"");const m=/^([a-zA-Z_]\w*)\s+AS\b\s*(?:(?:NOT\s+)?MATERIALIZED\s+)?\(/i.exec(s);if(!m)break;const name=m[1];s=s.slice(m[0].length);const{body,rest}=rb(s);ctes.push({name,body:body.trim()});s=rest.replace(/^\s+/,"");if(s[0]===","){s=s.slice(1);continue;}break;}return{ctes,final:s.trim()};}
function flatten(sql){const{ctes,final}=pw(sql);const flat=[];for(const cte of ctes){if(/^\s*WITH\b/i.test(cte.body)){const sub=flatten(cte.body);for(const f of sub.flat)flat.push(f);flat.push({name:cte.name,body:sub.final});}else flat.push({name:cte.name,body:cte.body});}return{flat,final};}
const sql=readFileSync("testSQL/rollforward_template.sql","utf8").replaceAll("{{PROJECT}}","1000073").replaceAll("{{DIVISION}}","1000339").replace(/;\s*$/,"");
const {flat}=flatten(sql);
const K=Number(process.env.K||"37");
const RUNS=Number(process.env.RUNS||"1");
const tgt=flat[K-1].name;
const defs=flat.slice(0,K).map(c=>`${c.name} AS (\n${c.body}\n)`).join(",\n");
const c=await mysql.createConnection({host:"127.0.0.1",port:7950,user:"root",password:"",database:"wayroll",rowsAsArray:true});
const[,fields]=await c.query({sql:`WITH ${defs}\nSELECT * FROM ${tgt} LIMIT 1`,timeout:120000});
const ord=fields.slice(0,Math.min(3,fields.length)).map(f=>`\`${f.name}\``).join(", ");
const q=`WITH ${defs}\nSELECT * FROM ${tgt} ORDER BY ${ord} LIMIT 100`;
for(let i=0;i<RUNS;i++){
  const s=performance.now();
  const[r]=await c.query({sql:q,timeout:600000});
  console.log(`K=${K} ${tgt}: ${(performance.now()-s).toFixed(0)}ms rows=${r.length}`);
}
await c.end();
