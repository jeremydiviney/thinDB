import mysql from "mysql2/promise";
import { readFileSync } from "fs";
function rb(s){let d=1,i=0,inS=false,q="";while(i<s.length){const c=s[i];if(inS){if(c===q)inS=false;i++;continue;}if(c==="'"||c==='"'||c==="`"){inS=true;q=c;i++;continue;}if(c==="(")d++;else if(c===")"){d--;if(d===0)return{body:s.slice(0,i),rest:s.slice(i+1)};}i++;}throw 0;}
function pw(sql){let s=sql.trim();if(!/^WITH\b/i.test(s))return{ctes:[],final:s};s=s.slice(4);const ctes=[];while(true){s=s.replace(/^[\s,]+/,"");const m=/^([a-zA-Z_]\w*)\s+AS\b\s*(?:(?:NOT\s+)?MATERIALIZED\s+)?\(/i.exec(s);if(!m)break;const name=m[1];s=s.slice(m[0].length);const{body,rest}=rb(s);ctes.push({name,body:body.trim()});s=rest.replace(/^\s+/,"");if(s[0]===","){s=s.slice(1);continue;}break;}return{ctes,final:s.trim()};}
function flatten(sql){const{ctes,final}=pw(sql);const flat=[];for(const cte of ctes){if(/^\s*WITH\b/i.test(cte.body)){const sub=flatten(cte.body);for(const f of sub.flat)flat.push(f);flat.push({name:cte.name,body:sub.final});}else flat.push({name:cte.name,body:cte.body});}return{flat,final};}
const sql=readFileSync("testSQL/rollforward_template.sql","utf8").replaceAll("{{PROJECT}}","1000073").replaceAll("{{DIVISION}}","1000339").replace(/;\s*$/,"");
const {flat}=flatten(sql);
const KS=(process.env.KS||"1,9,10,13,17,20,22,23,28,36,37").split(",").map(Number);
const t=await mysql.createConnection({host:"127.0.0.1",port:7950,user:"root",password:"",database:"wayroll",rowsAsArray:true});
const s=await mysql.createConnection({host:"64.20.36.26",port:9030,user:"root",password:process.env.SR_PWD,database:"wayroll",rowsAsArray:true});
await s.query("SET query_timeout=1200");
console.log("  K  target                                          thinDB       SR         match");
for(const K of KS){
  const tgt=flat[K-1].name;
  const defs=flat.slice(0,K).map(c=>`${c.name} AS (\n${c.body}\n)`).join(",\n");
  const q=`WITH ${defs}\nSELECT COUNT(*) FROM ${tgt}`;
  let tn="ERR",sn="ERR";
  try{const[r]=await t.query({sql:q,timeout:600000});tn=Number(r[0][0]);}catch(e){tn="ERR "+(e.sqlMessage||e.message).slice(0,40);}
  try{const[r]=await s.query({sql:q,timeout:600000});sn=Number(r[0][0]);}catch(e){sn="ERR "+(e.sqlMessage||e.message).slice(0,40);}
  console.log(` ${String(K).padStart(2)}  ${tgt.padEnd(46)} ${String(tn).padStart(9)}  ${String(sn).padStart(9)}   ${tn===sn?"✓":"✗ DIFF"}`);
}
await t.end();await s.end();
