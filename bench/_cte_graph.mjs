import { readFileSync } from "fs";
function rb(s){let d=1,i=0,inS=false,q="";while(i<s.length){const c=s[i];if(inS){if(c===q)inS=false;i++;continue;}if(c==="'"||c==='"'||c==="`"){inS=true;q=c;i++;continue;}if(c==="(")d++;else if(c===")"){d--;if(d===0)return{body:s.slice(0,i),rest:s.slice(i+1)};}i++;}throw 0;}
function pw(sql){let s=sql.trim();if(!/^WITH\b/i.test(s))return{ctes:[],final:s};s=s.slice(4);const ctes=[];while(true){s=s.replace(/^[\s,]+/,"");const m=/^([a-zA-Z_]\w*)\s+AS\b\s*(?:(?:NOT\s+)?MATERIALIZED\s+)?\(/i.exec(s);if(!m)break;const name=m[1];s=s.slice(m[0].length);const{body,rest}=rb(s);ctes.push({name,body:body.trim()});s=rest.replace(/^\s+/,"");if(s[0]===","){s=s.slice(1);continue;}break;}return{ctes,final:s.trim()};}
function flatten(sql){const{ctes,final}=pw(sql);const flat=[];for(const cte of ctes){if(/^\s*WITH\b/i.test(cte.body)){const sub=flatten(cte.body);for(const f of sub.flat)flat.push(f);flat.push({name:cte.name,body:sub.final});}else flat.push({name:cte.name,body:cte.body});}return{flat,final};}

const sql = readFileSync("testSQL/rollforward_template.sql","utf8")
  .replaceAll("{{PROJECT}}","1000073").replaceAll("{{DIVISION}}","1000339");
const { flat, final } = flatten(sql);
const names = flat.map(c=>c.name);
const strip = s => s.replace(/'(?:[^']|'')*'/g, "''");

// FROM/JOIN references only — that's what determines readers of a CTE
const refRe = name => new RegExp(String.raw`\b(?:FROM|JOIN)\s+` + name + String.raw`\b`, "gi");
const refs = Object.fromEntries(names.map(n=>[n,0]));
const deps = flat.map(()=>[]);
for (let i=0;i<flat.length;i++){
  const b = strip(flat[i].body);
  for (let j=0;j<i;j++){
    const m = b.match(refRe(names[j]));
    if (m){ refs[names[j]] += m.length; deps[i].push(j+1); }
  }
}
{ const b = strip(final);
  for (const n of names){ const m = b.match(refRe(n)); if (m) refs[n]+=m.length; } }

console.log("idx  name                                              refs  ops                       deps");
for (let i=0;i<flat.length;i++){
  const b = strip(flat[i].body);
  const ops=[];
  const cnt=(re)=> (b.match(re)||[]).length;
  const j=cnt(/\bJOIN\b/gi); if(j)ops.push("JOIN"+(j>1?"x"+j:""));
  if(/\bGROUP BY\b/i.test(b))ops.push("GROUP");
  const w=cnt(/\bOVER\s*\(/gi); if(w)ops.push("WIN"+(w>1?"x"+w:""));
  if(/\bUNION\b/i.test(b))ops.push("UNION");
  const base = /\bFROM\s+invoice_import_amortized\b/i.test(b) ? " [BASE-SCAN]" : "";
  console.log(String(i+1).padStart(3)+"  "+flat[i].name.padEnd(48)+String(refs[names[i]]).padStart(4)+"  "+ops.join(",").padEnd(24)+"  ←"+deps[i].join(",")+base);
}
const staged = names.filter(n=>refs[n]>1);
console.log(`\nmulti-ref (staged/materialized): ${staged.length} → ${staged.join(", ")}`);
