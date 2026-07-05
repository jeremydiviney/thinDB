// Flatten expanded_metrics_pipline_mat1's inner CTEs to top-level so each can
// be probed with COUNT(*). Builds: <prefix top-level CTEs> + <inner CTE
// members> + SELECT COUNT(*) FROM <target>. Runs the probe list on thinDB.
//   bun _probe_expanded.mjs <transformed.sql> [targetCte ...]
import mysql from "mysql2/promise";
import { readFileSync } from "fs";
const [FILE, ...targetsArg] = process.argv.slice(2);
const src = readFileSync(FILE, "utf8");

// Prefix = everything before `expanded_metrics_pipline_mat1 AS (`
const marker = "expanded_metrics_pipline_mat1 AS (";
const mi = src.indexOf(marker);
const prefix = src.slice(0, mi).replace(/,\s*$/, ""); // top-level CTEs, trailing comma trimmed

// The expanded block body: balanced-paren scan from the '(' after the marker.
let i = mi + marker.length;
let depth = 1, start = i;
for (; i < src.length && depth > 0; i++) {
  if (src[i] === "(") depth++;
  else if (src[i] === ")") depth--;
}
let body = src.slice(start, i - 1).trim(); // inner: `WITH m1 AS(...), ... <finalSelect>`
body = body.replace(/^WITH\s+/, "");

// Split inner into member list + final select via depth-0 scan.
const members = []; // {name, text}
let d = 0, seg = "", segName = null, mode = "expectName";
// Simpler: walk, tracking depth; a top-level member ends at a depth-0 comma.
let buf = "", names = [];
d = 0;
const parts = [];
let cur = "";
for (let k = 0; k < body.length; k++) {
  const ch = body[k];
  if (ch === "(") d++;
  else if (ch === ")") d--;
  if (ch === "," && d === 0) { parts.push(cur); cur = ""; }
  else cur += ch;
}
parts.push(cur); // last part contains `lastMember AS (...) <finalSelect>` OR just final select
// Each part except possibly the last is `name AS ( ... )`. The last part is
// `name AS ( ... ) \n <finalSelect>` — split off the finalSelect after the
// member's closing paren.
const memberTexts = [];
for (let p = 0; p < parts.length; p++) {
  let t = parts[p].trim();
  const m = t.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\(/);
  if (!m) continue;
  names.push(m[1]);
  // find matching close paren of this member
  let dd = 0, j = t.indexOf("("), endp = -1;
  for (; j < t.length; j++) { if (t[j] === "(") dd++; else if (t[j] === ")") { dd--; if (dd === 0) { endp = j; break; } } }
  memberTexts.push(t.slice(0, endp + 1));
}
const targets = targetsArg.length ? targetsArg : names;
console.log(`prefix CTEs kept; expanded inner members: ${names.length}`);

const cte_block = memberTexts.join(",\n");
const c = await mysql.createConnection({ host:"127.0.0.1", port:7950, user:"root", password:"", database:"wayroll" });
for (const tgt of targets) {
  if (!names.includes(tgt)) { console.log(`${tgt}: (not a member)`); continue; }
  const probe = `${prefix},\n${cte_block}\nSELECT COUNT(*) AS c FROM ${tgt}`;
  const t = performance.now();
  try { const [r] = await c.query({ sql: probe, timeout: 120000 });
    console.log(`${tgt}: ${r[0].c} rows  ${Math.round(performance.now()-t)}ms`); }
  catch(e){ console.log(`${tgt}: ERR ${e.sqlMessage||e.code||e.message}`); }
}
await c.end();
