import { readFileSync } from "node:fs";
const a = JSON.parse(readFileSync(process.argv[2], "utf8")); // dop1
const b = JSON.parse(readFileSync(process.argv[3], "utf8")); // dop12
const m1 = new Map(a.outcomes.map((o) => [o.idx, o]));
const rows = [];
for (const o of b.outcomes) {
  const base = m1.get(o.idx);
  const d1 = base && base.ok ? base.ms : null;
  const d12 = o.ok ? o.ms : null;
  rows.push({ idx: o.idx, d1, d12, ok1: base?.ok, ok12: o.ok, delta: d1 != null && d12 != null ? d12 - d1 : null });
}
rows.sort((x, y) => (y.delta ?? -1e9) - (x.delta ?? -1e9));
console.log("idx    DOP1ms   DOP12ms    delta   note");
let regr = 0;
for (const r of rows) {
  const note = !r.ok1 || !r.ok12 ? `FAIL1=${!r.ok1} FAIL12=${!r.ok12}` : r.delta > 20 ? "REGRESSION" : r.delta < -20 ? "win" : "";
  if (r.ok1 && r.ok12 && r.delta > 20) regr++;
  const f = (v) => (v == null ? "    -  " : v.toFixed(1).padStart(8));
  console.log(`Q${String(r.idx).padEnd(3)} ${f(r.d1)} ${f(r.d12)} ${f(r.delta)}   ${note}`);
}
const tot1 = rows.reduce((s, r) => s + (r.ok1 && r.ok12 ? r.d1 : 0), 0);
const tot12 = rows.reduce((s, r) => s + (r.ok1 && r.ok12 ? r.d12 : 0), 0);
console.log(`\n${regr} queries regress >20ms.  Comparable-total: DOP1=${tot1.toFixed(0)}ms  DOP12=${tot12.toFixed(0)}ms`);
