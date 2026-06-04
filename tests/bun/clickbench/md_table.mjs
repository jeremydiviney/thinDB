import { readFileSync } from "node:fs";
const d1 = JSON.parse(readFileSync(process.argv[2], "utf8"));
const d12 = JSON.parse(readFileSync(process.argv[3], "utf8"));
const m = new Map(d1.outcomes.map((o) => [o.idx, o]));
console.log("| Q | DOP1 (ms) | DOP12 (ms) | Δ (ms) | speedup | result |");
console.log("|---|---:|---:|---:|---:|:--|");
let t1 = 0, t12 = 0;
for (const o of [...d12.outcomes].sort((a, b) => a.idx - b.idx)) {
  const b = m.get(o.idx);
  const a = b && b.ok ? b.ms : null, c = o.ok ? o.ms : null;
  let dl = null, sp = "—";
  if (a != null && c != null) { dl = c - a; t1 += a; t12 += c; sp = (a / c).toFixed(2) + "×"; }
  const res = dl == null ? "FAIL" : dl > 20 ? "🔴 regress" : dl < -20 ? "🟢 win" : "tie";
  const fmt = (v) => (v == null ? "—" : v.toFixed(0));
  const ds = dl == null ? "—" : (dl > 0 ? "+" : "") + dl.toFixed(0);
  console.log(`| Q${o.idx} | ${fmt(a)} | ${fmt(c)} | ${ds} | ${sp} | ${res} |`);
}
console.log(`| **Total** | **${(t1 / 1000).toFixed(1)}s** | **${(t12 / 1000).toFixed(1)}s** | **${((t12 - t1) / 1000).toFixed(1)}s** | **${(t1 / t12).toFixed(2)}×** | |`);
