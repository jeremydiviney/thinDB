import { readFileSync } from "node:fs";
const d1 = JSON.parse(readFileSync(process.argv[2], "utf8"));
const d12 = JSON.parse(readFileSync(process.argv[3], "utf8"));
const m = new Map(d1.outcomes.map((o) => [o.idx, o]));
const f = (v) => (v == null ? "      -" : v.toFixed(1).padStart(9));
console.log("  Q      DOP1ms    DOP12ms      delta  speedup  query");
console.log("-".repeat(92));
let t1 = 0, t12 = 0, nReg = 0, nWin = 0;
for (const o of [...d12.outcomes].sort((a, b) => a.idx - b.idx)) {
  const b = m.get(o.idx);
  const d1ms = b && b.ok ? b.ms : null;
  const d12ms = o.ok ? o.ms : null;
  let delta = null, sp = "";
  if (d1ms != null && d12ms != null) {
    delta = d12ms - d1ms; t1 += d1ms; t12 += d12ms; sp = (d1ms / d12ms).toFixed(2) + "x";
    if (delta > 20) nReg++; if (delta < -20) nWin++;
  }
  const mark = delta == null ? "FAIL " : delta > 20 ? "REGR " : delta < -20 ? "win  " : " ·   ";
  const q = (o.sql || "").replace(/\s+/g, " ").slice(0, 38);
  console.log("Q" + String(o.idx).padEnd(3) + f(d1ms) + f(d12ms) + f(delta) + sp.padStart(8) + "  " + mark + q);
}
console.log("--------------------------------------------------------");
console.log("TOT " + f(t1) + f(t12) + f(t12 - t1) + ((t1 / t12).toFixed(2) + "x").padStart(8));
console.log("\n" + nWin + " queries win >20ms,  " + nReg + " regress >20ms,  rest within +/-20ms");
console.log("Overall: DOP12 " + (t1 / t12).toFixed(2) + "x faster  (" + (t1 / 1000).toFixed(1) + "s -> " + (t12 / 1000).toFixed(1) + "s)");
