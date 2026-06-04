import { readFileSync } from "node:fs";
const thin = JSON.parse(readFileSync("C:/development/thinDB/tests/bun/rebench.json", "utf8"));
const duck = JSON.parse(readFileSync("C:/development/thinDB/bench/clickbench/duckdb/_duck_best.json", "utf8"));
const rows = [];
let tThin = 0, tDuck = 0;
for (const o of thin.outcomes) {
  const d = duck[String(o.idx)];
  tThin += o.ms; tDuck += d;
  rows.push({ q: o.idx, thin: o.ms, duck: d, delta: o.ms - d, ratio: o.ms / d });
}
const losses = rows.filter((r) => r.delta > 0.5).sort((a, b) => b.delta - a.delta);
console.log("=== LOSSES (thinDB slower than DuckDB), sorted by absolute delta ===");
console.log("Qxx   thinDB    duck    delta   ratio");
for (const r of losses)
  console.log(`Q${String(r.q).padStart(2, "0")}  ${r.thin.toFixed(1).padStart(7)}  ${String(r.duck).padStart(5)}  ${((r.delta >= 0 ? "+" : "") + r.delta.toFixed(1)).padStart(6)}  ${r.ratio.toFixed(2)}x`);
const q28 = thin.outcomes.find((o) => o.idx === 28).ms;
console.log(`\nTOTAL  thinDB ${tThin.toFixed(0)}ms   duck ${tDuck}ms   (thinDB ${(tDuck / tThin).toFixed(2)}x faster overall)`);
console.log(`Excl. Q28 (regex)  thinDB ${(tThin - q28).toFixed(0)}ms   duck ${tDuck - duck["28"]}ms`);
