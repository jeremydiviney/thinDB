// Per-query config sweep. For each ClickBench query, run the full cartesian
// grid of 4 numeric silo knobs (81 configs), 2 runs/config keeping the best,
// against ONE warm server. Knobs are switched via `SET THINDB_V2_* = N` (the
// server mutates its own process env; paramsFromEnv reads getenv per query).
// GROUP BY queries get the full grid; inert queries (no GROUP BY) run once at
// baseline. Output: _sweep_results.json (full grid) + SWEEP.md (summary).

import mysql from "mysql2/promise";
import { readFileSync, writeFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT ?? "7880");
const DB = process.env.DB ?? "clickbench_fsst__public";
const TIMEOUT = Number(process.env.TIMEOUT_MS ?? "180000");
const SMOKE = process.env.SMOKE ? Number(process.env.SMOKE) : 0; // run only query index N (full grid)

const KNOBS = {
  THINDB_V2_BUCKET_COUNT: [64, 256, 1024],
  THINDB_V2_RAW_CHUNK_ROWS: [4096, 8192, 16384],
  THINDB_V2_RAW_GROUP_CHUNK_ROWS: [4096, 8192, 16384],
  THINDB_V2_SCAN_TILE_RGS: [8, 16, 32],
};
const DEFAULTS = {
  THINDB_V2_BUCKET_COUNT: 256,
  THINDB_V2_RAW_CHUNK_ROWS: 8192,
  THINDB_V2_RAW_GROUP_CHUNK_ROWS: 8192,
  THINDB_V2_SCAN_TILE_RGS: 16,
};
const names = Object.keys(KNOBS);

function grid() {
  let combos = [{}];
  for (const k of names) {
    const next = [];
    for (const c of combos) for (const v of KNOBS[k]) next.push({ ...c, [k]: v });
    combos = next;
  }
  return combos; // 81
}

const queries = readFileSync(resolve(here, "../../../bench/clickbench/queries.sql"), "utf8")
  .split("\n").map((l) => l.trim()).filter((l) => l.length > 0 && !l.startsWith("--"));

const c = await mysql.createConnection({ host: "127.0.0.1", port: PORT, user: "root", password: "", database: DB });

async function setKnobs(cfg) {
  for (const k of names) await c.query(`SET ${k} = ${cfg[k]}`);
}
function canonHash(rows) {
  // order-independent: stringify each row, sort, join. Tolerates GROUP BY
  // emission-order differences across configs; tie-at-LIMIT picks may still
  // differ (flagged, not fatal).
  const lines = rows.map((r) => Object.values(r).map((v) => (v === null ? "\\N" : String(v))).join("\t"));
  lines.sort();
  return lines.join("\n");
}
async function runOnce(sql) {
  const t0 = performance.now();
  const [rows] = await c.query({ sql, timeout: TIMEOUT });
  return { ms: performance.now() - t0, hash: canonHash(rows), n: rows.length };
}
async function bestOf2(sql) {
  const a = await runOnce(sql);
  const b = await runOnce(sql);
  return a.ms <= b.ms ? a : b;
}

const results = [];
const isGroupBy = (q) => /\bGROUP\s+BY\b/i.test(q);

for (let qi = 0; qi < queries.length; qi++) {
  if (SMOKE && qi !== SMOKE) continue;
  const q = queries[qi];
  const label = `Q${String(qi).padStart(2, "0")}`;
  await setKnobs(DEFAULTS);
  await runOnce(q).catch(() => {}); // cold warmup (discarded)

  if (!isGroupBy(q)) {
    let base;
    try { base = await bestOf2(q); } catch (e) { results.push({ label, error: String(e) }); continue; }
    results.push({ label, group_by: false, baseline_ms: +base.ms.toFixed(1), best_ms: +base.ms.toFixed(1), best_config: "default (inert)", delta_ms: 0, stable: true });
    process.stdout.write(`${label} inert  ${base.ms.toFixed(1)}ms\n`);
    continue;
  }

  // baseline
  await setKnobs(DEFAULTS);
  let base;
  try { base = await bestOf2(q); } catch (e) { results.push({ label, error: String(e) }); continue; }
  const baseHash = base.hash;

  const configs = grid();
  let best = { ms: Infinity, cfg: null };
  let stable = true;
  const allRuns = [];
  for (const cfg of configs) {
    await setKnobs(cfg);
    let r;
    try { r = await bestOf2(q); } catch (e) { allRuns.push({ cfg, error: String(e) }); continue; }
    if (r.hash !== baseHash) stable = false;
    allRuns.push({ cfg, ms: +r.ms.toFixed(1), match: r.hash === baseHash });
    if (r.ms < best.ms) best = { ms: r.ms, cfg };
  }
  const delta = +(base.ms - best.ms).toFixed(1);
  results.push({
    label, group_by: true,
    baseline_ms: +base.ms.toFixed(1),
    best_ms: +best.ms.toFixed(1),
    best_config: best.cfg,
    delta_ms: delta,
    delta_pct: +(100 * delta / base.ms).toFixed(1),
    stable, grid: allRuns,
  });
  const cfgStr = best.cfg ? names.map((n) => `${n.replace("THINDB_V2_", "")}=${best.cfg[n]}`).join(" ") : "?";
  process.stdout.write(`${label} base=${base.ms.toFixed(1)} best=${best.ms.toFixed(1)} (-${delta.toFixed(1)}, ${(100*delta/base.ms).toFixed(0)}%) ${cfgStr}${stable ? "" : "  [UNSTABLE]"}\n`);
}

await c.end();

writeFileSync(resolve(here, "../../../bench/_sweep_results.json"), JSON.stringify(results, null, 2));

// SWEEP.md summary
const md = [];
md.push("# ClickBench config sweep — best per query", "");
md.push("4 numeric silo knobs, full cartesian (81 configs), 2 runs/config keep-min, DB=" + DB + ", max-dop 12.", "");
md.push("| Query | baseline ms | best ms | Δ ms | Δ % | best config | stable |");
md.push("|---|--:|--:|--:|--:|---|---|");
for (const r of results) {
  if (r.error) { md.push(`| ${r.label} | — | — | — | — | ERROR | — |`); continue; }
  const cfg = typeof r.best_config === "string" ? r.best_config
    : names.map((n) => `${n.replace("THINDB_V2_", "")}=${r.best_config[n]}`).join(" ");
  md.push(`| ${r.label} | ${r.baseline_ms} | ${r.best_ms} | ${r.delta_ms ?? 0} | ${r.delta_pct ?? 0} | ${cfg} | ${r.stable ? "yes" : "**NO**"} |`);
}
writeFileSync(resolve(here, "../../../SWEEP.md"), md.join("\n") + "\n");
process.stdout.write(`\nwrote bench/_sweep_results.json + SWEEP.md (${results.length} queries)\n`);
