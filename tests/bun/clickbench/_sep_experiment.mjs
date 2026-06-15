// Concept proof for range-separability. Q27 = GROUP BY CounterID (the leading
// order key). Compare: (1) monolithic Q27 vs (2) N concurrent sub-queries each
// restricted to a balanced CounterID range, merged client-side. Validates the
// hypothesis that range-partitioning on the order-key prefix + zonemap pruning
// + parallel shards beats the single query. No engine changes — pure client
// orchestration against the existing server, simulating the proposed layer.
//
// Run the server at --max-dop 12 (shards share 12 cores, oversubscribed) AND
// at --max-dop 1 (each shard serial, 12 shards fill 12 cores = the real design)
// and compare both to the monolithic number.

import mysql from "mysql2/promise";
import { readFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT ?? "7880");
const DB = process.env.DB ?? "clickbench_fsst__public";
const N = Number(process.env.N ?? "12");
const TIMEOUT = Number(process.env.TIMEOUT_MS ?? "180000");

const BASE = "SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> ''";
const TAIL = "GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25";
const MONO = `${BASE} ${TAIL}`;

// --- compute N balanced contiguous CounterID ranges from the distribution ---
const dist = readFileSync(resolve(here, "../../../bench/_counterid_dist.tsv"), "utf8")
  .split("\n").map((l) => l.trim()).filter(Boolean)
  .map((l) => { const [cid, c] = l.split("\t"); return { cid: Number(cid), c: Number(c) }; })
  .sort((a, b) => a.cid - b.cid);
const total = dist.reduce((s, r) => s + r.c, 0);
const target = total / N;
// Greedy: close a shard when its OWN running sum reaches target, reset, repeat.
// Splits only between CounterID values (never mid-group). Last shard = remainder.
const bounds = [dist[0].cid];   // shard k covers [bounds[k], bounds[k+1])
let acc = 0;
for (let j = 0; j < dist.length; j++) {
  acc += dist[j].c;
  if (acc >= target && bounds.length < N && j + 1 < dist.length) {
    bounds.push(dist[j + 1].cid);
    acc = 0;
  }
}
const ranges = [];
for (let i = 0; i < bounds.length; i++) {
  const lo = bounds[i];
  const hi = i + 1 < bounds.length ? bounds[i + 1] : null;
  ranges.push({ lo, hi });
}
// per-range row estimate (from dist) for balance reporting
for (const rg of ranges) rg.rows = dist.filter((d) => d.cid >= rg.lo && (rg.hi === null || d.cid < rg.hi)).reduce((s, d) => s + d.c, 0);
console.log(`splits: ${ranges.length} ranges; rows min/max = ${Math.min(...ranges.map(r=>r.rows))/1e6}M / ${Math.max(...ranges.map(r=>r.rows))/1e6}M (target ${(total/N/1e6).toFixed(1)}M)`);

function shardSQL(rg) {
  let pred = `CounterID >= ${rg.lo}`;
  if (rg.hi !== null) pred += ` AND CounterID < ${rg.hi}`;
  return `${BASE} AND ${pred} ${TAIL}`;
}
function canon(rows) {
  return rows.map((r) => `${r.CounterID}|${Number(r.l).toFixed(4)}|${r.c}`).sort().join("\n");
}
async function timed(fn) { const t0 = performance.now(); const v = await fn(); return { ms: performance.now() - t0, v }; }

// --- monolithic baseline ---
const mc = await mysql.createConnection({ host: "127.0.0.1", port: PORT, user: "root", password: "", database: DB });
await mc.query({ sql: MONO, timeout: TIMEOUT }); // warm
let monoBest = Infinity, monoRows = null;
for (let i = 0; i < 2; i++) { const r = await timed(() => mc.query({ sql: MONO, timeout: TIMEOUT })); if (r.ms < monoBest) { monoBest = r.ms; monoRows = r.v[0]; } }
const monoHash = canon(monoRows);

// --- sharded: N concurrent connections, one per range ---
const conns = [];
for (let i = 0; i < ranges.length; i++) conns.push(await mysql.createConnection({ host: "127.0.0.1", port: PORT, user: "root", password: "", database: DB }));
async function runSharded() {
  const parts = await Promise.all(ranges.map((rg, i) => conns[i].query({ sql: shardSQL(rg), timeout: TIMEOUT }).then(([rows]) => rows)));
  const merged = parts.flat().sort((a, b) => Number(b.l) - Number(a.l)).slice(0, 25);
  return merged;
}
await runSharded(); // warm
let shardBest = Infinity, shardRows = null;
for (let i = 0; i < 2; i++) { const r = await timed(runSharded); if (r.ms < shardBest) { shardBest = r.ms; shardRows = r.v; } }
const shardHash = canon(shardRows);

console.log(`\nmonolithic Q27   : ${monoBest.toFixed(1)} ms`);
console.log(`${ranges.length}-shard range    : ${shardBest.toFixed(1)} ms  (${(monoBest/shardBest).toFixed(2)}x)`);
console.log(`correctness      : ${monoHash === shardHash ? "MATCH" : "*** MISMATCH ***"}`);
if (monoHash !== shardHash) { console.log("--- mono ---\n" + monoHash + "\n--- shard ---\n" + shardHash); }

await mc.end(); for (const c of conns) await c.end();
