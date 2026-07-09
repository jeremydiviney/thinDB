// #143 validation: full-key Bloom segment pruning on point lookups + IN-lists.
// Builds a table whose 3 flushed segments FULLY OVERLAP in key range
// (interleaved keys), so zonemaps can't skip any segment — only the Bloom
// can. Waits out the 30s time-trigger flush between batches, then runs keyed
// queries. Run against a --profile-ops server and check stderr for
// seg_skip=true + pruned rowgroups. Also asserts result correctness (a wrong
// prune = missing rows).
// Usage: node _bloom_prune_probe.mjs [port]
import mysql from "mysql2/promise";

const PORT = parseInt(process.argv[2] || "13399", 10);
const c = await mysql.createConnection({ host: "127.0.0.1", port: PORT, user: "root", password: "" });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

try { await c.query("DROP TABLE bp143"); } catch {}
await c.query("CREATE TABLE bp143 (id BIGINT NOT NULL, grp INT NOT NULL, tag STRING, PRIMARY KEY (id))");

// 3 interleaved batches: segment i holds ids ≡ i (mod 3), each spanning 0..29999.
for (let seg = 0; seg < 3; seg++) {
  const vals = [];
  for (let k = 0; k < 10000; k++) {
    const id = k * 3 + seg;
    vals.push(`(${id},${seg},'t-${id}')`);
  }
  await c.query("INSERT INTO bp143 (id,grp,tag) VALUES " + vals.join(","));
  process.stdout.write(`batch ${seg} inserted, waiting for time-trigger flush...`);
  await sleep(34000);
  console.log(" flushed");
}

function expectRows(label, rows, want) {
  const got = rows.map((r) => Number(r.id)).sort((a, b) => a - b).join(",");
  const exp = want.join(",");
  console.log(`${label}: ${got === exp ? "CORRECT" : `WRONG got=[${got}] want=[${exp}]`}`);
  if (got !== exp) process.exitCode = 1;
}

// Point lookup: id=15 lives only in segment 0 (15 % 3 === 0) → expect 2 segments bloom-skipped.
console.log("--- point lookup id=15 (segment 0 only; expect seg_skip on 2/3) ---");
const [r1] = await c.query("SELECT id, grp FROM bp143 WHERE id = 15");
expectRows("point", r1, [15]);

// IN-list across two segments: 15 (seg 0) + 16 (seg 1) → expect 1 segment skipped.
console.log("--- IN (15,16) (segments 0+1; expect seg_skip on 1/3) ---");
const [r2] = await c.query("SELECT id FROM bp143 WHERE id IN (15, 16)");
expectRows("in-list", r2, [15, 16]);

// Absent key: nothing matches anywhere → all 3 segments skipped, memtable empty.
console.log("--- absent key id=999999 (expect all 3 skipped, 0 rows) ---");
const [r3] = await c.query("SELECT id FROM bp143 WHERE id = 999999");
expectRows("absent", r3, []);

// Extra non-key conjunct must not break the pass: id=21 AND grp=0.
console.log("--- id=21 AND grp=0 (extra conjunct; still prunable) ---");
const [r4] = await c.query("SELECT id FROM bp143 WHERE id = 21 AND grp = 0");
expectRows("conjunct", r4, [21]);

await c.end();
console.log("DONE — now check the server stderr for [pscan] seg_skip=true lines");
