// Fault-injection harness for the CDC rig.
//   node rig.mjs setup             create thinDB sink db+table
//   node rig.mjs seed [n]          insert n seed rows into MySQL (default 5000)
//   node rig.mjs bigtxn [n] [tag]  one multi-VALUES insert of n rows (default 500)
//   node rig.mjs churn [secs]      mixed small inserts/updates at ~10/s
//   node rig.mjs verify            full rowset compare MySQL vs thinDB
//   node rig.mjs mode <pass|poison|storm> [minsize] [cutFrac] [prob]
//   node rig.mjs stats             proxy stats

import mysql from "mysql2/promise";

const SRC = { host: "127.0.0.1", port: 3316, user: "root", password: "rigroot", database: "rigdb", dateStrings: true };
const SINK = { host: "127.0.0.1", port: 13399, user: "root", password: "", dateStrings: true, multipleStatements: false };
const CTRL = "http://127.0.0.1:8123";
const SINK_DB = "rigdb__public";

const cmd = process.argv[2] || "verify";
const a3 = process.argv[3];
const a4 = process.argv[4];
const a5 = process.argv[5];
const a6 = process.argv[6];

function pad(n, w) { return String(n).padStart(w, "0"); }
// NONCE makes every run's values distinct so a LOST UPDATE is detectable as
// stale (deterministic values made re-updates indistinguishable from loss).
const NONCE = Date.now() % 1000000;
function rowValues(i, tag) {
  const proj = 1000 + (i % 7);
  const ic = 10 + (i % 3);
  const inv = `${tag}-INV-${pad(Math.floor(i / 4), 8)}`;
  const item = `${tag}-ITEM-${pad(i, 10)}`;
  const mt = i % 2 ? "mrr" : "one_time";
  const d = `20${20 + (i % 6)}-${pad(1 + (i % 12), 2)}-01`;
  const payload = "x".repeat(160 + (i % 40)); // sizes rows so ~40 rows/8KB event
  return [proj, ic, inv, item, mt, d, (i * 37 + NONCE) % 100000, `cust-${i % 997}-${NONCE}`, payload];
}

const SMALL_DDL = {
  small_a: `(id INT NOT NULL, name VARCHAR(64), val INT, updatedAt DATETIME(6) NOT NULL, PRIMARY KEY (id))`,
  small_b: `(id INT NOT NULL, label VARCHAR(64), qty BIGINT, updatedAt DATETIME(6) NOT NULL, PRIMARY KEY (id))`,
};
const SMALL_DDL_THIN = {
  small_a: `(id INT NOT NULL, name TEXT, val INT, updatedAt DATETIME, PRIMARY KEY (id))`,
  small_b: `(id INT NOT NULL, label TEXT, qty BIGINT, updatedAt DATETIME, PRIMARY KEY (id))`,
};

async function setup() {
  const c = await mysql.createConnection({ ...SINK });
  await c.query(`CREATE DATABASE rigdb`).catch((e) => console.log("createdb:", e.message));
  await c.query(`USE ${SINK_DB}`);
  await c
    .query(
      `CREATE TABLE invoice_test (
        projectId INT NOT NULL, integrationConfigId INT NOT NULL,
        invoiceId TEXT NOT NULL, invoiceItemId TEXT NOT NULL,
        modelType TEXT NOT NULL, date DATE NOT NULL,
        amount INT, customerName TEXT, payload TEXT, updatedAt DATETIME,
        PRIMARY KEY (projectId, integrationConfigId, invoiceId, invoiceItemId, modelType, date)
      )`
    )
    .catch((e) => console.log("createtable:", e.message));
  for (const [t, ddl] of Object.entries(SMALL_DDL_THIN)) {
    await c.query(`CREATE TABLE ${t} ${ddl}`).catch((e) => console.log(`${t}:`, e.message));
  }
  console.log("thinDB sink ready");
  await c.end();
  const s = await mysql.createConnection(SRC);
  for (const [t, ddl] of Object.entries(SMALL_DDL)) {
    await s.query(`CREATE TABLE IF NOT EXISTS ${t} ${ddl}`).catch((e) => console.log(`src ${t}:`, e.message));
  }
  console.log("mysql small tables ready");
  await s.end();
}

async function smallwrite() {
  const n = +(a3 || 200);
  const c = await mysql.createConnection(SRC);
  for (let i = 0; i < n; i++) {
    await c.query(
      `INSERT INTO small_a (id,name,val,updatedAt) VALUES (?,?,?,NOW(6))
       ON DUPLICATE KEY UPDATE name=VALUES(name), val=VALUES(val), updatedAt=NOW(6)`,
      [i % 500, `nm-${NONCE}-${i}`, (i * 13 + NONCE) % 9999]
    );
    await c.query(
      `INSERT INTO small_b (id,label,qty,updatedAt) VALUES (?,?,?,NOW(6))
       ON DUPLICATE KEY UPDATE label=VALUES(label), qty=VALUES(qty), updatedAt=NOW(6)`,
      [i % 300, `lb-${NONCE}-${i}`, (i * 7 + NONCE) % 100000]
    );
  }
  console.log(`smallwrite ${n} rounds (nonce ${NONCE})`);
  await c.end();
}

async function insertBatch(c, rows, tag) {
  const vals = rows.map((i) => rowValues(i, tag));
  const sql =
    "INSERT INTO invoice_test (projectId,integrationConfigId,invoiceId,invoiceItemId,modelType,`date`,amount,customerName,payload,updatedAt) VALUES " +
    vals.map(() => "(?,?,?,?,?,?,?,?,?,NOW(6))").join(",") +
    " ON DUPLICATE KEY UPDATE amount=VALUES(amount), payload=VALUES(payload), updatedAt=NOW(6)";
  await c.query(sql, vals.flat());
}

async function seed() {
  const n = +(a3 || 5000);
  const c = await mysql.createConnection(SRC);
  for (let at = 0; at < n; at += 500) {
    const rows = Array.from({ length: Math.min(500, n - at) }, (_, k) => at + k);
    await insertBatch(c, rows, "SEED");
  }
  console.log(`seeded ${n}`);
  await c.end();
}

async function bigtxn() {
  const n = +(a3 || 500);
  const tag = a4 || `BIG${Date.now() % 100000}`;
  const c = await mysql.createConnection(SRC);
  const rows = Array.from({ length: n }, (_, k) => k);
  await insertBatch(c, rows, tag); // one multi-VALUES stmt = large ROWS events
  console.log(`bigtxn ${n} rows tag=${tag}`);
  await c.end();
}

async function churn() {
  const secs = +(a3 || 60);
  const c = await mysql.createConnection(SRC);
  const t0 = Date.now();
  let i = 0;
  while (Date.now() - t0 < secs * 1000) {
    await insertBatch(c, [1000000 + (i % 5000)], "CHURN");
    i++;
    await new Promise((r) => setTimeout(r, 100));
  }
  console.log(`churn done: ${i} ops in ${secs}s`);
  await c.end();
}

const VERIFY_TABLES = {
  invoice_test: {
    sel: `SELECT projectId,integrationConfigId,invoiceId,invoiceItemId,modelType,
            DATE_FORMAT(\`date\`, '%Y-%m-%d') d, amount, customerName, LENGTH(payload) plen`,
    key: (r) => `${r.projectId}|${r.integrationConfigId}|${r.invoiceId}|${r.invoiceItemId}|${r.modelType}|${r.d}`,
    val: (r) => `${r.amount}|${r.customerName}|${r.plen}`,
  },
  small_a: {
    sel: `SELECT id, name, val`,
    key: (r) => `${r.id}`,
    val: (r) => `${r.name}|${r.val}`,
  },
  small_b: {
    sel: `SELECT id, label, qty`,
    key: (r) => `${r.id}`,
    val: (r) => `${r.label}|${r.qty}`,
  },
};

async function dumpTable(conn, db, table) {
  const spec = VERIFY_TABLES[table];
  const [rows] = await conn.query(`${spec.sel} FROM ${db}.${table}`);
  const map = new Map();
  for (const r of rows) map.set(spec.key(r), spec.val(r));
  return map;
}

async function verify() {
  const cs = await mysql.createConnection(SRC);
  const ct = await mysql.createConnection(SINK);
  let bad = 0;
  for (const table of Object.keys(VERIFY_TABLES)) {
    const src = await dumpTable(cs, "rigdb", table).catch(() => null);
    const snk = await dumpTable(ct, SINK_DB, table).catch(() => null);
    if (!src || !snk) { console.log(`  ${table}: skipped (absent)`); continue; }
    let missing = 0, stale = 0, extra = 0;
    for (const [k, v] of src) {
      if (!snk.has(k)) { if (missing++ < 3) console.log(`  ${table} MISSING:`, k); }
      else if (snk.get(k) !== v) { if (stale++ < 3) console.log(`  ${table} STALE:`, k, "src=", v, "sink=", snk.get(k)); }
    }
    for (const k of snk.keys()) if (!src.has(k)) { if (extra++ < 3) console.log(`  ${table} EXTRA:`, k); }
    const ok = missing + stale + extra === 0;
    if (!ok) bad++;
    console.log(`  ${table}: src=${src.size} sink=${snk.size} missing=${missing} stale=${stale} extra=${extra} => ${ok ? "EXACT" : "*** DIVERGED ***"}`);
  }
  console.log(bad === 0 ? "VERIFY ALL EXACT" : `VERIFY ${bad} TABLE(S) DIVERGED`);
  await cs.end();
  await ct.end();
  process.exit(bad === 0 ? 0 : 1);
}

async function setMode() {
  const p = new URLSearchParams({ m: a3 });
  if (a4) p.set("minsize", a4);
  if (a5) p.set("cutFrac", a5);
  if (a6) p.set("prob", a6);
  const r = await fetch(`${CTRL}/mode?${p}`);
  console.log(await r.text());
}

async function statsCmd() {
  const r = await fetch(CTRL);
  console.log(await r.text());
}

const fns = { setup, seed, bigtxn, churn, verify, mode: setMode, stats: statsCmd, smallwrite };
if (!fns[cmd]) { console.error("unknown cmd", cmd); process.exit(2); }
await fns[cmd]();
