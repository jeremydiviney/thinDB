// Direct bulk load: prod wayroll.<table> -> thinDB wayroll_prod__public.<table>.
// Streams a single PK-ordered SELECT (row-by-row, no resultset materialization)
// and batch-INSERTs into thinDB with hard backpressure (source paused while a
// batch writes). Constant memory by construction — replaces the Flink CDC
// snapshot for giant tables, whose chunk reader slurps whole chunk resultsets
// into heap and OOMs on skewed composite PKs. CDC then handles only the binlog
// tail from the pre-copy timestamp saved next to the generated SQL.
// Usage: node _bulk_load.mjs <table>   (creds via env APP_H/AU/AP)
import mysql from "mysql2/promise";
import mysqlCb from "mysql2";
import fs from "fs";

const table = process.argv[2];
if (!table) { console.error("usage: _bulk_load.mjs <table>"); process.exit(1); }
const BATCH = 2000; // same statement shape the JDBC sink used — proven against thinDB

const PROD = {
  host: process.env.APP_H, port: 3306, user: process.env.AU, password: process.env.AP,
  database: "wayroll", connectTimeout: 20000,
};
const SINK = { host: "127.0.0.1", port: 13310, user: "root", password: "", database: "wayroll_prod__public" };
const TS_OUT = `C:/Users/jerem/AppData/Local/Temp/claude/C--development-thinDB/c2cce1cf-9c7d-4061-98b7-3a1c71c0bfd8/scratchpad/bulk_${table}.start_ts`;

const meta = await mysql.createConnection(PROD);
const [cols] = await meta.query(
  `SELECT column_name c, data_type dt FROM information_schema.columns
   WHERE table_schema='wayroll' AND table_name=? ORDER BY ordinal_position`, [table]);
const [pk] = await meta.query(
  `SELECT column_name c FROM information_schema.key_column_usage
   WHERE table_schema='wayroll' AND table_name=? AND constraint_name='PRIMARY' ORDER BY ordinal_position`, [table]);
const [[now]] = await meta.query(`SELECT NOW() ts`);
await meta.end();
if (pk.length === 0) { console.error(`NO PRIMARY KEY on ${table}`); process.exit(2); }

// Binlog position anchor: CDC tail starts from this (pre-copy) timestamp, so
// every change that lands during the copy gets replayed; PK upsert dedups.
fs.writeFileSync(TS_OUT, String(now.ts));
console.log(`pre-copy timestamp saved: ${now.ts}`);

const numeric = new Set(["int", "integer", "mediumint", "smallint", "tinyint", "bigint", "decimal", "numeric", "float", "double", "real"]);
const isNum = cols.map((r) => numeric.has(r.dt));
const colList = cols.map((r) => "`" + r.c + "`").join(", ");
const orderBy = pk.map((r) => "`" + r.c + "`").join(", ");
const NUM_RE = /^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$/;

const sink = await mysql.createConnection({ ...SINK, connectTimeout: 12000 });
const insertPrefix = `INSERT INTO \`${table}\` (${colList}) VALUES `;

function sqlValue(v, num) {
  if (v === null || v === undefined) return "NULL";
  if (num) {
    const s = String(v);
    if (NUM_RE.test(s)) return s;
    return mysqlCb.escape(s); // pathological numeric — quote it, thinDB coerces
  }
  if (typeof v === "object" && !(v instanceof Date) && !Buffer.isBuffer(v)) return mysqlCb.escape(JSON.stringify(v));
  return mysqlCb.escape(v);
}

const src = mysqlCb.createConnection({
  ...PROD,
  rowsAsArray: true,
  dateStrings: true,       // DATE/DATETIME/TIMESTAMP as strings, no TZ surprises
  supportBigNumbers: true,
  bigNumberStrings: true,  // BIGINT/DECIMAL as exact strings
});

let batch = [];
let total = 0;
let inflight = Promise.resolve();
const t0 = Date.now();
let lastLog = t0;

async function flushBatch(rows) {
  if (rows.length === 0) return;
  const values = rows.map((r) => "(" + r.map((v, i) => sqlValue(v, isNum[i])).join(",") + ")").join(",");
  await sink.query(insertPrefix + values);
  total += rows.length;
  const nowMs = Date.now();
  if (nowMs - lastLog > 30000) {
    const rate = Math.round(total / ((nowMs - t0) / 1000));
    console.log(`${new Date().toISOString().slice(11, 19)}  ${total} rows  ~${rate}/s  rss=${Math.round(process.memoryUsage().rss / 1048576)}MB`);
    lastLog = nowMs;
  }
}

const q = src.query(`SELECT ${colList} FROM \`${table}\` ORDER BY ${orderBy}`);
q.on("result", (row) => {
  batch.push(row);
  if (batch.length >= BATCH) {
    const rows = batch;
    batch = [];
    src.pause(); // hard backpressure: stop reading until this batch lands
    inflight = flushBatch(rows)
      .then(() => src.resume())
      .catch((e) => { console.error("SINK ERROR:", e.message); process.exit(3); });
  }
});
q.on("error", (e) => { console.error("SOURCE ERROR:", e.message); process.exit(4); });
q.on("end", async () => {
  await inflight;
  await flushBatch(batch);
  await sink.end();
  src.end();
  const secs = (Date.now() - t0) / 1000;
  console.log(`DONE ${table}: ${total} rows in ${Math.round(secs)}s (~${Math.round(total / secs)}/s)`);
  process.exit(0);
});
