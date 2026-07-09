// Generate + apply a per-table CDC sync: prod wayroll.<table> -> thinDB
// wayroll_prod__public.<table>. Introspects the prod schema, creates the matching
// thinDB table, and writes a standalone Flink CDC job SQL (its own server-id).
// Usage: node _gen_cdc.mjs <table> <serverIdBase>   (creds via env)
import mysql from "mysql2/promise";
import fs from "fs";

const table = process.argv[2];
const sidBase = parseInt(process.argv[3], 10);
const par = parseInt(process.argv[4] || "1", 10); // snapshot parallelism (chunks read concurrently from RDS)
// Chunk key override: an INDEXED high-NDV column (e.g. updatedAt) beats the
// auto-pick when no PK column is both. Non-PK keys are fine for upsert sinks —
// mid-snapshot moves are healed by the binlog replay backfill.skip relies on.
const chunkOverride = process.argv[5] || null;
// Chunk size trade: bigger = fewer enumeration boundary queries (less WAN
// exposure while splitting), smaller = lighter per-split heap/GC and snappier
// RDS chunk reads. 64000 measured ~9-10K rows/s end-to-end, 16000 ~33-39K/s.
const chunkSize = parseInt(process.argv[6] || "64000", 10);
if (!table || !sidBase) { console.error("usage: _gen_cdc.mjs <table> <serverIdBase> [parallelism] [chunkKeyColumn] [chunkSize]"); process.exit(1); }

const PROD = {
  host: process.env.APP_H, port: 3306, user: process.env.AU, password: process.env.AP,
  database: "wayroll", connectTimeout: 20000,
};
const SINK = { host: "127.0.0.1", port: 13310, user: "root", password: "", database: "wayroll_prod__public" };
const OUT = `C:/Users/jerem/AppData/Local/Temp/claude/C--development-thinDB/c2cce1cf-9c7d-4061-98b7-3a1c71c0bfd8/scratchpad/cdc_${table}.sql`;

// MySQL type -> { thin, flink }. Native where thinDB is strong; STRING for the
// CDC-friction-prone (datetime/json/enum/text) on this first pass.
function mapType(dt, prec, scale, dtprec) {
  switch (dt) {
    case "int": case "integer": case "mediumint": case "smallint": case "tinyint":
      return { thin: "INT", flink: "INT" };
    case "bigint": return { thin: "BIGINT", flink: "BIGINT" };
    case "decimal": case "numeric":
      return { thin: `DECIMAL(${prec},${scale})`, flink: `DECIMAL(${prec},${scale})` };
    case "float": case "double": case "real": return { thin: "DOUBLE", flink: "DOUBLE" };
    case "date": return { thin: "DATE", flink: "DATE" };
    default: return { thin: "STRING", flink: "STRING" }; // varchar/text/enum/set/json/datetime/timestamp/time/...
  }
}

const p = await mysql.createConnection(PROD);
const [cols] = await p.query(
  `SELECT column_name c, data_type dt, numeric_precision np, numeric_scale ns, datetime_precision dp, is_nullable nul
   FROM information_schema.columns WHERE table_schema='wayroll' AND table_name=? ORDER BY ordinal_position`, [table]);
const [pk] = await p.query(
  `SELECT column_name c FROM information_schema.key_column_usage
   WHERE table_schema='wayroll' AND table_name=? AND constraint_name='PRIMARY' ORDER BY ordinal_position`, [table]);
// Estimate, not COUNT(*) — an exact count full-scans and times out on 34M-row
// tables over WAN. Progress is verified against thinDB's own count later.
const [[cnt]] = await p.query(
  `SELECT table_rows n FROM information_schema.tables WHERE table_schema='wayroll' AND table_name=?`, [table]);

if (pk.length === 0) { console.error(`NO PRIMARY KEY on ${table}`); process.exit(2); }
const pkCols = pk.map((r) => r.c);

// Chunk-split on the highest-NDV PK column, not the (default) first one. The
// splitter can't divide finer than one distinct value of the split key, so a
// skewed low-card leading column (projectId) yields million-row "chunks" whose
// SELECT the JDBC driver materializes wholesale in heap → snapshot-reader OOM.
// (Measured on invoice_import_amortized: projectId NDV=1 over a 200K sample.)
let chunkKey = chunkOverride;
if (!chunkKey && pkCols.length > 1) {
  const [[nd]] = await p.query(
    `SELECT ${pkCols.map((c) => `COUNT(DISTINCT \`${c}\`) AS \`${c}\``).join(", ")}
     FROM (SELECT ${pkCols.map((c) => `\`${c}\``).join(", ")} FROM \`${table}\` LIMIT 200000) s`);
  chunkKey = Object.entries(nd).sort((a, b) => Number(b[1]) - Number(a[1]))[0][0];
}
await p.end();
const mapped = cols.map((r) => ({ name: r.c, ...mapType(r.dt, r.np, r.ns, r.dp), nul: r.nul === "YES" }));

const thinDef = mapped.map((m) => `  \`${m.name}\` ${m.thin}${pkCols.includes(m.name) ? " NOT NULL" : ""}`).join(",\n");
const thinDDL = `CREATE TABLE \`${table}\` (\n${thinDef},\n  PRIMARY KEY (${pkCols.map((c) => "`" + c + "`").join(", ")})\n)`;

const flinkCols = mapped.map((m) => `  \`${m.name}\` ${m.flink}`).join(",\n");
const flinkPK = `PRIMARY KEY (${pkCols.map((c) => "`" + c + "`").join(", ")}) NOT ENFORCED`;
const colList = mapped.map((m) => "`" + m.name + "`").join(", ");

const flinkSQL = `SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.timeout' = '15min';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '20';
SET 'parallelism.default' = '${par}';
CREATE TEMPORARY TABLE src_${table} (
${flinkCols},
  ${flinkPK}
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = '${PROD.host}', 'port' = '3306',
  'username' = '${PROD.user}', 'password' = '${PROD.password}',
  'database-name' = 'wayroll', 'table-name' = '${table}',
  'server-id' = '${sidBase}-${sidBase + 15}',
  'scan.incremental.snapshot.enabled' = 'true',
  'scan.incremental.snapshot.chunk.size' = '${chunkSize}',
  'scan.snapshot.fetch.size' = '4096',
  'scan.incremental.snapshot.backfill.skip' = 'true',
  'jdbc.properties.tcpKeepAlive' = 'true'${chunkKey ? `,
  'scan.incremental.snapshot.chunk.key-column' = '${chunkKey}'` : ""}
);
CREATE TEMPORARY TABLE sink_${table} (
${flinkCols},
  ${flinkPK}
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://host.docker.internal:13310/wayroll_prod__public?rewriteBatchedStatements=true',
  'table-name' = '${table}', 'username' = 'root', 'password' = '',
  'sink.buffer-flush.max-rows' = '2000', 'sink.buffer-flush.interval' = '1s',
  'sink.max-retries' = '15'
);
INSERT INTO sink_${table} SELECT ${colList} FROM src_${table};
`;

fs.writeFileSync(OUT, flinkSQL);

const s = await mysql.createConnection(SINK);
try { await s.query(`DROP TABLE \`${table}\``); } catch (e) {} // thinDB has no IF EXISTS
try { await s.query(thinDDL); } catch (e) { if (e.code !== "ER_TABLE_EXISTS_ERROR") throw e; } // re-run keeps existing table
await s.end();

console.log(`OK ${table}: ${cols.length} cols, PK(${pkCols.join(",")}), chunk-key=${chunkKey ?? pkCols[0]}, prod rows=${cnt.n}, server-id ${sidBase}-${sidBase + 3}`);
console.log(`  thinDB table created; Flink job -> ${OUT.split("/").pop()}`);
