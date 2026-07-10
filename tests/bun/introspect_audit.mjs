// Two-wire introspection audit: every schema-info statement a real MySQL or
// PostgreSQL client/GUI commonly issues, probed live and classified.
//   node introspect_audit.mjs [mysqlPort] [pgPort]  (default 3306 / 5432)
// Seeds a scratch db (idempotent), then prints OK / EMPTY / FAIL per probe
// and a summary of gaps. EMPTY = accepted but returned no rows where rows
// were expected — usually a stub, worth knowing but not a hard failure.
import mysqlp from "mysql2/promise";
import pg from "pg";

const MYSQL_PORT = Number(process.argv[2] || 3306);
const PG_PORT = Number(process.argv[3] || 5432);
const DB = "introspect_audit";

const my = await mysqlp.createConnection({ host: "127.0.0.1", port: MYSQL_PORT, user: "root", password: "" });
try { await my.query(`CREATE DATABASE ${DB}`); } catch {}
await my.query(`USE ${DB}__public`);
try {
  await my.query(`CREATE TABLE widgets (id BIGINT PRIMARY KEY, name TEXT, qty INT NOT NULL, ts DATETIME)`);
  await my.query(`INSERT INTO widgets (id, name, qty, ts) VALUES (1, 'a', 2, '2026-01-01 00:00:00')`);
} catch {}

// [statement, expectRows] — expectRows=true means an empty result counts as EMPTY.
const MYSQL_PROBES = [
  ["SHOW DATABASES", true],
  ["SHOW SCHEMAS", true],
  ["SHOW TABLES", true],
  [`SHOW TABLES FROM ${DB}__public`, true],
  [`SHOW TABLES IN ${DB}__public`, true],
  ["SHOW TABLES LIKE 'wid%'", true],
  [`SHOW TABLES FROM ${DB}__public LIKE 'wid%'`, true],
  ["SHOW FULL TABLES", true],
  ["SHOW FULL TABLES LIKE 'wid%'", true],
  ["SHOW TABLE STATUS", true],
  ["SHOW TABLE STATUS LIKE 'wid%'", true],
  ["DESCRIBE widgets", true],
  ["DESC widgets", true],
  [`DESCRIBE ${DB}__public.widgets`, true],
  ["SHOW COLUMNS FROM widgets", true],
  [`SHOW COLUMNS FROM widgets FROM ${DB}__public`, true],
  ["SHOW COLUMNS FROM widgets LIKE 'q%'", true],
  ["SHOW FULL COLUMNS FROM widgets", true],
  ["SHOW FIELDS FROM widgets", true],
  ["SHOW CREATE TABLE widgets", true],
  [`SHOW CREATE TABLE ${DB}__public.widgets`, true],
  ["SHOW INDEX FROM widgets", true],
  ["SHOW INDEXES FROM widgets", true],
  ["SHOW KEYS FROM widgets", true],
  ["SHOW VARIABLES LIKE 'version'", true],
  ["SHOW STATUS", false],
  ["SHOW PROCESSLIST", false],
  ["SHOW ENGINES", false],
  ["SHOW WARNINGS", false],
  ["SHOW COLLATION", false],
  ["SHOW CHARACTER SET", false],
  ["SHOW GRANTS", false],
  ["SHOW TRIGGERS", false],
  ["SHOW EVENTS", false],
  ["SHOW FUNCTION STATUS", false],
  ["SHOW PROCEDURE STATUS", false],
  ["SELECT DATABASE()", true],
  ["SELECT USER()", true],
  ["SELECT VERSION()", true],
  ["SELECT CONNECTION_ID()", true],
  ["SELECT @@version, @@version_comment", true],
  [`SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = '${DB}__public'`, true],
  [`SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'widgets'`, true],
  ["SELECT schema_name FROM information_schema.schemata", true],
  [`SELECT * FROM information_schema.statistics WHERE table_name = 'widgets'`, false],
  [`SELECT * FROM information_schema.key_column_usage WHERE table_name = 'widgets'`, true],
  [`SELECT * FROM information_schema.table_constraints WHERE table_name = 'widgets'`, true],
  ["SELECT * FROM information_schema.views", false],
  ["SELECT * FROM information_schema.routines", false],
  // LIMIT must actually apply on virtual tables (regression for task #154)
  ["SELECT table_name FROM information_schema.tables LIMIT 1", "limit1"],
];

const PG_PROBES = [
  // psql / GUI backbone
  [`SELECT n.nspname, c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'r'`, true],
  ["SELECT schemaname, tablename FROM pg_catalog.pg_tables", true],
  ["SELECT tablename FROM pg_tables WHERE schemaname = 'public'", true],
  ["SELECT nspname FROM pg_catalog.pg_namespace", true],
  ["SELECT oid, relname, relkind FROM pg_class", true],
  ["SELECT attname FROM pg_catalog.pg_attribute LIMIT 5", true],
  ["SELECT typname FROM pg_catalog.pg_type LIMIT 5", true],
  ["SELECT datname FROM pg_catalog.pg_database", true],
  ["SELECT proname FROM pg_catalog.pg_proc LIMIT 5", false],
  ["SELECT schemaname, viewname FROM pg_catalog.pg_views", false],
  ["SELECT indexname FROM pg_catalog.pg_indexes LIMIT 5", false],
  ["SELECT current_database()", true],
  ["SELECT current_schema()", true],
  ["SELECT current_user", true],
  ["SELECT version()", true],
  ["SELECT pg_backend_pid()", true],
  ["SHOW search_path", true],
  ["SHOW server_version", true],
  // information_schema on the pg wire (task #153)
  ["SELECT table_schema, table_name FROM information_schema.tables", true],
  ["SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'widgets'", true],
  ["SELECT schema_name FROM information_schema.schemata", true],
];

let fails = [], empties = [];
async function probe(label, run, [q, expect]) {
  try {
    const n = await run(q);
    if (expect === "limit1" && n !== 1) {
      console.log(`BUG   [${label}] ${q}  -> ${n} rows (LIMIT 1 ignored)`);
      fails.push(`${label}: ${q} (LIMIT ignored)`);
    } else if (expect === true && n === 0) {
      console.log(`EMPTY [${label}] ${q}`);
      empties.push(`${label}: ${q}`);
    } else {
      console.log(`OK    [${label}] ${q}  -> ${n} rows`);
    }
  } catch (e) {
    console.log(`FAIL  [${label}] ${q}  -> ${e.message.slice(0, 60)}`);
    fails.push(`${label}: ${q}`);
  }
}

for (const p of MYSQL_PROBES) await probe("mysql", async (q) => (await my.query(q))[0].length ?? 0, p);
await my.end();

const pgc = new pg.Client({ host: "127.0.0.1", port: PG_PORT, user: "root", database: DB });
await pgc.connect();
for (const p of PG_PROBES) await probe("pg", async (q) => (await pgc.query(q)).rows.length, p);
await pgc.end();

console.log(`\n=== SUMMARY: ${fails.length} failing, ${empties.length} empty-but-accepted ===`);
for (const f of fails) console.log("FAIL :", f);
for (const e of empties) console.log("EMPTY:", e);
process.exit(fails.length ? 1 : 0);
