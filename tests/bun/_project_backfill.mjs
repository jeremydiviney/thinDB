// Targeted per-project backfill for invoice_import_amortized drift repair.
// NOT a snapshot rebuild: scopes to one projectId — DELETE that project's
// rows in thinDB, stream the project's rows from prod, re-insert in bulk
// batches, verify counts. CDC keeps running; verify step catches any
// interleaved churn (re-run for that project if it moved).
//
//   node _project_backfill.mjs <projectId> [batchRows]
import fs from 'fs';
import mysql from 'mysql2';
import mysqlp from 'mysql2/promise';

const SP = 'C:/Users/jerem/AppData/Local/Temp/claude/C--development-thinDB/c2cce1cf-9c7d-4061-98b7-3a1c71c0bfd8/scratchpad';
const cfg = fs.readFileSync(SP + '/cdc_invoice_import_amortized.sql', 'utf8');
const host = cfg.match(/'hostname' *= *'([^']+)'/)[1];
const user = cfg.match(/'username' *= *'([^']+)'/)[1];
const pass = cfg.match(/'password' *= *'([^']+)'/)[1];

const project = Number(process.argv[2]);
const BATCH = Number(process.argv[3] || 10000);
if (!project) { console.error('usage: node _project_backfill.mjs <projectId>'); process.exit(2); }
const TABLE = 'invoice_import_amortized';

const prodStream = mysql.createConnection({ host, user, password: pass, database: 'wayroll', dateStrings: true, connectTimeout: 30000 });
const prodQ = mysqlp.createPool({ host, user, password: pass, database: 'wayroll', dateStrings: true, connectTimeout: 30000, connectionLimit: 2 });
const tdb = await mysqlp.createConnection({ host: '127.0.0.1', port: 13310, user: 'root', password: '' });
await tdb.query('USE wayroll_prod__public');

const [[{ c: prodBefore }]] = await prodQ.query(
  `SELECT COUNT(*) AS c FROM ${TABLE} WHERE projectId = ?`, [project]);
console.log(`project ${project}: prod=${prodBefore} rows; deleting thinDB copy...`);

const t0 = Date.now();
const [del] = await tdb.query(`DELETE FROM ${TABLE} WHERE projectId = ${project}`);
console.log(`deleted ${del.affectedRows} thinDB rows in ${((Date.now() - t0) / 1000).toFixed(1)}s`);

const [descr] = await prodQ.query(`DESCRIBE ${TABLE}`);
const cols = descr.map(d => d.Field);
const colList = cols.map(c => '`' + c + '`').join(',');
// datetime/timestamp columns must land as µs-epoch numeric text — that's what
// the Flink sink writes ("1783668412632213"); dateStrings gives us
// "2026-07-06 02:33:39.950069" which would pollute the string column with a
// second representation (and break MAX/ORDER BY, since '2...' > '1...').
const dtCols = new Set(descr.filter(d => /^(datetime|timestamp)/i.test(d.Type)).map(d => d.Field));
function toMicros(s) {
  const m = /^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?$/.exec(s);
  if (!m) return null;
  const ms = Date.parse(`${m[1]}T${m[2]}Z`);
  if (Number.isNaN(ms)) return null;
  const frac = (m[3] || '').padEnd(6, '0');
  return (BigInt(ms) * 1000n + BigInt(frac)).toString();
}
function serialize(c, v) {
  if (v != null && dtCols.has(c) && typeof v === 'string') {
    const us = toMicros(v);
    if (us !== null) return `'${us}'`;
  }
  return prodStream.escape(v);
}

let buf = [], inserted = 0, tIns = Date.now();
async function flushBatch() {
  if (!buf.length) return;
  const values = buf.join(',');
  buf = [];
  await tdb.query(`INSERT INTO ${TABLE} (${colList}) VALUES ${values}`);
}

await new Promise((resolve, reject) => {
  const q = prodStream.query(`SELECT ${colList} FROM ${TABLE} WHERE projectId = ${project}`);
  q.on('error', reject);
  q.on('result', (row) => {
    buf.push('(' + cols.map(c => serialize(c, row[c])).join(',') + ')');
    inserted++;
    if (buf.length >= BATCH) {
      prodStream.pause();
      flushBatch().then(() => prodStream.resume()).catch(reject);
    }
  });
  q.on('end', () => flushBatch().then(resolve).catch(reject));
});
console.log(`inserted ${inserted} rows in ${((Date.now() - tIns) / 1000).toFixed(1)}s`);

const [[{ c: prodAfter }]] = await prodQ.query(
  `SELECT COUNT(*) AS c FROM ${TABLE} WHERE projectId = ?`, [project]);
const [[{ c: tdbAfter }]] = await tdb.query(
  `SELECT COUNT(*) AS c FROM ${TABLE} WHERE projectId = ${project}`);
console.log(`verify: prod=${prodAfter} thindb=${tdbAfter} ` +
  (Number(prodAfter) === Number(tdbAfter) ? 'MATCH' : 'DRIFT (prod moved mid-backfill — re-run)'));

prodStream.end(); await prodQ.end(); await tdb.end();
