#!/usr/bin/env node
// Expression-conformance corpus: generated expression permutations run against
// an ephemeral thinDB server AND DuckDB, diffing errors and values. Measures
// dialect brittleness empirically ("how many of N generated queries fail or
// diverge?") and guards the expression-grammar refactors against regressions.
//
//   node tests/bun/conformance.mjs [--server <path>] [--duckdb <bin>] [--max N]
//
// Requires: mysql CLI + duckdb CLI on PATH; a built thindb-server
// (default zig-out/bin/thindb-server.exe). Writes full per-query results to
// _conformance_results.jsonl (gitignored); prints a class summary.
//
// Known deliberate dialect differences are filtered via KNOWN_DIFF patterns.
import { spawn, execFileSync } from 'node:child_process'
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const args = process.argv.slice(2)
function argOf(flag, dflt) { const i = args.indexOf(flag); return i >= 0 ? args[i + 1] : dflt }
const SERVER_BIN = argOf('--server', 'zig-out/bin/thindb-server.exe')
const DUCKDB_BIN = argOf('--duckdb', 'duckdb')
const MAX = Number(argOf('--max', '0')) // 0 = no cap
const PORT = 13498 + Math.floor(Math.random() * 400)

// ---------------------------------------------------------------------------
// Seed. Identical DDL semantics in both engines. No zeros in numeric columns
// (division-by-zero semantics deliberately differ: IEEE inf vs error).
// ---------------------------------------------------------------------------
const SEED = [
   `CREATE TABLE c (id BIGINT PRIMARY KEY, g INT, i INT, b BIGINT, d DOUBLE, dc DECIMAL(10,2), dt DATE, s VARCHAR(20), n INT)`,
   `INSERT INTO c VALUES ` + [
      `(1, 1, 3, 1000, 1.25, '10.50', '2026-01-05', 'alpha', 7)`,
      `(2, 1, -4, 2000, -2.5, '-3.75', '2026-02-11', 'beta', NULL)`,
      `(3, 1, 7, 3000, 0.75, '99.99', '2026-03-17', 'gamma', 2)`,
      `(4, 2, 12, 4000, 3.5, '0.25', '2026-04-23', 'delta', NULL)`,
      `(5, 2, -9, 5000, -0.125, '-42.42', '2026-05-29', 'alpha', 11)`,
      `(6, 2, 25, 6000, 2.25, '7.77', '2026-06-30', 'beta', 5)`,
      `(7, 3, 31, 7000, 5.5, '1.10', '2026-07-04', 'gamma', 3)`,
   ].join(', '),
]

// ---------------------------------------------------------------------------
// Generator: template families x substitution axes.
// ---------------------------------------------------------------------------
const NUM_ATOMS = ['i', 'b', 'd', 'dc', 'n', '7', '2.5']
const BINOPS = ['+', '-', '*', '/', '%']
const CMPS = ['<', '<=', '=', '<>', '>', '>=']
const FN1 = ['ROUND', 'ABS', 'CEIL', 'FLOOR']
const AGGS = ['SUM', 'AVG', 'MIN', 'MAX', 'COUNT']
const WINAGGS = ['SUM', 'AVG', 'MIN', 'MAX']

function* genQueries() {
   // 1. Scalar binary expressions as select items.
   for (const a of NUM_ATOMS) for (const op of BINOPS) for (const b of ['i', 'd', '7', '2.5', 'n']) {
      yield `SELECT id, ${a} ${op} ${b} AS v FROM c ORDER BY id`
   }
   // 2. Function over binary expression.
   for (const f of FN1) for (const a of ['i', 'd', 'dc', 'n']) for (const op of ['+', '*', '/']) {
      yield `SELECT id, ${f}(${a} ${op} 3) AS v FROM c ORDER BY id`
   }
   // 3. CASE forms: expression branches, expression conditions, NULL branches.
   for (const a of ['i', 'd', 'dc', 'n']) for (const cmp of ['>', '<=']) {
      yield `SELECT id, CASE WHEN ${a} ${cmp} 2 THEN ${a} ELSE 0 END AS v FROM c ORDER BY id`
      yield `SELECT id, CASE WHEN ${a} ${cmp} 2 THEN ${a} ELSE NULL END AS v FROM c ORDER BY id`
      yield `SELECT id, CASE WHEN (CASE WHEN ${a} ${cmp} 2 THEN 1 ELSE 0 END) = 1 THEN 10 ELSE 20 END AS v FROM c ORDER BY id`
      yield `SELECT id, 1 + CASE WHEN ${a} ${cmp} 2 THEN ${a} ELSE 0 END AS v FROM c ORDER BY id`
   }
   // 4. WHERE predicates: comparisons over expressions, col-col mixes, logic.
   for (const a of ['i', 'd', 'b', 'dc', 'n']) for (const cmp of CMPS) {
      yield `SELECT id FROM c WHERE ${a} ${cmp} 2 ORDER BY id`
   }
   for (const l of ['i', 'd', 'b']) for (const r of ['i', 'd', 'b']) {
      if (l !== r) yield `SELECT id FROM c WHERE ${l} > ${r} ORDER BY id`
   }
   for (const a of ['i', 'd']) {
      yield `SELECT id FROM c WHERE ${a} + 1 > 3 ORDER BY id`
      yield `SELECT id FROM c WHERE (${a} * 2) BETWEEN 1 AND 20 ORDER BY id`
      yield `SELECT id FROM c WHERE ${a} + 1 IN (4, 8, 13) ORDER BY id`
      yield `SELECT id FROM c WHERE (CASE WHEN ${a} > 0 THEN 1 ELSE 0 END) = 1 ORDER BY id`
      yield `SELECT id FROM c WHERE ABS(${a}) > 2 AND id < 6 ORDER BY id`
      yield `SELECT id FROM c WHERE NOT (${a} > 2 OR id > 5) ORDER BY id`
   }
   // Fractional literals against integer columns (MySQL folding semantics).
   for (const cmp of CMPS) yield `SELECT id FROM c WHERE i ${cmp} 2.5 ORDER BY id`
   yield `SELECT id FROM c WHERE i IN (2.5, 7, 12) ORDER BY id`
   yield `SELECT id FROM c WHERE i NOT IN (2.5, 7) AND i > 0 ORDER BY id`
   yield `SELECT id FROM c WHERE b IN (1000, 3000.0, 5000) ORDER BY id`
   yield `SELECT id FROM c WHERE n IS NULL ORDER BY id`
   yield `SELECT id FROM c WHERE n IS NOT NULL AND n > 2 ORDER BY id`
   yield `SELECT id FROM c WHERE s = 'alpha' ORDER BY id`
   yield `SELECT id FROM c WHERE s <> 'beta' AND i > 0 ORDER BY id`
   // 5. Aggregates: global and grouped, bare and inside expressions.
   for (const f of AGGS) for (const a of ['i', 'd', 'dc', 'n']) {
      yield `SELECT ${f}(${a}) AS v FROM c`
      yield `SELECT g, ${f}(${a}) AS v FROM c GROUP BY g ORDER BY g`
   }
   for (const a of ['i', 'd', 'n']) {
      yield `SELECT SUM(${a}) / COUNT(*) AS v FROM c`
      yield `SELECT g, SUM(${a}) / COUNT(*) AS v FROM c GROUP BY g ORDER BY g`
      yield `SELECT ROUND(SUM(${a})) AS v FROM c`
      yield `SELECT 1 + SUM(${a}) AS v FROM c`
      yield `SELECT SUM(${a} * 2) AS v FROM c`
      yield `SELECT g, SUM(${a}) AS v FROM c GROUP BY g HAVING SUM(${a}) > 0 ORDER BY g`
   }
   // 6. Windows: bare, framed, inside expressions, LAG defaults.
   for (const f of WINAGGS) for (const a of ['i', 'd', 'dc']) {
      yield `SELECT id, ${f}(${a}) OVER (PARTITION BY g ORDER BY id) AS v FROM c ORDER BY id`
      yield `SELECT id, ${f}(${a}) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM c ORDER BY id`
   }
   for (const a of ['i', 'd', 'dc']) {
      yield `SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS v FROM c ORDER BY id`
      yield `SELECT id, LAG(${a}, 1, 0) OVER (PARTITION BY g ORDER BY id) AS v FROM c ORDER BY id`
      yield `SELECT id, ${a} - LAG(${a}, 1, 0) OVER (PARTITION BY g ORDER BY id) AS v FROM c ORDER BY id`
      yield `SELECT id, SUM(${a}) OVER (PARTITION BY g ORDER BY id) / 2 AS v FROM c ORDER BY id`
      yield `SELECT id, CASE WHEN ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) < 2 THEN ${a} ELSE 0 END AS v FROM c ORDER BY id`
   }
   // 7. Date expressions.
   yield `SELECT id, DATE_ADD(dt, INTERVAL 1 MONTH) AS v FROM c ORDER BY id`
   yield `SELECT id FROM c WHERE dt <= DATE '2026-04-01' ORDER BY id`
   yield `SELECT MIN(dt) AS a, MAX(dt) AS b FROM c`
}

// ---------------------------------------------------------------------------
// Known deliberate dialect differences — matching queries are classified
// KNOWN_DIFF instead of VALUE_DIFF/errors. Keep this list SHORT and justified.
// ---------------------------------------------------------------------------
const KNOWN_DIFF = [
   { re: /%/, why: 'MOD sign/typing differs between engines for negative and float operands' },
   { re: /INTERVAL/, why: 'DuckDB returns TIMESTAMP for date+interval; thinDB (like MySQL) returns DATE' },
   { re: /dc \//, why: 'MySQL DECIMAL division keeps scale+4 (few sig figs for small values); DuckDB widens to DOUBLE' },
]

// ---------------------------------------------------------------------------
// Runners.
// ---------------------------------------------------------------------------
function runThinDB(sql) {
   try {
      const out = execFileSync('mysql', ['-h', '127.0.0.1', '-P', String(PORT), '-u', 'root', '-D', 'conf__public', '-B', '-N', '-e', sql], { encoding: 'utf8', timeout: 30000 })
      return { ok: true, rows: parseTsv(out) }
   } catch (e) {
      return { ok: false, err: String(e.stderr || e.message).trim().split('\n')[0].slice(0, 160) }
   }
}
function runDuck(dbPath, sql) {
   try {
      const out = execFileSync(DUCKDB_BIN, [dbPath, '-csv', '-noheader', '-c', sql], { encoding: 'utf8', timeout: 30000 })
      return { ok: true, rows: parseCsv(out) }
   } catch (e) {
      return { ok: false, err: String(e.stderr || e.message).trim().split('\n')[0].slice(0, 160) }
   }
}

function parseTsv(text) {
   return text.split('\n').filter(l => l.length).map(l => l.split('\t').map(normCell))
}
function parseCsv(text) {
   // Seed data contains no quoted commas; a plain split is sufficient here.
   return text.split('\n').filter(l => l.length).map(l => l.split(',').map(normCell))
}
function normCell(cRaw) {
   const c = cRaw.trim()
   if (c === 'NULL' || c === '') return { s: 'NULLV' }
   const f = Number(c)
   if (c.length && Number.isFinite(f) && /^-?[0-9.eE+-]+$/.test(c)) return { n: f }
   return { s: c }
}
// Numeric cells compare with relative tolerance: engines legitimately differ
// in representation precision (MySQL-style DECIMAL scale+4 vs DuckDB's
// double widening). A wrong VALUE differs far beyond 1e-6.
function cellsEqual(x, y) {
   if (x.s !== undefined || y.s !== undefined) return x.s === y.s
   if (x.n === y.n) return true
   const mag = Math.max(Math.abs(x.n), Math.abs(y.n))
   return Math.abs(x.n - y.n) <= Math.max(1e-9, mag * 1e-6)
}
function rowsEqual(a, b) {
   if (a.length !== b.length) return false
   for (let r = 0; r < a.length; r += 1) {
      if (a[r].length !== b[r].length) return false
      for (let cIdx = 0; cIdx < a[r].length; cIdx += 1) {
         if (!cellsEqual(a[r][cIdx], b[r][cIdx])) return false
      }
   }
   return true
}

async function main() {
   // Ephemeral thinDB server.
   const dataDir = mkdtempSync(join(tmpdir(), 'thindb-conf-'))
   const duckPath = join(dataDir, 'conf.duckdb')
   const server = spawn(SERVER_BIN, ['--data-dir', dataDir + '/db', '--bind', '127.0.0.1', '--mysql-port', String(PORT), '--pg-port', '0', '--native-port', '0'], { stdio: 'ignore' })
   const kill = () => { try { server.kill('SIGKILL') } catch { /* gone */ } }
   process.on('exit', kill)
   for (let i = 0; i < 60; i += 1) {
      await new Promise(r => setTimeout(r, 250))
      try { execFileSync('mysql', ['-h', '127.0.0.1', '-P', String(PORT), '-u', 'root', '-e', 'SELECT 1'], { stdio: 'ignore', timeout: 5000 }); break } catch { /* not up yet */ }
   }

   execFileSync('mysql', ['-h', '127.0.0.1', '-P', String(PORT), '-u', 'root', '-e', 'CREATE DATABASE conf'], { timeout: 15000 })
   for (const s of SEED) execFileSync('mysql', ['-h', '127.0.0.1', '-P', String(PORT), '-u', 'root', '-D', 'conf__public', '-e', s], { timeout: 15000 })
   for (const s of SEED) execFileSync(DUCKDB_BIN, [duckPath, '-c', s], { timeout: 15000 })

   const all = [...genQueries()]
   const queries = MAX > 0 ? all.slice(0, MAX) : all
   const results = []
   const counts = { MATCH: 0, VALUE_DIFF: 0, THINDB_ERR: 0, DUCK_ERR: 0, BOTH_ERR: 0, KNOWN_DIFF: 0 }
   for (const sql of queries) {
      const t = runThinDB(sql)
      const d = runDuck(duckPath, sql)
      let cls
      if (t.ok && d.ok) cls = rowsEqual(t.rows, d.rows) ? 'MATCH' : 'VALUE_DIFF'
      else if (!t.ok && d.ok) cls = 'THINDB_ERR'
      else if (t.ok && !d.ok) cls = 'DUCK_ERR'
      else cls = 'BOTH_ERR'
      if (cls !== 'MATCH' && KNOWN_DIFF.some(k => k.re.test(sql))) cls = 'KNOWN_DIFF'
      counts[cls] += 1
      results.push({ cls, sql, thindb: t.ok ? undefined : t.err, duckdb: d.ok ? undefined : d.err })
   }

   writeFileSync('_conformance_results.jsonl', results.map(r => JSON.stringify(r)).join('\n'))
   console.log(`\n=== conformance: ${queries.length} queries ===`)
   for (const [k, v] of Object.entries(counts)) console.log(`  ${k.padEnd(11)} ${v}`)
   const interesting = results.filter(r => r.cls === 'THINDB_ERR' || r.cls === 'VALUE_DIFF')
   console.log(`\n--- first ${Math.min(25, interesting.length)} thinDB errors / value diffs ---`)
   for (const r of interesting.slice(0, 25)) {
      console.log(`  [${r.cls}] ${r.sql}${r.thindb ? `\n      thindb: ${r.thindb}` : ''}`)
   }
   kill()
   try { rmSync(dataDir, { recursive: true, force: true }) } catch { /* best effort */ }
   process.exit(0)
}

main().catch(e => { console.error(e); process.exit(2) })
