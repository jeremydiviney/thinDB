const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');
const { drain_query } = require('./ordinary_sql_diagnosis.cjs');

async function main() {
  const [archive, query_file, output, cte, include_not_materialized = 'yes'] = process.argv.slice(2);
  if (!cte || !/^[a-z_][a-z0-9_]*$/i.test(cte)) throw Error('Usage: node bench/ordinary_sql_materialization.cjs ARCHIVE QUERY_JSON OUTPUT CTE');
  fs.mkdirSync(output, { recursive: true });
  const mysql = createRequire(path.join(fs.realpathSync(path.join(archive, 'app')), 'package.json'))('mysql2');
  const source = JSON.parse(fs.readFileSync(query_file));
  const pattern = new RegExp(`\\b${cte} AS \\(`, 'g');
  if ([...source.sql.matchAll(pattern)].length !== 1) throw Error('Expected exactly one ordinary CTE declaration');
  const connection = mysql.createConnection({ host: '127.0.0.1', port: 13311, user: 'root', password: '',
    database: 'wayroll_prod__public', namedPlaceholders: true, multipleStatements: true });
  if (!['yes', 'no'].includes(include_not_materialized)) throw Error('Expected yes/no for NOT MATERIALIZED control');
  const variants = [
    { name: 'original', sql: source.sql },
    { name: 'materialized', sql: source.sql.replace(pattern, `${cte} AS MATERIALIZED (`) },
    { name: 'not-materialized', sql: source.sql.replace(pattern, `${cte} AS NOT MATERIALIZED (`) },
  ].filter(v => include_not_materialized === 'yes' || v.name !== 'not-materialized');
  const samples = [];
  const log_file = path.join(output, 'server.err');
  try {
    for (let iteration = 0; iteration < 7; iteration++) {
      const ordered = iteration % 2 ? [...variants].reverse() : variants;
      for (const variant of ordered) {
        const offset = fs.existsSync(log_file) ? fs.statSync(log_file).size : null;
        const result = await drain_query(connection, variant.sql, source.params, iteration === 6);
        if (offset !== null) fs.writeFileSync(path.join(output, `${variant.name}-${iteration}.trace`), fs.readFileSync(log_file).subarray(offset));
        samples.push({ variant: variant.name, iteration, warmup: iteration === 0, validation: iteration === 6, ...result });
        fs.writeFileSync(path.join(output, 'materialization.json'), JSON.stringify({ query_file, cte, samples }, null, 2));
      }
    }
    const validated = samples.filter(s => s.validation);
    if (validated.some(s => s.rows !== validated[0].rows || s.columns !== validated[0].columns || s.digest !== validated[0].digest)) throw Error('Materialization changed the result');
    console.log(JSON.stringify({ cte, fingerprint_match: true, variants: variants.map(v => ({ name: v.name,
      times: samples.filter(s => s.variant === v.name && !s.warmup && !s.validation).map(s => s.wire_ms) })) }));
  } finally { await connection.promise().end(); }
}
main().catch(error => { console.error(error.message); process.exitCode = 1; });
