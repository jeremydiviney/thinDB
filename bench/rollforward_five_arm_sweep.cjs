const fs = require('node:fs');
const path = require('node:path');
const Module = require('node:module');
const { performance } = require('node:perf_hooks');
const { drain_query } = require('./ordinary_sql_diagnosis.cjs');

const cases = [
  { label: 'base simple' }, { label: 'base cross', cross: true },
  { label: 'expanded simple', expanded: true }, { label: 'expanded cross', expanded: true, cross: true },
  { label: 'child simple', child: true }, { label: 'child cross', child: true, cross: true },
  { label: 'interval quarter', interval: 'quarter' }, { label: 'interval annual', interval: 'annual' },
  { label: 'fx latest', fx: 'LATEST' }, { label: 'fx average', fx: 'AVERAGE' },
  { label: 'noest simple', includeEstimates: false }, { label: 'plans simple', plans: true },
  { label: 'crossplans', plans: true, cross: true },
  { label: 'detail simple', detail: true }, { label: 'detail cross', detail: true, cross: true },
];
const arms = ['sr', 'sql', 'sql_keyed', 'zig_unkeyed', 'zig'];
const median = values => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];

async function main() {
  const [archive_arg, output_arg, project_arg, arm, dop_arg, label] = process.argv.slice(2);
  const project = Number(project_arg), dop = Number(dop_arg), item = cases.find(c => c.label === label);
  if (!archive_arg || !output_arg || ![1000049, 1000073].includes(project) || !arms.includes(arm) || ![12, 16].includes(dop) || !item) throw Error('Invalid benchmark arguments');
  const archive = path.resolve(archive_arg), output = path.resolve(output_arg);
  fs.mkdirSync(output, { recursive: true });
  const keyed = arm === 'sql_keyed' || arm === 'zig';
  const udf = arm === 'zig_unkeyed' || arm === 'zig';
  const app = fs.realpathSync(path.join(archive, 'app'));
  const app_require = Module.createRequire(path.join(app, 'package.json'));
  const mysql = app_require('mysql2'), create_pool = mysql.createPool;
  const session_sql = `SET pipeline_dop=${dop}; SET parallel_fragment_exec_instance_num=1; SET enable_runtime_adaptive_dop=false; SET enable_query_cache=false; SET enable_profile=false`;
  mysql.createPool = function (config) {
    if (config.host !== '127.0.0.1' || config.port !== (arm === 'sr' ? 9030 : 13311)) throw Error('Unexpected endpoint');
    const pool = create_pool.call(this, { ...config, connectionLimit: 1 });
    if (arm === 'sr') pool.on('connection', connection => connection.query(session_sql));
    return pool;
  };
  let calls = [], last_query, fingerprint = false;
  const load = Module._load;
  Module._load = function (request, parent) {
    const exports = load.apply(this, arguments);
    let file;
    try { file = Module._resolveFilename(request, parent).replaceAll('\\', '/'); } catch { return exports; }
    if (!file.endsWith('/src/helpers/doris.ts')) return exports;
    return { ...exports, execDorisSQL: async function (sql, params = {}, infile, final = false, rows_as_array = false, supplied) {
      if (!final) {
        const began = performance.now();
        const result = await exports.execDorisSQL(sql, params, infile, final, rows_as_array, supplied);
        calls.push({ kind: 'application_lookup', sql, ms: performance.now() - began, rows: result?.length });
        return result;
      }
      if (infile) throw Error('Unexpected final-query infile');
      const plain_sql = sql;
      if (arm === 'sql_keyed') {
        if (/\brf_\w+\s*\(/i.test(sql) || !/^WITH[ \t]+(?!KEYED\b)/m.test(sql)) throw Error('Expected ordinary SQL before region declaration');
        sql = sql.replace(/^WITH[ \t]+(?!KEYED\b)/m, match => match + 'KEYED BY (customerNumberLC) ');
        if (sql.replace('KEYED BY (customerNumberLC) ', '') !== plain_sql) throw Error('Unexpected SQL rewrite');
      }
      const has_udf = /\brf_\w+\s*\(/i.test(sql), has_region = /\bKEYED\s+BY\b/i.test(sql);
      if (has_udf !== udf || has_region !== keyed) throw Error('Method switches did not produce the requested SQL');
      last_query = { sql, params, plain_sql, has_udf, has_region };
      const connection = supplied ?? await exports.getDorisConnection();
      try { return await drain_query(connection.connection ?? connection, sql, params, fingerprint); }
      finally { if (!supplied) connection.release(); }
    } };
  };
  Object.assign(process.env, {
    TSX_TSCONFIG_PATH: path.join(app, 'tsconfig.json'), BENCH_DISABLE_QUERY_CACHE: '1',
    THINDB_SQL_FNS: udf ? '1' : '0', THINDB_ZIG_FNS: udf ? '1' : '0', THINDB_KEYED_REGIONS: keyed ? '1' : '0',
  });
  for (const unit of ['UPDOWN', 'ESTIMATES', 'GAPFILL', 'EXPANDED', 'CURRENCY', 'CUSTMONTHS']) process.env['THINDB_ZIG_' + unit] = udf ? '1' : '0';
  const began = performance.now();
  app_require('tsx/cjs'); app_require('reflect-metadata');
  const doris = app_require(path.join(app, 'src/helpers/doris.ts'));
  const { getDorisRollforward } = app_require(path.join(app, 'src/workers/rollforwardProcess/helpers.ts'));
  const { RevenueModelType } = app_require(path.join(app, 'src/models/reportCustomerRevenueRollforward.ts'));
  const { CurrencyExchangeRateToUse } = app_require(path.join(app, 'src/helpers/currency.ts'));
  const { getRevenueModel } = app_require(path.join(app, 'src/routes/workspace/project/reporting/rollforwardReports/daily.ts'));
  const module_setup_ms = performance.now() - began;
  const previous = JSON.parse(fs.readFileSync(path.join(archive, 'results', project + '.json')));
  const as_of = previous.cases.find(c => c.label === label).asOf;
  const connection_start = performance.now();
  await doris.makeDorisConnection({ type: 'mysql', connectorPackage: 'mysql2', host: '127.0.0.1',
    username: 'root', timezone: 'local', connectionLimit: 1, port: arm === 'sr' ? 9030 : 13311,
    password: arm === 'sr' ? process.env.SR_PW : '', database: arm === 'sr' ? 'wayroll' : 'wayroll_prod__public' });
  const report = { project, company: project === 1000049 ? 'Sierra' : 'AirDNA', arm, dop, label, as_of,
    division: previous.divisionId, hash_range: ['a', 'd'], module_setup_ms,
    connection_setup_ms: performance.now() - connection_start, started_at: new Date().toISOString(), samples: [] };
  try {
    if (arm === 'sr') {
      const connection = await doris.getDorisConnection();
      try {
        const [settings] = await connection.query("SHOW VARIABLES WHERE Variable_name IN ('pipeline_dop','parallel_fragment_exec_instance_num','enable_runtime_adaptive_dop','enable_query_cache','enable_profile')");
        report.session = settings;
        const actual = Object.fromEntries(settings.map(r => [r.Variable_name, String(r.Value).toLowerCase()]));
        if (actual.pipeline_dop !== String(dop) || actual.parallel_fragment_exec_instance_num !== '1' || actual.enable_runtime_adaptive_dop !== 'false' || actual.enable_query_cache !== 'false' || actual.enable_profile !== 'false') throw Error('StarRocks settings did not take effect');
      } finally { connection.release(); }
    }
    for (let iteration = 0; iteration < 7; iteration++) {
      fingerprint = iteration === 6;
      calls = [];
      const offset = arm === 'sr' ? null : fs.statSync(path.join(output, 'server.err')).size;
      const cpu = process.cpuUsage(), start = performance.now();
      let plans;
      if (item.plans) {
        const condition = item.cross ? `divisionId IN (SELECT id FROM division WHERE projectId=${project})` : `divisionId=${previous.divisionId}`;
        plans = (await doris.execDorisSQL(`SELECT DISTINCT planId FROM invoice_import_amortized WHERE ${condition} AND planId IS NOT NULL`)).map(r => String(r.planId));
      }
      const result = await getDorisRollforward({
        revenueModel: getRevenueModel(RevenueModelType.WayrollNetMRR, project).modelType,
        projectId: project, divisionIds: [item.cross ? -2 : previous.divisionId], currentDate: new Date(as_of), externalPlanIds: plans,
        customerNumberHashRange: { hashStart: 'a', hashEnd: 'd' }, childCustomer: item.child ?? false,
        targetCurrency: 'USD', includeEstimates: item.includeEstimates ?? true, interval: item.interval ?? 'month',
        currencyExchangeRateToUse: CurrencyExchangeRateToUse[item.fx ?? 'INVOICE_DATE'], expandedRollforward: item.expanded ?? false,
      }, !item.detail);
      const ms = performance.now() - start, used = process.cpuUsage(cpu);
      const trace = offset === null ? '' : fs.readFileSync(path.join(output, 'server.err')).subarray(offset).toString();
      const engaged = (trace.match(/region engaged/g) ?? []).length;
      if (arm !== 'sr' && Boolean(engaged) !== keyed) throw Error('Unexpected region engagement');
      if (result.decoded_rows !== 0 || typeof result.rows !== 'number') throw Error('Packet drain did not intercept final query');
      const sample = { iteration, warmup: iteration === 0, validation: fingerprint, ms,
        client_cpu_ms: (used.user + used.system) / 1000, calls, plans, engaged, ...result };
      report.samples.push(sample);
      if (offset !== null) fs.writeFileSync(path.join(output, `sample-${iteration}.trace`), trace);
      fs.writeFileSync(path.join(output, 'query.json'), JSON.stringify(last_query, null, 2));
      fs.writeFileSync(path.join(output, 'result.json'), JSON.stringify(report, null, 2));
      console.log(JSON.stringify({ project, arm, label, iteration, ms, rows: result.rows, validation: fingerprint }));
    }
    report.median_ms = median(report.samples.filter(s => !s.warmup && !s.validation).map(s => s.ms));
    fs.writeFileSync(path.join(output, 'result.json'), JSON.stringify(report, null, 2));
  } finally {
    await doris.closeDorisConnectionPool();
    await doris.closeDorisDataSource().catch(error => { if (error.message !== 'DataSource default Not Found') throw error; });
  }
}
if (require.main === module) main().catch(error => { console.error(error.message); process.exitCode = 1; }).finally(() => process.exit(process.exitCode ?? 0));
module.exports = { cases, arms };
