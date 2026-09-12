const fs = require('node:fs');
const path = require('node:path');
const Module = require('node:module');
const { performance } = require('node:perf_hooks');
const { createHash } = require('node:crypto');

const cases = [
  { label: 'base simple' }, { label: 'noest simple', includeEstimates: false },
  { label: 'plans simple', plans: true }, { label: 'crossplans', plans: true, cross: true },
  { label: 'base cross', cross: true }, { label: 'child cross', child: true, cross: true },
  { label: 'detail cross', detail: true, cross: true },
  { label: 'expanded simple', expanded: true }, { label: 'interval quarter', interval: 'quarter' },
];

function drain_query(connection, sql, params, fingerprint) {
  return new Promise((resolve, reject) => {
    const sets = [], acknowledgements = [];
    const start = performance.now();
    const command = connection.query(sql, params, error => {
      if (error) return reject(error);
      if (command._rowParser !== null) return reject(Error('Discard mode decoded rows'));
      const result = sets.filter(Boolean).at(-1);
      if (!result) return reject(Error('Missing result set'));
      resolve({ ...result, digest: fingerprint ? result.digest.map(n => n.toString(16).padStart(8, '0')).join('') : undefined,
        wire_ms: performance.now() - start, acknowledgements, decoded_rows: 0 });
    });
    const original = Object.getPrototypeOf(command);
    command.doneInsert = function (...args) {
      acknowledgements.push({ index: this._resultIndex, ms: performance.now() - start });
      return original.doneInsert.apply(this, args);
    };
    command.readField = function () {
      if (this._receivedFieldsCount === 0) sets[this._resultIndex] = {
        columns: this._fieldCount, rows: 0, payload_bytes: 0,
        metadata_ms: performance.now() - start, digest: Array(8).fill(0),
      };
      this._receivedFieldsCount++;
      return this._receivedFieldsCount === this._fieldCount ? original.fieldsEOF : this.readField;
    };
    command.row = function (packet, conn) {
      const set = sets[this._resultIndex];
      if (packet.isEOF()) {
        set.eof_ms = performance.now() - start;
        return original.row.call(this, packet, conn);
      }
      if (!set.rows) set.first_row_ms = performance.now() - start;
      set.last_row_ms = performance.now() - start;
      set.rows++;
      set.payload_bytes += packet.length() - 4;
      if (fingerprint) {
        const hash = createHash('sha256').update(packet.buffer.subarray(packet.start + 4, packet.end)).digest();
        for (let i = 0; i < 8; i++) set.digest[i] = (set.digest[i] + hash.readUInt32LE(i * 4)) >>> 0;
      }
      return this.row;
    };
  });
}

async function main() {
  const [archive_arg, output_arg, engine, dop_arg, label, mode = 'timing'] = process.argv.slice(2);
  const dop = Number(dop_arg), item = cases.find(c => c.label === label);
  if (!archive_arg || !output_arg || !['sr', 'thin'].includes(engine) || ![12, 16].includes(dop) || !item || !['timing', 'profile'].includes(mode)) {
    throw Error('Usage: node bench/ordinary_sql_diagnosis.cjs ARCHIVE OUTPUT sr|thin 12|16 "CASE" [timing|profile]');
  }
  const archive = path.resolve(archive_arg), output = path.resolve(output_arg);
  fs.mkdirSync(output, { recursive: true });
  const app = fs.realpathSync(path.join(archive, 'app'));
  const app_require = Module.createRequire(path.join(app, 'package.json'));
  const mysql = app_require('mysql2');
  const create_pool = mysql.createPool;
  const session_sql = `SET pipeline_dop=${dop}; SET parallel_fragment_exec_instance_num=1; SET enable_runtime_adaptive_dop=false; SET enable_query_cache=false; SET enable_profile=false`;
  mysql.createPool = function (config) {
    if (config.host !== '127.0.0.1' || config.port !== (engine === 'thin' ? 13311 : 9030)) throw Error('Unexpected database endpoint');
    const pool = create_pool.call(this, { ...config, connectionLimit: 1 });
    if (engine === 'sr') pool.on('connection', connection => connection.query(session_sql));
    return pool;
  };
  let calls = [], last_query, fingerprint = false, profile_iteration = 0;
  const load = Module._load;
  Module._load = function (request, parent) {
    const exports = load.apply(this, arguments);
    let file;
    try { file = Module._resolveFilename(request, parent).replaceAll('\\', '/'); } catch { return exports; }
    if (!file.endsWith('/src/helpers/doris.ts')) return exports;
    return { ...exports, execDorisSQL: async function (sql, params = {}, infile, final = false, rows_as_array = false, supplied) {
      if (!final) {
        const start = performance.now();
        const result = await exports.execDorisSQL(sql, params, infile, final, rows_as_array, supplied);
        calls.push({ kind: 'application_lookup', sql, ms: performance.now() - start, rows: result?.length });
        return result;
      }
      if (infile || /\bKEYED\s+BY\b|\brf_\w+\s*\(/i.test(sql)) throw Error('Expected ordinary SQL');
      last_query = { sql, params };
      const connection = supplied ?? await exports.getDorisConnection();
      try {
        const raw = connection.connection ?? connection;
        if (mode === 'profile') {
          const boundary = sql.indexOf('\nWITH ');
          if (boundary < 0) throw Error('Missing outer WITH boundary');
          const prefix = sql.slice(0, boundary), settings = prefix.indexOf('SET @');
          if (settings < 0) throw Error('Missing session variable setup');
          for (const [kind, text] of [['temporary_table_setup', prefix.slice(0, settings)], ['variable_setup', prefix.slice(settings)]]) {
            if (!text.trim()) continue;
            const began = performance.now();
            await connection.query(text, params);
            calls.push({ kind, ms: performance.now() - began });
          }
          if (engine === 'sr') {
            await connection.query('SET pipeline_profile_level=1; SET enable_profile=true');
            const [rows] = await connection.query('EXPLAIN ANALYZE ' + sql.slice(boundary), params);
            const explain = rows.map(r => Object.values(r).join('\t')).join('\n');
            fs.writeFileSync(path.join(output, 'explain-analyze.txt'), explain);
            fs.writeFileSync(path.join(output, `explain-analyze-${profile_iteration}.txt`), explain);
            const [id] = await connection.query('SELECT last_query_id() AS id');
            fs.writeFileSync(path.join(output, 'profile-query-id.json'), JSON.stringify(id, null, 2));
            const [profile] = await connection.query('SELECT get_query_profile(?) AS profile', [id[0].id]);
            if (!profile[0].profile) throw Error('Missing raw StarRocks profile');
            fs.writeFileSync(path.join(output, 'profile.txt'), profile[0].profile);
            fs.writeFileSync(path.join(output, `profile-${profile_iteration}.txt`), profile[0].profile);
            return { profile: true };
          }
          return await drain_query(raw, sql.slice(boundary), params, false);
        }
        return await drain_query(raw, sql, params, fingerprint);
      } finally { if (!supplied) connection.release(); }
    } };
  };
  Object.assign(process.env, {
    TSX_TSCONFIG_PATH: path.join(app, 'tsconfig.json'), BENCH_DISABLE_QUERY_CACHE: '1',
    THINDB_SQL_FNS: '0', THINDB_ZIG_FNS: '0', THINDB_KEYED_REGIONS: '0',
  });
  for (const unit of ['UPDOWN', 'ESTIMATES', 'GAPFILL', 'EXPANDED', 'CURRENCY', 'CUSTMONTHS']) process.env['THINDB_ZIG_' + unit] = '0';
  const app_start = performance.now();
  app_require('tsx/cjs'); app_require('reflect-metadata');
  const doris = app_require(path.join(app, 'src/helpers/doris.ts'));
  const { getDorisRollforward } = app_require(path.join(app, 'src/workers/rollforwardProcess/helpers.ts'));
  const { RevenueModelType } = app_require(path.join(app, 'src/models/reportCustomerRevenueRollforward.ts'));
  const { CurrencyExchangeRateToUse } = app_require(path.join(app, 'src/helpers/currency.ts'));
  const { getRevenueModel } = app_require(path.join(app, 'src/routes/workspace/project/reporting/rollforwardReports/daily.ts'));
  const module_setup_ms = performance.now() - app_start;
  const previous = JSON.parse(fs.readFileSync(path.join(archive, 'results/1000049.json')));
  const as_of = previous.cases.find(c => c.label === label).asOf;
  const connect_start = performance.now();
  await doris.makeDorisConnection({ type: 'mysql', connectorPackage: 'mysql2', host: '127.0.0.1',
    username: 'root', timezone: 'local', connectionLimit: 1, port: engine === 'sr' ? 9030 : 13311,
    password: engine === 'sr' ? process.env.SR_PW : '', database: engine === 'sr' ? 'wayroll' : 'wayroll_prod__public' });
  const connection_setup_ms = performance.now() - connect_start;
  const report = { engine, dop, label, mode, as_of, project: 1000049, division: previous.divisionId,
    hash_range: ['a', 'd'], started_at: new Date().toISOString(), module_setup_ms, connection_setup_ms, samples: [] };
  const receipt = await doris.getDorisConnection();
  try {
    if (engine === 'sr') {
      const [settings] = await receipt.query("SHOW VARIABLES WHERE Variable_name IN ('pipeline_dop','parallel_fragment_exec_instance_num','enable_runtime_adaptive_dop','enable_query_cache','enable_profile')");
      report.session = settings;
      const actual = Object.fromEntries(settings.map(r => [r.Variable_name, String(r.Value).toLowerCase()]));
      if (actual.pipeline_dop !== String(dop) || actual.enable_query_cache !== 'false' || actual.enable_runtime_adaptive_dop !== 'false' || actual.parallel_fragment_exec_instance_num !== '1' || actual.enable_profile !== 'false') throw Error('Session settings did not take effect');
    }
  } finally { receipt.release(); }
  try {
    for (let iteration = 0; iteration < (mode === 'profile' ? 2 : 7); iteration++) {
      profile_iteration = iteration;
      fingerprint = mode === 'timing' && iteration === 6;
      calls = [];
      const trace_offset = engine === 'thin' && mode === 'profile' ? fs.statSync(path.join(output, 'server.err')).size : null;
      const start = performance.now(), cpu = process.cpuUsage();
      let plans;
      if (item.plans) {
        const condition = item.cross ? 'divisionId IN (SELECT id FROM division WHERE projectId=1000049)' : `divisionId=${previous.divisionId}`;
        plans = (await doris.execDorisSQL(`SELECT DISTINCT planId FROM invoice_import_amortized WHERE ${condition} AND planId IS NOT NULL`)).map(r => String(r.planId));
        if (JSON.stringify(plans) !== '[""]') throw Error('Plan selectivity changed');
      }
      const result = await getDorisRollforward({
        revenueModel: getRevenueModel(RevenueModelType.WayrollNetMRR, 1000049).modelType,
        projectId: 1000049, divisionIds: [item.cross ? -2 : previous.divisionId], currentDate: new Date(as_of), externalPlanIds: plans,
        customerNumberHashRange: { hashStart: 'a', hashEnd: 'd' }, childCustomer: item.child ?? false,
        targetCurrency: 'USD', includeEstimates: item.includeEstimates ?? true, interval: item.interval ?? 'month',
        currencyExchangeRateToUse: CurrencyExchangeRateToUse.INVOICE_DATE, expandedRollforward: item.expanded ?? false,
      }, !item.detail);
      const ms = performance.now() - start, used = process.cpuUsage(cpu);
      if (trace_offset !== null) fs.writeFileSync(path.join(output, `profile-${iteration}.trace`), fs.readFileSync(path.join(output, 'server.err')).subarray(trace_offset));
      report.samples.push({ iteration, warmup: iteration === 0, validation: fingerprint,
        ms, client_cpu_ms: (used.user + used.system) / 1000, calls, plans, ...result });
      fs.writeFileSync(path.join(output, 'query.json'), JSON.stringify(last_query, null, 2));
      fs.writeFileSync(path.join(output, 'result.json'), JSON.stringify(report, null, 2));
      console.log(JSON.stringify({ engine, dop, label, iteration, ms, rows: result.rows, validation: fingerprint }));
    }
    if (item.detail) {
      const connection = await doris.getDorisConnection();
      try {
        const [sets, fields] = await connection.query({ sql: last_query.sql, rowsAsArray: true }, last_query.params);
        const value = { columns: fields.at(-1).map(f => ({ name: f.name, type: f.columnType })), rows: sets.at(-1) };
        fs.writeFileSync(path.join(output, 'decoded-values.json.gz'), require('node:zlib').gzipSync(JSON.stringify(value)));
        const [raw_sets, raw_fields] = await connection.query({ sql: last_query.sql, rowsAsArray: true, typeCast: field => field.string() }, last_query.params);
        const raw_value = { columns: raw_fields.at(-1).map(f => ({ name: f.name, type: f.columnType })), rows: raw_sets.at(-1) };
        fs.writeFileSync(path.join(output, 'raw-values.json.gz'), require('node:zlib').gzipSync(JSON.stringify(raw_value)));
      } finally { connection.release(); }
    }
  } finally {
    await doris.closeDorisConnectionPool();
    await doris.closeDorisDataSource().catch(error => {
      if (error.message !== 'DataSource default Not Found') throw error;
    });
  }
}

if (require.main === module) main().catch(error => { console.error(error.message); process.exitCode = 1; }).finally(() => process.exit(process.exitCode ?? 0));
module.exports = { cases, drain_query };
