const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');
const { gunzipSync } = require('node:zlib');
const { cases } = require('./ordinary_sql_diagnosis.cjs');
const median = values => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];
const stats = values => ({ median: median(values), min: Math.min(...values), max: Math.max(...values) });
const same_result = (a, b) => a.rows === b.rows && a.columns === b.columns && a.digest === b.digest;

const [root_arg, archive_arg] = process.argv.slice(2);
if (!root_arg || !archive_arg) throw Error('Usage: node bench/ordinary_sql_report.cjs RESULTS ARCHIVE');
const root = path.resolve(root_arg), archive = path.resolve(archive_arg);
const baseline = JSON.parse(fs.readFileSync(path.join(archive, 'five-arm-results-thin/1000049.json')));
const results = [], samples = [], validations = [], profiles = [];
for (const folder of fs.readdirSync(root)) {
  const dir = path.join(root, folder);
  if (!fs.existsSync(path.join(dir, 'complete.json'))) continue;
  if (!fs.existsSync(path.join(dir, 'result.json'))) continue;
  const report = JSON.parse(fs.readFileSync(path.join(dir, 'result.json')));
  if (report.mode === 'profile') {
    const profile = { folder, label: report.label, engine: report.engine, dop: report.dop, sample: report.samples.at(-1) };
    if (report.engine === 'thin') {
      const trace = fs.readFileSync(path.join(dir, 'profile-1.trace'), 'utf8');
      profile.stages = [...trace.matchAll(/\[cte\] stage#(\d+)\s+(\S+)\s+rows=(\d+)\s+setup=\s*([\d.]+)ms execute=\s*([\d.]+)ms teardown=\s*([\d.]+)ms/g)].map(m => ({ id: +m[1], name: m[2], rows: +m[3], setup_ms: +m[4], exclusive_ms: +m[5], teardown_ms: +m[6] }));
      const last = trace.slice(trace.lastIndexOf('[self] query'));
      profile.exclusive_operators = [...last.matchAll(/\[self\]\s+(\w+)\s+([\d.]+) ms\s+([\d.]+)%\s+\((\d+) calls\)/g)].map(m => ({ operator: m[1], ms: +m[2], percent: +m[3], calls: +m[4] }));
      profile.parallel_scans = [...trace.matchAll(/\[pscan\] threads=(\d+)\(eff=(\d+)\) chunks=(\d+) drain_wall=([\d.]+)ms survivors=(\d+)/g)].map(m => ({ requested: +m[1], effective: +m[2], chunks: +m[3], drain_ms: +m[4], rows: +m[5] }));
      profile.phase_ms = Object.fromEntries([...last.matchAll(/\[hprof\]\s+([\w.()]+)\s+([\d.]+) ms/g)].map(m => [m[1], +m[2]]));
    } else {
      const raw = fs.readFileSync(path.join(dir, 'profile.txt'), 'utf8');
      profile.pipeline_dop_counts = {};
      for (const m of raw.matchAll(/^\s+- DegreeOfParallelism: (\d+)$/gm)) profile.pipeline_dop_counts[m[1]] = (profile.pipeline_dop_counts[m[1]] ?? 0) + 1;
      const text = fs.readFileSync(path.join(dir, 'explain-analyze.txt'), 'utf8').replace(/\x1b\[[0-9;]*m/g, '');
      profile.summary = text.slice(0, text.indexOf('    Top Most Memory-consuming Nodes:'));
    }
    profiles.push(profile);
  }
  if (report.mode !== 'timing') continue;
  const measured = report.samples.filter(s => !s.warmup && !s.validation);
  const validation = report.samples.filter(s => s.validation);
  if (measured.length !== 5 || validation.length !== 1 || report.samples.length !== 7) throw Error('Incomplete block: ' + folder);
  if (measured.some(s => s.decoded_rows !== 0 || s.rows !== validation[0].rows || s.columns !== validation[0].columns)) throw Error('Result shape changed: ' + folder);
  const summary = { folder, engine: report.engine, dop: report.dop, label: report.label,
    rows: validation[0].rows, columns: validation[0].columns, digest: validation[0].digest,
    total_ms: stats(measured.map(s => s.ms)), wire_ms: stats(measured.map(s => s.wire_ms)),
    first_row_ms: measured[0].first_row_ms === undefined ? null : stats(measured.map(s => s.first_row_ms)),
    response_tail_ms: measured[0].first_row_ms === undefined ? null : stats(measured.map(s => s.wire_ms - s.first_row_ms)),
    eof_tail_ms: measured[0].last_row_ms === undefined ? null : stats(measured.map(s => s.wire_ms - s.last_row_ms)),
    application_ms: stats(measured.map(s => s.ms - s.wire_ms)),
    lookup_ms: stats(measured.map(s => s.calls.reduce((sum, c) => sum + c.ms, 0))),
    module_setup_ms: report.module_setup_ms, connection_setup_ms: report.connection_setup_ms };
  results.push(summary);
  for (const sample of report.samples) samples.push({ folder, engine: report.engine, dop: report.dop, label: report.label, ...sample });
  if (report.engine === 'thin') {
    const prior = baseline.cases.find(c => c.label === report.label).samples.find(s => s.arm === 'sql' && s.validation);
    validations.push({ folder, comparison: 'archived-thin-fingerprint', match: same_result(summary, { ...prior, digest: prior.wireDigest }) });
  }
}
for (const item of cases) for (const dop of [12, 16]) {
  const thin = results.find(r => r.folder === `timing-${dop}-${item.label.replaceAll(' ', '-')}-thin`);
  const sr = results.find(r => r.folder === `timing-${dop}-${item.label.replaceAll(' ', '-')}-sr`);
  if (thin && sr) validations.push({ label: item.label, dop, comparison: 'cross-engine-raw-fingerprint', match: same_result(thin, sr) });
}
for (const item of cases) for (const engine of ['thin', 'sr']) {
  const pair = [12, 16].map(dop => results.find(r => r.folder === `timing-${dop}-${item.label.replaceAll(' ', '-')}-${engine}`));
  if (pair.every(Boolean)) validations.push({ label: item.label, engine, comparison: 'DOP-12-to-16', match: same_result(...pair) });
}
fs.writeFileSync(path.join(root, 'summary.json'), JSON.stringify({ results, validations }, null, 2));
fs.writeFileSync(path.join(root, 'profiles.json'), JSON.stringify(profiles, null, 2));
fs.writeFileSync(path.join(root, 'samples.json'), JSON.stringify(samples, null, 2));
const fields = ['folder', 'engine', 'dop', 'label', 'iteration', 'warmup', 'validation', 'ms', 'wire_ms', 'first_row_ms', 'last_row_ms', 'eof_ms', 'rows', 'columns', 'payload_bytes', 'client_cpu_ms'];
fs.writeFileSync(path.join(root, 'samples.csv'), [fields.join(','), ...samples.map(s => fields.map(k => JSON.stringify(s[k] ?? '')).join(','))].join('\n') + '\n');
const rows = ['| Case | DOP | StarRocks median (range), ms | ThinDB median (range), ms | Thin/SR |', '|---|---:|---:|---:|---:|'];
for (const item of cases) for (const dop of [12, 16]) {
  const pair = ['sr', 'thin'].map(engine => results.find(r => r.folder === `timing-${dop}-${item.label.replaceAll(' ', '-')}-${engine}`));
  if (!pair.every(Boolean)) continue;
  const fmt = r => `${r.total_ms.median.toFixed(0)} (${r.total_ms.min.toFixed(0)}–${r.total_ms.max.toFixed(0)})`;
  rows.push(`| ${item.label} | ${dop} | ${fmt(pair[0])} | ${fmt(pair[1])} | ${(pair[1].total_ms.median / pair[0].total_ms.median).toFixed(2)} |`);
}
fs.writeFileSync(path.join(root, 'timing-table.md'), rows.join('\n') + '\n');
const packet_gaps = [];
for (const folder of fs.readdirSync(root).filter(name => name.startsWith('transport-'))) {
  const file = path.join(root, folder, 'packets.txt');
  if (!fs.existsSync(file)) continue;
  const connections = new Map(), gaps = [];
  for (const line of fs.readFileSync(file, 'utf8').split('\n')) {
    const match = line.match(/^(\d+\.\d+) IP 127\.0\.0\.1\.(\d+) > 127\.0\.0\.1\.(\d+):.*length (\d+)/);
    if (!match) continue;
    const time = +match[1], source = +match[2], destination = +match[3], length = +match[4];
    const port = source === 13311 ? destination : source;
    const state = connections.get(port) ?? {};
    connections.set(port, state);
    if (source !== 13311) { if (!length) state.ack = time; continue; }
    const sequence = line.match(/seq (\d+):(\d+)/);
    if (!length || !sequence) continue;
    const current = { time, start: +sequence[1], end: +sequence[2] };
    const gap_ms = 1000 * (time - state.last?.time), ack_to_send_ms = 1000 * (time - state.ack);
    if (state.last?.end === current.start && gap_ms > 35 && gap_ms < 60 && ack_to_send_ms >= 0 && ack_to_send_ms < 0.1) {
      gaps.push({ at: time, gap_ms, ack_to_send_ms, length });
    }
    state.last = current;
  }
  packet_gaps.push({ folder, gaps });
}
fs.writeFileSync(path.join(root, 'packet-gaps.json'), JSON.stringify(packet_gaps, null, 2));
const exact_value_checks = [];
for (const dop of [12, 16]) {
  const files = ['thin', 'sr'].map(engine => path.join(root, `timing-${dop}-detail-cross-${engine}`, 'raw-values.json.gz'));
  if (!files.every(fs.existsSync)) continue;
  const values = files.map(file => JSON.parse(gunzipSync(fs.readFileSync(file))));
  const names_equal = JSON.stringify(values[0].columns.map(c => c.name)) === JSON.stringify(values[1].columns.map(c => c.name));
  if (!names_equal) throw Error('Detail column names differ');
  const type_differences = values[0].columns.flatMap((column, index) => column.type === values[1].columns[index].type ? [] : [{
    name: column.name, index, thin_type: column.type, sr_type: values[1].columns[index].type,
  }]);
  const date_columns = new Set(type_differences.filter(d => [10, 12].includes(d.thin_type) && [10, 12].includes(d.sr_type)).map(d => d.index));
  const hashes = values.map(value => value.rows.map(row => {
    const normalized = row.map((v, i) => date_columns.has(i) && typeof v === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(v) ? v + ' 00:00:00' : v);
    return createHash('sha256').update(JSON.stringify(normalized)).digest('hex');
  }).sort());
  exact_value_checks.push({ dop, rows: values[0].rows.length, columns: values[0].columns.length, type_differences,
    date_to_midnight_normalized_equal: JSON.stringify(hashes[0]) === JSON.stringify(hashes[1]),
    rule: 'Normalize DATE text to midnight only where the counterpart column is DATETIME; compare every other field exactly, with duplicate counts and without numeric rounding.' });
}
fs.writeFileSync(path.join(root, 'exact-value-check.json'), JSON.stringify(exact_value_checks, null, 2));
console.log(rows.join('\n'));
console.log(JSON.stringify({ blocks: results.length, samples: samples.length, validations: validations.length, mismatches: validations.filter(v => !v.match) }, null, 2));
