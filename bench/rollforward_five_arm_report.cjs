const fs = require('node:fs');
const path = require('node:path');
const { cases, arms } = require('./rollforward_five_arm_sweep.cjs');
const root = path.resolve(process.argv[2]);
const baseline = process.argv[3] ? path.resolve(process.argv[3]) : null;
const median = v => [...v].sort((a, b) => a - b)[Math.floor(v.length / 2)];
const quote = v => '"' + String(v ?? '').replaceAll('"', '""') + '"';
const same = (a, b) => a.rows === b.rows && a.columns === b.columns && a.digest === b.digest;
const summaries = [], samples = [], comparisons = [], memory = [];
for (const folder of fs.readdirSync(root)) {
  const dir = path.join(root, folder);
  if (!fs.existsSync(path.join(dir, 'complete.json'))) continue;
  const r = JSON.parse(fs.readFileSync(path.join(dir, 'result.json')));
  const m = r.samples.filter(s => !s.warmup && !s.validation), v = r.samples.filter(s => s.validation);
  if (m.length !== 5 || v.length !== 1 || r.samples.length !== 7) throw Error('Incomplete block ' + folder);
  if (r.samples.some(s => s.decoded_rows !== 0 || s.rows !== v[0].rows || s.columns !== v[0].columns)) throw Error('Changing result shape ' + folder);
  const times = m.map(s => s.ms);
  const summary = { folder, company: r.company, project: r.project, label: r.label, arm: r.arm, dop: r.dop,
    as_of: r.as_of, rows: v[0].rows, columns: v[0].columns, digest: v[0].digest,
    median_ms: median(times), min_ms: Math.min(...times), max_ms: Math.max(...times), samples_ms: times,
    wire_ms: median(m.map(s => s.wire_ms)), application_ms: median(m.map(s => s.ms - s.wire_ms)),
    module_setup_ms: r.module_setup_ms, connection_setup_ms: r.connection_setup_ms,
    keyed: ['sql_keyed', 'zig'].includes(r.arm), region_samples: r.samples.filter(s => s.engaged).length };
  const query = JSON.parse(fs.readFileSync(path.join(dir, 'query.json')));
  if (query.has_udf !== ['zig_unkeyed', 'zig'].includes(r.arm) || query.has_region !== summary.keyed) throw Error('Wrong query method ' + folder);
  if (r.arm !== 'sr' && summary.region_samples !== (summary.keyed ? 7 : 0)) throw Error('Wrong region engagement ' + folder);
  summaries.push(summary);
  for (const s of r.samples) samples.push({ project: r.project, company: r.company, label: r.label, arm: r.arm, dop: r.dop,
    iteration: s.iteration, warmup: s.warmup, validation: s.validation, ms: s.ms, wire_ms: s.wire_ms,
    first_row_ms: s.first_row_ms, rows: s.rows, columns: s.columns, digest: s.digest, payload_bytes: s.payload_bytes,
    application_ms: s.ms - s.wire_ms, client_cpu_ms: s.client_cpu_ms, engaged: s.engaged });
  if (r.arm !== 'sr') {
    const state = Object.fromEntries(fs.readFileSync(path.join(dir, 'memory.txt'), 'utf8').trim().split('\n').map(line => line.split('=')));
    const events = Object.fromEntries(fs.readFileSync(path.join(dir, 'memory-events.txt'), 'utf8').trim().split('\n').map(line => line.trim().split(/\s+/)));
    if (['max', 'oom', 'oom_kill', 'oom_group_kill'].some(k => Number(events[k]))) throw Error('Memory event ' + folder);
    memory.push({ folder, pid: +state.MainPID, peak_bytes: +state.MemoryPeak, events });
    if (baseline) {
      const old = JSON.parse(fs.readFileSync(path.join(baseline, 'five-arm-results-thin', r.project + '.json')));
      const prior = old.cases.find(c => c.label === r.label)?.samples.find(s => s.arm === r.arm && s.validation);
      if (prior) comparisons.push({ project: r.project, label: r.label, comparison: r.arm + ' vs archived DOP12', match: same(summary, { ...prior, digest: prior.wireDigest }) });
    }
  }
}
const matrix = [];
for (const project of [1000049, 1000073]) for (const item of cases) {
  const found = summaries.filter(s => s.project === project && s.label === item.label);
  if (!found.length) continue;
  const by_arm = Object.fromEntries(found.map(s => [s.arm, s]));
  for (const [a, b] of [['sql_keyed', 'sql'], ['zig', 'zig_unkeyed'], ['zig_unkeyed', 'sql'], ['sr', 'sql']]) {
    if (by_arm[a] && by_arm[b]) comparisons.push({ project, label: item.label, comparison: a + ' vs ' + b,
      match: same(by_arm[a], by_arm[b]), rows_a: by_arm[a].rows, rows_b: by_arm[b].rows });
  }
  matrix.push({ company: project === 1000049 ? 'Sierra' : 'AirDNA', project, label: item.label,
    ...Object.fromEntries(arms.map(arm => [arm, by_arm[arm]?.median_ms])),
    row_counts: Object.fromEntries(found.map(s => [s.arm, s.rows])) });
}
const totals = [1000049, 1000073].map(project => ({ company: project === 1000049 ? 'Sierra' : 'AirDNA', project,
  ...Object.fromEntries(arms.map(arm => [arm, summaries.filter(s => s.project === project && s.arm === arm).reduce((n, s) => n + s.median_ms, 0) / 1000])) }));
const report = { complete: summaries.length === 150, blocks: summaries.length,
  measured_samples: samples.filter(s => !s.warmup && !s.validation).length,
  fingerprints: samples.filter(s => s.validation).length, matrix, totals, comparisons, memory,
  max_candidate_gib: Math.max(0, ...memory.map(s => s.peak_bytes)) / 2 ** 30 };
fs.writeFileSync(path.join(root, 'summary.json'), JSON.stringify(summaries, null, 2));
fs.writeFileSync(path.join(root, 'matrix.json'), JSON.stringify(report, null, 2));
const headers = Object.keys(samples[0] ?? {});
fs.writeFileSync(path.join(root, 'samples.csv'), [headers, ...samples.map(s => headers.map(h => s[h]))].map(r => r.map(quote).join(',')).join('\n') + '\n');
const table = [];
for (const company of ['Sierra', 'AirDNA']) {
  table.push('## ' + company, '', '| Variant | StarRocks SQL | ThinDB SQL | SQL + regions | Zig UDF | UDF + regions |', '|---|---:|---:|---:|---:|---:|');
  for (const row of matrix.filter(r => r.company === company)) {
    const values = arms.map(a => row[a]);
    const best = Math.min(...values.filter(v => v !== undefined));
    table.push('| ' + [row.label, ...values.map(v => v === undefined ? 'pending' : (v === best ? '**' : '') + Math.round(v).toLocaleString('en-US') + (v === best ? '**' : ''))].join(' | ') + ' |');
  }
  table.push('');
}
fs.writeFileSync(path.join(root, 'timing-tables.md'), table.join('\n'));
console.log(JSON.stringify({ complete: report.complete, blocks: report.blocks, measured_samples: report.measured_samples,
  fingerprints: report.fingerprints, mismatches: comparisons.filter(c => !c.match), max_candidate_gib: report.max_candidate_gib, totals }, null, 2));
