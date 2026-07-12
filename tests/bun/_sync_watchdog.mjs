// CDC sync watchdog: detects BOTH failure modes from the 2026-07-09 incident —
// a job that leaves RUNNING state (report's NPE crash-loop shows as
// RESTARTING), and the nastier silent wedge (job RUNNING, checkpoints fine,
// but no data flowing — invoice). Freshness = thinDB MAX(updatedAt) vs wall
// clock on tables that churn continuously; job state via the Flink REST API.
// Emits one line per check only when something is wrong (or on recovery), so
// it can drive a Monitor/alert pipe. Exit code stays 0; this observes.
// Usage: node _sync_watchdog.mjs [intervalSecs] [lagThresholdSecs]
import mysql from "mysql2/promise";
import fs from "fs";
import { execFile } from "child_process";

const INTERVAL = (parseInt(process.argv[2], 10) || 120) * 1000;
const LAG_MAX = (parseInt(process.argv[3], 10) || 900) * 1000; // 15 min default
const FLINK = "http://localhost:8081";
// Prod creds (scratchpad only, never committed) — used ONLY to confirm a
// suspected-stale table against prod's own frontier before alerting. A quiet
// prod (no writes for >LAG_MAX) is indistinguishable from a wedged reader by
// wall clock alone and caused recurring off-hours false alarms.
const SP = "C:/Users/jerem/AppData/Local/Temp/claude/C--development-thinDB/c2cce1cf-9c7d-4061-98b7-3a1c71c0bfd8/scratchpad";
let PROD = null;
try {
  const cfg = fs.readFileSync(SP + "/cdc_invoice_import_amortized.sql", "utf8");
  PROD = {
    host: cfg.match(/'hostname' *= *'([^']+)'/)[1],
    user: cfg.match(/'username' *= *'([^']+)'/)[1],
    password: cfg.match(/'password' *= *'([^']+)'/)[1],
  };
} catch { /* no creds — fall back to wall-clock-only alerts */ }

// True when the apparent staleness is benign: prod's frontier matches
// thinDB's exactly (prod is quiet, sync at head), OR prod is ahead only
// because it wrote a burst moments before the check — a 15s grace re-read of
// thinDB catching up to prod's frontier clears it (uniform datetime text
// compares lexicographically). Fail-open: any error → false, real wedges
// still alert.
async function staleIsBenign(table, thindbMx, tdbConn) {
  if (!PROD) return false;
  try {
    const p = await mysql.createConnection({ ...PROD, database: "wayroll", dateStrings: true, connectTimeout: 15000 });
    const [[r]] = await p.query(`SELECT MAX(updatedAt) mx FROM \`${table}\``);
    await p.end();
    if (String(r.mx) === String(thindbMx)) return true;
    await new Promise((res) => setTimeout(res, 15000));
    const [[again]] = await tdbConn.query(`SELECT MAX(updatedAt) mx FROM \`${table}\``);
    return String(again.mx) >= String(r.mx);
  } catch {
    return false;
  }
}
// Tables whose updatedAt advances continuously in prod — freshness is a valid
// health signal. Small/rarely-written tables are state-checked only.
const FRESH_TABLES = ["invoice_import_amortized", "report_customer_revenue_rollforward"];
// All synced tables — the hourly count-parity probe covers every one.
const ALL_TABLES = [
  "invoice_import_amortized", "report_customer_revenue_rollforward",
  "estimate_date_map", "number_sequence", "currency_exchange_rate",
  "division", "external_plan",
];
const EXPECTED_JOBS = 7;

let lastBad = false;
let checkCount = 0;
const PARITY_EVERY = Math.max(1, Math.round(3600000 / INTERVAL)); // ~hourly

// Count-parity probe (#164d): the sink's delivery contract is at-least-once
// JDBC + idempotent keyed upserts/deletes — XA was evaluated and rejected
// (see INGEST_DESIGN.md). The safety net for any acked-but-lost class is
// VERIFICATION: compare a recent-window row count per table between prod and
// thinDB over IDENTICAL literal bounds (client-computed; window ends 15 min
// ago so in-flight CDC lag can't false-positive; 24 h deep so it catches
// yesterday's losses too). Confirmed drifts survive a 60 s recheck, which
// absorbs the read race between the two queries. Fail-open: probe errors
// never alert on their own.
async function parityDrift() {
  if (!PROD) return [];
  const fmt = (ms) => new Date(ms).toISOString().slice(0, 19).replace("T", " ");
  const where = `updatedAt >= '${fmt(Date.now() - 24 * 3600000)}' AND updatedAt < '${fmt(Date.now() - 15 * 60000)}'`;
  const p = await mysql.createConnection({ ...PROD, database: "wayroll", connectTimeout: 15000 });
  const t = await mysql.createConnection({ host: "127.0.0.1", port: 13310, user: "root", password: "", database: "wayroll_prod__public", connectTimeout: 8000 });
  // Small tables (estimate_date_map etc.) have no updatedAt on prod — full
  // COUNT(*) is cheap there; only the churn giants need the window bound.
  const countBoth = async (table) => {
    let pr, tr;
    try {
      [[pr]] = await p.query({ sql: `SELECT COUNT(*) c FROM \`${table}\` WHERE ${where}`, timeout: 120000 });
      [[tr]] = await t.query({ sql: `SELECT COUNT(*) c FROM \`${table}\` WHERE ${where}`, timeout: 120000 });
    } catch (e) {
      if (e.code !== "ER_BAD_FIELD_ERROR") throw e;
      [[pr]] = await p.query({ sql: `SELECT COUNT(*) c FROM \`${table}\``, timeout: 120000 });
      [[tr]] = await t.query({ sql: `SELECT COUNT(*) c FROM \`${table}\``, timeout: 120000 });
    }
    return [Number(pr.c), Number(tr.c)];
  };
  try {
    const suspects = [];
    for (const table of ALL_TABLES) {
      const [pc, tc] = await countBoth(table);
      if (pc !== tc) suspects.push(table);
    }
    if (!suspects.length) return [];
    await new Promise((r) => setTimeout(r, 60000));
    const confirmed = [];
    for (const table of suspects) {
      const [pc, tc] = await countBoth(table);
      if (pc !== tc) confirmed.push(`${table} parity drift (24h window): prod=${pc} thindb=${tc}`);
    }
    return confirmed;
  } finally {
    p.end().catch(() => {});
    t.end().catch(() => {});
  }
}

async function jobStates() {
  const r = await fetch(`${FLINK}/jobs/overview`).then((x) => x.json());
  // The JM remembers every job it ever ran (cancelled/failed history
  // included), and after a recovery cycle a table's newest-by-start job can
  // be a cancelled duplicate. Health per table = exactly one RUNNING job:
  // zero means the pipeline is down, two+ means duplicate readers (server-id
  // collisions on the RDS side).
  const counts = new Map();
  for (const j of r.jobs) {
    if (!j.name.includes("sink_")) continue;
    const name = j.name.split("sink_")[1];
    counts.set(name, (counts.get(name) ?? 0) + (j.state === "RUNNING" ? 1 : 0));
  }
  return [...counts.entries()].map(([name, running]) => ({ name, running }));
}

async function check() {
  const problems = [];
  try {
    const jobs = await jobStates();
    for (const j of jobs) {
      if (j.running === 0) problems.push(`no RUNNING job for ${j.name}`);
      if (j.running > 1) problems.push(`${j.running} duplicate RUNNING jobs for ${j.name}`);
    }
    const healthy = jobs.filter((j) => j.running === 1).length;
    if (healthy < EXPECTED_JOBS) problems.push(`only ${healthy}/${EXPECTED_JOBS} tables healthy`);
  } catch (e) {
    problems.push(`flink REST unreachable: ${e.message}`);
  }
  try {
    const c = await mysql.createConnection({ host: "127.0.0.1", port: 13310, user: "root", password: "", database: "wayroll_prod__public", connectTimeout: 8000, dateStrings: true });
    for (const t of FRESH_TABLES) {
      const [[r]] = await c.query(`SELECT MAX(updatedAt) mx FROM \`${t}\``);
      // native DATETIME returns 'YYYY-MM-DD HH:MM:SS[.ffffff]' UTC text
      // (was µs-epoch numeric text pre-resync); parse explicitly as UTC.
      const mxMs = /^\d{4}-/.test(String(r.mx)) ? Date.parse(String(r.mx).replace(" ", "T") + "Z") : Math.floor(Number(r.mx) / 1000);
      const lagMs = Date.now() - mxMs;
      if (lagMs > LAG_MAX && !(await staleIsBenign(t, r.mx, c))) {
        problems.push(`${t} stale: newest row ${Math.round(lagMs / 60000)} min old, prod frontier ahead (wedged reader?)`);
      }
    }
    await c.end();
  } catch (e) {
    problems.push(`thinDB unreachable: ${e.message}`);
  }
  // Sink-error storm: the 2026-07-09 wire bug had every batch failing for 2h
  // while jobs stayed RUNNING and checkpoints completed — the only external
  // signal was JdbcOutputFormat retry errors in the TM log. Any at all inside
  // one interval means a sink is failing NOW.
  try {
    const errCount = await new Promise((resolve) => {
      execFile("docker", ["logs", "flink-taskmanager-1", "--since", `${Math.ceil(INTERVAL / 1000)}s`],
        { maxBuffer: 64 * 1024 * 1024 }, (err, stdout, stderr) => {
          if (err && !stdout && !stderr) return resolve(-1);
          const all = stdout + stderr;
          resolve((all.match(/JDBC executeBatch error/g) ?? []).length);
        });
    });
    if (errCount > 0) problems.push(`sink errors: ${errCount} executeBatch failures in last interval`);
  } catch { /* docker unavailable — job-state checks still cover us */ }

  checkCount++;
  if ((checkCount - 1) % PARITY_EVERY === 0) {
    try {
      problems.push(...(await parityDrift()));
    } catch { /* fail-open — parity is a supplementary signal */ }
  }

  const ts = new Date().toISOString().slice(11, 19);
  // Emit only on state CHANGE (problem set differs from last check), so a
  // known-degraded state during a long recovery doesn't spam every interval.
  // The signature ignores the staleness magnitude entirely — during a
  // from-scratch snapshot the updatedAt frontier drifts continuously and
  // would re-alert on every bucket. Which tables/kinds are unhealthy is the
  // signal; the current minutes are still in the printed message.
  const sig = problems.map((p) => p.replace(/\d+ min old/, "N min old").replace(/sink errors: \d+/, "sink errors: N")).join(" | ");
  if (problems.length && sig !== lastBad) {
    console.log(`${ts} SYNC-ALERT: ${problems.join(" | ")}`);
    lastBad = sig;
  } else if (!problems.length && lastBad) {
    console.log(`${ts} SYNC-RECOVERED: all ${EXPECTED_JOBS} tables healthy, freshness within ${LAG_MAX / 60000} min`);
    lastBad = false;
  }
}

for (;;) {
  await check();
  await new Promise((r) => setTimeout(r, INTERVAL));
}
