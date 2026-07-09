// CDC sync watchdog: detects BOTH failure modes from the 2026-07-09 incident —
// a job that leaves RUNNING state (report's NPE crash-loop shows as
// RESTARTING), and the nastier silent wedge (job RUNNING, checkpoints fine,
// but no data flowing — invoice). Freshness = thinDB MAX(updatedAt) vs wall
// clock on tables that churn continuously; job state via the Flink REST API.
// Emits one line per check only when something is wrong (or on recovery), so
// it can drive a Monitor/alert pipe. Exit code stays 0; this observes.
// Usage: node _sync_watchdog.mjs [intervalSecs] [lagThresholdSecs]
import mysql from "mysql2/promise";

const INTERVAL = (parseInt(process.argv[2], 10) || 120) * 1000;
const LAG_MAX = (parseInt(process.argv[3], 10) || 900) * 1000; // 15 min default
const FLINK = "http://localhost:8081";
// Tables whose updatedAt advances continuously in prod — freshness is a valid
// health signal. Small/rarely-written tables are state-checked only.
const FRESH_TABLES = ["invoice_import_amortized", "report_customer_revenue_rollforward"];
const EXPECTED_JOBS = 7;

let lastBad = false;

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
    const c = await mysql.createConnection({ host: "127.0.0.1", port: 13310, user: "root", password: "", database: "wayroll_prod__public", connectTimeout: 8000 });
    for (const t of FRESH_TABLES) {
      const [[r]] = await c.query(`SELECT MAX(updatedAt) mx FROM \`${t}\``);
      const lagMs = Date.now() - Math.floor(Number(r.mx) / 1000);
      if (lagMs > LAG_MAX) problems.push(`${t} stale: newest row ${Math.round(lagMs / 60000)} min old (wedged reader?)`);
    }
    await c.end();
  } catch (e) {
    problems.push(`thinDB unreachable: ${e.message}`);
  }

  const ts = new Date().toISOString().slice(11, 19);
  if (problems.length) {
    console.log(`${ts} SYNC-ALERT: ${problems.join(" | ")}`);
    lastBad = true;
  } else if (lastBad) {
    console.log(`${ts} SYNC-RECOVERED: all ${EXPECTED_JOBS} jobs RUNNING, freshness within ${LAG_MAX / 60000} min`);
    lastBad = false;
  }
}

for (;;) {
  await check();
  await new Promise((r) => setTimeout(r, INTERVAL));
}
