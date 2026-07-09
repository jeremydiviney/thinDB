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
  return r.jobs.filter((j) => j.name.includes("sink_")).map((j) => ({ name: j.name.split("sink_")[1], state: j.state }));
}

async function check() {
  const problems = [];
  try {
    const jobs = await jobStates();
    const running = jobs.filter((j) => j.state === "RUNNING");
    for (const j of jobs) if (j.state !== "RUNNING") problems.push(`job ${j.name} is ${j.state}`);
    if (running.length < EXPECTED_JOBS) problems.push(`only ${running.length}/${EXPECTED_JOBS} sink jobs RUNNING`);
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
