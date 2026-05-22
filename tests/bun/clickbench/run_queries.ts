// ClickBench query coverage probe.
//
// Runs each of the 43 canonical ClickBench queries against a *running*
// thindb-server (MySQL wire), one at a time, and catalogs which succeed
// and which error. This is a coverage audit, not a benchmark — timings
// are informational only (single run, no warm-up).
//
// Prereqs:
//   * server running with the clickbench DB loaded, e.g.
//       thindb-server --data-dir .clickbench-db --mysql-port 7880 ...
//   * queries file at bench/clickbench/queries.sql (one query per line)
//
// Usage (from tests/bun so node_modules resolves):
//   bun run clickbench/run_queries.ts
//   THINDB_MYSQL_PORT=7880 THINDB_DB=clickbench__public bun run clickbench/run_queries.ts
//
// Env knobs:
//   THINDB_HOST        default 127.0.0.1
//   THINDB_MYSQL_PORT  default 7880
//   THINDB_DB          default clickbench__public
//   THINDB_QUERIES     default ../../bench/clickbench/queries.sql (rel to this file)
//   THINDB_TIMEOUT_MS  default 120000  (per-query cap)
//   THINDB_JSON_OUT    optional path; write the catalog as JSON

import mysql, { type Connection } from "mysql2/promise";
import { readFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));

const HOST = process.env.THINDB_HOST ?? "127.0.0.1";
const PORT = Number(process.env.THINDB_MYSQL_PORT ?? "7880");
const DB = process.env.THINDB_DB ?? "clickbench__public";
const QUERIES_PATH =
  process.env.THINDB_QUERIES ??
  resolve(here, "../../../bench/clickbench/queries.sql");
const TIMEOUT_MS = Number(process.env.THINDB_TIMEOUT_MS ?? "120000");
const JSON_OUT = process.env.THINDB_JSON_OUT;
// THINDB_ONLY: comma-separated query indices to run in isolation (e.g. "33"
// or "28,33,34"). Empty = run all 43.
const ONLY = (process.env.THINDB_ONLY ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter((s) => s.length > 0)
  .map(Number);
// THINDB_REPEAT: run each selected query N times (prints each run + best),
// for warm-cache timing of an isolated query.
const REPEAT = Math.max(1, Number(process.env.THINDB_REPEAT ?? "1"));

type Outcome = {
  idx: number;
  ok: boolean;
  ms: number;
  rows?: number;
  errno?: number;
  code?: string;
  message?: string;
  sql: string;
};

function loadQueries(path: string): string[] {
  const text = readFileSync(path, "utf8");
  return text
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l.length > 0 && !l.startsWith("--"));
}

async function runOne(
  conn: Connection,
  idx: number,
  sql: string,
): Promise<Outcome> {
  const start = performance.now();
  try {
    const [rows] = await conn.query({ sql, timeout: TIMEOUT_MS });
    const ms = performance.now() - start;
    const rowCount = Array.isArray(rows) ? rows.length : 0;
    return { idx, ok: true, ms, rows: rowCount, sql };
  } catch (err) {
    const ms = performance.now() - start;
    // mysql2 attaches errno/code/sqlMessage to SQL errors.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const e = err as any;
    return {
      idx,
      ok: false,
      ms,
      errno: e.errno,
      code: e.code,
      message: e.sqlMessage ?? e.message ?? String(err),
      sql,
    };
  }
}

function truncate(s: string, n: number): string {
  return s.length <= n ? s : s.slice(0, n - 1) + "…";
}

async function main(): Promise<void> {
  const queries = loadQueries(QUERIES_PATH);
  console.log(
    `ClickBench coverage probe → ${HOST}:${PORT} db=${DB}\n` +
      `queries: ${queries.length} from ${QUERIES_PATH}\n` +
      `per-query timeout: ${TIMEOUT_MS}ms\n` +
      "─".repeat(80),
  );

  let conn: Connection;
  try {
    conn = await mysql.createConnection({
      host: HOST,
      port: PORT,
      user: "thindb",
      password: "",
      database: DB,
      // big result sets: don't let the driver cap rows
      rowsAsArray: true,
    });
  } catch (err) {
    console.error(`FATAL: could not connect: ${String(err)}`);
    process.exit(2);
  }

  const connect = (): Promise<Connection> =>
    mysql.createConnection({
      host: HOST,
      port: PORT,
      user: "thindb",
      password: "",
      database: DB,
      rowsAsArray: true,
    });

  const outcomes: Outcome[] = [];
  for (let i = 0; i < queries.length; i++) {
    if (ONLY.length > 0 && !ONLY.includes(i)) continue;
    const sql = queries[i]!;

    // Run REPEAT times; keep the fastest as the recorded outcome (warm-cache
    // timing for an isolated query). Print every run when REPEAT > 1.
    let out = await runOne(conn, i, sql);
    const runs: number[] = out.ok ? [out.ms] : [];
    for (let r = 1; r < REPEAT && out.ok; r++) {
      const next = await runOne(conn, i, sql);
      if (next.ok) {
        runs.push(next.ms);
        if (next.ms < out.ms) out = next;
      }
    }
    outcomes.push(out);

    // A query that errors at the connection level (server closed the
    // socket) poisons every later query with "closed state". Reconnect
    // so the rest of the catalog reflects real per-query behavior
    // instead of cascade noise.
    const connDead =
      !out.ok &&
      (out.code === "PROTOCOL_CONNECTION_LOST" ||
        (out.message ?? "").includes("closed state"));
    if (connDead) {
      await conn.end().catch(() => undefined);
      try {
        conn = await connect();
      } catch (err) {
        console.error(`reconnect failed after Q${i}: ${String(err)}`);
        break;
      }
    }
    if (out.ok) {
      const detail =
        REPEAT > 1
          ? `best ${out.ms.toFixed(1)}ms of [${runs.map((m) => m.toFixed(0)).join(", ")}]`
          : `${out.ms.toFixed(1).padStart(8)}ms`;
      console.log(
        `Q${String(i).padStart(2, "0")}  OK    ${detail}  ` +
          `rows=${String(out.rows).padStart(6)}  ${truncate(sql, 46)}`,
      );
    } else {
      console.log(
        `Q${String(i).padStart(2, "0")}  FAIL  ${out.ms.toFixed(1).padStart(8)}ms  ` +
          `[${out.code ?? "?"}/${out.errno ?? "?"}] ${truncate(out.message ?? "", 60)}`,
      );
    }
  }

  await conn.end().catch(() => undefined);

  const passed = outcomes.filter((o) => o.ok);
  const failed = outcomes.filter((o) => !o.ok);

  console.log("─".repeat(80));
  console.log(`PASS ${passed.length}/${outcomes.length}   FAIL ${failed.length}/${outcomes.length}`);

  if (failed.length > 0) {
    console.log("\nFailures grouped by error message:");
    const groups = new Map<string, number[]>();
    for (const f of failed) {
      const key = `[${f.code ?? "?"}] ${f.message ?? ""}`;
      const arr = groups.get(key) ?? [];
      arr.push(f.idx);
      groups.set(key, arr);
    }
    const sorted = [...groups.entries()].sort((a, b) => b[1].length - a[1].length);
    for (const [msg, idxs] of sorted) {
      console.log(`  (${idxs.length})  ${msg}`);
      console.log(`        queries: ${idxs.map((i) => `Q${i}`).join(", ")}`);
    }
  }

  if (JSON_OUT) {
    writeJson(JSON_OUT, outcomes);
    console.log(`\nWrote catalog → ${JSON_OUT}`);
  }
}

function writeJson(path: string, outcomes: Outcome[]): void {
  const payload = {
    host: HOST,
    port: PORT,
    db: DB,
    when: new Date().toISOString(),
    total: outcomes.length,
    passed: outcomes.filter((o) => o.ok).length,
    failed: outcomes.filter((o) => !o.ok).length,
    outcomes,
  };
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const fs = require("node:fs") as typeof import("node:fs");
  fs.writeFileSync(path, JSON.stringify(payload, null, 2));
}

await main();
