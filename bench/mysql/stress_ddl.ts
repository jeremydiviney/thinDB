// Concurrent DDL stress (#90). Each client owns a database and loops CREATE,
// a doubling INSERT ... SELECT, CTAS, DELETE, UPDATE, ALTER, RENAME, TRUNCATE
// and DROP, dropping and recreating its whole database every few cycles and
// sometimes abandoning a connection mid-query. The server's background
// flusher and compactor run underneath all of it. Every step checks its row
// counts; a crashed server shows up as refused connections, a wedged one as
// a statement that outlives STALL_MS.
//
//   bun run bench/mysql/stress_ddl.ts --port 3307 --clients 6 --seconds 600

import mysql, { type Connection } from "mysql2/promise";
import { parseArgs } from "node:util";

const { values: args } = parseArgs({
  options: {
    host: { type: "string", default: "127.0.0.1" },
    port: { type: "string", default: "3307" },
    user: { type: "string", default: "thindb" },
    password: { type: "string", default: "" },
    clients: { type: "string", default: "6" },
    seconds: { type: "string", default: "600" },
    // Rows per cycle = 2^doublings. 20 matches the incident's 1M-row seed.
    doublings: { type: "string", default: "20" },
    seed: { type: "string", default: "1" },
  },
});

const clientCount = Number(args.clients);
const deadline = Date.now() + Number(args.seconds) * 1000;
const doublings = Number(args.doublings);

class InvariantError extends Error {}
class StallError extends Error {}

// A statement slower than this is reported as a stall: at these sizes every
// step takes seconds, so minutes means the server is wedged.
const STALL_MS = 300_000;

function withTimeout<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new StallError(`${what} took over ${ms / 1000}s`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

const stats = {
  cycles: 0,
  statements: 0,
  abandoned: 0,
  sqlErrors: new Map<string, number>(),
  invariantFailures: [] as string[],
  stalls: [] as string[],
  serverDown: false,
};

let rngState = Number(args.seed) >>> 0 || 1;
function random(): number {
  rngState ^= rngState << 13;
  rngState ^= rngState >>> 17;
  rngState ^= rngState << 5;
  return (rngState >>> 0) / 0x100000000;
}

function connect(database?: string): Promise<Connection> {
  return mysql.createConnection({
    host: args.host,
    port: Number(args.port),
    user: args.user,
    password: args.password,
    database,
    multipleStatements: false,
  });
}

function isServerDown(err: unknown): boolean {
  const code = (err as { code?: string }).code ?? "";
  return code === "ECONNREFUSED" || code === "ECONNRESET" || code === "PROTOCOL_CONNECTION_LOST" || code === "EPIPE";
}

async function run(conn: Connection, sql: string): Promise<any[]> {
  stats.statements++;
  const [rows] = await withTimeout(conn.query(sql), STALL_MS, sql);
  return rows as any[];
}

async function scalar(conn: Connection, sql: string): Promise<number> {
  const rows = await run(conn, sql);
  const row = rows[0] as Record<string, unknown> | undefined;
  if (!row) throw new InvariantError(`no row: ${sql}`);
  return Number(Object.values(row)[0]);
}

async function expectCount(conn: Connection, table: string, expected: number, step: string): Promise<void> {
  const got = await scalar(conn, `SELECT COUNT(*) FROM ${table}`);
  if (got !== expected) throw new InvariantError(`${step}: ${table} has ${got} rows, expected ${expected}`);
}

// Open a side connection, start a heavy query and drop the socket while it
// runs: the server's disconnect path races the in-flight statement.
async function abandonQuery(database: string): Promise<void> {
  const side = await connect(database);
  const pending = side.query("SELECT COUNT(*) FROM seq a JOIN seq b ON a.n = b.n").catch(() => undefined);
  await Bun.sleep(Math.floor(random() * 50));
  side.destroy();
  // mysql2 may never settle a query whose socket was destroyed under it.
  await withTimeout(pending, 5_000, "abandoned query").catch(() => undefined);
  stats.abandoned++;
}

function binomial(n: number, k: number): number {
  let result = 1;
  for (let i = 1; i <= k; i++) result = (result * (n - k + i)) / i;
  return Math.round(result);
}

async function cycle(client: number, cycleNo: number): Promise<void> {
  const database = `stress_c${client}`;
  const admin = await connect();
  try {
    if (cycleNo % 3 === 2) await run(admin, `DROP DATABASE IF EXISTS ${database}`);
    await run(admin, `CREATE DATABASE IF NOT EXISTS ${database}`);
  } finally {
    await admin.end();
  }

  const conn = await connect(`${database}__public`);
  try {
    for (const t of ["seq", "seq_copy", "seq_moved"]) await run(conn, `DROP TABLE IF EXISTS ${t}`);
    await run(conn, "CREATE TABLE seq (n BIGINT NOT NULL, v INT NOT NULL, s VARCHAR(32) NOT NULL, PRIMARY KEY (n))");
    await run(conn, "INSERT INTO seq (n, v, s) VALUES (1, 1, 'a')");
    let rows = 1;
    for (let d = 0; d < doublings; d++) {
      await run(conn, `INSERT INTO seq (n, v, s) SELECT n + ${rows}, v + 1, CONCAT(s, 'b') FROM seq`);
      rows *= 2;
      if (d % 5 === 4 || d === doublings - 1) await expectCount(conn, "seq", rows, `doubling ${d}`);
      if (random() < 0.05) await abandonQuery(`${database}__public`);
    }

    await run(conn, "CREATE TABLE seq_copy AS SELECT n, v, s FROM seq");
    await expectCount(conn, "seq_copy", rows, "ctas");

    // Each doubling copies every row with v + 1, so v - 1 counts the copies a
    // row went through: C(doublings, v - 1) rows hold each v, spread across
    // the whole key range.
    const deleteAbove = Math.floor(doublings / 2) + 1;
    let deleted = 0;
    for (let v = deleteAbove + 1; v <= doublings + 1; v++) deleted += binomial(doublings, v - 1);
    await run(conn, `DELETE FROM seq_copy WHERE v > ${deleteAbove}`);
    const kept = rows - deleted;
    await expectCount(conn, "seq_copy", kept, "delete");

    const updateV = Math.max(1, deleteAbove - 1);
    await run(conn, `UPDATE seq_copy SET s = 'updated' WHERE v = ${updateV}`);
    const updated = await scalar(conn, "SELECT COUNT(*) FROM seq_copy WHERE s = 'updated'");
    if (updated !== binomial(doublings, updateV - 1)) {
      throw new InvariantError(`update: ${updated} rows updated, expected ${binomial(doublings, updateV - 1)}`);
    }
    await run(conn, "ALTER TABLE seq_copy ADD COLUMN extra INT NULL");
    await run(conn, "RENAME TABLE seq_copy TO seq_moved");
    await expectCount(conn, "seq_moved", kept, "rename");
    await run(conn, "SELECT SUM(v), COUNT(DISTINCT s), MAX(extra) FROM seq_moved");

    await run(conn, "TRUNCATE TABLE seq_moved");
    await expectCount(conn, "seq_moved", 0, "truncate");
    await run(conn, "DROP TABLE seq_moved");
    if (random() < 0.3) await run(conn, "DROP TABLE seq");
  } finally {
    await withTimeout(conn.end(), 10_000, "connection close").catch(() => conn.destroy());
  }
}

async function client(id: number): Promise<void> {
  for (let cycleNo = 0; Date.now() < deadline && !stats.serverDown; cycleNo++) {
    try {
      await cycle(id, cycleNo);
      stats.cycles++;
    } catch (err) {
      if (err instanceof StallError) {
        stats.stalls.push(`client ${id} cycle ${cycleNo}: ${err.message}`);
        console.error(`STALL client ${id}: ${err.message}`);
        return;
      } else if (err instanceof InvariantError) {
        stats.invariantFailures.push(`client ${id} cycle ${cycleNo}: ${err.message}`);
        console.error(`INVARIANT client ${id}: ${err.message}`);
      } else if (isServerDown(err)) {
        stats.serverDown = true;
        console.error(`SERVER DOWN (client ${id}): ${(err as Error).message}`);
      } else {
        const message = (err as Error).message.slice(0, 160);
        if (!stats.sqlErrors.has(message)) console.error(`SQL ERROR client ${id}: ${message}`);
        stats.sqlErrors.set(message, (stats.sqlErrors.get(message) ?? 0) + 1);
      }
    }
  }
}

const started = Date.now();
const progress = setInterval(() => {
  console.error(`[${Math.round((Date.now() - started) / 1000)}s] cycles=${stats.cycles} statements=${stats.statements} abandoned=${stats.abandoned} errors=${[...stats.sqlErrors.values()].reduce((a, b) => a + b, 0)}`);
}, 30_000);
await Promise.all(Array.from({ length: clientCount }, (_, i) => client(i)));
clearInterval(progress);

const summary = {
  seconds: Math.round((Date.now() - started) / 1000),
  clients: clientCount,
  cycles: stats.cycles,
  statements: stats.statements,
  abandoned: stats.abandoned,
  serverDown: stats.serverDown,
  invariantFailures: stats.invariantFailures,
  stalls: stats.stalls,
  sqlErrors: Object.fromEntries(stats.sqlErrors),
};
console.log(JSON.stringify(summary, null, 2));
process.exit(stats.serverDown || stats.invariantFailures.length > 0 || stats.stalls.length > 0 ? 1 : 0);
