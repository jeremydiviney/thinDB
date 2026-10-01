// Concurrent DDL stress (#90). Each client owns a database and loops CREATE,
// a doubling INSERT ... SELECT, CTAS, DELETE, UPDATE, ALTER, RENAME, TRUNCATE
// and DROP, dropping and recreating its whole database every few cycles and
// sometimes abandoning a connection mid-query. The server's background
// flusher and compactor run underneath all of it. Every step checks its row
// counts; a crashed server shows up as refused connections, a wedged one as
// a statement that outlives STALL_MS.
//
// Optional pressure on the connection-teardown paths:
//   --killers N     read the process list and KILL QUERY / KILL CONNECTION
//                   random stress connections mid-statement;
//   --readers N     read other clients' tables while those clients run DDL;
//   --pg-clients N  PostgreSQL-wire clients (needs --pg-port) that abandon
//                   queries, send CancelRequests and pg_cancel_backend /
//                   pg_terminate_backend stress connections of either wire;
//   --drop-writes P the share of cycles in which the client drops its own
//                   socket in the middle of one of its statements, so the
//                   server sees the disconnect while it runs a write or DDL.
// And on view and SQL-function definitions, which the parser reads before its
// statement holds a lease (#368):
//   --view-churners N  replace, drop and recreate views and table functions
//                      in a database of their own;
//   --view-readers N   expand those views and functions from other
//                      connections meanwhile.
// And on many connections racing over one table's catalog entry, memtable
// and segment handles, as a test runner's parallel slices do:
//   --slices N  databases, each worked by a pool of connections at once;
//   --pool M    connections per slice (default 16). Every one mixes point and
//               INSERT ... SELECT writes, UPDATE, DELETE, CTAS, ALTER,
//               TRUNCATE, DROP / CREATE and reads over the slice's shared
//               tables. Every write keeps invariants a reader checks in one
//               statement: ledger rows come in pairs summing to zero, keyed
//               rows have unique keys, and every row's chk matches the rest
//               of the row.
// A killed write must leave all-or-nothing state: the client checks that on
// a connection nobody kills, then starts its next cycle.
//
//   bun run bench/mysql/stress_ddl.ts --port 3307 --clients 6 --seconds 600
//   bun run bench/mysql/stress_ddl.ts --port 3307 --pg-port 5433 --clients 6 \
//     --killers 1 --readers 2 --pg-clients 2 --seconds 900
//   bun run bench/mysql/stress_ddl.ts --port 3307 --clients 0 --slices 6 \
//     --pool 16 --killers 1 --drop-writes 0.1 --seconds 1800

import mysql, { type Connection } from "mysql2/promise";
import net from "node:net";
import { parseArgs } from "node:util";

const { values: args } = parseArgs({
  options: {
    host: { type: "string", default: "127.0.0.1" },
    port: { type: "string", default: "3307" },
    "pg-port": { type: "string", default: "0" },
    user: { type: "string", default: "thindb" },
    password: { type: "string", default: "" },
    clients: { type: "string", default: "6" },
    seconds: { type: "string", default: "600" },
    // Rows per cycle = 2^doublings. 20 matches the incident's 1M-row seed.
    doublings: { type: "string", default: "20" },
    seed: { type: "string", default: "1" },
    killers: { type: "string", default: "0" },
    readers: { type: "string", default: "0" },
    "pg-clients": { type: "string", default: "0" },
    "view-churners": { type: "string", default: "0" },
    "view-readers": { type: "string", default: "0" },
    "drop-writes": { type: "string", default: "0" },
    slices: { type: "string", default: "0" },
    pool: { type: "string", default: "16" },
  },
});

const clientCount = Number(args.clients);
const deadline = Date.now() + Number(args.seconds) * 1000;
const doublings = Number(args.doublings);
const pgPort = Number(args["pg-port"]);
const pgClientCount = Number(args["pg-clients"]);
if (pgClientCount > 0 && pgPort === 0) throw new Error("--pg-clients needs --pg-port");
const viewChurnerCount = Number(args["view-churners"]);
const viewReaderCount = Number(args["view-readers"]);
if (viewReaderCount > 0 && viewChurnerCount === 0) throw new Error("--view-readers needs --view-churners");
const dropWriteShare = Number(args["drop-writes"]);
// The statements one cycle issues: three DROP IF EXISTS, CREATE, the seed
// insert, the doublings and their periodic counts, and the fixed tail.
const stepsPerCycle = 19 + doublings + Math.ceil(doublings / 5);

const sliceCount = Number(args.slices);
const poolSize = Number(args.pool);

class InvariantError extends Error {}
class StallError extends Error {}
// A kill aimed at this client ended its cycle; the next cycle starts over.
// `lost` = the connection is gone, not just its statement.
class KilledError extends Error {
  constructor(
    message: string,
    readonly lost: boolean,
  ) {
    super(message);
  }
}

// A statement slower than this is reported as a stall: at these sizes every
// step takes seconds, so minutes means the server is wedged.
const STALL_MS = 300_000;
// A killed connection's thread unwinds its statement before it leaves the
// process list; one that stays listed this long never let go.
const GONE_MS = 60_000;
const HEAVY_JOIN = "SELECT COUNT(*) FROM seq a JOIN seq b ON a.n = b.n";

function withTimeout<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new StallError(`${what} took over ${ms / 1000}s`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

const stats = {
  cycles: 0,
  killedCycles: 0,
  statements: 0,
  abandoned: 0,
  droppedWrites: 0,
  kills: { query: 0, connection: 0, unknownId: 0, pgCancel: 0, pgTerminate: 0, cancelRequest: 0 },
  processListReads: 0,
  killedStatements: 0,
  atomicityChecks: 0,
  readerQueries: 0,
  expectedReaderErrors: 0,
  pgQueries: 0,
  pgAbandoned: 0,
  viewDdl: 0,
  viewReads: 0,
  expectedViewErrors: 0,
  pool: { reads: 0, writes: 0, ddl: 0, doublings: 0, killed: 0, reconnects: 0, databaseDrops: 0, maxLedger: 0 },
  expectedPoolErrors: new Map<string, number>(),
  tornReads: [] as string[],
  refusedConnects: 0,
  sqlErrors: new Map<string, number>(),
  invariantFailures: [] as string[],
  stalls: [] as string[],
  serverDown: false,
};

// Stress connections the killers may aim at, by connection id.
const killable = new Map<number, string>();
// Every id a KILL, pg_cancel_backend, pg_terminate_backend or CancelRequest
// was aimed at. Ids are never reused, so an interrupt or a dropped connection
// whose id is missing here is one the server made on its own.
const targeted = new Set<number>();

let rngState = Number(args.seed) >>> 0 || 1;
function random(): number {
  rngState ^= rngState << 13;
  rngState ^= rngState >>> 17;
  rngState ^= rngState << 5;
  return (rngState >>> 0) / 0x100000000;
}

function pick<T>(items: readonly T[]): T {
  return items[Math.floor(random() * items.length)];
}

async function connect(database?: string): Promise<Connection> {
  const conn = await mysql.createConnection({
    host: args.host,
    port: Number(args.port),
    user: args.user,
    password: args.password,
    database,
    multipleStatements: false,
  });
  // A connection killed while idle fails with no command to reject; without
  // a listener that error would take the whole harness down.
  conn.on("error", () => undefined);
  return conn;
}

// `dropAt` is the index of the statement whose socket the client drops
// mid-flight, or -1 for none.
type Session = { conn: Connection; id: number; role: string; database?: string; steps: number; dropAt: number };

async function open(role: string, database?: string, canBeKilled = true): Promise<Session> {
  const conn = await connect(database);
  const session = { conn, id: conn.threadId ?? 0, role, database, steps: 0, dropAt: -1 };
  if (canBeKilled) killable.set(session.id, role);
  return session;
}

async function close(session: Session): Promise<void> {
  killable.delete(session.id);
  await withTimeout(session.conn.end(), 10_000, "connection close").catch(() => session.conn.destroy());
}

function errorFields(err: unknown): { code: string; errno: number; sqlState: string; message: string } {
  const e = err as { code?: string; errno?: number; sqlState?: string; message?: string };
  return { code: e.code ?? "", errno: e.errno ?? 0, sqlState: e.sqlState ?? "", message: e.message ?? String(err) };
}

const LOST_CODES = new Set([
  "PROTOCOL_CONNECTION_LOST",
  "ECONNRESET",
  "ECONNABORTED",
  "EPIPE",
  "PROTOCOL_ENQUEUE_AFTER_FATAL_ERROR",
  "PROTOCOL_ENQUEUE_AFTER_QUIT",
  "PG_CONNECTION_LOST",
]);

function killOutcome(err: unknown): "interrupted" | "lost" | null {
  const { code, errno, sqlState, message } = errorFields(err);
  if (errno === 1317 || sqlState === "57014") return "interrupted";
  if (sqlState === "57P01" || LOST_CODES.has(code) || message.includes("closed state")) return "lost";
  return null;
}

// A burst of short-lived connections can fill the listen backlog or the
// client's ephemeral ports, so one refused connect does not mean the server
// died; it has to refuse every attempt for a couple of seconds.
async function serverAlive(): Promise<boolean> {
  for (let attempt = 0; attempt < 10; attempt++) {
    try {
      const conn = await connect();
      await conn.end().catch(() => conn.destroy());
      return true;
    } catch (err) {
      if (errorFields(err).code !== "ECONNREFUSED") return true;
    }
    await Bun.sleep(250);
  }
  return false;
}

function markServerDown(who: string, err: unknown): void {
  if (!stats.serverDown) console.error(`SERVER DOWN (${who}): ${errorFields(err).message}`);
  stats.serverDown = true;
}

// Whether a failed statement is a kill; one nobody aimed at `id` is recorded
// as an invariant failure, since the server cancelled or closed it on its own.
async function isKill(err: unknown, id: number, role: string): Promise<boolean> {
  const outcome = killOutcome(err);
  if (outcome === null) return false;
  if (targeted.has(id)) return true;
  if (outcome === "lost" && !(await serverAlive())) {
    markServerDown(role, err);
    return false;
  }
  const message = `${role} (connection ${id}) ${outcome} without a kill: ${errorFields(err).message}`;
  stats.invariantFailures.push(message);
  console.error(`INVARIANT ${message}`);
  return true;
}

async function run(conn: Connection, sql: string): Promise<any[]> {
  stats.statements++;
  const [rows] = await withTimeout(conn.query(sql), STALL_MS, sql);
  return rows as any[];
}

async function scalarOn(conn: Connection, sql: string): Promise<number> {
  const rows = await run(conn, sql);
  const row = rows[0] as Record<string, unknown> | undefined;
  if (!row) throw new InvariantError(`no row: ${sql}`);
  return Number(Object.values(row)[0]);
}

// The table's row count, or null when it doesn't exist.
async function countOrAbsent(conn: Connection, table: string): Promise<number | null> {
  try {
    return await scalarOn(conn, `SELECT COUNT(*) FROM ${table}`);
  } catch (err) {
    if (errorFields(err).errno === 1146) return null;
    throw err;
  }
}

function expectOneOf(table: string, allowed: (number | null)[], what: string): (conn: Connection) => Promise<void> {
  return async (conn) => {
    const got = await countOrAbsent(conn, table);
    if (!allowed.includes(got)) {
      throw new InvariantError(`killed ${what} left ${table} at ${got ?? "absent"}, expected one of ${allowed.map((a) => a ?? "absent").join(", ")}`);
    }
  };
}

// Wait until connection `id` has left the process list.
async function awaitGone(conn: Connection, id: number, role: string): Promise<void> {
  const until = Date.now() + GONE_MS;
  while ((await scalarOn(conn, `SELECT COUNT(*) FROM information_schema.processlist WHERE ID = ${id}`)) > 0) {
    if (Date.now() > until) {
      stats.stalls.push(`${role}: killed connection ${id} still listed after ${GONE_MS / 1000}s`);
      return;
    }
    await Bun.sleep(50);
  }
}

class DroppedError extends Error {
  readonly code = "PROTOCOL_CONNECTION_LOST";
}

// Start `sql`, then destroy the socket under it: the server's disconnect path
// races its own in-flight statement. A statement that finishes first passes
// the drop on to the next one.
async function runThenDrop(session: Session, sql: string): Promise<any[]> {
  const pending = run(session.conn, sql);
  const settled = pending.then(
    () => "done" as const,
    () => "done" as const,
  );
  // Skewed short, so statements of a few milliseconds get dropped too.
  const delayMs = Math.floor(random() ** 3 * 200);
  const winner = await Promise.race([settled, Bun.sleep(delayMs).then(() => "drop" as const)]);
  if (winner === "done") {
    session.dropAt++;
    return pending;
  }
  targeted.add(session.id);
  session.conn.destroy();
  stats.droppedWrites++;
  // mysql2 may never settle a query whose socket was destroyed under it.
  await withTimeout(settled, 5_000, "dropped statement").catch(() => undefined);
  throw new DroppedError(`dropped the socket under ${sql.slice(0, 80)}`);
}

// Run one statement of a cycle. A kill aimed at this connection ends the
// cycle, after `verify` confirms on a connection nobody kills that the
// killed statement took effect entirely or not at all.
async function step(session: Session, sql: string, verify?: (conn: Connection) => Promise<void>): Promise<any[]> {
  const drop = session.steps++ === session.dropAt;
  try {
    return drop ? await runThenDrop(session, sql) : await run(session.conn, sql);
  } catch (err) {
    if (!(await isKill(err, session.id, session.role))) throw err;
    stats.killedStatements++;
    const lost = killOutcome(err) === "lost";
    if (lost) session.conn.destroy();
    if (lost || verify) {
      const checker = await connect(session.database);
      try {
        if (lost) await awaitGone(checker, session.id, session.role);
        if (verify) {
          stats.atomicityChecks++;
          await verify(checker);
        }
      } finally {
        await checker.end().catch(() => checker.destroy());
      }
    }
    throw new KilledError(`${session.role}: ${sql.slice(0, 80)}`, lost);
  }
}

async function scalar(session: Session, sql: string): Promise<number> {
  const rows = await step(session, sql);
  const row = rows[0] as Record<string, unknown> | undefined;
  if (!row) throw new InvariantError(`no row: ${sql}`);
  return Number(Object.values(row)[0]);
}

async function expectCount(session: Session, table: string, expected: number, what: string): Promise<void> {
  const got = await scalar(session, `SELECT COUNT(*) FROM ${table}`);
  if (got !== expected) throw new InvariantError(`${what}: ${table} has ${got} rows, expected ${expected}`);
}

// Open a side connection, start a heavy query and drop the socket while it
// runs: the server's disconnect path races the in-flight statement.
async function abandonQuery(database: string, sql = HEAVY_JOIN): Promise<void> {
  const side = await connect(database);
  const pending = side.query(sql).catch(() => undefined);
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
  const admin = await open(`admin c${client}`);
  try {
    if (cycleNo % 3 === 2) await step(admin, `DROP DATABASE IF EXISTS ${database}`);
    await step(admin, `CREATE DATABASE IF NOT EXISTS ${database}`);
  } finally {
    await close(admin);
  }

  const s = await open(`writer c${client}`, `${database}__public`);
  if (random() < dropWriteShare) s.dropAt = Math.floor(random() * stepsPerCycle);
  try {
    for (const t of ["seq", "seq_copy", "seq_moved"]) await step(s, `DROP TABLE IF EXISTS ${t}`);
    await step(s, "CREATE TABLE seq (n BIGINT NOT NULL, v INT NOT NULL, s VARCHAR(32) NOT NULL, PRIMARY KEY (n))");
    await step(s, "INSERT INTO seq (n, v, s) VALUES (1, 1, 'a')", expectOneOf("seq", [0, 1], "insert"));
    let rows = 1;
    for (let d = 0; d < doublings; d++) {
      await step(s, `INSERT INTO seq (n, v, s) SELECT n + ${rows}, v + 1, CONCAT(s, 'b') FROM seq`, expectOneOf("seq", [rows, rows * 2], `doubling ${d}`));
      rows *= 2;
      if (d % 5 === 4 || d === doublings - 1) await expectCount(s, "seq", rows, `doubling ${d}`);
      if (random() < 0.05) await abandonQuery(`${database}__public`);
    }

    await step(s, "CREATE TABLE seq_copy AS SELECT n, v, s FROM seq", expectOneOf("seq_copy", [null, rows], "ctas"));
    await expectCount(s, "seq_copy", rows, "ctas");

    // Each doubling copies every row with v + 1, so v - 1 counts the copies a
    // row went through: C(doublings, v - 1) rows hold each v, spread across
    // the whole key range.
    const deleteAbove = Math.floor(doublings / 2) + 1;
    let deleted = 0;
    for (let v = deleteAbove + 1; v <= doublings + 1; v++) deleted += binomial(doublings, v - 1);
    const kept = rows - deleted;
    await step(s, `DELETE FROM seq_copy WHERE v > ${deleteAbove}`, expectOneOf("seq_copy", [rows, kept], "delete"));
    await expectCount(s, "seq_copy", kept, "delete");

    const updateV = Math.max(1, deleteAbove - 1);
    const toUpdate = binomial(doublings, updateV - 1);
    const updatedSql = "SELECT COUNT(*) FROM seq_copy WHERE s = 'updated'";
    await step(s, `UPDATE seq_copy SET s = 'updated' WHERE v = ${updateV}`, async (conn) => {
      const got = await scalarOn(conn, updatedSql);
      if (got !== 0 && got !== toUpdate) throw new InvariantError(`killed update left ${got} rows updated, expected 0 or ${toUpdate}`);
      await expectOneOf("seq_copy", [kept], "update")(conn);
    });
    const updated = await scalar(s, updatedSql);
    if (updated !== toUpdate) throw new InvariantError(`update: ${updated} rows updated, expected ${toUpdate}`);
    await step(s, "ALTER TABLE seq_copy ADD COLUMN extra INT NULL", expectOneOf("seq_copy", [kept], "alter"));
    await step(s, "RENAME TABLE seq_copy TO seq_moved", async (conn) => {
      const from = await countOrAbsent(conn, "seq_copy");
      const to = await countOrAbsent(conn, "seq_moved");
      if (!((from === kept && to === null) || (from === null && to === kept))) {
        throw new InvariantError(`killed rename left seq_copy at ${from ?? "absent"} and seq_moved at ${to ?? "absent"}, expected ${kept} rows in exactly one`);
      }
    });
    await expectCount(s, "seq_moved", kept, "rename");
    await step(s, "SELECT SUM(v), COUNT(DISTINCT s), MAX(extra) FROM seq_moved");

    await step(s, "TRUNCATE TABLE seq_moved", expectOneOf("seq_moved", [kept, 0], "truncate"));
    await expectCount(s, "seq_moved", 0, "truncate");
    await step(s, "DROP TABLE seq_moved", expectOneOf("seq_moved", [0, null], "drop"));
    if (random() < 0.3) await step(s, "DROP TABLE seq", expectOneOf("seq", [rows, null], "drop"));
  } finally {
    await close(s);
  }
}

// Sort out a failure that ended a round of a client's loop. False = stop.
async function recordError(who: string, err: unknown): Promise<boolean> {
  if (err instanceof KilledError) {
    stats.killedCycles++;
    return true;
  }
  if (err instanceof StallError) {
    stats.stalls.push(`${who}: ${err.message}`);
    console.error(`STALL ${who}: ${err.message}`);
    return false;
  }
  if (err instanceof InvariantError) {
    stats.invariantFailures.push(`${who}: ${err.message}`);
    console.error(`INVARIANT ${who}: ${err.message}`);
    return true;
  }
  const { code, message } = errorFields(err);
  if (stats.serverDown || ((code === "ECONNREFUSED" || killOutcome(err) === "lost") && !(await serverAlive()))) {
    markServerDown(who, err);
    return false;
  }
  if (code === "ECONNREFUSED") {
    stats.refusedConnects++;
    await Bun.sleep(500);
    return true;
  }
  const key = message.slice(0, 160);
  if (!stats.sqlErrors.has(key)) console.error(`SQL ERROR ${who}: ${key}`);
  stats.sqlErrors.set(key, (stats.sqlErrors.get(key) ?? 0) + 1);
  return true;
}

async function client(id: number): Promise<void> {
  for (let cycleNo = 0; Date.now() < deadline && !stats.serverDown; cycleNo++) {
    try {
      await cycle(id, cycleNo);
      stats.cycles++;
    } catch (err) {
      if (!(await recordError(`client ${id} cycle ${cycleNo}`, err))) return;
    }
  }
}

const READER_QUERIES = [
  "SELECT COUNT(*) FROM seq",
  "SELECT COUNT(*), SUM(v), MAX(n) FROM seq_copy",
  "SELECT s, COUNT(*) FROM seq GROUP BY s ORDER BY 2 DESC, 1 LIMIT 5",
  "SELECT n, v, s FROM seq_moved WHERE n = 777",
  "SELECT COUNT(*) FROM seq a JOIN seq_copy b ON a.n = b.n",
  "SELECT MAX(extra), COUNT(*) FROM seq_moved",
  "SELECT n, v FROM seq ORDER BY n DESC LIMIT 3",
  "SHOW TABLES",
];
// Unknown table, database or column, or no database selected because the
// reader's current one was dropped: the owner dropped or has not yet created
// what the reader asked for.
const READER_EXPECTED_ERRNOS = new Set([1146, 1049, 1054, 1046]);
const READER_EXPECTED_SQLSTATES = new Set(["42P01", "3D000", "42703", "3F000"]);

function isPowerOfTwo(n: number): boolean {
  return n > 0 && Number.isInteger(Math.log2(n));
}

// `seq` only ever holds 2^k rows between statements, so any other count is a
// read that saw part of a concurrent INSERT ... SELECT.
function checkSeqCount(sql: string, count: number, who: string): void {
  if (sql === READER_QUERIES[0] && count !== 0 && !isPowerOfTwo(count)) {
    stats.tornReads.push(`${who}: seq had ${count} rows`);
  }
}

// Read another client's tables while that client runs DDL on them. Missing
// tables and databases are expected; a crash or any other error is not.
async function reader(id: number): Promise<void> {
  const who = `reader ${id}`;
  while (Date.now() < deadline && !stats.serverDown) {
    let session: Session | undefined;
    try {
      session = await open(who, `stress_c${Math.floor(random() * clientCount)}__public`);
      for (let i = 0; i < 25 && Date.now() < deadline; i++) {
        const sql = pick(READER_QUERIES);
        try {
          const rows = await run(session.conn, sql);
          stats.readerQueries++;
          checkSeqCount(sql, Number(Object.values(rows[0] ?? {})[0] ?? 0), who);
        } catch (err) {
          if (READER_EXPECTED_ERRNOS.has(errorFields(err).errno)) {
            stats.expectedReaderErrors++;
            continue;
          }
          if (!(await isKill(err, session.id, who))) throw err;
          stats.killedStatements++;
          if (killOutcome(err) === "lost") break;
        }
      }
    } catch (err) {
      if (READER_EXPECTED_ERRNOS.has(errorFields(err).errno)) {
        stats.expectedReaderErrors++;
        await Bun.sleep(20);
      } else if (!(await recordError(who, err))) {
        return;
      }
    } finally {
      if (session) await close(session);
    }
  }
}

const VIEW_SLOTS = 4;
const GROWTH_ENTRIES = 40;
const BASE_ROWS = 64;
const VIEW_VERSIONS = [1, 2, 3, 4];
// Every version of every view and function selects a multiple of 16 rows, or
// none while a recreated base table is still empty, so any other count is a
// reader that expanded a definition nobody registered.
const VIEW_COUNTS = new Set([0, ...VIEW_VERSIONS.map((v) => 16 * v)]);

function viewBody(version: number): string {
  return `SELECT n, g FROM base WHERE n <= ${16 * version}`;
}

function functionBody(version: number): string {
  return `SELECT n, g FROM base WHERE n <= ${16 * version} AND n > x`;
}

// Fill both registries well past their size and empty them again, so their
// maps grow and rehash, moving every entry, while readers look them up.
async function growRegistries(session: Session, id: number): Promise<void> {
  for (let i = 0; i < GROWTH_ENTRIES; i++) {
    await step(session, `CREATE OR REPLACE VIEW grow${id}_${i} AS SELECT 1 AS one`);
    await step(session, `CREATE OR REPLACE FUNCTION growf${id}_${i}(x BIGINT) RETURNS TABLE AS (SELECT x AS one)`);
  }
  for (let i = 0; i < GROWTH_ENTRIES; i++) {
    await step(session, `DROP VIEW IF EXISTS grow${id}_${i}`);
    await step(session, `DROP FUNCTION IF EXISTS growf${id}_${i}`);
  }
}

// Replace, drop and recreate views v0..v3 and functions f0..f3 over one base
// table, dropping the whole database every tenth round.
async function viewChurner(id: number): Promise<void> {
  const who = `view churner ${id}`;
  const database = `stress_v${id}`;
  for (let round = 0; Date.now() < deadline && !stats.serverDown; round++) {
    let session: Session | undefined;
    try {
      const admin = await open(`${who} admin`);
      try {
        if (round % 10 === 9) await step(admin, `DROP DATABASE IF EXISTS ${database}`);
        await step(admin, `CREATE DATABASE IF NOT EXISTS ${database}`);
      } finally {
        await close(admin);
      }
      session = await open(who, `${database}__public`);
      await step(session, "CREATE TABLE IF NOT EXISTS base (n BIGINT NOT NULL, g INT NOT NULL, PRIMARY KEY (n))");
      const baseRows = await scalar(session, "SELECT COUNT(*) FROM base");
      if (baseRows === 0) {
        const values = Array.from({ length: BASE_ROWS }, (_, i) => `(${i + 1}, ${(i + 1) % 7})`).join(", ");
        await step(session, `INSERT INTO base (n, g) VALUES ${values}`, expectOneOf("base", [0, BASE_ROWS], "base insert"));
      } else if (baseRows !== BASE_ROWS) {
        throw new InvariantError(`base has ${baseRows} rows, expected 0 or ${BASE_ROWS}`);
      }
      for (let i = 0; i < 30 && Date.now() < deadline; i++) {
        const slot = Math.floor(random() * VIEW_SLOTS);
        const version = pick(VIEW_VERSIONS);
        const roll = random();
        if (roll < 0.35) await step(session, `CREATE OR REPLACE VIEW v${slot} AS ${viewBody(version)}`);
        else if (roll < 0.5) await step(session, `DROP VIEW IF EXISTS v${slot}`);
        else if (roll < 0.8) await step(session, `CREATE OR REPLACE FUNCTION f${slot}(x BIGINT) RETURNS TABLE AS (${functionBody(version)})`);
        else if (roll < 0.95) await step(session, `DROP FUNCTION IF EXISTS f${slot}`);
        else await growRegistries(session, id);
        stats.viewDdl++;
      }
    } catch (err) {
      if (!(await recordError(who, err))) return;
    } finally {
      if (session) await close(session);
    }
  }
}

const VIEW_READS: { sql: (slot: number) => string; counts: boolean }[] = [
  { sql: (slot) => `SELECT COUNT(*) FROM v${slot}`, counts: true },
  { sql: (slot) => `SELECT COUNT(*) FROM f${slot}(0)`, counts: true },
  { sql: (slot) => `SELECT COUNT(*) FROM v${slot} a JOIN f${slot}(0) b ON a.n = b.n`, counts: true },
  { sql: (slot) => `SELECT g, COUNT(*) FROM v${slot} GROUP BY g ORDER BY g LIMIT 3`, counts: false },
  { sql: (slot) => `SHOW CREATE FUNCTION f${slot}`, counts: false },
];

// A view, function, table or database the churner has just dropped.
function isExpectedViewError(err: unknown): boolean {
  const { errno, message } = errorFields(err);
  return READER_EXPECTED_ERRNOS.has(errno) || message.includes("SqlUnsupportedFileFunction") || message.includes("FunctionNotFound");
}

function checkViewRead(sql: string, rows: any[], counts: boolean, who: string): void {
  if (counts) {
    const got = Number(Object.values(rows[0] ?? {})[0] ?? 0);
    if (!VIEW_COUNTS.has(got)) {
      const message = `${who}: ${sql} returned ${got}, expected one of ${[...VIEW_COUNTS].join(", ")}`;
      stats.invariantFailures.push(message);
      console.error(`INVARIANT ${message}`);
    }
    return;
  }
  if (!sql.startsWith("SHOW CREATE FUNCTION") || rows.length === 0) return;
  const text = String(Object.values(rows[0])[0] ?? "");
  if (!VIEW_VERSIONS.some((v) => text.includes(functionBody(v)))) {
    const message = `${who}: ${sql} returned ${JSON.stringify(text.slice(0, 200))}`;
    stats.invariantFailures.push(message);
    console.error(`INVARIANT ${message}`);
  }
}

// Expand the churners' views and functions while they are being replaced and
// dropped. Every read that succeeds must match some version that was
// registered.
async function viewReader(id: number): Promise<void> {
  const who = `view reader ${id}`;
  while (Date.now() < deadline && !stats.serverDown) {
    let session: Session | undefined;
    try {
      session = await open(who, `stress_v${Math.floor(random() * viewChurnerCount)}__public`);
      for (let i = 0; i < 25 && Date.now() < deadline; i++) {
        const read = pick(VIEW_READS);
        const sql = read.sql(Math.floor(random() * VIEW_SLOTS));
        try {
          const rows = await run(session.conn, sql);
          stats.viewReads++;
          checkViewRead(sql, rows, read.counts, who);
        } catch (err) {
          if (isExpectedViewError(err)) {
            stats.expectedViewErrors++;
            continue;
          }
          if (!(await isKill(err, session.id, who))) throw err;
          stats.killedStatements++;
          if (killOutcome(err) === "lost") break;
        }
      }
    } catch (err) {
      if (isExpectedViewError(err)) {
        stats.expectedViewErrors++;
        await Bun.sleep(20);
      } else if (!(await recordError(who, err))) {
        return;
      }
    } finally {
      if (session) await close(session);
    }
  }
}

const PROCESS_LIST_READS = [
  { sql: "SHOW PROCESSLIST", complete: true },
  { sql: "SHOW FULL PROCESSLIST", complete: true },
  { sql: "SELECT * FROM performance_schema.processlist", complete: true },
  { sql: "SELECT ID, USER, DB, COMMAND, TIME, STATE, INFO FROM information_schema.processlist WHERE COMMAND <> 'Sleep' ORDER BY TIME DESC", complete: false },
  { sql: "SELECT COMMAND, COUNT(*) AS n, MAX(TIME) AS t FROM information_schema.PROCESSLIST GROUP BY COMMAND", complete: false },
];

function listedProcess(row: Record<string, unknown>): { id: number; command: string } | null {
  let id: number | null = null;
  let command = "";
  for (const [key, value] of Object.entries(row)) {
    if (key.toLowerCase() === "id") id = Number(value);
    if (key.toLowerCase() === "command") command = String(value);
  }
  return id === null ? null : { id, command };
}

// Read the process list, then KILL QUERY or KILL CONNECTION a stress
// connection, preferring one that is running a statement.
async function killer(id: number): Promise<void> {
  const who = `killer ${id}`;
  let session: Session | undefined;
  while (Date.now() < deadline && !stats.serverDown) {
    try {
      session ??= await open(who, undefined, false);
      await Bun.sleep(20 + Math.floor(random() * 300));
      const read = pick(PROCESS_LIST_READS);
      const listed = (await run(session.conn, read.sql)).map(listedProcess).filter((p) => p !== null);
      stats.processListReads++;
      if (read.complete) {
        const own = listed.find((p) => p.id === session!.id);
        if (!own || own.command !== "Query") throw new InvariantError(`${read.sql} lists the reading connection ${session.id} as ${own?.command ?? "absent"}`);
      }
      const running = listed.filter((p) => p.command !== "Sleep" && killable.has(p.id)).map((p) => p.id);
      const pool = running.length > 0 && random() < 0.8 ? running : [...killable.keys()];
      if (pool.length === 0) continue;
      const victim = pick(pool);
      const roll = random();
      const [sql, kind] = roll < 0.55 ? [`KILL QUERY ${victim}`, "query"] : roll < 0.8 ? [`KILL CONNECTION ${victim}`, "connection"] : [`KILL ${victim}`, "connection"];
      targeted.add(victim);
      try {
        await run(session.conn, sql);
        stats.kills[kind as "query" | "connection"]++;
      } catch (err) {
        if (errorFields(err).errno !== 1094) throw err;
        stats.kills.unknownId++;
      }
    } catch (err) {
      if (session && (await isKill(err, session.id, who))) stats.killedStatements++;
      if (session) session.conn.destroy();
      session = undefined;
      if (!(await recordError(who, err))) return;
    }
  }
  if (session) await close(session);
}

class PgError extends Error {
  constructor(
    message: string,
    readonly sqlState: string,
    readonly code = "",
  ) {
    super(message);
  }
}

type PgResult = { rows: (string | null)[][] };

// Just enough of the PostgreSQL simple-query protocol to start a query and
// drop the socket under it, which a driver would not let us do.
class PgConnection {
  pid = 0;
  secret = 0;
  private buffer = Buffer.alloc(0);
  private messages: { type: string; body: Buffer }[] = [];
  private wake: (() => void) | null = null;
  private closed = false;

  private constructor(private readonly socket: net.Socket) {
    socket.on("data", (chunk: Buffer) => {
      this.buffer = Buffer.concat([this.buffer, chunk]);
      while (this.buffer.length >= 5) {
        const length = this.buffer.readInt32BE(1);
        if (this.buffer.length < 1 + length) break;
        this.messages.push({ type: String.fromCharCode(this.buffer[0]), body: this.buffer.subarray(5, 1 + length) });
        this.buffer = this.buffer.subarray(1 + length);
      }
      this.notify();
    });
    socket.on("close", () => {
      this.closed = true;
      this.notify();
    });
    socket.on("error", () => undefined);
  }

  static async open(database: string): Promise<PgConnection> {
    const socket = await new Promise<net.Socket>((resolve, reject) => {
      const s = net.connect({ host: args.host, port: pgPort }, () => resolve(s));
      s.once("error", reject);
    });
    const pg = new PgConnection(socket);
    const params = Buffer.from(`user\0${args.user}\0database\0${database}\0application_name\0stress_ddl\0\0`);
    const head = Buffer.alloc(8);
    head.writeInt32BE(8 + params.length, 0);
    head.writeInt32BE(196608, 4);
    socket.write(Buffer.concat([head, params]));
    for (;;) {
      const m = await pg.next();
      if (m.type === "E") throw pg.error(m.body);
      if (m.type === "R" && m.body.readInt32BE(0) !== 0) throw new PgError("PG wire asks for a password", "28000");
      if (m.type === "K") {
        pg.pid = m.body.readInt32BE(0);
        pg.secret = m.body.readInt32BE(4);
      }
      if (m.type === "Z") return pg;
    }
  }

  private notify(): void {
    const wake = this.wake;
    this.wake = null;
    wake?.();
  }

  private async next(): Promise<{ type: string; body: Buffer }> {
    for (;;) {
      const m = this.messages.shift();
      if (m) return m;
      if (this.closed) throw new PgError("PG connection lost", "", "PG_CONNECTION_LOST");
      await new Promise<void>((resolve) => (this.wake = resolve));
    }
  }

  private error(body: Buffer): PgError {
    const fields = new Map<string, string>();
    let at = 0;
    while (at < body.length && body[at] !== 0) {
      const end = body.indexOf(0, at + 1);
      fields.set(String.fromCharCode(body[at]), body.toString("utf8", at + 1, end));
      at = end + 1;
    }
    return new PgError(fields.get("M") ?? "PG error", fields.get("C") ?? "");
  }

  send(sql: string): void {
    const text = Buffer.from(`${sql}\0`);
    const head = Buffer.alloc(5);
    head.write("Q", 0);
    head.writeInt32BE(4 + text.length, 1);
    this.socket.write(Buffer.concat([head, text]));
  }

  async query(sql: string): Promise<PgResult> {
    stats.pgQueries++;
    this.send(sql);
    return withTimeout(this.collect(), STALL_MS, `pg: ${sql}`);
  }

  private async collect(): Promise<PgResult> {
    const rows: (string | null)[][] = [];
    let failure: PgError | null = null;
    for (;;) {
      const m = await this.next();
      if (m.type === "E") failure ??= this.error(m.body);
      if (m.type === "D") rows.push(this.dataRow(m.body));
      if (m.type === "Z") break;
    }
    if (failure) throw failure;
    return { rows };
  }

  private dataRow(body: Buffer): (string | null)[] {
    const cells: (string | null)[] = [];
    let at = 2;
    for (let i = body.readInt16BE(0); i > 0; i--) {
      const length = body.readInt32BE(at);
      at += 4;
      cells.push(length < 0 ? null : body.toString("utf8", at, at + length));
      if (length > 0) at += length;
    }
    return cells;
  }

  // Cancel this connection's running statement the way libpq does: a
  // CancelRequest on a fresh socket.
  async cancelRequest(): Promise<void> {
    const request = Buffer.alloc(16);
    request.writeInt32BE(16, 0);
    request.writeInt32BE(80877102, 4);
    request.writeInt32BE(this.pid, 8);
    request.writeInt32BE(this.secret, 12);
    await new Promise<void>((resolve) => {
      const s = net.connect({ host: args.host, port: pgPort }, () => s.end(request));
      s.on("close", () => resolve());
      s.on("error", () => resolve());
    });
  }

  destroy(): void {
    this.socket.destroy();
  }

  async close(): Promise<void> {
    if (!this.closed && !this.socket.destroyed) this.socket.end(Buffer.from([0x58, 0, 0, 0, 4]));
    this.socket.destroy();
  }
}

// A PostgreSQL-wire client. Each round it connects to a random stress
// database and either abandons a heavy query, cancels its own query with a
// CancelRequest, reads pg_stat_activity and signals a stress connection of
// either wire, or just reads.
async function pgClient(id: number): Promise<void> {
  const who = `pg ${id}`;
  while (Date.now() < deadline && !stats.serverDown) {
    let pg: PgConnection | undefined;
    try {
      pg = await PgConnection.open(`stress_c${Math.floor(random() * clientCount)}`);
      killable.set(pg.pid, who);
      const roll = random();
      if (roll < 0.3) {
        pg.send(HEAVY_JOIN);
        await Bun.sleep(Math.floor(random() * 50));
        pg.destroy();
        stats.pgAbandoned++;
      } else if (roll < 0.5) {
        const failure = pg.query(HEAVY_JOIN).then(
          () => null,
          (err: unknown) => err,
        );
        await Bun.sleep(Math.floor(random() * 50));
        targeted.add(pg.pid);
        await pg.cancelRequest();
        stats.kills.cancelRequest++;
        const err = await failure;
        if (err !== null) {
          if (killOutcome(err) !== "interrupted") throw err;
          stats.killedStatements++;
        }
        await pg.query("SELECT 1");
      } else if (roll < 0.75) {
        const active = await pg.query("SELECT pid, state, query FROM pg_stat_activity WHERE state = 'active'");
        stats.processListReads++;
        const self = active.rows.find((row) => Number(row[0]) === pg!.pid);
        if (!self) throw new InvariantError(`pg_stat_activity misses the reading connection ${pg.pid}`);
        const running = active.rows.map((row) => Number(row[0])).filter((pid) => pid !== pg!.pid && killable.has(pid));
        const pool = running.length > 0 ? running : [...killable.keys()].filter((pid) => pid !== pg!.pid);
        if (pool.length > 0) {
          const victim = pick(pool);
          const terminate = random() < 0.4;
          targeted.add(victim);
          await pg.query(`SELECT ${terminate ? "pg_terminate_backend" : "pg_cancel_backend"}(${victim})`);
          stats.kills[terminate ? "pgTerminate" : "pgCancel"]++;
        }
      } else {
        for (let i = 0; i < 10 && Date.now() < deadline; i++) {
          const sql = pick(READER_QUERIES.filter((q) => q !== "SHOW TABLES"));
          try {
            const result = await pg.query(sql);
            stats.readerQueries++;
            checkSeqCount(sql, Number(result.rows[0]?.[0] ?? 0), who);
          } catch (err) {
            if (!READER_EXPECTED_SQLSTATES.has(errorFields(err).sqlState)) throw err;
            stats.expectedReaderErrors++;
          }
        }
      }
    } catch (err) {
      if (READER_EXPECTED_SQLSTATES.has(errorFields(err).sqlState)) {
        stats.expectedReaderErrors++;
        await Bun.sleep(20);
      } else if (pg && (await isKill(err, pg.pid, who))) {
        stats.killedStatements++;
      } else if (!(await recordError(who, err))) {
        return;
      }
    } finally {
      if (pg) {
        killable.delete(pg.pid);
        await pg.close();
      }
      // Every round is a fresh connection; pacing them keeps the client's
      // ephemeral ports from running out on a long run.
      await Bun.sleep(20 + Math.floor(random() * 80));
    }
  }
}

// Statements a pool connection runs before it rolls whether to drop its
// socket under one of the next batch, as a client's cycle does.
const POOL_ROUND = 20;
// Concurrent doublings copy each other's rows, so the table is only doubled
// below the cap, by a few connections at a time, and halved past the trim.
const LEDGER_CAP = 262_144;
const LEDGER_TRIM = 1_500_000;
const MAX_DOUBLERS = 3;
const SIDE_SLOTS = 4;
const LEDGER_DDL = "CREATE TABLE IF NOT EXISTS ledger (pair BIGINT NOT NULL, amount BIGINT NOT NULL, chk BIGINT NOT NULL, note VARCHAR(8) NOT NULL)";
const KEYED_DDL = "CREATE TABLE IF NOT EXISTS keyed (k BIGINT NOT NULL, v BIGINT NOT NULL, chk BIGINT NOT NULL, PRIMARY KEY (k))";
const doublers = new Array<number>(sliceCount).fill(0);

function poolDatabase(slice: number): string {
  return `stress_p${slice}`;
}

// What the slice's other connections cause: a table, column or database one
// of them just dropped, recreated or altered.
const POOL_EXPECTED_ERRNOS = new Set([1146, 1049, 1046, 1050, 1054, 1007]);

function isExpectedPoolError(err: unknown): boolean {
  if (err instanceof InvariantError || err instanceof KilledError || err instanceof StallError) return false;
  return POOL_EXPECTED_ERRNOS.has(errorFields(err).errno);
}

function notePoolError(err: unknown): void {
  const { errno, message } = errorFields(err);
  const key = `${errno} ${message.slice(0, 80)}`;
  stats.expectedPoolErrors.set(key, (stats.expectedPoolErrors.get(key) ?? 0) + 1);
}

function ledgerCheck(table: string): string {
  return `SELECT SUM(amount) AS total, COUNT(*) AS n, SUM(CASE WHEN chk = pair * 31 + amount THEN 0 ELSE 1 END) AS bad FROM ${table}`;
}

function keyedCheck(table: string): string {
  return `SELECT COUNT(*) AS n, COUNT(DISTINCT k) AS keys, SUM(CASE WHEN chk = k * 7 + v THEN 0 ELSE 1 END) AS bad FROM ${table}`;
}

function assertLedger(rows: any[], table: string): void {
  const row = rows[0] ?? {};
  const total = Number(row.total ?? 0);
  const n = Number(row.n ?? 0);
  const bad = Number(row.bad ?? 0);
  if (table === "ledger") stats.pool.maxLedger = Math.max(stats.pool.maxLedger, n);
  if (total !== 0 || n % 2 !== 0 || bad !== 0) {
    throw new InvariantError(`${table} has ${n} rows summing to ${total} with ${bad} bad checksums; pairs must sum to 0 and every chk match`);
  }
}

function assertKeyed(rows: any[], table: string): void {
  const row = rows[0] ?? {};
  const n = Number(row.n ?? 0);
  const keys = Number(row.keys ?? 0);
  const bad = Number(row.bad ?? 0);
  if (n !== keys || bad !== 0) throw new InvariantError(`${table} has ${n} rows over ${keys} keys with ${bad} bad checksums`);
}

function assertLedgerRows(rows: any[]): void {
  for (const row of rows) {
    if (Number(row.chk) !== Number(row.pair) * 31 + Number(row.amount)) throw new InvariantError(`ledger row ${JSON.stringify(row)} fails its checksum`);
  }
}

function assertKeyedRows(rows: any[]): void {
  if (rows.length > 1) throw new InvariantError(`keyed holds ${rows.length} rows for key ${rows[0].k}`);
  for (const row of rows) {
    if (Number(row.chk) !== Number(row.k) * 7 + Number(row.v)) throw new InvariantError(`keyed row ${JSON.stringify(row)} fails its checksum`);
  }
}

// After a kill, on a connection nobody kills; another connection may have
// dropped the table meanwhile.
function verifyLedger(table: string): (conn: Connection) => Promise<void> {
  return async (conn) => {
    try {
      assertLedger(await run(conn, ledgerCheck(table)), table);
    } catch (err) {
      if (!isExpectedPoolError(err)) throw err;
    }
  };
}

function verifyKeyed(table: string): (conn: Connection) => Promise<void> {
  return async (conn) => {
    try {
      assertKeyed(await run(conn, keyedCheck(table)), table);
    } catch (err) {
      if (!isExpectedPoolError(err)) throw err;
    }
  };
}

async function ensureSlice(slice: number): Promise<void> {
  const admin = await connect();
  try {
    await run(admin, `CREATE DATABASE IF NOT EXISTS ${poolDatabase(slice)}`);
    await run(admin, `USE ${poolDatabase(slice)}__public`);
    await run(admin, LEDGER_DDL);
    await run(admin, KEYED_DDL);
  } finally {
    await admin.end().catch(() => admin.destroy());
  }
}

const pairId = () => Math.floor(random() * 1_000_000);
const keyId = () => Math.floor(random() * 100_000);
const sideSlot = () => Math.floor(random() * SIDE_SLOTS);

async function insertPairs(s: Session): Promise<void> {
  const values: string[] = [];
  for (let i = 1 + Math.floor(random() * 8); i > 0; i--) {
    const pair = pairId();
    const amount = 1 + Math.floor(random() * 1000);
    values.push(`(${pair}, ${amount}, ${pair * 31 + amount}, 'i')`, `(${pair}, ${-amount}, ${pair * 31 - amount}, 'i')`);
  }
  await step(s, `INSERT INTO ledger (pair, amount, chk, note) VALUES ${values.join(", ")}`, verifyLedger("ledger"));
}

async function doubleLedger(s: Session, slice: number): Promise<void> {
  if (doublers[slice] >= MAX_DOUBLERS) return insertPairs(s);
  doublers[slice]++;
  try {
    const rows = await scalar(s, "SELECT COUNT(*) FROM ledger");
    if (rows > LEDGER_TRIM) {
      await step(s, `DELETE FROM ledger WHERE pair % 2 = ${Math.floor(random() * 2)}`, verifyLedger("ledger"));
      return;
    }
    if (rows >= LEDGER_CAP) return;
    const shift = 1_000_000 * (1 + Math.floor(random() * 1_000_000));
    await step(s, `INSERT INTO ledger (pair, amount, chk, note) SELECT pair + ${shift}, amount, (pair + ${shift}) * 31 + amount, 'd' FROM ledger`, verifyLedger("ledger"));
    stats.pool.doublings++;
  } finally {
    doublers[slice]--;
  }
}

async function updateLedger(s: Session): Promise<void> {
  const where = random() < 0.6 ? `pair = ${pairId()}` : `pair % 97 = ${Math.floor(random() * 97)}`;
  // `chk = 62 * pair - chk` reads no column this statement assigns, so it
  // holds whichever order the assignments are evaluated in.
  await step(s, `UPDATE ledger SET amount = -amount, chk = 62 * pair - chk, note = 'u' WHERE ${where}`, verifyLedger("ledger"));
}

async function deleteLedger(s: Session): Promise<void> {
  const where = random() < 0.6 ? `pair = ${pairId()}` : `pair % 211 = ${Math.floor(random() * 211)}`;
  await step(s, `DELETE FROM ledger WHERE ${where}`, verifyLedger("ledger"));
}

async function insertKeyed(s: Session): Promise<void> {
  const values: string[] = [];
  for (let i = 1 + Math.floor(random() * 4); i > 0; i--) {
    const k = keyId();
    const v = Math.floor(random() * 1000);
    values.push(`(${k}, ${v}, ${k * 7 + v})`);
  }
  await step(s, `INSERT INTO keyed (k, v, chk) VALUES ${values.join(", ")}`, verifyKeyed("keyed"));
}

async function copyKeyed(s: Session): Promise<void> {
  const shift = 100_000 * (1 + Math.floor(random() * 1000));
  await step(s, `INSERT INTO keyed (k, v, chk) SELECT k + ${shift}, v, (k + ${shift}) * 7 + v FROM keyed WHERE k < 100000`, verifyKeyed("keyed"));
}

async function ctasCopy(s: Session): Promise<void> {
  const table = `copy_${sideSlot()}`;
  await step(s, `DROP TABLE IF EXISTS ${table}`);
  await step(s, `CREATE TABLE ${table} AS SELECT pair, amount, chk, note FROM ledger`, verifyLedger(table));
}

async function ctasSide(s: Session): Promise<void> {
  const table = `side_${sideSlot()}`;
  await step(s, `DROP TABLE IF EXISTS ${table}`);
  await step(s, `CREATE TABLE ${table} AS SELECT k, v, chk FROM keyed`, verifyKeyed(table));
}

async function alterExtra(s: Session): Promise<void> {
  const table = random() < 0.7 ? "ledger" : "keyed";
  const verify = table === "ledger" ? verifyLedger(table) : verifyKeyed(table);
  const change = random() < 0.5 ? "ADD COLUMN extra INT NULL" : "DROP COLUMN extra";
  try {
    await step(s, `ALTER TABLE ${table} ${change}`, verify);
  } catch (err) {
    // The column is already there, or already gone: another connection's ALTER got in first.
    if (errorFields(err).errno !== 1064 || !/column|exist/i.test(errorFields(err).message)) throw err;
    notePoolError(err);
  }
}

async function truncateShared(s: Session): Promise<void> {
  const roll = random();
  const table = roll < 0.2 ? "ledger" : roll < 0.4 ? "keyed" : roll < 0.7 ? `copy_${sideSlot()}` : `side_${sideSlot()}`;
  await step(s, `TRUNCATE TABLE ${table}`);
}

async function recreateShared(s: Session): Promise<void> {
  if (random() < 0.5) {
    await step(s, "DROP TABLE IF EXISTS ledger");
    await step(s, LEDGER_DDL);
  } else {
    await step(s, "DROP TABLE IF EXISTS keyed");
    await step(s, KEYED_DDL);
  }
}

async function dropSlice(_s: Session, slice: number): Promise<void> {
  const admin = await connect();
  try {
    await run(admin, `DROP DATABASE IF EXISTS ${poolDatabase(slice)}`);
    stats.pool.databaseDrops++;
  } finally {
    await admin.end().catch(() => admin.destroy());
  }
  await ensureSlice(slice);
}

type PoolAction = { weight: number; kind: "reads" | "writes" | "ddl"; run: (s: Session, slice: number) => Promise<void> };

const POOL_ACTIONS: PoolAction[] = [
  { weight: 8, kind: "reads", run: async (s) => assertLedger(await step(s, ledgerCheck("ledger")), "ledger") },
  { weight: 4, kind: "reads", run: async (s) => assertKeyed(await step(s, keyedCheck("keyed")), "keyed") },
  {
    weight: 2,
    kind: "reads",
    run: async (s) => {
      const table = `copy_${sideSlot()}`;
      assertLedger(await step(s, ledgerCheck(table)), table);
    },
  },
  {
    weight: 2,
    kind: "reads",
    run: async (s) => {
      const table = `side_${sideSlot()}`;
      assertKeyed(await step(s, keyedCheck(table)), table);
    },
  },
  { weight: 5, kind: "reads", run: async (s) => assertLedgerRows(await step(s, `SELECT pair, amount, chk FROM ledger WHERE pair = ${pairId()}`)) },
  { weight: 4, kind: "reads", run: async (s) => assertKeyedRows(await step(s, `SELECT k, v, chk FROM keyed WHERE k = ${keyId()}`)) },
  { weight: 4, kind: "reads", run: async (s) => void (await step(s, "SELECT note, COUNT(*), SUM(amount) FROM ledger GROUP BY note ORDER BY note")) },
  { weight: 3, kind: "reads", run: async (s) => void (await step(s, "SELECT COUNT(*) FROM ledger a JOIN keyed b ON a.pair = b.k")) },
  { weight: 2, kind: "reads", run: async (s) => void (await step(s, "SELECT MAX(extra), COUNT(*) FROM ledger")) },
  { weight: 2, kind: "reads", run: async (s) => void (await step(s, "SHOW TABLES")) },
  { weight: 12, kind: "writes", run: insertPairs },
  { weight: 8, kind: "writes", run: doubleLedger },
  { weight: 5, kind: "writes", run: updateLedger },
  { weight: 5, kind: "writes", run: deleteLedger },
  { weight: 6, kind: "writes", run: insertKeyed },
  {
    weight: 3,
    kind: "writes",
    run: async (s) => void (await step(s, `UPDATE keyed SET v = v + 1, chk = chk + 1 WHERE k = ${keyId()}`, verifyKeyed("keyed"))),
  },
  { weight: 2, kind: "writes", run: async (s) => void (await step(s, `DELETE FROM keyed WHERE k % 50 = ${Math.floor(random() * 50)}`, verifyKeyed("keyed"))) },
  { weight: 2, kind: "writes", run: copyKeyed },
  {
    weight: 1,
    kind: "writes",
    run: async (s) => {
      await abandonQuery(s.database!, "SELECT COUNT(*) FROM ledger a JOIN ledger b ON a.pair = b.pair");
    },
  },
  { weight: 2, kind: "ddl", run: ctasCopy },
  { weight: 1, kind: "ddl", run: ctasSide },
  { weight: 2, kind: "ddl", run: async (s) => void (await step(s, `DROP TABLE IF EXISTS ${random() < 0.5 ? "copy" : "side"}_${sideSlot()}`)) },
  { weight: 0.5, kind: "ddl", run: async (s) => void (await step(s, `RENAME TABLE copy_${sideSlot()} TO copy_${sideSlot()}`)) },
  { weight: 0.6, kind: "ddl", run: alterExtra },
  { weight: 1, kind: "ddl", run: truncateShared },
  { weight: 0.2, kind: "ddl", run: recreateShared },
  { weight: 0.01, kind: "ddl", run: dropSlice },
];
const POOL_WEIGHT = POOL_ACTIONS.reduce((sum, action) => sum + action.weight, 0);

function pickPoolAction(): PoolAction {
  let roll = random() * POOL_WEIGHT;
  for (const action of POOL_ACTIONS) {
    roll -= action.weight;
    if (roll < 0) return action;
  }
  return POOL_ACTIONS[0];
}

// One pooled connection of a slice: it keeps its connection across
// statements and reconnects only when a kill or a dropped socket took it.
async function poolWorker(slice: number, worker: number): Promise<void> {
  const who = `pool s${slice}w${worker}`;
  let session: Session | undefined;
  while (Date.now() < deadline && !stats.serverDown) {
    try {
      if (!session) {
        session = await open(who, `${poolDatabase(slice)}__public`);
        stats.pool.reconnects++;
      }
      if (random() < dropWriteShare) session.dropAt = session.steps + Math.floor(random() * POOL_ROUND);
      for (let i = 0; i < POOL_ROUND && Date.now() < deadline; i++) {
        const action = pickPoolAction();
        try {
          await action.run(session, slice);
          stats.pool[action.kind]++;
        } catch (err) {
          if (err instanceof KilledError) {
            stats.pool.killed++;
            if (!err.lost) continue;
            await close(session);
            session = undefined;
            break;
          }
          if (!isExpectedPoolError(err)) throw err;
          notePoolError(err);
          const { errno } = errorFields(err);
          if (errno === 1049 || errno === 1046) {
            await ensureSlice(slice);
            await close(session);
            session = undefined;
            break;
          }
          if (errno === 1146) {
            await step(session, LEDGER_DDL);
            await step(session, KEYED_DDL);
          }
        }
      }
    } catch (err) {
      if (session) {
        await close(session);
        session = undefined;
      }
      if (err instanceof KilledError) {
        stats.pool.killed++;
        continue;
      }
      if (isExpectedPoolError(err)) {
        notePoolError(err);
        continue;
      }
      if (!(await recordError(who, err))) return;
    }
  }
  if (session) await close(session);
}

async function poolSlice(slice: number): Promise<void> {
  await ensureSlice(slice);
  await Promise.all(Array.from({ length: poolSize }, (_, worker) => poolWorker(slice, worker)));
}

const started = Date.now();
const progress = setInterval(() => {
  const errors = [...stats.sqlErrors.values()].reduce((a, b) => a + b, 0);
  const k = stats.kills;
  console.error(
    `[${Math.round((Date.now() - started) / 1000)}s] cycles=${stats.cycles} killedCycles=${stats.killedCycles} statements=${stats.statements} abandoned=${stats.abandoned} droppedWrites=${stats.droppedWrites} ` +
      `kills=${k.query}q/${k.connection}c/${k.pgCancel}pc/${k.pgTerminate}pt/${k.cancelRequest}cr readerQueries=${stats.readerQueries} pgQueries=${stats.pgQueries} ` +
      `viewDdl=${stats.viewDdl} viewReads=${stats.viewReads} pool=${stats.pool.reads}r/${stats.pool.writes}w/${stats.pool.ddl}d ledger<=${stats.pool.maxLedger} errors=${errors}`,
  );
}, 30_000);
const count = (value: string | undefined) => Number(value ?? "0");
await Promise.all([
  ...Array.from({ length: clientCount }, (_, i) => client(i)),
  ...Array.from({ length: count(args.readers) }, (_, i) => reader(i)),
  ...Array.from({ length: count(args.killers) }, (_, i) => killer(i)),
  ...Array.from({ length: pgClientCount }, (_, i) => pgClient(i)),
  ...Array.from({ length: viewChurnerCount }, (_, i) => viewChurner(i)),
  ...Array.from({ length: viewReaderCount }, (_, i) => viewReader(i)),
  ...Array.from({ length: sliceCount }, (_, i) => poolSlice(i)),
]);
clearInterval(progress);

const summary = {
  seconds: Math.round((Date.now() - started) / 1000),
  clients: clientCount,
  readers: count(args.readers),
  killers: count(args.killers),
  pgClients: pgClientCount,
  viewChurners: viewChurnerCount,
  viewReaders: viewReaderCount,
  cycles: stats.cycles,
  killedCycles: stats.killedCycles,
  statements: stats.statements,
  abandoned: stats.abandoned,
  droppedWrites: stats.droppedWrites,
  kills: stats.kills,
  processListReads: stats.processListReads,
  killedStatements: stats.killedStatements,
  atomicityChecks: stats.atomicityChecks,
  readerQueries: stats.readerQueries,
  expectedReaderErrors: stats.expectedReaderErrors,
  pgQueries: stats.pgQueries,
  pgAbandoned: stats.pgAbandoned,
  viewDdl: stats.viewDdl,
  viewReads: stats.viewReads,
  expectedViewErrors: stats.expectedViewErrors,
  slices: sliceCount,
  pool: sliceCount > 0 ? { size: poolSize, ...stats.pool } : undefined,
  expectedPoolErrors: Object.fromEntries(stats.expectedPoolErrors),
  tornReads: stats.tornReads.length,
  tornReadSamples: stats.tornReads.slice(0, 5),
  refusedConnects: stats.refusedConnects,
  serverDown: stats.serverDown,
  invariantFailures: stats.invariantFailures,
  stalls: stats.stalls,
  sqlErrors: Object.fromEntries(stats.sqlErrors),
};
console.log(JSON.stringify(summary, null, 2));
process.exit(stats.serverDown || stats.invariantFailures.length > 0 || stats.stalls.length > 0 ? 1 : 0);
