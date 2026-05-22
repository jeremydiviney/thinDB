import mysql, { type Connection } from "mysql2/promise";
import { mkdir, writeFile } from "node:fs/promises";
import { isAbsolute, join, resolve } from "node:path";
import type {
  ColumnConfig,
  MysqlBenchConfig,
  NormalizedMysqlBenchConfig,
  NormalizedQueryConfig,
  SqlParam,
} from "./config.ts";
import { generateBatch, Rng } from "./generator.ts";
import { createTableSql, dropTableSql, insertSql, insertTemplateSql } from "./sql.ts";
import { formatMs, summarizeLatencies, type LatencyStats } from "./stats.ts";
import { normalizeConfig } from "./validate.ts";

const REPO_ROOT = resolve(import.meta.dir, "..", "..", "..");

export type BenchmarkResult = {
  name: string;
  startedAt: string;
  connection: {
    host: string;
    port: number;
    database: string;
  };
  table: string;
  data: {
    rows: number;
    insertBatchSize: number;
    insertConcurrency: number;
    columns: Array<Pick<ColumnConfig, "name" | "sqlType" | "primaryKey" | "nullable">>;
  };
  insert: InsertResult;
  queries: QueryResult[];
};

export type InsertResult = {
  name: "insert";
  rows: number;
  batches: number;
  concurrency: number;
  stats: LatencyStats;
  rowsPerSec: number;
};

export type QueryResult = {
  name: string;
  sql: string;
  params: SqlParam[];
  prepared: boolean;
  iterations: number;
  concurrency: number;
  rowsReturned: number;
  stats: LatencyStats;
};

type SqlResult = {
  rows: number;
  affectedRows: number | null;
};

export async function runBenchmark(input: MysqlBenchConfig): Promise<BenchmarkResult> {
  const config = normalizeConfig(input);
  const startedAt = new Date().toISOString();
  const admin = await openConnection(config);

  try {
    await setupTable(admin, config);
    const insert = await runInsertPhase(config);
    const queries: QueryResult[] = [];
    for (const query of config.queries) {
      queries.push(await runQueryPhase(config, query));
    }

    const result: BenchmarkResult = {
      name: config.name,
      startedAt,
      connection: {
        host: config.connection.host,
        port: config.connection.port,
        database: config.connection.database,
      },
      table: config.setup.table,
      data: {
        rows: config.data.rows,
        insertBatchSize: config.data.insertBatchSize,
        insertConcurrency: config.data.insertConcurrency,
        columns: config.data.columns.map((column) => ({
          name: column.name,
          sqlType: column.sqlType,
          primaryKey: column.primaryKey,
          nullable: column.nullable,
        })),
      },
      insert,
      queries,
    };

    await writeResults(config, result);
    return result;
  } finally {
    await admin.end().catch(() => undefined);
  }
}

async function setupTable(conn: Connection, config: NormalizedMysqlBenchConfig): Promise<void> {
  if (config.setup.recreateTable) {
    const drop = dropTableSql(config.setup.table);
    if (config.output.printSql) console.log(`setup sql: ${drop}`);
    await conn.query(drop);
  }

  const create = createTableSql(config.setup.table, config.data.columns, !config.setup.recreateTable);
  if (config.output.printSql) console.log(`setup sql: ${create}`);
  await conn.query(create);
}

async function runInsertPhase(config: NormalizedMysqlBenchConfig): Promise<InsertResult> {
  const totalRows = config.data.rows;
  const batchSize = config.data.insertBatchSize;
  const totalBatches = Math.ceil(totalRows / batchSize);
  const concurrency = Math.min(config.data.insertConcurrency, Math.max(totalBatches, 1));
  const latencies: number[] = [];
  const seed = config.data.seed ?? 0xdecafbad;
  const sqlCache = new Map<number, string>();
  let nextBatch = 0;
  let insertedRows = 0;

  if (config.output.printSql) {
    console.log(`insert template: ${insertTemplateSql(config.setup.table, config.data.columns)}`);
    console.log(
      `insert batches: rows=${totalRows}, batchSize=${batchSize}, batches=${totalBatches}, concurrency=${concurrency}`,
    );
  }

  const started = performance.now();
  const conns = await openConnections(config, concurrency);
  try {
    await Promise.all(
      conns.map(async (conn) => {
        while (true) {
          const batchIndex = nextBatch;
          nextBatch += 1;
          if (batchIndex >= totalBatches) return;

          const startRowIndex = batchIndex * batchSize;
          const rowCount = Math.min(batchSize, totalRows - startRowIndex);
          const sql = cachedInsertSql(sqlCache, config.setup.table, config.data.columns, rowCount);
          const rng = new Rng((seed + batchIndex) >>> 0);
          const values = generateBatch(config.data.columns, startRowIndex, rowCount, rng);
          const batchStarted = performance.now();
          const [raw] = await conn.execute(sql, values);
          const elapsed = performance.now() - batchStarted;
          const affectedRows = affectedRowsFromResult(raw);
          if (affectedRows !== null && affectedRows !== rowCount) {
            throw new Error(`insert batch ${batchIndex} affected ${affectedRows} rows; expected ${rowCount}`);
          }
          latencies.push(elapsed);
          insertedRows += rowCount;
        }
      }),
    );
  } finally {
    await closeConnections(conns);
  }

  const totalMs = performance.now() - started;
  const stats = summarizeLatencies(latencies, totalMs);
  const result: InsertResult = {
    name: "insert",
    rows: insertedRows,
    batches: totalBatches,
    concurrency,
    stats,
    rowsPerSec: totalMs > 0 ? (insertedRows / totalMs) * 1000 : 0,
  };
  printInsertSummary(result);
  return result;
}

async function runQueryPhase(
  config: NormalizedMysqlBenchConfig,
  query: NormalizedQueryConfig,
): Promise<QueryResult> {
  if (config.output.printSql) {
    console.log(`query "${query.name}": ${query.sql}`);
    if ((query.params?.length ?? 0) > 0) console.log(`query params: ${JSON.stringify(query.params)}`);
  }

  const params = query.params ?? [];
  const warmupConn = await openConnection(config);
  try {
    for (let i = 0; i < query.warmupIterations; i++) {
      await executeSql(warmupConn, query.sql, params, query.prepared);
    }
  } finally {
    await warmupConn.end().catch(() => undefined);
  }

  const latencies: number[] = [];
  let nextIteration = 0;
  let rowsReturned = 0;
  const conns = await openConnections(config, query.concurrency);
  const started = performance.now();
  try {
    await Promise.all(
      conns.map(async (conn) => {
        while (true) {
          const iteration = nextIteration;
          nextIteration += 1;
          if (iteration >= query.iterations) return;

          const queryStarted = performance.now();
          const result = await executeSql(conn, query.sql, params, query.prepared);
          latencies.push(performance.now() - queryStarted);
          rowsReturned += result.rows;
        }
      }),
    );
  } finally {
    await closeConnections(conns);
  }

  const totalMs = performance.now() - started;
  const result: QueryResult = {
    name: query.name,
    sql: query.sql,
    params,
    prepared: query.prepared,
    iterations: query.iterations,
    concurrency: query.concurrency,
    rowsReturned,
    stats: summarizeLatencies(latencies, totalMs),
  };
  printQuerySummary(result);
  return result;
}

async function executeSql(
  conn: Connection,
  sql: string,
  params: SqlParam[],
  prepared: boolean,
): Promise<SqlResult> {
  const [raw] = prepared ? await conn.execute(sql, params) : await conn.query(sql, params);
  return {
    rows: Array.isArray(raw) ? raw.length : 0,
    affectedRows: affectedRowsFromResult(raw),
  };
}

function cachedInsertSql(
  cache: Map<number, string>,
  table: string,
  columns: ColumnConfig[],
  rowCount: number,
): string {
  const existing = cache.get(rowCount);
  if (existing !== undefined) return existing;
  const sql = insertSql(table, columns, rowCount);
  cache.set(rowCount, sql);
  return sql;
}

async function openConnection(config: NormalizedMysqlBenchConfig): Promise<Connection> {
  return await mysql.createConnection({
    host: config.connection.host,
    port: config.connection.port,
    user: config.connection.user,
    password: config.connection.password,
    database: config.connection.database,
    supportBigNumbers: true,
    bigNumberStrings: true,
    multipleStatements: false,
  });
}

async function openConnections(config: NormalizedMysqlBenchConfig, count: number): Promise<Connection[]> {
  return await Promise.all(Array.from({ length: count }, () => openConnection(config)));
}

async function closeConnections(conns: Connection[]): Promise<void> {
  await Promise.all(conns.map((conn) => conn.end().catch(() => undefined)));
}

function affectedRowsFromResult(raw: unknown): number | null {
  if (raw !== null && typeof raw === "object" && "affectedRows" in raw) {
    const value = (raw as { affectedRows?: unknown }).affectedRows;
    return typeof value === "number" ? value : null;
  }
  return null;
}

async function writeResults(config: NormalizedMysqlBenchConfig, result: BenchmarkResult): Promise<void> {
  if (config.output.formats.length === 0) return;

  const outputDirectory = isAbsolute(config.output.directory)
    ? config.output.directory
    : resolve(REPO_ROOT, config.output.directory);
  await mkdir(outputDirectory, { recursive: true });
  const stamp = result.startedAt.replace(/[:.]/g, "-");
  const base = join(outputDirectory, `${result.name}-${stamp}`);

  if (config.output.formats.includes("json")) {
    await writeFile(`${base}.json`, `${JSON.stringify(result, null, 2)}\n`);
  }
  if (config.output.formats.includes("csv")) {
    await writeFile(`${base}.csv`, renderCsv(result));
  }
}

function renderCsv(result: BenchmarkResult): string {
  const rows = [
    [
      "phase",
      "name",
      "samples",
      "total_ms",
      "mean_ms",
      "min_ms",
      "max_ms",
      "p50_ms",
      "p95_ms",
      "p99_ms",
      "ops_sec",
      "rows",
      "rows_sec",
      "sql",
    ],
    resultRow("insert", "insert", result.insert.stats, result.insert.rows, result.insert.rowsPerSec, ""),
    ...result.queries.map((query) =>
      resultRow("query", query.name, query.stats, query.rowsReturned, 0, query.sql),
    ),
  ];
  return `${rows.map((row) => row.map(csvCell).join(",")).join("\n")}\n`;
}

function resultRow(
  phase: string,
  name: string,
  stats: LatencyStats,
  rows: number,
  rowsPerSec: number,
  sql: string,
): Array<string | number> {
  return [
    phase,
    name,
    stats.samples,
    stats.totalMs,
    stats.meanMs,
    stats.minMs,
    stats.maxMs,
    stats.p50Ms,
    stats.p95Ms,
    stats.p99Ms,
    stats.opsPerSec,
    rows,
    rowsPerSec,
    sql,
  ];
}

function csvCell(value: string | number): string {
  if (typeof value === "number") return Number.isFinite(value) ? value.toString() : "";
  return `"${value.replaceAll('"', '""')}"`;
}

function printInsertSummary(result: InsertResult): void {
  console.log(
    `insert: rows=${result.rows}, batches=${result.batches}, total=${formatMs(result.stats.totalMs)}, ` +
      `mean=${formatMs(result.stats.meanMs)}, p95=${formatMs(result.stats.p95Ms)}, ` +
      `rows/sec=${result.rowsPerSec.toFixed(2)}`,
  );
}

function printQuerySummary(result: QueryResult): void {
  console.log(
    `query "${result.name}": iterations=${result.iterations}, rows=${result.rowsReturned}, ` +
      `total=${formatMs(result.stats.totalMs)}, mean=${formatMs(result.stats.meanMs)}, ` +
      `p95=${formatMs(result.stats.p95Ms)}, ops/sec=${result.stats.opsPerSec.toFixed(2)}`,
  );
}
