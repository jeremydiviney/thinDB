# MySQL Client Benchmarks

This directory contains a small Bun/TypeScript benchmark harness for the MySQL wire path. It uses `mysql2/promise`, generates configured random rows, runs explicit SQL queries, and writes JSON/CSV result files.

## Install

```powershell
cd bench/mysql
bun install
```

Build `thindb-server` before running against thinDB:

```powershell
cd C:\Code\thinDB
zig build
```

Start thinDB in another shell, or point the config at an existing MySQL-compatible endpoint:

```powershell
.\zig-out\bin\thindb-server.exe --data-dir .bench-data\mysql-client --bind 127.0.0.1 --mysql-port 3307 --pg-port 0 --native-port 0
```

## Run

From the repo root:

```powershell
bun run bench/mysql/run.ts bench/mysql/configs/basic-orders.ts
```

Or from `bench/mysql`:

```powershell
bun run bench configs/basic-orders.ts
```

Results are written under `bench/mysql/results/` by default.

## Config Model

Configs are TypeScript files that default-export `MysqlBenchConfig`.

```ts
export default {
  name: "basic-orders",
  connection: {
    host: "127.0.0.1",
    port: 3307,
    user: "thindb",
    password: "",
    database: "main__public",
  },
  setup: {
    table: "record",
    recreateTable: true,
  },
  data: {
    rows: 200_000,
    insertBatchSize: 10_000,
    insertConcurrency: 5,
    seed: 42,
    columns: [
      { name: "id", sqlType: "BIGINT", primaryKey: true, generator: { type: "sequence", start: 1 } },
      { name: "status", sqlType: "TEXT", generator: { type: "enum", values: ["new", "paid"] } },
      { name: "amount", sqlType: "INTEGER", generator: { type: "int", min: 1, max: 10_000 } },
    ],
  },
  queries: [
    {
      name: "high amount",
      sql: "SELECT * FROM record WHERE amount >= ? LIMIT 10",
      params: [5000],
      iterations: 20,
      concurrency: 1,
    },
  ],
} satisfies MysqlBenchConfig;
```

The insert phase uses prepared multi-row inserts. Keep `insertBatchSize * columns.length` under roughly `65,000` parameters.

Supported generators: `sequence`, `int`, `float`, `string`, `text`, `enum`, `bool`, `datetime`, and `uuid`.

## Concurrent DDL stress

`stress_ddl.ts` is a correctness and crash harness rather than a benchmark (#90). Each client owns a database and loops through a fixed sequence:
- CREATE, a doubling `INSERT ... SELECT`, CTAS, DELETE, UPDATE, ALTER, RENAME, TRUNCATE and DROP;
- dropping and recreating its whole database every third cycle;
- now and then abandoning a connection in the middle of a join.

The server's background flusher and compactor run underneath all of it.

Every step checks its exact row counts. It reports:
- a server that refuses connections for a few seconds as a crash;
- a statement slower than five minutes as a stall.

It exits non-zero on either, and on any invariant failure.

```powershell
.\zig-out\bin\thindb-server.exe --data-dir .bench-data\stress --bind 127.0.0.1 --mysql-port 3307 --pg-port 5433 --native-port 0 --max-dop 4
bun run bench/mysql/stress_ddl.ts --port 3307 --clients 6 --seconds 600 --doublings 20
```

Three options put pressure on connection teardown and on reads racing DDL:

| Option | What it runs |
|---|---|
| `--killers N` | Reads the process list in every supported spelling (`SHOW [FULL] PROCESSLIST`, `information_schema.processlist`, `performance_schema.processlist`), checks that the reader lists itself, then sends `KILL QUERY`, `KILL CONNECTION` or a bare `KILL` at a random stress connection, preferring ones mid-statement. |
| `--readers N` | Runs counts, aggregates, a join, point and top-N lookups and `SHOW TABLES` against other clients' databases while those clients create, alter and drop them. A missing table, database or column is expected. A `seq` count that is not a power of two is reported as a torn read. |
| `--pg-clients N` | PostgreSQL-wire clients (needs `--pg-port`) with a built-in wire client, so no extra dependency. Each round connects to a random stress database and does one of four things: abandons a join mid-query, cancels its own query with a CancelRequest, reads `pg_stat_activity` and sends `pg_cancel_backend` / `pg_terminate_backend` at a stress connection of either wire, or just reads. |

```powershell
bun run bench/mysql/stress_ddl.ts --port 3307 --pg-port 5433 --clients 8 --doublings 12 --killers 1 --readers 2 --pg-clients 2 --seconds 900
```

Every id a kill is aimed at is recorded before the kill is sent, and ids are never reused. So an interrupted statement or a dropped connection whose id was never aimed at is reported as an invariant failure: the server did it on its own.

When a client's write is killed, it opens a checker connection that nobody kills. It waits until the killed connection leaves the process list, and fails if that takes over a minute. It then checks that the write left all-or-nothing state. For example, a killed doubling insert leaves either the old row count or twice it. A killed RENAME leaves the rows under exactly one of the two names. After the check, the client starts its next cycle.

A ReleaseSafe build adds Zig's safety checks, such as bounds and alignment, but the server links libc, so its allocator is still the C allocator in every mode. A use-after-free is only caught when a safety check happens to trip on the freed memory. Otherwise it corrupts the heap exactly as it does in ReleaseFast.
