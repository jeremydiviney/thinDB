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
- a refused or reset connection as a server crash;
- a statement slower than five minutes as a stall.

It exits non-zero on any of these.

```powershell
.\zig-out\bin\thindb-server.exe --data-dir .bench-data\stress --bind 127.0.0.1 --mysql-port 3307 --pg-port 0 --native-port 0 --max-dop 4
bun run bench/mysql/stress_ddl.ts --port 3307 --clients 6 --seconds 600 --doublings 20
```

A ReleaseSafe server build turns memory corruption into a panic at the fault instead of a later heap-corruption crash.
