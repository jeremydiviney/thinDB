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
