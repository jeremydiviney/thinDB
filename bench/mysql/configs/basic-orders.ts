import type { MysqlBenchConfig } from "../src/config.ts";

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
      {
        name: "id",
        sqlType: "BIGINT",
        primaryKey: true,
        generator: { type: "sequence", start: 1 },
      },
      {
        name: "customer",
        sqlType: "TEXT",
        generator: { type: "string", length: 20 },
      },
      {
        name: "status",
        sqlType: "TEXT",
        generator: {
          type: "enum",
          values: ["new", "paid", "shipped", "cancelled"],
        },
      },
      {
        name: "amount",
        sqlType: "INTEGER",
        generator: { type: "int", min: 1, max: 10_000 },
      },
      {
        name: "score",
        sqlType: "REAL",
        generator: { type: "float", min: 0, max: 100 },
      },
    ],
  },

  queries: [
    {
      name: "select id limit 10",
      sql: "SELECT id FROM record LIMIT 10",
      iterations: 20,
      concurrency: 1,
    },
    {
      name: "select full rows limit 10",
      sql: "SELECT * FROM record LIMIT 10",
      iterations: 20,
      concurrency: 1,
    },
    {
      name: "high amount",
      sql: "SELECT * FROM record WHERE amount >= ? LIMIT 10",
      params: [5000],
      iterations: 20,
      concurrency: 1,
    },
    {
      name: "paid orders",
      sql: "SELECT * FROM record WHERE status = ? LIMIT 10",
      params: ["paid"],
      iterations: 20,
      concurrency: 1,
    },
    {
      name: "top amounts",
      sql: "SELECT * FROM record ORDER BY amount DESC LIMIT 10",
      iterations: 20,
      concurrency: 1,
    },
  ],

  output: {
    directory: "bench/mysql/results",
    formats: ["json", "csv"],
    printSql: true,
  },
} satisfies MysqlBenchConfig;
