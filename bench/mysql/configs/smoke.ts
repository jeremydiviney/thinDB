import type { MysqlBenchConfig } from "../src/config.ts";

export default {
  name: "smoke",

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
    rows: 10,
    insertBatchSize: 5,
    insertConcurrency: 1,
    seed: 1,
    columns: [
      {
        name: "id",
        sqlType: "BIGINT",
        primaryKey: true,
        generator: { type: "sequence", start: 1 },
      },
      {
        name: "text",
        sqlType: "TEXT",
        generator: { type: "string", length: 50 },
      },
      {
        name: "integer",
        sqlType: "INTEGER",
        generator: { type: "int", min: 1, max: 1000 },
      },
    ],
  },

  queries: [
    {
      name: "select id limit 10",
      sql: "SELECT id FROM record LIMIT 10",
      iterations: 3,
      concurrency: 1,
    },
  ],

  output: {
    directory: "bench/mysql/results",
    formats: ["json", "csv"],
    printSql: true,
  },
} satisfies MysqlBenchConfig;
