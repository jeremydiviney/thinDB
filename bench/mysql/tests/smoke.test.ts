import { describe, expect, test } from "bun:test";
import { existsSync } from "node:fs";
import type { MysqlBenchConfig } from "../src/config.ts";

const smokeTest = process.env.THINDB_MYSQL_BENCH_SMOKE === "1" ? test : test.skip;

describe("mysql bench smoke", () => {
  smokeTest("loads rows and runs a query against thindb-server", async () => {
    const [{ startServer }, { serverBinary }, { runBenchmark }] = await Promise.all([
      import("../../../tests/bun/helpers/server.ts"),
      import("../../../tests/bun/helpers/paths.ts"),
      import("../src/runner.ts"),
    ]);

    if (!existsSync(serverBinary())) {
      throw new Error("thindb-server binary missing; run zig build first");
    }

    const server = await startServer({
      label: "mysql-bench-smoke",
      overrides: { pg: 0, native: 0 },
    });
    try {
      const result = await runBenchmark(smokeConfig(server.bind, server.ports.mysql));
      expect(result.insert.rows).toBe(10);
      expect(result.queries[0]?.rowsReturned).toBe(5);
    } finally {
      await server.close();
    }
  });
});

function smokeConfig(host: string, port: number): MysqlBenchConfig {
  return {
    name: "bench-smoke-test",
    connection: {
      host,
      port,
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
        { name: "id", sqlType: "BIGINT", primaryKey: true, generator: { type: "sequence" } },
        { name: "amount", sqlType: "INTEGER", generator: { type: "int", min: 1, max: 100 } },
      ],
    },
    queries: [{ name: "limit", sql: "SELECT id FROM record LIMIT 5", iterations: 1 }],
    output: {
      directory: "bench/mysql/results",
      formats: [],
      printSql: false,
    },
  };
}
