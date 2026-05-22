import { describe, expect, test } from "bun:test";
import type { MysqlBenchConfig } from "../src/config.ts";
import { normalizeConfig, validateConfig } from "../src/validate.ts";

describe("mysql bench config validation", () => {
  test("accepts a minimal valid config and applies defaults", () => {
    const normalized = normalizeConfig(baseConfig());

    expect(normalized.output.directory).toBe("bench/mysql/results");
    expect(normalized.output.formats).toEqual(["json", "csv"]);
    expect(normalized.queries[0]?.concurrency).toBe(1);
    expect(normalized.queries[0]?.warmupIterations).toBe(0);
  });

  test("rejects duplicate columns and missing primary key", () => {
    const config = baseConfig();
    config.data.columns = [
      { name: "id", sqlType: "BIGINT", generator: { type: "sequence" } },
      { name: "ID", sqlType: "BIGINT", generator: { type: "sequence" } },
    ];

    const errors = validateConfig(config);
    expect(errors.some((error) => error.includes("duplicate column name"))).toBe(true);
    expect(errors.some((error) => error.includes("primaryKey"))).toBe(true);
  });

  test("rejects invalid row counts and empty query names", () => {
    const config = baseConfig();
    config.data.rows = -1;
    config.queries[0] = { name: "", sql: "SELECT 1", iterations: 0 };

    const errors = validateConfig(config);
    expect(errors.some((error) => error.includes("data.rows"))).toBe(true);
    expect(errors.some((error) => error.includes("query.name"))).toBe(true);
    expect(errors.some((error) => error.includes("iterations"))).toBe(true);
  });
});

function baseConfig(): MysqlBenchConfig {
  return {
    name: "test",
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
      columns: [
        {
          name: "id",
          sqlType: "BIGINT",
          primaryKey: true,
          generator: { type: "sequence", start: 1 },
        },
      ],
    },
    queries: [{ name: "select", sql: "SELECT id FROM record", iterations: 1 }],
  };
}
