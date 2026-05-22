import { describe, expect, test } from "bun:test";
import type { ColumnConfig } from "../src/config.ts";
import { createTableSql, dropTableSql, insertSql } from "../src/sql.ts";

describe("mysql bench sql builders", () => {
  const columns: ColumnConfig[] = [
    {
      name: "id",
      sqlType: "BIGINT",
      primaryKey: true,
      generator: { type: "sequence" },
    },
    {
      name: "amount",
      sqlType: "INTEGER",
      generator: { type: "int", min: 1, max: 10 },
    },
  ];

  test("builds table DDL", () => {
    expect(createTableSql("record", columns, false)).toBe(
      "CREATE TABLE `record` (`id` BIGINT PRIMARY KEY NOT NULL, `amount` INTEGER NOT NULL)",
    );
    expect(dropTableSql("record")).toBe("DROP TABLE IF EXISTS `record`");
  });

  test("builds multi-row insert placeholders", () => {
    expect(insertSql("record", columns, 3)).toBe(
      "INSERT INTO `record` (`id`, `amount`) VALUES (?, ?), (?, ?), (?, ?)",
    );
  });

  test("rejects batches beyond prepared parameter limit", () => {
    expect(() => insertSql("record", columns, 40_000)).toThrow("prepared parameters");
  });
});
