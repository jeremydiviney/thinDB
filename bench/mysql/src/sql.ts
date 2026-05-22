import type { ColumnConfig } from "./config.ts";

export const MAX_PREPARED_PARAMS = 65_000;

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

export function quoteIdentifier(identifier: string): string {
  if (!IDENTIFIER.test(identifier)) {
    throw new Error(`Unsafe SQL identifier: ${identifier}`);
  }
  return `\`${identifier}\``;
}

export function createTableSql(table: string, columns: ColumnConfig[], ifNotExists: boolean): string {
  const columnSql = columns
    .map((column) => {
      const parts = [quoteIdentifier(column.name), column.sqlType];
      if (column.primaryKey === true) parts.push("PRIMARY KEY");
      if (column.nullable !== true) parts.push("NOT NULL");
      return parts.join(" ");
    })
    .join(", ");
  const exists = ifNotExists ? " IF NOT EXISTS" : "";
  return `CREATE TABLE${exists} ${quoteIdentifier(table)} (${columnSql})`;
}

export function dropTableSql(table: string): string {
  return `DROP TABLE IF EXISTS ${quoteIdentifier(table)}`;
}

export function insertSql(table: string, columns: ColumnConfig[], rowCount: number): string {
  const paramCount = columns.length * rowCount;
  if (paramCount > MAX_PREPARED_PARAMS) {
    throw new Error(
      `insert batch would use ${paramCount} prepared parameters; reduce insertBatchSize below ${MAX_PREPARED_PARAMS}`,
    );
  }
  const columnList = columns.map((column) => quoteIdentifier(column.name)).join(", ");
  const rowPlaceholders = `(${columns.map(() => "?").join(", ")})`;
  const values = Array.from({ length: rowCount }, () => rowPlaceholders).join(", ");
  return `INSERT INTO ${quoteIdentifier(table)} (${columnList}) VALUES ${values}`;
}

export function insertTemplateSql(table: string, columns: ColumnConfig[]): string {
  return insertSql(table, columns, 1);
}
