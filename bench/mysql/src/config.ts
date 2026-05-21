export type MysqlBenchConfig = {
  name: string;
  connection: MysqlConnectionConfig;
  setup: SetupConfig;
  data: DataConfig;
  queries: QueryConfig[];
  output?: OutputConfig;
};

export type SqlParam = string | number | bigint | boolean | Date | null | Buffer | Uint8Array;

export type MysqlConnectionConfig = {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
};

export type SetupConfig = {
  table: string;
  recreateTable: boolean;
};

export type DataConfig = {
  rows: number;
  insertBatchSize: number;
  insertConcurrency: number;
  seed?: number;
  columns: ColumnConfig[];
};

export type ColumnConfig = {
  name: string;
  sqlType: string;
  primaryKey?: boolean;
  nullable?: boolean;
  generator: GeneratorConfig;
};

export type GeneratorConfig =
  | { type: "sequence"; start?: number; step?: number }
  | { type: "int"; min: number; max: number }
  | { type: "float"; min: number; max: number }
  | { type: "string"; length: number }
  | { type: "text"; minLength: number; maxLength: number }
  | { type: "enum"; values: Array<string | number> }
  | { type: "bool" }
  | { type: "datetime"; start?: string; end?: string }
  | { type: "uuid" };

export type QueryConfig = {
  name: string;
  sql: string;
  params?: SqlParam[];
  iterations: number;
  concurrency?: number;
  warmupIterations?: number;
  prepared?: boolean;
};

export type OutputConfig = {
  directory?: string;
  formats?: Array<"json" | "csv">;
  printSql?: boolean;
};

export type NormalizedQueryConfig = QueryConfig & {
  concurrency: number;
  warmupIterations: number;
  prepared: boolean;
};

export type NormalizedOutputConfig = {
  directory: string;
  formats: Array<"json" | "csv">;
  printSql: boolean;
};

export type NormalizedMysqlBenchConfig = Omit<MysqlBenchConfig, "queries" | "output"> & {
  queries: NormalizedQueryConfig[];
  output: NormalizedOutputConfig;
};
