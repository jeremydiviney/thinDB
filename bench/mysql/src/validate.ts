import type {
  GeneratorConfig,
  MysqlBenchConfig,
  NormalizedMysqlBenchConfig,
  NormalizedOutputConfig,
  NormalizedQueryConfig,
} from "./config.ts";

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

export function normalizeConfig(config: MysqlBenchConfig): NormalizedMysqlBenchConfig {
  assertValidConfig(config);

  const output: NormalizedOutputConfig = {
    directory: config.output?.directory ?? "bench/mysql/results",
    formats: config.output?.formats ?? ["json", "csv"],
    printSql: config.output?.printSql ?? true,
  };

  const queries: NormalizedQueryConfig[] = config.queries.map((query) => ({
    ...query,
    concurrency: query.concurrency ?? 1,
    warmupIterations: query.warmupIterations ?? 0,
    prepared: query.prepared ?? ((query.params?.length ?? 0) > 0),
  }));

  return { ...config, queries, output };
}

export function assertValidConfig(config: MysqlBenchConfig): void {
  const errors = validateConfig(config);
  if (errors.length > 0) {
    throw new Error(`Invalid MySQL benchmark config:\n${errors.map((e) => `- ${e}`).join("\n")}`);
  }
}

export function validateConfig(config: MysqlBenchConfig): string[] {
  const errors: string[] = [];

  if (config.name.trim() === "") errors.push("name must be non-empty");
  validateIdentifier("setup.table", config.setup.table, errors);
  if (!Number.isInteger(config.connection.port) || config.connection.port < 1 || config.connection.port > 65535) {
    errors.push("connection.port must be an integer from 1 to 65535");
  }
  if (config.connection.host.trim() === "") errors.push("connection.host must be non-empty");
  if (config.connection.user.trim() === "") errors.push("connection.user must be non-empty");
  if (config.connection.database.trim() === "") errors.push("connection.database must be non-empty");

  if (!Number.isInteger(config.data.rows) || config.data.rows < 0) {
    errors.push("data.rows must be a non-negative integer");
  }
  if (!Number.isInteger(config.data.insertBatchSize) || config.data.insertBatchSize < 1) {
    errors.push("data.insertBatchSize must be a positive integer");
  }
  if (!Number.isInteger(config.data.insertConcurrency) || config.data.insertConcurrency < 1) {
    errors.push("data.insertConcurrency must be a positive integer");
  }
  if (config.data.seed !== undefined && !Number.isInteger(config.data.seed)) {
    errors.push("data.seed must be an integer when provided");
  }

  const seen = new Set<string>();
  let primaryKeys = 0;
  for (const column of config.data.columns) {
    validateIdentifier(`column ${column.name || "<empty>"}.name`, column.name, errors);
    if (column.sqlType.trim() === "") errors.push(`column ${column.name}.sqlType must be non-empty`);
    const lower = column.name.toLowerCase();
    if (seen.has(lower)) errors.push(`duplicate column name: ${column.name}`);
    seen.add(lower);
    if (column.primaryKey === true) primaryKeys += 1;
    validateGenerator(column.name, column.generator, errors);
  }
  if (config.data.columns.length === 0) errors.push("data.columns must include at least one column");
  if (primaryKeys === 0) errors.push("data.columns must include one primaryKey column");
  if (primaryKeys > 1) errors.push("data.columns must include only one primaryKey column");

  for (const query of config.queries) {
    if (query.name.trim() === "") errors.push("query.name must be non-empty");
    if (query.sql.trim() === "") errors.push(`query ${query.name || "<empty>"}.sql must be non-empty`);
    if (!Number.isInteger(query.iterations) || query.iterations < 1) {
      errors.push(`query ${query.name}.iterations must be a positive integer`);
    }
    if (query.concurrency !== undefined && (!Number.isInteger(query.concurrency) || query.concurrency < 1)) {
      errors.push(`query ${query.name}.concurrency must be a positive integer`);
    }
    if (
      query.warmupIterations !== undefined &&
      (!Number.isInteger(query.warmupIterations) || query.warmupIterations < 0)
    ) {
      errors.push(`query ${query.name}.warmupIterations must be a non-negative integer`);
    }
  }

  return errors;
}

function validateIdentifier(label: string, value: string, errors: string[]): void {
  if (!IDENTIFIER.test(value)) {
    errors.push(`${label} must match ${IDENTIFIER}`);
  }
}

function validateGenerator(column: string, generator: GeneratorConfig, errors: string[]): void {
  switch (generator.type) {
    case "sequence":
      if (generator.start !== undefined && !Number.isInteger(generator.start)) {
        errors.push(`column ${column} sequence.start must be an integer`);
      }
      if (generator.step !== undefined && !Number.isInteger(generator.step)) {
        errors.push(`column ${column} sequence.step must be an integer`);
      }
      break;
    case "int":
      if (!Number.isInteger(generator.min) || !Number.isInteger(generator.max) || generator.min > generator.max) {
        errors.push(`column ${column} int generator must have integer min <= max`);
      }
      break;
    case "float":
      if (!Number.isFinite(generator.min) || !Number.isFinite(generator.max) || generator.min > generator.max) {
        errors.push(`column ${column} float generator must have finite min <= max`);
      }
      break;
    case "string":
      if (!Number.isInteger(generator.length) || generator.length < 1) {
        errors.push(`column ${column} string.length must be a positive integer`);
      }
      break;
    case "text":
      if (
        !Number.isInteger(generator.minLength) ||
        !Number.isInteger(generator.maxLength) ||
        generator.minLength < 1 ||
        generator.minLength > generator.maxLength
      ) {
        errors.push(`column ${column} text generator must have positive minLength <= maxLength`);
      }
      break;
    case "enum":
      if (generator.values.length === 0) errors.push(`column ${column} enum.values must not be empty`);
      break;
    case "datetime":
      for (const field of ["start", "end"] as const) {
        if (generator[field] !== undefined && Number.isNaN(Date.parse(generator[field]))) {
          errors.push(`column ${column} datetime.${field} must parse as a date`);
        }
      }
      break;
    case "bool":
    case "uuid":
      break;
  }
}
