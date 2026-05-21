import { pathToFileURL } from "node:url";
import { resolve } from "node:path";
import type { MysqlBenchConfig } from "./src/config.ts";
import { runBenchmark } from "./src/runner.ts";

const configPath = process.argv[2];

if (configPath === undefined || configPath === "-h" || configPath === "--help") {
  console.error("usage: bun run bench/mysql/run.ts <config.ts>");
  process.exit(configPath === undefined ? 1 : 0);
}

const moduleUrl = pathToFileURL(resolve(configPath)).href;
const loaded = (await import(moduleUrl)) as { default?: MysqlBenchConfig };

if (loaded.default === undefined) {
  throw new Error(`Config ${configPath} must default-export a MysqlBenchConfig`);
}

await runBenchmark(loaded.default);
