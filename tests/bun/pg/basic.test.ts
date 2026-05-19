import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Client } from "pg";
import { startServer, type ServerHandle } from "../helpers/server.ts";

describe("pg basic", () => {
  let server: ServerHandle;
  let client: Client;

  beforeAll(async () => {
    server = await startServer({ label: "pg-basic" });
    client = new Client({
      host: server.bind,
      port: server.ports.pg,
      user: "thindb",
      password: "anything",
      database: "main",
    });
    await client.connect();
  });

  afterAll(async () => {
    if (client !== undefined) await client.end().catch(() => undefined);
    if (server !== undefined) await server.close();
  });

  test("connects on a non-zero port", () => {
    expect(server.ports.pg).toBeGreaterThan(0);
  });

  test("SELECT 1 returns 1", async () => {
    const r = await client.query("SELECT 1");
    expect(r.rows.length).toBe(1);
    const row = r.rows[0];
    expect(row).toBeDefined();
    // pg uses "?column?" for unaliased expressions.
    // The canned matcher returns the value as a string ("1");
    // pg's text parsing for an unknown column hands it back as-is.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(String((row as any)["?column?"])).toBe("1");
  });

  test("version() contains 'thinDB'", async () => {
    const r = await client.query("SELECT version()");
    expect(r.rows.length).toBe(1);
    const row = r.rows[0];
    expect(row).toBeDefined();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(String((row as any).version)).toContain("thinDB");
  });

  test("current_database() reports 'main'", async () => {
    const r = await client.query("SELECT current_database()");
    expect(r.rows.length).toBe(1);
    const row = r.rows[0];
    expect(row).toBeDefined();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(String((row as any).current_database)).toBe("main");
  });
});
