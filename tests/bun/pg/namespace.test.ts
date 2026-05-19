import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Client } from "pg";
import { startServer, type ServerHandle } from "../helpers/server.ts";

describe("pg namespace", () => {
  let server: ServerHandle;
  let client: Client;

  beforeAll(async () => {
    server = await startServer({ label: "pg-ns" });
    client = new Client({
      host: server.bind,
      port: server.ports.pg,
      user: "thindb",
      password: "x",
      database: "main",
    });
    await client.connect();
  });

  afterAll(async () => {
    if (client !== undefined) await client.end().catch(() => undefined);
    if (server !== undefined) await server.close();
  });

  test("pg_catalog.pg_database lists 'main'", async () => {
    const r = await client.query(
      "SELECT datname FROM pg_catalog.pg_database ORDER BY 1",
    );
    const names = r.rows.map((row) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return String((row as any).datname);
    });
    expect(names).toContain("main");
  });

  test("CREATE DATABASE adds a row", async () => {
    await client.query("CREATE DATABASE foo");
    const r = await client.query(
      "SELECT datname FROM pg_catalog.pg_database ORDER BY 1",
    );
    const names = r.rows.map((row) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return String((row as any).datname);
    });
    expect(names).toContain("foo");
    expect(names).toContain("main");
  });

  test("pg_namespace lists 'public' schema", async () => {
    const r = await client.query("SELECT nspname FROM pg_namespace");
    const names = r.rows.map((row) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return String((row as any).nspname);
    });
    expect(names).toContain("public");
  });

  test("CREATE SCHEMA registers a new namespace", async () => {
    await client.query("CREATE SCHEMA analytics");
    const r = await client.query("SELECT nspname FROM pg_namespace");
    const names = r.rows.map((row) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return String((row as any).nspname);
    });
    expect(names).toContain("analytics");
  });

  test("SHOW server_version reports our banner", async () => {
    const r = await client.query("SHOW server_version");
    const row = r.rows[0];
    expect(row).toBeDefined();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(String((row as any).server_version)).toContain("thinDB");
  });

  test("SET search_path is accepted (canned)", async () => {
    // pg's `query` returns command tag in `command` field; we just check
    // that this round-trips without error.
    const r = await client.query("SET search_path = analytics");
    expect(r.command).toBe("SET");
  });
});
