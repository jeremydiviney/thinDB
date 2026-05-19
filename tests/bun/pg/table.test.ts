import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Client } from "pg";
import { startServer, type ServerHandle } from "../helpers/server.ts";

describe("pg table", () => {
  let server: ServerHandle;
  let client: Client;

  beforeAll(async () => {
    server = await startServer({ label: "pg-table" });
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

  test("CREATE TABLE + INSERT + SELECT round-trip", async () => {
    await client.query("CREATE TABLE t1 (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
    const insertRes = await client.query(
      "INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30)",
    );
    // pg surfaces `rowCount` for INSERT/UPDATE/DELETE from CommandComplete.
    expect(insertRes.rowCount).toBe(3);

    const r = await client.query("SELECT id, qty FROM t1 ORDER BY id ASC");
    expect(r.rows.length).toBe(3);
    // BIGINT comes back as string via pg's default type parser.
    expect(String(r.rows[0]?.id)).toBe("1");
    expect(Number(r.rows[0]?.qty)).toBe(10);
    expect(String(r.rows[2]?.id)).toBe("3");

    await client.query("DROP TABLE t1");
  });

  test("DROP TABLE removes the table", async () => {
    await client.query("CREATE TABLE drop_me (id BIGINT PRIMARY KEY)");
    await client.query("DROP TABLE drop_me");
    let code: string | undefined;
    try {
      await client.query("SELECT id FROM drop_me");
    } catch (err) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      code = (err as any).code;
    }
    expect(code).toBe("42P01");
  });

  test("INSERT with named column list reorders values", async () => {
    await client.query(
      "CREATE TABLE t2 (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL, qty INT NOT NULL)",
    );
    await client.query("INSERT INTO t2 (name, id, qty) VALUES ('alice', 1, 100)");
    const r = await client.query("SELECT id, name, qty FROM t2");
    expect(r.rows.length).toBe(1);
    expect(String(r.rows[0]?.id)).toBe("1");
    expect(String(r.rows[0]?.name)).toBe("alice");
    expect(Number(r.rows[0]?.qty)).toBe(100);
    await client.query("DROP TABLE t2");
  });

  test("pg_class listing works on empty catalog", async () => {
    // Server intercepts any FROM pg_class probe with a table listing.
    // With no user tables we get an empty result, but the round-trip
    // still succeeds — that's what we assert.
    const r = await client.query("SELECT relname AS \"Name\" FROM pg_class");
    expect(Array.isArray(r.rows)).toBe(true);
  });
});
