import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Client, DatabaseError } from "pg";
import { startServer, type ServerHandle } from "../helpers/server.ts";

describe("pg errors", () => {
  let server: ServerHandle;
  let client: Client;

  beforeAll(async () => {
    server = await startServer({ label: "pg-err" });
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

  test("SELECT from nonexistent table → relation does not exist (42P01)", async () => {
    let caught: DatabaseError | undefined;
    try {
      await client.query("SELECT * FROM nonexistent_table");
    } catch (err) {
      if (err instanceof DatabaseError) caught = err;
      else throw err;
    }
    expect(caught).toBeDefined();
    if (caught === undefined) return;
    expect(caught.code).toBe("42P01");
    expect(caught.message).toContain("relation");
  });

  test("syntax error → SQLSTATE on parse failure", async () => {
    let caught: DatabaseError | undefined;
    try {
      await client.query("SELECT FROM");
    } catch (err) {
      if (err instanceof DatabaseError) caught = err;
      else throw err;
    }
    expect(caught).toBeDefined();
    if (caught === undefined) return;
    // Server maps unknown parser errors to 42000 fallback.
    expect(caught.code).toBe("42000");
  });

  test("DROP DATABASE that doesn't exist → 3D000", async () => {
    let caught: DatabaseError | undefined;
    try {
      await client.query("DROP DATABASE no_such_db");
    } catch (err) {
      if (err instanceof DatabaseError) caught = err;
      else throw err;
    }
    expect(caught).toBeDefined();
    if (caught === undefined) return;
    expect(caught.code).toBe("3D000");
  });
});
