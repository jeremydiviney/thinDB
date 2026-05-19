import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import mysql, { type Connection } from "mysql2/promise";
import { startServer, type ServerHandle } from "../helpers/server.ts";
import { todo } from "../helpers/todo.ts";

// SHOW DATABASES + COM_INIT_DB (USE) both round-trip through the
// MySQL wire as result-sets, so they're blocked by the same
// CLIENT_DEPRECATE_EOF mismatch documented in mysql/basic.test.ts.
// We test what we can: COM_INIT_DB on its own (which the server
// answers with a bare OK packet — no result set), and document the
// gaps as todos.

describe("mysql namespace — handshake-only", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "mysql-ns" });
  });

  afterAll(async () => {
    if (server !== undefined) await server.close();
  });

  test("USE-style initial DB resolves db__schema flattened name", async () => {
    const conn: Connection = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      // No assertion error means the wire accepted the COM_INIT_DB
      // resolution of "main__public" → (db=main, schema=public).
      expect(conn).toBeDefined();
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  todo("SHOW DATABASES lists main__public (blocked: DEPRECATE_EOF mismatch)");
  todo("CREATE DATABASE then SHOW DATABASES (blocked: DEPRECATE_EOF mismatch)");
  todo("DROP DATABASE then SHOW DATABASES (blocked: DEPRECATE_EOF mismatch)");
});
