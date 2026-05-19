import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import mysql from "mysql2/promise";
import { startServer, type ServerHandle } from "../helpers/server.ts";

describe("mysql errors", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "mysql-err" });
  });

  afterAll(async () => {
    if (server !== undefined) await server.close();
  });

  test("nonexistent database in init-db → ER_BAD_DB_ERROR 1049 / 42000", async () => {
    let code: string | undefined;
    let errno: number | undefined;
    let sqlState: string | undefined;
    try {
      const conn = await mysql.createConnection({
        host: server.bind,
        port: server.ports.mysql,
        user: "thindb",
        password: "",
        database: "definitely__missing",
      });
      await conn.end().catch(() => undefined);
    } catch (err) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const e = err as any;
      code = e.code;
      errno = e.errno;
      sqlState = e.sqlState;
    }
    expect(code).toBe("ER_BAD_DB_ERROR");
    expect(errno).toBe(1049);
    expect(sqlState).toBe("42000");
  });

  test("SELECT from nonexistent table → 1146 / 42S02", async () => {
    const conn = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    let code: string | undefined;
    let errno: number | undefined;
    let sqlState: string | undefined;
    try {
      try {
        await conn.query("SELECT * FROM definitely_missing_table");
      } catch (err) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const e = err as any;
        code = e.code;
        errno = e.errno;
        sqlState = e.sqlState;
      }
    } finally {
      await conn.end().catch(() => undefined);
    }
    expect(errno).toBe(1146);
    expect(sqlState).toBe("42S02");
    expect(code).toBe("ER_NO_SUCH_TABLE");
  });

  test("syntax error → 1064 / 42000", async () => {
    const conn = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    let errno: number | undefined;
    let sqlState: string | undefined;
    try {
      try {
        await conn.query("WAT IS THIS NOT SQL");
      } catch (err) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const e = err as any;
        errno = e.errno;
        sqlState = e.sqlState;
      }
    } finally {
      await conn.end().catch(() => undefined);
    }
    expect(errno).toBe(1064);
    expect(sqlState).toBe("42000");
  });
});
