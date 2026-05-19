import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import mysql, { type Connection } from "mysql2/promise";
import { startServer, type ServerHandle } from "../helpers/server.ts";

// Each test below opens and closes its own connection, because the
// MySQL listener is single-threaded — one in-flight connection blocks
// new accepts.

describe("mysql basic", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "mysql-basic" });
  });

  afterAll(async () => {
    if (server !== undefined) await server.close();
  });

  test("listens on a non-zero port", () => {
    expect(server.ports.mysql).toBeGreaterThan(0);
  });

  test("handshake + COM_INIT_DB to main__public succeeds", async () => {
    const conn: Connection = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      expect(conn).toBeDefined();
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("handshake with no initial DB leaves session at main/public", async () => {
    // mysql2 always sets CONNECT_WITH_DB and sends an empty database
    // string when none is configured. The server should treat that as
    // a no-op and leave the session at the default main/public.
    const conn: Connection = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
    });
    try {
      const [rows] = (await conn.query("SELECT DATABASE()")) as [
        Array<Record<string, string | null>>,
        unknown,
      ];
      expect(rows.length).toBe(1);
      expect(rows[0]?.["DATABASE()"]).toBe("public");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("handshake to nonexistent db fails with ER_BAD_DB_ERROR (1049)", async () => {
    let code: string | undefined;
    let errno: number | undefined;
    try {
      const conn = await mysql.createConnection({
        host: server.bind,
        port: server.ports.mysql,
        user: "thindb",
        password: "",
        database: "no__such_db",
      });
      await conn.end().catch(() => undefined);
    } catch (err) {
      // mysql2 attaches code/errno to the auth/init-db error.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const e = err as any;
      code = e.code;
      errno = e.errno;
    }
    expect(code).toBe("ER_BAD_DB_ERROR");
    expect(errno).toBe(1049);
  });

  test("SELECT @@version returns the server marker", async () => {
    const conn = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      const [rows] = (await conn.query("SELECT @@version")) as [
        Array<Record<string, string>>,
        unknown,
      ];
      expect(rows.length).toBe(1);
      expect(rows[0]?.["@@version"]).toContain("thinDB");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("SELECT @@version_comment returns 'thinDB'", async () => {
    const conn = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      const [rows] = (await conn.query("SELECT @@version_comment")) as [
        Array<Record<string, string>>,
        unknown,
      ];
      expect(rows.length).toBe(1);
      expect(rows[0]?.["@@version_comment"]).toBe("thinDB");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("SELECT 1 returns 1", async () => {
    const conn = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      const [rows] = (await conn.query("SELECT 1")) as [
        Array<Record<string, string>>,
        unknown,
      ];
      expect(rows.length).toBe(1);
      expect(String(rows[0]?.["1"])).toBe("1");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("BEGIN/COMMIT/ROLLBACK flip SERVER_STATUS_IN_TRANS on OK packets", async () => {
    // mysql2 surfaces the OK packet's status_flags as `serverStatus`.
    // SERVER_STATUS_AUTOCOMMIT = 0x0002, SERVER_STATUS_IN_TRANS = 0x0001.
    const conn = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const [begin] = (await conn.query("BEGIN")) as [any, unknown];
      expect((begin.serverStatus & 0x0001) !== 0).toBe(true);

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const [commit] = (await conn.query("COMMIT")) as [any, unknown];
      expect((commit.serverStatus & 0x0001) === 0).toBe(true);

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const [start] = (await conn.query("START TRANSACTION")) as [any, unknown];
      expect((start.serverStatus & 0x0001) !== 0).toBe(true);

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const [rb] = (await conn.query("ROLLBACK")) as [any, unknown];
      expect((rb.serverStatus & 0x0001) === 0).toBe(true);
    } finally {
      await conn.end().catch(() => undefined);
    }
  });
});
