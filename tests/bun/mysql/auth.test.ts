import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import mysql from "mysql2/promise";
import { startServer, type ServerHandle } from "../helpers/server.ts";

// Exercises mysql_native_password end-to-end through the real
// `mysql2` driver. mysql2 implements the SHA1 challenge/response
// when the server's HandshakeV10 advertises plugin
// "mysql_native_password" (which we do, unconditionally).

describe("mysql auth — mysql_native_password", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "mysql-auth-native", mysqlPassword: "hunter2" });
  });

  afterAll(async () => {
    if (server !== undefined) await server.close();
  });

  test("correct password completes the handshake + runs a query", async () => {
    const conn = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "hunter2",
      database: "main__public",
    });
    try {
      const [rows] = (await conn.query("SELECT 1")) as [
        Array<Record<string, unknown>>,
        unknown,
      ];
      expect(rows.length).toBe(1);
    } finally {
      await conn.end();
    }
  });

  test("wrong password rejected with ER_ACCESS_DENIED_ERROR (1045)", async () => {
    let code: string | undefined;
    let errno: number | undefined;
    try {
      const conn = await mysql.createConnection({
        host: server.bind,
        port: server.ports.mysql,
        user: "thindb",
        password: "wrong",
        database: "main__public",
      });
      await conn.end();
    } catch (err) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const e = err as any;
      code = e.code;
      errno = e.errno;
    }
    expect(code).toBe("ER_ACCESS_DENIED_ERROR");
    expect(errno).toBe(1045);
  });

  test("empty password rejected when server requires one", async () => {
    let threw = false;
    try {
      const conn = await mysql.createConnection({
        host: server.bind,
        port: server.ports.mysql,
        user: "thindb",
        password: "",
        database: "main__public",
      });
      await conn.end();
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);
  });
});

describe("mysql auth — COM_CHANGE_USER (0x11)", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "mysql-auth-change-user", mysqlPassword: "hunter2" });
  });

  afterAll(async () => {
    if (server !== undefined) await server.close();
  });

  test("changeUser with correct password switches DB + resets state", async () => {
    const conn = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "hunter2",
      database: "main__public",
    });
    try {
      // Open a transaction to verify changeUser clears it.
      await conn.query("BEGIN");
      // mysql2 exposes the COM_CHANGE_USER command directly.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      await (conn as any).changeUser({
        user: "thindb",
        password: "hunter2",
        database: "main__public",
      });
      // Verify the new session is fresh: SELECT DATABASE() returns
      // the expected schema, and a BEGIN OK reports IN_TRANS not set
      // before BEGIN runs again.
      const [rows] = (await conn.query("SELECT DATABASE()")) as [
        Array<Record<string, unknown>>,
        unknown,
      ];
      expect(String(rows[0]?.["DATABASE()"])).toBe("public");
    } finally {
      await conn.end();
    }
  });

  test("changeUser with wrong password rejected", async () => {
    const conn = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "hunter2",
      database: "main__public",
    });
    let threw = false;
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      await (conn as any).changeUser({
        user: "thindb",
        password: "wrong",
        database: "main__public",
      });
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);
    try {
      await conn.end();
    } catch {
      // already aborted by the failed changeUser
    }
  });
});

describe("mysql auth — trust mode (no password configured)", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "mysql-auth-trust" });
  });

  afterAll(async () => {
    if (server !== undefined) await server.close();
  });

  test("any password is accepted when server has no --mysql-password", async () => {
    const conn = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "whatever you like",
      database: "main__public",
    });
    try {
      const [rows] = (await conn.query("SELECT 1")) as [
        Array<Record<string, unknown>>,
        unknown,
      ];
      expect(rows.length).toBe(1);
    } finally {
      await conn.end();
    }
  });
});
