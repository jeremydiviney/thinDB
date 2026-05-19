import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import mysql, { type Connection } from "mysql2/promise";
import { startServer, type ServerHandle } from "../helpers/server.ts";

// Temp tables are session-scoped: visible to the creating connection, invisible
// to peers, and dropped at disconnect or on RESET CONNECTION.

describe("mysql temp tables", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "mysql-temp" });
  });

  afterAll(async () => {
    if (server !== undefined) await server.close();
  });

  function connect(): Promise<Connection> {
    return mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
  }

  test("CREATE TEMP TABLE + INSERT + SELECT round-trip", async () => {
    const conn = await connect();
    try {
      await conn.query("CREATE TEMP TABLE scratch (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
      await conn.query("INSERT INTO scratch VALUES (1, 10), (2, 20)");
      const [rows] = (await conn.query("SELECT id, qty FROM scratch ORDER BY id ASC")) as [
        Array<Record<string, unknown>>,
        unknown,
      ];
      expect(rows.length).toBe(2);
      expect(String(rows[0]?.id)).toBe("1");
      expect(Number(rows[0]?.qty)).toBe(10);
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("TEMPORARY keyword is accepted as a synonym for TEMP", async () => {
    const conn = await connect();
    try {
      await conn.query("CREATE TEMPORARY TABLE scratch2 (id BIGINT PRIMARY KEY)");
      await conn.query("INSERT INTO scratch2 VALUES (42)");
      const [rows] = (await conn.query("SELECT id FROM scratch2")) as [
        Array<Record<string, unknown>>,
        unknown,
      ];
      expect(rows.length).toBe(1);
      expect(String(rows[0]?.id)).toBe("42");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("temp table is invisible to a second connection", async () => {
    const a = await connect();
    const b = await connect();
    try {
      await a.query("CREATE TEMP TABLE only_a (id BIGINT PRIMARY KEY)");
      await a.query("INSERT INTO only_a VALUES (7)");

      let code: string | undefined;
      try {
        await b.query("SELECT id FROM only_a");
      } catch (err) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        code = (err as any).code;
      }
      expect(code).toBe("ER_NO_SUCH_TABLE");
    } finally {
      await a.end().catch(() => undefined);
      await b.end().catch(() => undefined);
    }
  });

  test("temp table shadows a persistent table of the same name", async () => {
    const setup = await connect();
    try {
      await setup.query("CREATE TABLE shadowed (id BIGINT PRIMARY KEY, src VARCHAR(8) NOT NULL)");
      await setup.query("INSERT INTO shadowed VALUES (1, 'real')");
    } finally {
      await setup.end().catch(() => undefined);
    }

    const conn = await connect();
    try {
      await conn.query("CREATE TEMP TABLE shadowed (id BIGINT PRIMARY KEY, src VARCHAR(8) NOT NULL)");
      await conn.query("INSERT INTO shadowed VALUES (2, 'temp')");

      const [rows] = (await conn.query("SELECT src FROM shadowed")) as [
        Array<Record<string, unknown>>,
        unknown,
      ];
      expect(rows.length).toBe(1);
      expect(String(rows[0]?.src)).toBe("temp");

      // DROP TABLE removes the temp shadow; persistent stays.
      await conn.query("DROP TABLE shadowed");
      const [after] = (await conn.query("SELECT src FROM shadowed")) as [
        Array<Record<string, unknown>>,
        unknown,
      ];
      expect(after.length).toBe(1);
      expect(String(after[0]?.src)).toBe("real");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("RESET CONNECTION drops the session's temp tables", async () => {
    const conn = await connect();
    try {
      await conn.query("CREATE TEMP TABLE ephemeral (id BIGINT PRIMARY KEY)");
      await conn.query("INSERT INTO ephemeral VALUES (1)");

      // mysql2 exposes the binary COM_RESET_CONNECTION via `changeUser`
      // with no overrides — but the simplest portable trigger is the
      // text-protocol `RESET CONNECTION` statement, which the canned
      // matcher routes to the same handler.
      await conn.query("RESET CONNECTION");

      let code: string | undefined;
      try {
        await conn.query("SELECT id FROM ephemeral");
      } catch (err) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        code = (err as any).code;
      }
      expect(code).toBe("ER_NO_SUCH_TABLE");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("temp table dropped on disconnect — fresh connection can't see it", async () => {
    const a = await connect();
    try {
      await a.query("CREATE TEMP TABLE goes_away (id BIGINT PRIMARY KEY)");
      await a.query("INSERT INTO goes_away VALUES (1)");
    } finally {
      await a.end().catch(() => undefined);
    }

    const b = await connect();
    try {
      let code: string | undefined;
      try {
        await b.query("SELECT id FROM goes_away");
      } catch (err) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        code = (err as any).code;
      }
      expect(code).toBe("ER_NO_SUCH_TABLE");
    } finally {
      await b.end().catch(() => undefined);
    }
  });
});
