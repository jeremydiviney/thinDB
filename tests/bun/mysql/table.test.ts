import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import mysql, { type Connection } from "mysql2/promise";
import { startServer, type ServerHandle } from "../helpers/server.ts";

// mysql2 returns BIGINT as a JS string by default to avoid precision loss
// on values larger than 2^53. The tests assert against the string form
// when reading a BIGINT column.

describe("mysql table", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "mysql-table" });
  });

  afterAll(async () => {
    if (server !== undefined) await server.close();
  });

  test("CREATE TABLE + INSERT + SELECT round-trip", async () => {
    const conn: Connection = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      await conn.query("CREATE TABLE t1 (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
      const [insertRes] = await conn.query(
        "INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30)",
      );
      // mysql2 returns an OkPacket whose `affectedRows` reflects the OK_Packet field.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect((insertRes as any).affectedRows).toBe(3);

      const [rows] = (await conn.query("SELECT id, qty FROM t1 ORDER BY id ASC")) as [
        Array<Record<string, unknown>>,
        unknown,
      ];
      expect(rows.length).toBe(3);
      expect(String(rows[0]?.id)).toBe("1");
      expect(Number(rows[0]?.qty)).toBe(10);
      expect(String(rows[2]?.id)).toBe("3");
      expect(Number(rows[2]?.qty)).toBe(30);

      await conn.query("DROP TABLE t1");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("DROP TABLE removes the table", async () => {
    const conn: Connection = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      await conn.query("CREATE TABLE drop_me (id BIGINT PRIMARY KEY)");
      await conn.query("DROP TABLE drop_me");
      // After drop the table is gone — SELECT errors.
      let code: string | undefined;
      try {
        await conn.query("SELECT id FROM drop_me");
      } catch (err) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        code = (err as any).code;
      }
      expect(code).toBe("ER_NO_SUCH_TABLE");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("INSERT with named column list reorders values", async () => {
    const conn: Connection = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      await conn.query("CREATE TABLE t2 (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL, qty INT NOT NULL)");
      await conn.query("INSERT INTO t2 (name, id, qty) VALUES ('alice', 1, 100)");
      const [rows] = (await conn.query("SELECT id, name, qty FROM t2")) as [
        Array<Record<string, unknown>>,
        unknown,
      ];
      expect(rows.length).toBe(1);
      expect(String(rows[0]?.id)).toBe("1");
      expect(String(rows[0]?.name)).toBe("alice");
      expect(Number(rows[0]?.qty)).toBe(100);
      await conn.query("DROP TABLE t2");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });
});
