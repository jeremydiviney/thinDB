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

  test("RENAME, ALTER ADD COLUMN, and TRUNCATE", async () => {
    const conn: Connection = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      await conn.query("CREATE TABLE ddl_src (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
      await conn.query("INSERT INTO ddl_src VALUES (1, 10), (2, 20)");
      await conn.query("RENAME TABLE ddl_src TO ddl_dst");
      await conn.query("ALTER TABLE ddl_dst ADD COLUMN note TEXT");
      await conn.query("ALTER TABLE ddl_dst ADD COLUMN score INT NOT NULL DEFAULT 0");

      const [altered] = (await conn.query(
        "SELECT id, note, score FROM ddl_dst ORDER BY id ASC",
      )) as [Array<Record<string, unknown>>, unknown];
      expect(altered.length).toBe(2);
      expect(altered[0]?.note).toBeNull();
      expect(Number(altered[0]?.score)).toBe(0);
      expect(altered[1]?.note).toBeNull();

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const [truncateRes] = (await conn.query("TRUNCATE TABLE ddl_dst")) as [any, unknown];
      expect(truncateRes.affectedRows).toBe(0);

      const [empty] = (await conn.query("SELECT id FROM ddl_dst")) as [
        Array<Record<string, unknown>>,
        unknown,
      ];
      expect(empty.length).toBe(0);

      await conn.query("INSERT INTO ddl_dst (id, qty) VALUES (3, 30)");
      const [after] = (await conn.query("SELECT id, score FROM ddl_dst")) as [
        Array<Record<string, unknown>>,
        unknown,
      ];
      expect(after.length).toBe(1);
      expect(String(after[0]?.id)).toBe("3");
      expect(Number(after[0]?.score)).toBe(0);

      await conn.query("DROP TABLE ddl_dst");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("REPLACE returns OK packets and replaces by primary key", async () => {
    const conn: Connection = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      await conn.query("CREATE TABLE repl_t (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
      await conn.query("CREATE TABLE repl_src (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
      await conn.query("INSERT INTO repl_t VALUES (1, 10)");
      await conn.query("INSERT INTO repl_src VALUES (1, 100), (2, 200)");

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const [replaceRes] = (await conn.query("REPLACE INTO repl_t VALUES (1, 99)")) as [
        any,
        unknown,
      ];
      expect(replaceRes.affectedRows).toBe(1);

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const [replaceNoIntoRes] = (await conn.query("REPLACE repl_t (id, qty) VALUES (3, 300)")) as [
        any,
        unknown,
      ];
      expect(replaceNoIntoRes.affectedRows).toBe(1);

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const [replaceSelectRes] = (await conn.query("REPLACE INTO repl_t SELECT id, qty FROM repl_src")) as [
        any,
        unknown,
      ];
      expect(replaceSelectRes.affectedRows).toBe(2);

      const [rows] = (await conn.query("SELECT id, qty FROM repl_t ORDER BY id ASC")) as [
        Array<Record<string, unknown>>,
        unknown,
      ];
      expect(rows.length).toBe(3);
      expect(String(rows[0]?.id)).toBe("1");
      expect(Number(rows[0]?.qty)).toBe(100);
      expect(String(rows[1]?.id)).toBe("2");
      expect(Number(rows[1]?.qty)).toBe(200);
      expect(String(rows[2]?.id)).toBe("3");
      expect(Number(rows[2]?.qty)).toBe(300);

      await conn.query("DROP TABLE repl_src");
      await conn.query("DROP TABLE repl_t");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });
});
