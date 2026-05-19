import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import mysql, { type Connection } from "mysql2/promise";
import { startServer, type ServerHandle } from "../helpers/server.ts";

// mysql2's `conn.execute(...)` uses the binary prepared-statement
// protocol (COM_STMT_PREPARE + COM_STMT_EXECUTE). `conn.query(...)`
// uses text-mode COM_QUERY. These tests exercise the binary path.

describe("mysql prepared statements", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "mysql-prepared" });
  });

  afterAll(async () => {
    if (server !== undefined) await server.close();
  });

  test("execute parameterized SELECT with int param returns matching rows", async () => {
    const conn: Connection = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      await conn.query("CREATE TABLE t_exec_int (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
      await conn.query(
        "INSERT INTO t_exec_int VALUES (1, 10), (2, 50), (3, 100)",
      );

      const [rows] = (await conn.execute(
        "SELECT id, qty FROM t_exec_int WHERE qty >= ?",
        [50],
      )) as [Array<Record<string, unknown>>, unknown];
      expect(rows.length).toBe(2);
      expect(String(rows[0]?.id)).toBe("2");
      expect(Number(rows[0]?.qty)).toBe(50);
      expect(String(rows[1]?.id)).toBe("3");
      expect(Number(rows[1]?.qty)).toBe(100);

      await conn.query("DROP TABLE t_exec_int");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("execute parameterized INSERT writes rows in a loop", async () => {
    const conn: Connection = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      await conn.query(
        "CREATE TABLE t_exec_ins (id BIGINT PRIMARY KEY, qty INT NOT NULL, tag VARCHAR(32) NOT NULL)",
      );
      for (let i = 1; i <= 5; i++) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const [res] = (await conn.execute(
          "INSERT INTO t_exec_ins VALUES (?, ?, ?)",
          [i, i * 10, `row-${i}`],
        )) as [any, unknown];
        expect(res.affectedRows).toBe(1);
      }
      const [rows] = (await conn.query(
        "SELECT id, qty, tag FROM t_exec_ins ORDER BY id ASC",
      )) as [Array<Record<string, unknown>>, unknown];
      expect(rows.length).toBe(5);
      expect(rows[0]?.tag).toBe("row-1");
      expect(rows[4]?.tag).toBe("row-5");

      await conn.query("DROP TABLE t_exec_ins");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("parameter types: int, bigint, double, string round-trip", async () => {
    const conn: Connection = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      await conn.query(
        "CREATE TABLE t_types (" +
          "id INT PRIMARY KEY, " +
          "i32 INT NOT NULL, " +
          "i64 BIGINT NOT NULL, " +
          "dbl DOUBLE NOT NULL, " +
          "txt VARCHAR(64) NOT NULL)",
      );

      // i64 column needs a value the parser will type as BIGINT, i.e.
      // outside the i32 range. (The v1 parser/predicate layer is
      // strict-typed: an int literal won't match a BIGINT column.)
      await conn.execute(
        "INSERT INTO t_types VALUES (?, ?, ?, ?, ?)",
        [1, 42, 9000000000, 3.14, "hello, world"],
      );

      const [rows] = (await conn.execute(
        "SELECT id, i32, i64, dbl, txt FROM t_types WHERE id = ?",
        [1],
      )) as [Array<Record<string, unknown>>, unknown];
      expect(rows.length).toBe(1);
      expect(Number(rows[0]?.i32)).toBe(42);
      expect(String(rows[0]?.i64)).toBe("9000000000");
      expect(Number(rows[0]?.dbl)).toBeCloseTo(3.14, 5);
      expect(rows[0]?.txt).toBe("hello, world");

      await conn.query("DROP TABLE t_types");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("BIGINT param + result round-trips via Number when within safe range", async () => {
    const conn: Connection = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      await conn.query("CREATE TABLE t_big (id BIGINT PRIMARY KEY)");
      const safe = 4503599627370495; // > i32 range, < MAX_SAFE_INTEGER
      await conn.execute("INSERT INTO t_big VALUES (?)", [safe]);
      const [rows] = (await conn.execute(
        "SELECT id FROM t_big WHERE id = ?",
        [safe],
      )) as [Array<Record<string, unknown>>, unknown];
      expect(rows.length).toBe(1);
      expect(String(rows[0]?.id)).toBe(String(safe));
      await conn.query("DROP TABLE t_big");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("wrong arg count surfaces as a driver-side error before hitting the server", async () => {
    const conn: Connection = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      await conn.query("CREATE TABLE t_argcount (id INT PRIMARY KEY)");
      let saw_error = false;
      try {
        // Two `?` in the SQL but only one bound value → mysql2 raises
        // a client-side error before touching the wire.
        await conn.execute("SELECT id FROM t_argcount WHERE id = ? AND id = ?", [1]);
      } catch {
        saw_error = true;
      }
      expect(saw_error).toBe(true);
      await conn.query("DROP TABLE t_argcount");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("execute multiple times reuses statement under the hood", async () => {
    const conn: Connection = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      await conn.query("CREATE TABLE t_reuse (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
      await conn.query(
        "INSERT INTO t_reuse VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
      );
      for (const threshold of [10, 25, 40]) {
        const [rows] = (await conn.execute(
          "SELECT id FROM t_reuse WHERE qty >= ?",
          [threshold],
        )) as [Array<Record<string, unknown>>, unknown];
        // qty>=10 → 4 rows; qty>=25 → 2 rows; qty>=40 → 1 row.
        const expected = threshold === 10 ? 4 : threshold === 25 ? 2 : 1;
        expect(rows.length).toBe(expected);
      }
      await conn.query("DROP TABLE t_reuse");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });
});
