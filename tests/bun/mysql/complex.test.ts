import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import mysql, { type Connection } from "mysql2/promise";
import { startServer, type ServerHandle } from "../helpers/server.ts";

// Shared connection across the suite — matches the pattern used by
// table.test.ts and types.test.ts. There is a known engine bug where
// data inserted on one wire connection isn't visible on a subsequent
// connection (tracked as task #169); these tests don't exercise that
// path.
//
// `tag` uses TEXT (.string in the engine) so the upper() scalar overload
// — registered for .string only — matches at compile time.

describe("mysql complex queries", () => {
  let server: ServerHandle;
  let conn: Connection;

  beforeAll(async () => {
    server = await startServer({ label: "mysql-complex" });
    conn = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    await conn.query(
      "CREATE TABLE events (id BIGINT PRIMARY KEY, qty INT NOT NULL, tag TEXT NOT NULL)",
    );
    await conn.query(
      "INSERT INTO events VALUES " +
        "(1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b'), (4, 40, 'b'), (5, 50, 'c'), (6, 60, 'c')",
    );
  });

  afterAll(async () => {
    if (conn !== undefined) await conn.end().catch(() => undefined);
    if (server !== undefined) await server.close();
  });

  test("three-level nested CTE chain", async () => {
    // The third CTE filters by tag rather than by the BIGINT aggregate
    // result (sum returns BIGINT; comparing to an INT literal trips the
    // predicate-type-mismatch check).
    const [rows] = (await conn.query(
      "WITH filtered AS (SELECT id, qty, tag FROM events WHERE qty >= 20)," +
        "     by_tag AS (SELECT tag, sum(qty) AS total FROM filtered GROUP BY tag)," +
        "     big AS (SELECT tag, total FROM by_tag WHERE tag <> 'a') " +
        "SELECT tag, total FROM big ORDER BY total DESC",
    )) as [Array<Record<string, unknown>>, unknown];
    expect(rows.length).toBe(2);
    expect(rows[0]?.tag).toBe("c");
    expect(Number(rows[0]?.total)).toBe(110);
    expect(rows[1]?.tag).toBe("b");
    expect(Number(rows[1]?.total)).toBe(70);
  });

  test("CTE referenced twice — auto materialize via refcount=2", async () => {
    const [rows] = (await conn.query(
      "WITH narrow AS (SELECT id FROM events WHERE qty >= 30) " +
        "SELECT count(*) AS n FROM narrow JOIN narrow AS o ON narrow.id = o.id",
    )) as [Array<Record<string, unknown>>, unknown];
    expect(Number(rows[0]?.n)).toBe(4);
  });

  test("MATERIALIZED hint forces buffer on single-use CTE", async () => {
    const [rows] = (await conn.query(
      "WITH narrow AS MATERIALIZED (SELECT id, qty FROM events WHERE qty >= 30) " +
        "SELECT id, qty FROM narrow ORDER BY id",
    )) as [Array<Record<string, unknown>>, unknown];
    expect(rows.length).toBe(4);
    expect(String(rows[0]?.id)).toBe("3");
    expect(String(rows[3]?.id)).toBe("6");
  });

  test("NOT MATERIALIZED suppresses auto on multi-use CTE", async () => {
    const [rows] = (await conn.query(
      "WITH narrow AS NOT MATERIALIZED (SELECT id FROM events WHERE qty >= 30) " +
        "SELECT count(*) AS n FROM narrow JOIN narrow AS o ON narrow.id = o.id",
    )) as [Array<Record<string, unknown>>, unknown];
    expect(Number(rows[0]?.n)).toBe(4);
  });

  test("FROM-clause subquery", async () => {
    const [rows] = (await conn.query(
      "SELECT id, qty FROM (SELECT id, qty FROM events WHERE qty >= 30) AS sub ORDER BY id",
    )) as [Array<Record<string, unknown>>, unknown];
    expect(rows.length).toBe(4);
    expect(String(rows[0]?.id)).toBe("3");
    expect(String(rows[3]?.id)).toBe("6");
  });

  test("GROUP BY + multiple aggregates + ORDER BY DESC", async () => {
    const [rows] = (await conn.query(
      "SELECT tag, count(*) AS n, sum(qty) AS total FROM events GROUP BY tag ORDER BY total DESC",
    )) as [Array<Record<string, unknown>>, unknown];
    expect(rows.length).toBe(3);
    expect(rows[0]?.tag).toBe("c");
    expect(Number(rows[0]?.n)).toBe(2);
    expect(Number(rows[0]?.total)).toBe(110);
    expect(rows[2]?.tag).toBe("a");
    expect(Number(rows[2]?.total)).toBe(30);
  });

  test("inline line + block comments don't break the query", async () => {
    const [rows] = (await conn.query(
      "-- pick rows with high qty\n" +
        "SELECT id /* row id */, qty\n" +
        "FROM events\n" +
        "WHERE qty >= 30 /* inline filter */\n" +
        "ORDER BY id\n" +
        "-- trailing comment",
    )) as [Array<Record<string, unknown>>, unknown];
    expect(rows.length).toBe(4);
    expect(String(rows[0]?.id)).toBe("3");
  });

  test("compound predicates: AND + OR + parens", async () => {
    const [rows] = (await conn.query(
      "SELECT id FROM events WHERE (qty >= 20 AND tag = 'a') OR qty >= 50 ORDER BY id",
    )) as [Array<Record<string, unknown>>, unknown];
    expect(rows.length).toBe(3);
    expect(String(rows[0]?.id)).toBe("2");
    expect(String(rows[1]?.id)).toBe("5");
    expect(String(rows[2]?.id)).toBe("6");
  });

  test("scalar function in SELECT", async () => {
    const [rows] = (await conn.query(
      "SELECT upper(tag) AS tag_upper, id FROM events ORDER BY id LIMIT 3",
    )) as [Array<Record<string, unknown>>, unknown];
    expect(rows.length).toBe(3);
    expect(rows[0]?.tag_upper).toBe("A");
    expect(rows[2]?.tag_upper).toBe("B");
  });

  test("multi-row INSERT then aggregate", async () => {
    await conn.query("CREATE TABLE bulk (id BIGINT PRIMARY KEY, v INT NOT NULL)");
    try {
      const [insertRes] = await conn.query(
        "INSERT INTO bulk VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
      );
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect((insertRes as any).affectedRows).toBe(5);
      const [rows] = (await conn.query("SELECT sum(v) AS total FROM bulk")) as [
        Array<Record<string, unknown>>,
        unknown,
      ];
      expect(Number(rows[0]?.total)).toBe(150);
    } finally {
      await conn.query("DROP TABLE bulk").catch(() => undefined);
    }
  });

  test("multi-statement requires CLIENT_MULTI_STATEMENTS — default conn rejects", async () => {
    let threw = false;
    try {
      await conn.query("SELECT id FROM events LIMIT 1; SELECT id FROM events LIMIT 2");
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);
  });

  test("multi-statement: with multipleStatements=true, two SELECTs return array-of-result-sets", async () => {
    const multi = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
      multipleStatements: true,
    });
    try {
      const [results] = (await multi.query(
        "SELECT id FROM events LIMIT 1; SELECT id FROM events LIMIT 2",
      )) as [Array<Array<Record<string, unknown>>>, unknown];
      expect(results.length).toBe(2);
      expect(results[0].length).toBe(1);
      expect(results[1].length).toBe(2);
    } finally {
      await multi.end().catch(() => undefined);
    }
  });

  test("multi-statement: CREATE + INSERT + SELECT count(*) round-trips", async () => {
    const multi = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
      multipleStatements: true,
    });
    try {
      const [results] = (await multi.query(
        "CREATE TABLE multi_t (id BIGINT PRIMARY KEY); " +
          "INSERT INTO multi_t VALUES (1),(2),(3); " +
          "SELECT count(*) AS n FROM multi_t",
      )) as [Array<unknown>, unknown];
      // Last entry in the result chain is the SELECT — mysql2 returns
      // rows for SELECTs and OkPacket-shaped objects for side-effect
      // statements.
      const last = results[results.length - 1] as Array<Record<string, unknown>>;
      expect(Array.isArray(last)).toBe(true);
      expect(Number(last[0]?.n)).toBe(3);
    } finally {
      await multi.query("DROP TABLE multi_t").catch(() => undefined);
      await multi.end().catch(() => undefined);
    }
  });

  test("multi-statement: error in the middle does not deadlock the server", async () => {
    // mysql2 by design marks the connection closed on any error from a
    // multi-statement query; we only verify that the error surfaces and
    // a *fresh* connection still works (the server itself is healthy).
    const multi = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
      multipleStatements: true,
    });
    let threw = false;
    try {
      await multi.query(
        "SELECT id FROM events LIMIT 1; SELECT id FROM ghost_table; SELECT id FROM events LIMIT 1",
      );
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);
    await multi.end().catch(() => undefined);

    const fresh = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      const [rows] = (await fresh.query("SELECT id FROM events LIMIT 1")) as [
        Array<Record<string, unknown>>,
        unknown,
      ];
      expect(rows.length).toBe(1);
    } finally {
      await fresh.end().catch(() => undefined);
    }
  });
});
