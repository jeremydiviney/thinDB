import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Client } from "pg";
import { startServer, type ServerHandle } from "../helpers/server.ts";

// Shared connection across the suite — matches the pattern used by
// table.test.ts and types.test.ts. There is a known engine bug where
// data inserted on one wire connection isn't visible on a subsequent
// connection (tracked as task #169); these tests don't exercise that
// path.

describe("pg complex queries", () => {
  let server: ServerHandle;
  let client: Client;

  beforeAll(async () => {
    server = await startServer({ label: "pg-complex" });
    client = new Client({
      host: server.bind,
      port: server.ports.pg,
      user: "thindb",
      password: "anything",
      database: "main",
    });
    await client.connect();
    await client.query(
      "CREATE TABLE events (id BIGINT PRIMARY KEY, qty INT NOT NULL, tag TEXT NOT NULL)",
    );
    await client.query(
      "INSERT INTO events VALUES " +
        "(1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b'), (4, 40, 'b'), (5, 50, 'c'), (6, 60, 'c')",
    );
  });

  afterAll(async () => {
    if (client !== undefined) await client.end().catch(() => undefined);
    if (server !== undefined) await server.close();
  });

  test("three-level nested CTE chain", async () => {
    const r = await client.query(
      "WITH filtered AS (SELECT id, qty, tag FROM events WHERE qty >= 20)," +
        "     by_tag AS (SELECT tag, sum(qty) AS total FROM filtered GROUP BY tag)," +
        "     big AS (SELECT tag, total FROM by_tag WHERE tag <> 'a') " +
        "SELECT tag, total FROM big ORDER BY total DESC",
    );
    expect(r.rows.length).toBe(2);
    expect(r.rows[0].tag).toBe("c");
    expect(Number(r.rows[0].total)).toBe(110);
    expect(r.rows[1].tag).toBe("b");
    expect(Number(r.rows[1].total)).toBe(70);
  });

  test("CTE referenced twice — auto materialize via refcount=2", async () => {
    const r = await client.query(
      "WITH narrow AS (SELECT id FROM events WHERE qty >= 30) " +
        "SELECT count(*) AS n FROM narrow JOIN narrow AS o ON narrow.id = o.id",
    );
    expect(Number(r.rows[0].n)).toBe(4);
  });

  test("MATERIALIZED hint forces buffer on single-use CTE", async () => {
    const r = await client.query(
      "WITH narrow AS MATERIALIZED (SELECT id, qty FROM events WHERE qty >= 30) " +
        "SELECT id, qty FROM narrow ORDER BY id",
    );
    expect(r.rows.length).toBe(4);
    expect(String(r.rows[0].id)).toBe("3");
    expect(String(r.rows[3].id)).toBe("6");
  });

  test("NOT MATERIALIZED suppresses auto on multi-use CTE", async () => {
    const r = await client.query(
      "WITH narrow AS NOT MATERIALIZED (SELECT id FROM events WHERE qty >= 30) " +
        "SELECT count(*) AS n FROM narrow JOIN narrow AS o ON narrow.id = o.id",
    );
    expect(Number(r.rows[0].n)).toBe(4);
  });

  test("FROM-clause subquery", async () => {
    const r = await client.query(
      "SELECT id, qty FROM (SELECT id, qty FROM events WHERE qty >= 30) AS sub ORDER BY id",
    );
    expect(r.rows.length).toBe(4);
    expect(String(r.rows[0].id)).toBe("3");
    expect(String(r.rows[3].id)).toBe("6");
  });

  test("GROUP BY + multiple aggregates + ORDER BY DESC", async () => {
    const r = await client.query(
      "SELECT tag, count(*) AS n, sum(qty) AS total FROM events GROUP BY tag ORDER BY total DESC",
    );
    expect(r.rows.length).toBe(3);
    expect(r.rows[0].tag).toBe("c");
    expect(Number(r.rows[0].n)).toBe(2);
    expect(Number(r.rows[0].total)).toBe(110);
    expect(r.rows[2].tag).toBe("a");
    expect(Number(r.rows[2].total)).toBe(30);
  });

  test("inline line + block comments don't break the query", async () => {
    const r = await client.query(
      "-- pick rows with high qty\n" +
        "SELECT id /* row id */, qty\n" +
        "FROM events\n" +
        "WHERE qty >= 30 /* inline filter */\n" +
        "ORDER BY id\n" +
        "-- trailing comment",
    );
    expect(r.rows.length).toBe(4);
    expect(String(r.rows[0].id)).toBe("3");
  });

  test("compound predicates: AND + OR + parens", async () => {
    const r = await client.query(
      "SELECT id FROM events WHERE (qty >= 20 AND tag = 'a') OR qty >= 50 ORDER BY id",
    );
    expect(r.rows.length).toBe(3);
    expect(String(r.rows[0].id)).toBe("2");
    expect(String(r.rows[1].id)).toBe("5");
    expect(String(r.rows[2].id)).toBe("6");
  });

  test("scalar function in SELECT", async () => {
    const r = await client.query(
      "SELECT upper(tag) AS tag_upper, id FROM events ORDER BY id LIMIT 3",
    );
    expect(r.rows.length).toBe(3);
    expect(r.rows[0].tag_upper).toBe("A");
    expect(r.rows[2].tag_upper).toBe("B");
  });

  test("multi-row INSERT then aggregate", async () => {
    await client.query("CREATE TABLE bulk (id BIGINT PRIMARY KEY, v INT NOT NULL)");
    try {
      const insertRes = await client.query(
        "INSERT INTO bulk VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
      );
      expect(insertRes.rowCount).toBe(5);
      const r = await client.query("SELECT sum(v) AS total FROM bulk");
      expect(Number(r.rows[0].total)).toBe(150);
    } finally {
      await client.query("DROP TABLE bulk").catch(() => undefined);
    }
  });

  test("multi-statement: two SELECTs in one Query frame — pg returns array of results", async () => {
    // PG's simple-Query protocol natively supports `;`-separated
    // statements. The node-pg driver surfaces the chain as an array
    // when more than one result set comes back.
    const r = (await client.query(
      "SELECT id FROM events LIMIT 1; SELECT id FROM events LIMIT 2",
    )) as unknown as Array<{ rows: Array<Record<string, unknown>> }>;
    expect(Array.isArray(r)).toBe(true);
    expect(r.length).toBe(2);
    expect(r[0].rows.length).toBe(1);
    expect(r[1].rows.length).toBe(2);
  });

  test("multi-statement: CREATE + INSERT + SELECT count(*) lands the data", async () => {
    try {
      // node-pg returns an array-of-results; CREATE + INSERT show as
      // command tags (rows=[]), the final SELECT carries the row.
      const r = (await client.query(
        "CREATE TABLE multi_t (id BIGINT PRIMARY KEY); " +
          "INSERT INTO multi_t VALUES (1),(2),(3); " +
          "SELECT count(*) AS n FROM multi_t",
      )) as unknown as Array<{ rows: Array<Record<string, unknown>> }>;
      expect(Array.isArray(r)).toBe(true);
      const last = r[r.length - 1];
      expect(last.rows.length).toBe(1);
      expect(Number(last.rows[0].n)).toBe(3);
    } finally {
      await client.query("DROP TABLE multi_t").catch(() => undefined);
    }
  });

  test("multi-statement: error in the middle does not deadlock the connection", async () => {
    let threw = false;
    try {
      await client.query(
        "SELECT id FROM events LIMIT 1; SELECT id FROM ghost_table; SELECT id FROM events LIMIT 1",
      );
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);
    const after = await client.query("SELECT id FROM events LIMIT 1");
    expect(after.rows.length).toBe(1);
  });
});
