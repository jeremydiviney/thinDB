import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import mysql, { type Connection } from "mysql2/promise";
import { startServer, type ServerHandle } from "../helpers/server.ts";

// Window functions (Tier 1) over the MySQL wire. The output of every
// window call is a BIGINT/DOUBLE (or input-type for value-access),
// which round-trips through mysql2 as the appropriate JS type.

describe("mysql window functions", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "mysql-window" });
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

  async function seed(conn: Connection): Promise<void> {
    await conn.query("DROP TABLE IF EXISTS t");
    await conn.query("CREATE TABLE t (id BIGINT PRIMARY KEY, grp BIGINT, qty BIGINT)");
    await conn.query(
      "INSERT INTO t VALUES (1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 2, 100), (5, 2, 200)",
    );
  }

  test("ROW_NUMBER over PARTITION BY", async () => {
    const conn = await connect();
    try {
      await seed(conn);
      const [rows] = (await conn.query(
        "SELECT id, row_number() OVER (PARTITION BY grp ORDER BY id ASC) AS rn FROM t ORDER BY id ASC",
      )) as [Array<Record<string, unknown>>, unknown];
      const got = rows.map((r) => String(r.rn));
      expect(got).toEqual(["1", "2", "3", "1", "2"]);
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("running SUM via aggregate window", async () => {
    const conn = await connect();
    try {
      await seed(conn);
      const [rows] = (await conn.query(
        "SELECT id, sum(qty) OVER (PARTITION BY grp ORDER BY id ASC) AS running FROM t ORDER BY id ASC",
      )) as [Array<Record<string, unknown>>, unknown];
      const got = rows.map((r) => String(r.running));
      expect(got).toEqual(["10", "30", "60", "100", "300"]);
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("LAG with column-ref default returns the current row's value at partition start", async () => {
    const conn = await connect();
    try {
      await seed(conn);
      // LAG(qty, 1, qty): at the first row of each partition there's no
      // previous row, so the default (= current row's qty) is returned.
      const [rows] = (await conn.query(
        "SELECT id, lag(qty, 1, qty) OVER (PARTITION BY grp ORDER BY id ASC) AS lagged FROM t ORDER BY id ASC",
      )) as [Array<Record<string, unknown>>, unknown];
      const got = rows.map((r) => String(r.lagged));
      // grp=1: id1 → 10 (self), id2 → 10, id3 → 20.
      // grp=2: id4 → 100 (self), id5 → 100.
      expect(got).toEqual(["10", "10", "20", "100", "100"]);
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("named window via WINDOW clause", async () => {
    const conn = await connect();
    try {
      await seed(conn);
      const [rows] = (await conn.query(
        "SELECT id, row_number() OVER w AS rn, sum(qty) OVER w AS rs FROM t WINDOW w AS (PARTITION BY grp ORDER BY id ASC) ORDER BY id ASC",
      )) as [Array<Record<string, unknown>>, unknown];
      const rns = rows.map((r) => String(r.rn));
      const rss = rows.map((r) => String(r.rs));
      expect(rns).toEqual(["1", "2", "3", "1", "2"]);
      expect(rss).toEqual(["10", "30", "60", "100", "300"]);
    } finally {
      await conn.end().catch(() => undefined);
    }
  });
});
