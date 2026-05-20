import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Client } from "pg";
import { startServer, type ServerHandle } from "../helpers/server.ts";

// Window functions (Tier 1) over the PG wire. pg returns BIGINTs as
// strings by default, so the assertions compare via String() to skip
// the precision question.

describe("pg window functions", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "pg-window" });
  });

  afterAll(async () => {
    if (server !== undefined) await server.close();
  });

  function makeClient(): Client {
    return new Client({
      host: server.bind,
      port: server.ports.pg,
      user: "thindb",
      password: "x",
      database: "main",
    });
  }

  async function seed(client: Client): Promise<void> {
    await client.query("DROP TABLE IF EXISTS t");
    await client.query("CREATE TABLE t (id BIGINT PRIMARY KEY, grp BIGINT, qty BIGINT)");
    await client.query(
      "INSERT INTO t VALUES (1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 2, 100), (5, 2, 200)",
    );
  }

  test("ROW_NUMBER over PARTITION BY", async () => {
    const client = makeClient();
    await client.connect();
    try {
      await seed(client);
      const r = await client.query(
        "SELECT id, row_number() OVER (PARTITION BY grp ORDER BY id ASC) AS rn FROM t ORDER BY id ASC",
      );
      const got = r.rows.map((row) => String(row.rn));
      expect(got).toEqual(["1", "2", "3", "1", "2"]);
    } finally {
      await client.end().catch(() => undefined);
    }
  });

  test("RANK with ties", async () => {
    const client = makeClient();
    await client.connect();
    try {
      await client.query("DROP TABLE IF EXISTS s");
      await client.query("CREATE TABLE s (id BIGINT PRIMARY KEY, score BIGINT)");
      await client.query("INSERT INTO s VALUES (1, 90), (2, 90), (3, 80), (4, 70), (5, 70)");
      const r = await client.query(
        "SELECT id, rank() OVER (ORDER BY score DESC) AS rk FROM s ORDER BY id ASC",
      );
      const got = r.rows.map((row) => String(row.rk));
      expect(got).toEqual(["1", "1", "3", "4", "4"]);
    } finally {
      await client.end().catch(() => undefined);
    }
  });

  test("running AVG via aggregate window", async () => {
    const client = makeClient();
    await client.connect();
    try {
      await seed(client);
      const r = await client.query(
        "SELECT id, avg(qty) OVER (PARTITION BY grp ORDER BY id ASC) AS run_avg FROM t ORDER BY id ASC",
      );
      const got = r.rows.map((row) => Number(row.run_avg));
      // grp=1: 10, 15, 20.   grp=2: 100, 150.
      expect(got).toEqual([10, 15, 20, 100, 150]);
    } finally {
      await client.end().catch(() => undefined);
    }
  });

  test("LAG with literal default", async () => {
    const client = makeClient();
    await client.connect();
    try {
      await seed(client);
      const r = await client.query(
        "SELECT id, lag(qty, 1, 0) OVER (PARTITION BY grp ORDER BY id ASC) AS prev FROM t ORDER BY id ASC",
      );
      const got = r.rows.map((row) => String(row.prev));
      expect(got).toEqual(["0", "10", "20", "0", "100"]);
    } finally {
      await client.end().catch(() => undefined);
    }
  });

  test("QUALIFY keeps only the top-ranked row per partition", async () => {
    const client = makeClient();
    await client.connect();
    try {
      await seed(client);
      const r = await client.query(
        "SELECT id, rank() OVER (PARTITION BY grp ORDER BY qty DESC) AS rk FROM t QUALIFY rk = 1 ORDER BY id ASC",
      );
      const got = r.rows.map((row) => String(row.id));
      // grp=1 top is id=3 (qty=30); grp=2 top is id=5 (qty=200).
      expect(got).toEqual(["3", "5"]);
    } finally {
      await client.end().catch(() => undefined);
    }
  });

  test("NTILE(2) splits a partition", async () => {
    const client = makeClient();
    await client.connect();
    try {
      await seed(client);
      const r = await client.query(
        "SELECT id, ntile(2) OVER (PARTITION BY grp ORDER BY id ASC) AS bucket FROM t ORDER BY id ASC",
      );
      const got = r.rows.map((row) => String(row.bucket));
      // grp=1 has 3 rows (N=3, n=2): 2/1 = bucket sizes 2,1 → 1,1,2
      // grp=2 has 2 rows (N=2, n=2): 1,1 → 1,2
      expect(got).toEqual(["1", "1", "2", "1", "2"]);
    } finally {
      await client.end().catch(() => undefined);
    }
  });

  test("ROWS BETWEEN 1 PRECEDING AND CURRENT ROW (trailing sum)", async () => {
    const client = makeClient();
    await client.connect();
    try {
      await seed(client);
      const r = await client.query(
        "SELECT id, sum(qty) OVER (PARTITION BY grp ORDER BY id ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trailing FROM t ORDER BY id ASC",
      );
      const got = r.rows.map((row) => String(row.trailing));
      expect(got).toEqual(["10", "30", "50", "100", "300"]);
    } finally {
      await client.end().catch(() => undefined);
    }
  });
});
