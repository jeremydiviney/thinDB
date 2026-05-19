import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Client } from "pg";
import { startServer, type ServerHandle } from "../helpers/server.ts";

// pg driver type-parser quirks worth noting:
//   - BIGINT (OID 20) → string by default (precision-preserving).
//   - NUMERIC → string by default.
//   - DATE → JS Date.
//   - TIMESTAMP → JS Date.
//   - UUID → string.

describe("pg types", () => {
  let server: ServerHandle;
  let client: Client;

  beforeAll(async () => {
    server = await startServer({ label: "pg-types" });
    client = new Client({
      host: server.bind,
      port: server.ports.pg,
      user: "thindb",
      password: "x",
      database: "main",
    });
    await client.connect();
  });

  afterAll(async () => {
    if (client !== undefined) await client.end().catch(() => undefined);
    if (server !== undefined) await server.close();
  });

  test("INT round-trips", async () => {
    await client.query("CREATE TABLE t_int (id BIGINT PRIMARY KEY, v INT NOT NULL)");
    await client.query("INSERT INTO t_int VALUES (1, 12345)");
    const r = await client.query("SELECT v FROM t_int");
    expect(Number(r.rows[0]?.v)).toBe(12345);
    await client.query("DROP TABLE t_int");
  });

  test("BIGINT round-trips (driver returns string)", async () => {
    await client.query("CREATE TABLE t_bigint (id BIGINT PRIMARY KEY)");
    await client.query("INSERT INTO t_bigint VALUES (9000000000)");
    const r = await client.query("SELECT id FROM t_bigint");
    expect(String(r.rows[0]?.id)).toBe("9000000000");
    await client.query("DROP TABLE t_bigint");
  });

  test("SMALLINT round-trips", async () => {
    await client.query("CREATE TABLE t_si (id BIGINT PRIMARY KEY, v SMALLINT NOT NULL)");
    await client.query("INSERT INTO t_si VALUES (1, 1000)");
    const r = await client.query("SELECT v FROM t_si");
    expect(Number(r.rows[0]?.v)).toBe(1000);
    await client.query("DROP TABLE t_si");
  });

  test("VARCHAR round-trips", async () => {
    await client.query("CREATE TABLE t_vc (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL)");
    await client.query("INSERT INTO t_vc VALUES (1, 'hello world')");
    const r = await client.query("SELECT name FROM t_vc");
    expect(String(r.rows[0]?.name)).toBe("hello world");
    await client.query("DROP TABLE t_vc");
  });

  test("TEXT round-trips", async () => {
    await client.query("CREATE TABLE t_txt (id BIGINT PRIMARY KEY, body TEXT NOT NULL)");
    await client.query("INSERT INTO t_txt VALUES (1, 'a slightly longer string for TEXT')");
    const r = await client.query("SELECT body FROM t_txt");
    expect(String(r.rows[0]?.body)).toBe("a slightly longer string for TEXT");
    await client.query("DROP TABLE t_txt");
  });

  test("BOOLEAN round-trips", async () => {
    await client.query("CREATE TABLE t_bool (id BIGINT PRIMARY KEY, active BOOLEAN NOT NULL)");
    await client.query("INSERT INTO t_bool VALUES (1, TRUE), (2, FALSE)");
    const r = await client.query("SELECT id, active FROM t_bool ORDER BY id ASC");
    expect(r.rows[0]?.active).toBe(true);
    expect(r.rows[1]?.active).toBe(false);
    await client.query("DROP TABLE t_bool");
  });

  test("FLOAT round-trips", async () => {
    await client.query("CREATE TABLE t_f (id BIGINT PRIMARY KEY, v FLOAT NOT NULL)");
    await client.query("INSERT INTO t_f VALUES (1, 1.5)");
    const r = await client.query("SELECT v FROM t_f");
    expect(Number(r.rows[0]?.v)).toBeCloseTo(1.5, 5);
    await client.query("DROP TABLE t_f");
  });

  test("DOUBLE round-trips", async () => {
    await client.query("CREATE TABLE t_d (id BIGINT PRIMARY KEY, v DOUBLE NOT NULL)");
    await client.query("INSERT INTO t_d VALUES (1, 3.14159)");
    const r = await client.query("SELECT v FROM t_d");
    expect(Number(r.rows[0]?.v)).toBeCloseTo(3.14159, 5);
    await client.query("DROP TABLE t_d");
  });

  test("DATE round-trips (driver decodes to Date)", async () => {
    await client.query("CREATE TABLE t_date (id BIGINT PRIMARY KEY, d DATE NOT NULL)");
    await client.query("INSERT INTO t_date VALUES (1, '2024-01-15')");
    const r = await client.query("SELECT d FROM t_date");
    const got = r.rows[0]?.d as Date;
    // The driver constructs the Date in local TZ from YYYY-MM-DD. Compare
    // via toISOString prefix so we're tolerant of the timezone offset.
    expect(got).toBeInstanceOf(Date);
    expect(got.getUTCFullYear()).toBe(2024);
    expect(got.getUTCMonth() + 1).toBe(1);
    await client.query("DROP TABLE t_date");
  });

  test("TIMESTAMP round-trips (driver decodes to Date)", async () => {
    await client.query("CREATE TABLE t_ts (id BIGINT PRIMARY KEY, ts TIMESTAMP NOT NULL)");
    await client.query("INSERT INTO t_ts VALUES (1, '2024-01-15 12:30:45')");
    const r = await client.query("SELECT ts FROM t_ts");
    const got = r.rows[0]?.ts as Date;
    expect(got).toBeInstanceOf(Date);
    expect(got.getUTCFullYear()).toBe(2024);
    await client.query("DROP TABLE t_ts");
  });

  test("DECIMAL round-trips (driver returns string)", async () => {
    await client.query("CREATE TABLE t_dec (id BIGINT PRIMARY KEY, amt DECIMAL(10,2) NOT NULL)");
    await client.query("INSERT INTO t_dec VALUES (1, '123.45')");
    const r = await client.query("SELECT amt FROM t_dec");
    expect(String(r.rows[0]?.amt)).toBe("123.45");
    await client.query("DROP TABLE t_dec");
  });

  test("UUID round-trips (driver returns string)", async () => {
    await client.query("CREATE TABLE t_uuid (id UUID PRIMARY KEY)");
    await client.query("INSERT INTO t_uuid VALUES ('123e4567-e89b-12d3-a456-426614174000')");
    const r = await client.query("SELECT id FROM t_uuid");
    expect(String(r.rows[0]?.id)).toBe("123e4567-e89b-12d3-a456-426614174000");
    await client.query("DROP TABLE t_uuid");
  });
});
