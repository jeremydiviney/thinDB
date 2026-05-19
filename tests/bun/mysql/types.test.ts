import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import mysql, { type Connection } from "mysql2/promise";
import { startServer, type ServerHandle } from "../helpers/server.ts";

// Per-type round-trip via the MySQL wire. mysql2 driver quirks worth
// noting:
//   - BIGINT comes back as a string by default (precision-preserving).
//   - DATE / DATETIME come back as JS Date objects (or strings depending
//     on `dateStrings` — we don't set it, so the driver decodes).
//   - DECIMAL also comes back as a string by default.

describe("mysql types", () => {
  let server: ServerHandle;
  let conn: Connection;

  beforeAll(async () => {
    server = await startServer({ label: "mysql-types" });
    conn = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
      dateStrings: true,
    });
  });

  afterAll(async () => {
    if (conn !== undefined) await conn.end().catch(() => undefined);
    if (server !== undefined) await server.close();
  });

  test("INT round-trips", async () => {
    await conn.query("CREATE TABLE t_int (id BIGINT PRIMARY KEY, v INT NOT NULL)");
    await conn.query("INSERT INTO t_int VALUES (1, 12345)");
    const [rows] = (await conn.query("SELECT v FROM t_int")) as [
      Array<Record<string, unknown>>,
      unknown,
    ];
    expect(Number(rows[0]?.v)).toBe(12345);
    await conn.query("DROP TABLE t_int");
  });

  test("BIGINT round-trips (driver returns string)", async () => {
    await conn.query("CREATE TABLE t_bigint (id BIGINT PRIMARY KEY)");
    await conn.query("INSERT INTO t_bigint VALUES (9000000000)");
    const [rows] = (await conn.query("SELECT id FROM t_bigint")) as [
      Array<Record<string, unknown>>,
      unknown,
    ];
    expect(String(rows[0]?.id)).toBe("9000000000");
    await conn.query("DROP TABLE t_bigint");
  });

  test("SMALLINT round-trips", async () => {
    await conn.query("CREATE TABLE t_si (id BIGINT PRIMARY KEY, v SMALLINT NOT NULL)");
    await conn.query("INSERT INTO t_si VALUES (1, 1000)");
    const [rows] = (await conn.query("SELECT v FROM t_si")) as [
      Array<Record<string, unknown>>,
      unknown,
    ];
    expect(Number(rows[0]?.v)).toBe(1000);
    await conn.query("DROP TABLE t_si");
  });

  test("VARCHAR round-trips", async () => {
    await conn.query("CREATE TABLE t_vc (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL)");
    await conn.query("INSERT INTO t_vc VALUES (1, 'hello world')");
    const [rows] = (await conn.query("SELECT name FROM t_vc")) as [
      Array<Record<string, unknown>>,
      unknown,
    ];
    expect(String(rows[0]?.name)).toBe("hello world");
    await conn.query("DROP TABLE t_vc");
  });

  test("TEXT round-trips", async () => {
    await conn.query("CREATE TABLE t_txt (id BIGINT PRIMARY KEY, body TEXT NOT NULL)");
    await conn.query("INSERT INTO t_txt VALUES (1, 'a slightly longer string for TEXT')");
    const [rows] = (await conn.query("SELECT body FROM t_txt")) as [
      Array<Record<string, unknown>>,
      unknown,
    ];
    expect(String(rows[0]?.body)).toBe("a slightly longer string for TEXT");
    await conn.query("DROP TABLE t_txt");
  });

  test("BOOLEAN round-trips", async () => {
    await conn.query("CREATE TABLE t_bool (id BIGINT PRIMARY KEY, active BOOLEAN NOT NULL)");
    await conn.query("INSERT INTO t_bool VALUES (1, TRUE), (2, FALSE)");
    const [rows] = (await conn.query("SELECT id, active FROM t_bool ORDER BY id ASC")) as [
      Array<Record<string, unknown>>,
      unknown,
    ];
    // Server formats BOOLEAN as "1" / "0" text — read it back as a number.
    expect(Number(rows[0]?.active)).toBe(1);
    expect(Number(rows[1]?.active)).toBe(0);
    await conn.query("DROP TABLE t_bool");
  });

  test("FLOAT round-trips", async () => {
    await conn.query("CREATE TABLE t_f (id BIGINT PRIMARY KEY, v FLOAT NOT NULL)");
    await conn.query("INSERT INTO t_f VALUES (1, 1.5)");
    const [rows] = (await conn.query("SELECT v FROM t_f")) as [
      Array<Record<string, unknown>>,
      unknown,
    ];
    expect(Number(rows[0]?.v)).toBeCloseTo(1.5, 5);
    await conn.query("DROP TABLE t_f");
  });

  test("DOUBLE round-trips", async () => {
    await conn.query("CREATE TABLE t_d (id BIGINT PRIMARY KEY, v DOUBLE NOT NULL)");
    await conn.query("INSERT INTO t_d VALUES (1, 3.14159)");
    const [rows] = (await conn.query("SELECT v FROM t_d")) as [
      Array<Record<string, unknown>>,
      unknown,
    ];
    expect(Number(rows[0]?.v)).toBeCloseTo(3.14159, 5);
    await conn.query("DROP TABLE t_d");
  });

  test("DATE round-trips (string form)", async () => {
    await conn.query("CREATE TABLE t_date (id BIGINT PRIMARY KEY, d DATE NOT NULL)");
    await conn.query("INSERT INTO t_date VALUES (1, '2024-01-15')");
    const [rows] = (await conn.query("SELECT d FROM t_date")) as [
      Array<Record<string, unknown>>,
      unknown,
    ];
    expect(String(rows[0]?.d)).toBe("2024-01-15");
    await conn.query("DROP TABLE t_date");
  });

  test("DATETIME round-trips (string form)", async () => {
    await conn.query("CREATE TABLE t_dt (id BIGINT PRIMARY KEY, ts DATETIME NOT NULL)");
    await conn.query("INSERT INTO t_dt VALUES (1, '2024-01-15 12:30:45')");
    const [rows] = (await conn.query("SELECT ts FROM t_dt")) as [
      Array<Record<string, unknown>>,
      unknown,
    ];
    // Server emits microsecond precision via wire_format.formatDateTime;
    // the substring assertion is robust to trailing fractional zeros.
    expect(String(rows[0]?.ts)).toContain("2024-01-15 12:30:45");
    await conn.query("DROP TABLE t_dt");
  });

  test("DECIMAL round-trips (driver returns string)", async () => {
    await conn.query("CREATE TABLE t_dec (id BIGINT PRIMARY KEY, amt DECIMAL(10,2) NOT NULL)");
    await conn.query("INSERT INTO t_dec VALUES (1, '123.45')");
    const [rows] = (await conn.query("SELECT amt FROM t_dec")) as [
      Array<Record<string, unknown>>,
      unknown,
    ];
    expect(String(rows[0]?.amt)).toBe("123.45");
    await conn.query("DROP TABLE t_dec");
  });

  test("UUID round-trips (driver returns string)", async () => {
    await conn.query("CREATE TABLE t_uuid (id UUID PRIMARY KEY)");
    await conn.query("INSERT INTO t_uuid VALUES ('123e4567-e89b-12d3-a456-426614174000')");
    const [rows] = (await conn.query("SELECT id FROM t_uuid")) as [
      Array<Record<string, unknown>>,
      unknown,
    ];
    expect(String(rows[0]?.id)).toBe("123e4567-e89b-12d3-a456-426614174000");
    await conn.query("DROP TABLE t_uuid");
  });
});
