import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Client } from "pg";
import { startServer, type ServerHandle } from "../helpers/server.ts";

// node-pg switches to the Extended Query protocol the moment a query
// has bound values. Every test in this suite exercises Parse / Bind /
// Describe / Execute / Sync at the wire level.

describe("pg prepared statements (Extended Query)", () => {
  let server: ServerHandle;
  let client: Client;

  beforeAll(async () => {
    server = await startServer({ label: "pg-prepared" });
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

  test("parameterized SELECT with int param returns matching rows", async () => {
    await client.query(
      "CREATE TABLE p_int (id BIGINT PRIMARY KEY, qty INT NOT NULL)",
    );
    await client.query("INSERT INTO p_int VALUES (1, 10), (2, 20), (3, 30)");
    const r = await client.query("SELECT id, qty FROM p_int WHERE qty = $1", [20]);
    expect(r.rows.length).toBe(1);
    expect(String(r.rows[0]?.id)).toBe("2");
    expect(Number(r.rows[0]?.qty)).toBe(20);
    await client.query("DROP TABLE p_int");
  });

  test("parameterized INSERT lands 3 rows via Extended Query", async () => {
    await client.query(
      "CREATE TABLE p_ins (id BIGINT PRIMARY KEY, qty INT NOT NULL, tag TEXT NOT NULL)",
    );
    const rows = [
      [10, 100, "alpha"],
      [11, 110, "beta"],
      [12, 120, "gamma"],
    ];
    for (const [id, qty, tag] of rows) {
      await client.query(
        "INSERT INTO p_ins VALUES ($1, $2, $3)",
        [id, qty, tag],
      );
    }
    const r = await client.query("SELECT id, qty, tag FROM p_ins ORDER BY id");
    expect(r.rows.length).toBe(3);
    expect(String(r.rows[0]?.tag)).toBe("alpha");
    expect(Number(r.rows[2]?.qty)).toBe(120);
    await client.query("DROP TABLE p_ins");
  });

  test("int param round-trip", async () => {
    await client.query(
      "CREATE TABLE p_t_int (id BIGINT PRIMARY KEY, v INT NOT NULL)",
    );
    await client.query("INSERT INTO p_t_int VALUES (1, 42), (2, 99)");
    const r = await client.query("SELECT v FROM p_t_int WHERE v = $1", [99]);
    expect(r.rows.length).toBe(1);
    expect(Number(r.rows[0]?.v)).toBe(99);
    await client.query("DROP TABLE p_t_int");
  });

  test("bigint param (driver returns string)", async () => {
    await client.query(
      "CREATE TABLE p_t_bi (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    );
    await client.query("INSERT INTO p_t_bi VALUES (1, 9000000000), (2, 1)");
    // node-pg sends bigints as text-format with type-OID hints. Pass
    // as a string to force PG's text-format numeric path.
    const r = await client.query(
      "SELECT v FROM p_t_bi WHERE v = $1",
      ["9000000000"],
    );
    expect(r.rows.length).toBe(1);
    expect(String(r.rows[0]?.v)).toBe("9000000000");
    await client.query("DROP TABLE p_t_bi");
  });

  test("float param round-trip", async () => {
    await client.query(
      "CREATE TABLE p_t_f (id BIGINT PRIMARY KEY, v DOUBLE NOT NULL)",
    );
    await client.query("INSERT INTO p_t_f VALUES (1, 1.5), (2, 2.5)");
    const r = await client.query("SELECT v FROM p_t_f WHERE v = $1", [2.5]);
    expect(r.rows.length).toBe(1);
    expect(Number(r.rows[0]?.v)).toBeCloseTo(2.5, 5);
    await client.query("DROP TABLE p_t_f");
  });

  test("string param round-trip", async () => {
    await client.query(
      "CREATE TABLE p_t_str (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
    );
    await client.query("INSERT INTO p_t_str VALUES (1, 'alpha'), (2, 'beta')");
    const r = await client.query(
      "SELECT name FROM p_t_str WHERE name = $1",
      ["beta"],
    );
    expect(r.rows.length).toBe(1);
    expect(String(r.rows[0]?.name)).toBe("beta");
    await client.query("DROP TABLE p_t_str");
  });

  test("string param with embedded single quote escapes correctly", async () => {
    await client.query(
      "CREATE TABLE p_t_qt (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
    );
    await client.query("INSERT INTO p_t_qt VALUES (1, 'O''Brien')");
    const r = await client.query(
      "SELECT name FROM p_t_qt WHERE name = $1",
      ["O'Brien"],
    );
    expect(r.rows.length).toBe(1);
    expect(String(r.rows[0]?.name)).toBe("O'Brien");
    await client.query("DROP TABLE p_t_qt");
  });

  test("bool param round-trip", async () => {
    await client.query(
      "CREATE TABLE p_t_b (id BIGINT PRIMARY KEY, ok BOOLEAN NOT NULL)",
    );
    await client.query("INSERT INTO p_t_b VALUES (1, TRUE), (2, FALSE)");
    const r = await client.query("SELECT ok FROM p_t_b WHERE ok = $1", [true]);
    expect(r.rows.length).toBe(1);
    expect(r.rows[0]?.ok).toBe(true);
    await client.query("DROP TABLE p_t_b");
  });

  test("named prepared statement reused across queries", async () => {
    await client.query(
      "CREATE TABLE p_named (id BIGINT PRIMARY KEY, qty INT NOT NULL)",
    );
    await client.query("INSERT INTO p_named VALUES (1, 1), (2, 2), (3, 3)");
    const cfg = { name: "p_named_lookup", text: "SELECT id FROM p_named WHERE qty = $1" };
    const r1 = await client.query({ ...cfg, values: [1] });
    expect(r1.rows.length).toBe(1);
    expect(String(r1.rows[0]?.id)).toBe("1");
    const r2 = await client.query({ ...cfg, values: [3] });
    expect(r2.rows.length).toBe(1);
    expect(String(r2.rows[0]?.id)).toBe("3");
    await client.query("DROP TABLE p_named");
  });

  test("wrong argument count surfaces as a driver error", async () => {
    await client.query("CREATE TABLE p_wrong (id BIGINT PRIMARY KEY)");
    let saw_err = false;
    try {
      // Two placeholders but only one bound value — node-pg validates
      // and rejects before the bytes go on the wire on some versions;
      // on others the server replies with an error. Either way the
      // promise rejects.
      await client.query("SELECT * FROM p_wrong WHERE id = $1 OR id = $2", [1]);
    } catch {
      saw_err = true;
    }
    expect(saw_err).toBe(true);
    await client.query("DROP TABLE p_wrong");
  });
});
