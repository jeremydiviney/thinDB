import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { createPool, type Pool } from "mysql2/promise";
import { startServer, type ServerHandle } from "../helpers/server.ts";

describe("mysql pool", () => {
  let server: ServerHandle;
  let pool: Pool;

  beforeAll(async () => {
    server = await startServer({ label: "mysql-pool" });
    pool = createPool({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
      connectionLimit: 4,
    });
  });

  afterAll(async () => {
    if (pool !== undefined) await pool.end().catch(() => undefined);
    if (server !== undefined) await server.close();
  });

  test("8 parallel queries succeed across a 4-conn pool", async () => {
    const queries = Array.from({ length: 8 }, () =>
      pool.query("SELECT @@version_comment"),
    );
    const results = (await Promise.all(queries)) as Array<
      [Array<Record<string, string>>, unknown]
    >;
    expect(results.length).toBe(8);
    for (const [rows] of results) {
      expect(rows.length).toBe(1);
      expect(rows[0]?.["@@version_comment"]).toBe("thinDB");
    }
  });

  test("RESET CONNECTION between pool borrows is silently accepted", async () => {
    const [rows] = (await pool.query("RESET CONNECTION")) as [
      Array<Record<string, unknown>>,
      unknown,
    ];
    expect(rows).toBeDefined();
  });
});
