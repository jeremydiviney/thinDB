import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Pool } from "pg";
import { startServer, type ServerHandle } from "../helpers/server.ts";

describe("pg pool", () => {
  let server: ServerHandle;
  let pool: Pool;

  beforeAll(async () => {
    server = await startServer({ label: "pg-pool" });
    pool = new Pool({
      host: server.bind,
      port: server.ports.pg,
      user: "thindb",
      password: "x",
      database: "main",
      max: 4,
    });
  });

  afterAll(async () => {
    if (pool !== undefined) await pool.end().catch(() => undefined);
    if (server !== undefined) await server.close();
  });

  test("8 parallel queries succeed across a 4-conn pool", async () => {
    const queries = Array.from({ length: 8 }, () =>
      pool.query("SELECT version()"),
    );
    const results = await Promise.all(queries);
    expect(results.length).toBe(8);
    for (const r of results) {
      expect(r.rows.length).toBe(1);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect(String((r.rows[0] as any).version)).toContain("thinDB");
    }
  });

  test("DISCARD ALL between pool borrows returns CommandComplete", async () => {
    const r = await pool.query("DISCARD ALL");
    expect(r.command).toBe("DISCARD");
  });

  test("RESET ALL is silently accepted", async () => {
    const r = await pool.query("RESET ALL");
    expect(r.command).toBe("RESET");
  });
});
