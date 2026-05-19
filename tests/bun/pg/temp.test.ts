import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Client } from "pg";
import { startServer, type ServerHandle } from "../helpers/server.ts";

// PG temp tables are session-scoped. DISCARD ALL / DISCARD TEMP drops them
// explicitly; RESET ALL does NOT (per PG spec — RESET resets GUCs, not temps).

describe("pg temp tables", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "pg-temp" });
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

  test("CREATE TEMP TABLE + INSERT + SELECT round-trip", async () => {
    const client = makeClient();
    await client.connect();
    try {
      await client.query("CREATE TEMP TABLE scratch (id BIGINT PRIMARY KEY, qty INT NOT NULL)");
      await client.query("INSERT INTO scratch VALUES (1, 10), (2, 20)");
      const r = await client.query("SELECT id, qty FROM scratch ORDER BY id ASC");
      expect(r.rows.length).toBe(2);
      // pg returns BIGINT as a string by default.
      expect(String(r.rows[0]?.id)).toBe("1");
      expect(Number(r.rows[0]?.qty)).toBe(10);
    } finally {
      await client.end().catch(() => undefined);
    }
  });

  test("TEMPORARY keyword is accepted as a synonym for TEMP", async () => {
    const client = makeClient();
    await client.connect();
    try {
      await client.query("CREATE TEMPORARY TABLE scratch2 (id BIGINT PRIMARY KEY)");
      await client.query("INSERT INTO scratch2 VALUES (42)");
      const r = await client.query("SELECT id FROM scratch2");
      expect(r.rows.length).toBe(1);
      expect(String(r.rows[0]?.id)).toBe("42");
    } finally {
      await client.end().catch(() => undefined);
    }
  });

  test("temp table is invisible to a second connection", async () => {
    const a = makeClient();
    const b = makeClient();
    await a.connect();
    await b.connect();
    try {
      await a.query("CREATE TEMP TABLE only_a (id BIGINT PRIMARY KEY)");
      await a.query("INSERT INTO only_a VALUES (7)");

      let errored = false;
      try {
        await b.query("SELECT id FROM only_a");
      } catch {
        errored = true;
      }
      expect(errored).toBe(true);
    } finally {
      await a.end().catch(() => undefined);
      await b.end().catch(() => undefined);
    }
  });

  test("temp table shadows a persistent table of the same name", async () => {
    const setup = makeClient();
    await setup.connect();
    try {
      await setup.query("CREATE TABLE shadowed (id BIGINT PRIMARY KEY, src VARCHAR(8) NOT NULL)");
      await setup.query("INSERT INTO shadowed VALUES (1, 'real')");
    } finally {
      await setup.end().catch(() => undefined);
    }

    const client = makeClient();
    await client.connect();
    try {
      await client.query("CREATE TEMP TABLE shadowed (id BIGINT PRIMARY KEY, src VARCHAR(8) NOT NULL)");
      await client.query("INSERT INTO shadowed VALUES (2, 'temp')");

      const r = await client.query("SELECT src FROM shadowed");
      expect(r.rows.length).toBe(1);
      expect(String(r.rows[0]?.src)).toBe("temp");

      await client.query("DROP TABLE shadowed");
      const after = await client.query("SELECT src FROM shadowed");
      expect(after.rows.length).toBe(1);
      expect(String(after.rows[0]?.src)).toBe("real");
    } finally {
      await client.end().catch(() => undefined);
    }
  });

  test("DISCARD ALL drops the session's temp tables", async () => {
    const client = makeClient();
    await client.connect();
    try {
      await client.query("CREATE TEMP TABLE ephemeral (id BIGINT PRIMARY KEY)");
      await client.query("INSERT INTO ephemeral VALUES (1)");
      await client.query("DISCARD ALL");

      let errored = false;
      try {
        await client.query("SELECT id FROM ephemeral");
      } catch {
        errored = true;
      }
      expect(errored).toBe(true);
    } finally {
      await client.end().catch(() => undefined);
    }
  });

  test("DISCARD TEMP drops temp tables", async () => {
    const client = makeClient();
    await client.connect();
    try {
      await client.query("CREATE TEMP TABLE ephemeral2 (id BIGINT PRIMARY KEY)");
      await client.query("DISCARD TEMP");

      let errored = false;
      try {
        await client.query("SELECT id FROM ephemeral2");
      } catch {
        errored = true;
      }
      expect(errored).toBe(true);
    } finally {
      await client.end().catch(() => undefined);
    }
  });

  test("RESET ALL does NOT drop temp tables (per PG spec)", async () => {
    const client = makeClient();
    await client.connect();
    try {
      await client.query("CREATE TEMP TABLE survivor (id BIGINT PRIMARY KEY)");
      await client.query("INSERT INTO survivor VALUES (1)");
      await client.query("RESET ALL");

      const r = await client.query("SELECT id FROM survivor");
      expect(r.rows.length).toBe(1);
      expect(String(r.rows[0]?.id)).toBe("1");
    } finally {
      await client.end().catch(() => undefined);
    }
  });

  test("temp table dropped on disconnect — fresh connection can't see it", async () => {
    const a = makeClient();
    await a.connect();
    try {
      await a.query("CREATE TEMP TABLE goes_away (id BIGINT PRIMARY KEY)");
      await a.query("INSERT INTO goes_away VALUES (1)");
    } finally {
      await a.end().catch(() => undefined);
    }

    const b = makeClient();
    await b.connect();
    try {
      let errored = false;
      try {
        await b.query("SELECT id FROM goes_away");
      } catch {
        errored = true;
      }
      expect(errored).toBe(true);
    } finally {
      await b.end().catch(() => undefined);
    }
  });
});
