import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Client } from "pg";
import { startServer, type ServerHandle } from "../helpers/server.ts";

// Connection-cancel coverage via the real pg driver. Uses
// pg_cancel_backend(pid) directly — it routes to the same registry
// that PG's wire-level CancelRequest frame would hit.

describe("pg cancel", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "pg-cancel" });
  });

  afterAll(async () => {
    if (server !== undefined) await server.close();
  });

  test("pg_cancel_backend(<unknown>) returns false", async () => {
    const c = new Client({
      host: server.bind,
      port: server.ports.pg,
      user: "thindb",
      password: "anything",
      database: "main",
    });
    await c.connect();
    try {
      const r = await c.query("SELECT pg_cancel_backend(99999)");
      expect(r.rows.length).toBe(1);
      // pg returns booleans as JS boolean false when parsed.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect((r.rows[0] as any).pg_cancel_backend).toBe(false);
    } finally {
      await c.end();
    }
  });

  test("pg_terminate_backend(<unknown>) routes to the same path", async () => {
    const c = new Client({
      host: server.bind,
      port: server.ports.pg,
      user: "thindb",
      password: "anything",
      database: "main",
    });
    await c.connect();
    try {
      const r = await c.query("SELECT pg_terminate_backend(99999)");
      expect(r.rows.length).toBe(1);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect((r.rows[0] as any).pg_cancel_backend).toBe(false);
    } finally {
      await c.end();
    }
  });
});
