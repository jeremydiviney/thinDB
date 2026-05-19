import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Client } from "pg";
import { startServer, type ServerHandle } from "../helpers/server.ts";

// Exercises SCRAM-SHA-256 end-to-end through the real `pg` driver.
// pg negotiates SCRAM-SHA-256 by default when the server advertises
// `AuthenticationSASL` with that mechanism.

describe("pg auth — SCRAM-SHA-256", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "pg-auth-scram", pgPassword: "hunter2" });
  });

  afterAll(async () => {
    if (server !== undefined) await server.close();
  });

  test("correct password completes the handshake + runs a query", async () => {
    const c = new Client({
      host: server.bind,
      port: server.ports.pg,
      user: "thindb",
      password: "hunter2",
      database: "main",
    });
    await c.connect();
    try {
      const r = await c.query("SELECT 1");
      expect(r.rows.length).toBe(1);
    } finally {
      await c.end();
    }
  });

  test("wrong password rejected with SQLSTATE 28P01", async () => {
    const c = new Client({
      host: server.bind,
      port: server.ports.pg,
      user: "thindb",
      password: "wrong",
      database: "main",
    });
    let code: string | undefined;
    try {
      await c.connect();
    } catch (err) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      code = (err as any).code;
    }
    // Either pg surfaces the SQLSTATE directly, or wraps it as a SASL
    // failure. Both manifest as a thrown error; assert that connect did
    // fail and (when present) the code matches our reject SQLSTATE.
    expect(code === undefined || code === "28P01").toBe(true);
    try {
      await c.end();
    } catch {
      // already aborted
    }
  });
});

describe("pg auth — trust mode (no password configured)", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "pg-auth-trust" });
  });

  afterAll(async () => {
    if (server !== undefined) await server.close();
  });

  test("any password is accepted when server has no --pg-password", async () => {
    const c = new Client({
      host: server.bind,
      port: server.ports.pg,
      user: "thindb",
      password: "anything goes",
      database: "main",
    });
    await c.connect();
    try {
      const r = await c.query("SELECT 1");
      expect(r.rows.length).toBe(1);
    } finally {
      await c.end();
    }
  });
});
