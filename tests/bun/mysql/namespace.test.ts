import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import mysql, { type Connection } from "mysql2/promise";
import { startServer, type ServerHandle } from "../helpers/server.ts";

describe("mysql namespace", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "mysql-ns" });
  });

  afterAll(async () => {
    if (server !== undefined) await server.close();
  });

  test("USE-style initial DB resolves db__schema flattened name", async () => {
    const conn: Connection = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      expect(conn).toBeDefined();
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("SHOW DATABASES lists main__public", async () => {
    const conn = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      const [rows] = (await conn.query("SHOW DATABASES")) as [
        Array<Record<string, string>>,
        unknown,
      ];
      const names = rows.map((r) => r.Database);
      expect(names).toContain("main__public");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("CREATE DATABASE then SHOW DATABASES picks up the new entry", async () => {
    const conn = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      await conn.query("CREATE DATABASE analytics_ns");
      const [rows] = (await conn.query("SHOW DATABASES")) as [
        Array<Record<string, string>>,
        unknown,
      ];
      const names = rows.map((r) => r.Database);
      expect(names).toContain("analytics_ns__public");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });

  test("DROP DATABASE removes the entry from SHOW DATABASES", async () => {
    const conn = await mysql.createConnection({
      host: server.bind,
      port: server.ports.mysql,
      user: "thindb",
      password: "",
      database: "main__public",
    });
    try {
      await conn.query("CREATE DATABASE droppable_ns");
      await conn.query("DROP DATABASE droppable_ns");
      const [rows] = (await conn.query("SHOW DATABASES")) as [
        Array<Record<string, string>>,
        unknown,
      ];
      const names = rows.map((r) => r.Database);
      expect(names).not.toContain("droppable_ns__public");
    } finally {
      await conn.end().catch(() => undefined);
    }
  });
});
