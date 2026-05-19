import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Client } from "pg";
import { startServer, type ServerHandle } from "../helpers/server.ts";
import { todo } from "../helpers/todo.ts";

describe("pg table", () => {
  let server: ServerHandle;
  let client: Client;

  beforeAll(async () => {
    server = await startServer({ label: "pg-table" });
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

  // CREATE TABLE / INSERT are not (yet) supported via the SQL parser
  // (CREATE/DROP DATABASE/SCHEMA, USE, SHOW DATABASES/SCHEMAS/TABLES,
  // and SELECT … FROM <table> are the v1 surface). Once the parser
  // gains DDL/DML, expand these.
  todo("CREATE TABLE via SQL (parser does not support yet)");
  todo("INSERT INTO via SQL (parser does not support yet)");
  todo("SELECT round-trips inserted rows (depends on INSERT)");

  test("pg_class listing works on empty catalog", async () => {
    // Server intercepts any FROM pg_class probe with a table listing.
    // With no user tables we get an empty result, but the round-trip
    // still succeeds — that's what we assert.
    const r = await client.query("SELECT relname AS \"Name\" FROM pg_class");
    expect(Array.isArray(r.rows)).toBe(true);
  });
});
