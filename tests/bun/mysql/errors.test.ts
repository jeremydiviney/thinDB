import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import mysql from "mysql2/promise";
import { startServer, type ServerHandle } from "../helpers/server.ts";
import { todo } from "../helpers/todo.ts";

// Error packets at HANDSHAKE / COM_INIT_DB time are reachable —
// the server sends a bare ERR_Packet and mysql2 surfaces the
// code/errno/sqlState verbatim. Errors that fire mid-result-set
// (e.g. for a COM_QUERY that fails to parse) are NOT testable here
// because mysql2's result-set state machine has already tripped
// over the DEPRECATE_EOF mismatch — see mysql/basic.test.ts.

describe("mysql errors — handshake-path", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "mysql-err" });
  });

  afterAll(async () => {
    if (server !== undefined) await server.close();
  });

  test("nonexistent database in init-db → ER_BAD_DB_ERROR 1049 / 42000", async () => {
    let code: string | undefined;
    let errno: number | undefined;
    let sqlState: string | undefined;
    try {
      const conn = await mysql.createConnection({
        host: server.bind,
        port: server.ports.mysql,
        user: "thindb",
        password: "",
        database: "definitely__missing",
      });
      await conn.end().catch(() => undefined);
    } catch (err) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const e = err as any;
      code = e.code;
      errno = e.errno;
      sqlState = e.sqlState;
    }
    expect(code).toBe("ER_BAD_DB_ERROR");
    expect(errno).toBe(1049);
    expect(sqlState).toBe("42000");
  });

  todo("SELECT from nonexistent table → 1146 / 42S02 (blocked: wire mismatch)");
  todo("syntax error → 1064 / 42000 (blocked: wire mismatch)");
});
