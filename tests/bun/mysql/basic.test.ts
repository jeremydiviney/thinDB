import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import mysql, { type Connection } from "mysql2/promise";
import { startServer, type ServerHandle } from "../helpers/server.ts";
import { todo } from "../helpers/todo.ts";

// IMPORTANT: thinDB's MySQL wire is currently incompatible with the
// `mysql2` npm driver for COM_QUERY result-set traffic. The server
// unconditionally emits result-sets in the CLIENT_DEPRECATE_EOF format
// (no intermediate EOF packet between column defs and rows), but
// `mysql2` does not advertise that capability and always expects the
// older format. The first SELECT therefore trips
// `PROTOCOL_UNEXPECTED_PACKET` and the connection is torn down.
//
// Until the server honors the negotiated capability flags, the only
// thing this Bun suite can verify over `mysql2` is HANDSHAKE +
// COM_INIT_DB. We assert that here and keep the query-shaped tests as
// `test.todo` so the gap is visible in the report.
//
// Each test below opens and closes its own connection, because the
// MySQL listener is single-threaded — one in-flight connection blocks
// new accepts.

describe("mysql basic — handshake", () => {
  let server: ServerHandle;

  beforeAll(async () => {
    server = await startServer({ label: "mysql-basic" });
  });

  afterAll(async () => {
    if (server !== undefined) await server.close();
  });

  test("listens on a non-zero port", () => {
    expect(server.ports.mysql).toBeGreaterThan(0);
  });

  test("handshake + COM_INIT_DB to main__public succeeds", async () => {
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

  // mysql2 always sets the CONNECT_WITH_DB capability and sends a
  // database name (empty string if no `database` was configured). The
  // server then tries to resolve that empty name as a real DB/schema
  // and returns ER_BAD_DB_ERROR, so an "omit database" test is not
  // representable via mysql2. Skipped for that reason.
  todo("handshake with no initial DB (mysql2 always sets CONNECT_WITH_DB)");

  test("handshake to nonexistent db fails with ER_BAD_DB_ERROR (1049)", async () => {
    let code: string | undefined;
    let errno: number | undefined;
    try {
      const conn = await mysql.createConnection({
        host: server.bind,
        port: server.ports.mysql,
        user: "thindb",
        password: "",
        database: "no__such_db",
      });
      await conn.end().catch(() => undefined);
    } catch (err) {
      // mysql2 attaches code/errno to the auth/init-db error.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const e = err as any;
      code = e.code;
      errno = e.errno;
    }
    expect(code).toBe("ER_BAD_DB_ERROR");
    expect(errno).toBe(1049);
  });

  // Tests below all hit COM_QUERY and currently fail at the wire
  // (mysql2 ↔ server flag mismatch). Reinstate as real tests once the
  // server is updated to honor the client's CLIENT_DEPRECATE_EOF flag.
  todo("SELECT @@version returns the server marker (blocked: DEPRECATE_EOF mismatch)");
  todo("SELECT @@version_comment returns 'thinDB' (blocked: DEPRECATE_EOF mismatch)");
  todo("SELECT 1 (parser does not support bare exprs; canned matcher only handles @@vars)");
});
