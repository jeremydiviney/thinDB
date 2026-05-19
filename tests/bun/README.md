# thinDB Bun + TypeScript wire tests

A standalone integration suite that drives thinDB's MySQL and Postgres
wire listeners using the real npm-package drivers (`mysql2` and `pg`).
It runs against a freshly-spawned `thindb-server` process on
ephemeral ports.

## Prerequisites

1. **Build the server first.** From the repo root:

   ```
   zig build
   ```

   This produces `zig-out/bin/thindb-server[.exe]`. The helpers locate
   the binary relative to the repo root and refuse to run if it's
   missing.

2. **Bun 1.3+**. The suite uses Bun's built-in test runner
   (`bun:test`) and `bun install` for the driver dependencies.

## Install + run

```
cd tests/bun
bun install
bun test
```

Filter to one wire:

```
bun test mysql
bun test pg
```

Each test file spawns its own server on three free ephemeral ports,
talks to it via the driver, then tears down the server and removes its
temp data directory.

## What's covered

### Postgres (`pg/`) — fully driver-verified

| File              | Asserts                                                                                       |
|-------------------|-----------------------------------------------------------------------------------------------|
| `basic.test.ts`   | Connect, `SELECT 1` (canned `?column?`), `SELECT version()`, `SELECT current_database()`.     |
| `namespace.test.ts` | `pg_catalog.pg_database`, `CREATE DATABASE`, `pg_namespace`, `CREATE SCHEMA`, `SHOW server_version`, `SET search_path`. |
| `errors.test.ts`  | Unknown table → `42P01`, syntax error → `42000`, `DROP DATABASE no_such_db` → `3D000`.        |
| `table.test.ts`   | Empty `pg_class` listing round-trips; `CREATE TABLE` / `INSERT` are `test.todo` (parser gap). |
| `types.test.ts`   | All `test.todo` (depends on INSERT via SQL).                                                  |

### MySQL (`mysql/`) — handshake only

The `mysql2` driver and the thinDB MySQL wire are **not compatible**
at the COM_QUERY result-set level right now — see the **Known gaps**
section below. The MySQL suite verifies what *is* reachable
(handshake, COM_INIT_DB, ERR_Packet at init-db time) and uses
`test.todo` for everything that requires result-set traffic.

| File                | Asserts                                                                                |
|---------------------|----------------------------------------------------------------------------------------|
| `basic.test.ts`     | Listener accepts connections; handshake + COM_INIT_DB succeed for `main__public`; init-db with a bogus DB yields ER_BAD_DB_ERROR (1049). |
| `namespace.test.ts` | COM_INIT_DB resolves the flattened `db__schema` form; SHOW DATABASES variants are `test.todo`. |
| `errors.test.ts`    | Unknown initial DB → `1049` / `42000`. Query-time errors are `test.todo`.              |
| `table.test.ts`     | All `test.todo` (parser + wire gaps).                                                  |
| `types.test.ts`     | All `test.todo`.                                                                       |

## Known gaps surfaced by this suite

These are real product issues to fix before the MySQL surface is
useful from off-the-shelf drivers:

1. **MySQL result-set format ignores client capabilities.** The
   server unconditionally writes result sets in `CLIENT_DEPRECATE_EOF`
   form (no EOF packet between column defs and rows, single OK-shaped
   EOF after rows). `mysql2` never advertises `DEPRECATE_EOF` — it
   defaults to the older format with two EOF packets — so it errors
   with `PROTOCOL_UNEXPECTED_PACKET` on the very first SELECT and
   tears the connection down. Fix would be either honoring the
   negotiated capability bits on the server side, or advertising
   `DEPRECATE_EOF` from the server and emitting two EOF packets
   when the client doesn't set it.

2. **MySQL empty initial-DB is treated as a missing DB.** `mysql2`
   always sets `CLIENT_CONNECT_WITH_DB` and sends an empty string
   when no `database` is configured. The server's `applyInitDb`
   tries to resolve the empty string as a real lookup and returns
   `DatabaseNotFound`. Should be a no-op (leave session at the
   default `main/public`).

3. **No CREATE TABLE / INSERT via SQL.** The parser supports
   CREATE/DROP DATABASE | SCHEMA, USE, SHOW DATABASES/SCHEMAS/TABLES,
   and SELECT. Until INSERT and CREATE TABLE are added, type
   round-trip tests over the wire have nothing to seed against.

4. **MySQL canned matcher doesn't recognize bare `SELECT 1`.** Only
   `@@version`, `@@version_comment`, `SELECT DATABASE()`, etc. are
   matched. Plain expression selects fall through to the parser,
   which doesn't (yet) accept bare expressions. The PG canned
   matcher *does* recognize `select 1` and the PG suite asserts that.

## Layout

```
helpers/
  paths.ts    Resolves the server binary cross-platform; tmp-dir helper.
  ports.ts    Picks free TCP ports via Node's `net` module.
  server.ts   Spawns thindb-server, waits for the "listening on …"
              startup banner, drains stdout/stderr in the background,
              exposes a `close()` for teardown.
mysql/        MySQL-wire tests (see table above).
pg/           Postgres-wire tests (see table above).
package.json  Driver + types deps.
tsconfig.json Strict TS, ESM, Bun + Node types.
bun.lock      Committed lockfile (text format; Bun 1.3+).
```

## Notes for maintainers

- Each test that needs an open connection opens AND closes it within
  the test body. The MySQL listener is single-threaded per wire, so
  any leftover connection prevents the next test from connecting.
- Tests always bind to `127.0.0.1`. The startup helper rejects
  `0.0.0.0` exposure by default to keep the surface local-only.
- The Bun runner is the only framework here — no Jest/Vitest.
