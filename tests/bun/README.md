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
| `pool.test.ts`    | `pg.Pool` with `max=4` runs 8 parallel queries; `DISCARD ALL` and `RESET ALL` round-trip.     |
| `table.test.ts`   | Empty `pg_class` listing round-trips; `CREATE TABLE` / `INSERT` are `test.todo` (parser gap). |
| `types.test.ts`   | All `test.todo` (depends on INSERT via SQL).                                                  |

### MySQL (`mysql/`) — driver-verified

| File                | Asserts                                                                                |
|---------------------|----------------------------------------------------------------------------------------|
| `basic.test.ts`     | Handshake; empty initial DB stays at `main/public`; init-db with a bogus DB → `ER_BAD_DB_ERROR` (1049); `SELECT @@version`, `SELECT @@version_comment`, bare `SELECT 1`. |
| `namespace.test.ts` | Flattened `db__schema` init-db; `SHOW DATABASES`; `CREATE DATABASE` then `SHOW DATABASES`; `DROP DATABASE` removes the entry. |
| `errors.test.ts`    | Unknown initial DB → `1049` / `42000`; missing table → `1146` / `42S02`; syntax error → `1064` / `42000`. |
| `pool.test.ts`      | `mysql2.createPool` with `connectionLimit=4` runs 8 parallel `SELECT`s; `RESET CONNECTION` round-trips. |
| `table.test.ts`     | All `test.todo` (parser gap — no CREATE TABLE / INSERT via SQL).                       |
| `types.test.ts`     | All `test.todo`.                                                                       |

## Known gaps surfaced by this suite

These remain after the v2 round of fixes:

1. **No CREATE TABLE / INSERT via SQL.** The parser supports
   CREATE/DROP DATABASE | SCHEMA, USE, SHOW DATABASES/SCHEMAS/TABLES,
   and SELECT. Until INSERT and CREATE TABLE are added, type
   round-trip tests over the wire have nothing to seed against.
2. **Windows: keepalive + idle-timeout are no-ops.** On Linux/macOS,
   `SO_KEEPALIVE` is set on every accepted socket and
   `SO_RCVTIMEO` enforces `--idle-timeout-secs`. On Windows the
   `Io.net` sockets are AFD-backed NT handles that `ws2_32!setsockopt`
   rejects, so the helpers silently no-op. Documented in
   `src/net/sock_opts.zig`.

## Fixed since the v1 audit

- MySQL result-set format now honors the client's `CLIENT_DEPRECATE_EOF`
  bit — legacy clients (`mysql2`) get the two-EOF terminator they
  expect.
- MySQL empty initial-DB is treated as a no-op (session stays at the
  default `main/public`).
- MySQL canned matcher now answers bare `SELECT 1`.
- Pool-friendly probes added: MySQL `RESET CONNECTION` and PG
  `DISCARD TEMP` / `DISCARD PLANS` / `RESET ALL`.

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
