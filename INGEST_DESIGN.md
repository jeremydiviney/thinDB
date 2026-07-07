# Flink → thinDB ingest (#132)

Two-stage plan, **both SHIPPED and validated with real Flink** (2026-07-07).

## Status — COMPLETE

- **Stage 1 (JDBC): done.** Snapshot + CDC (insert/update/delete) via
  flink-connector-jdbc / mysql-cdc, effectively-once for PK tables. Proven with a
  real MySQL-CDC → Flink → thinDB pipeline (`tests/flink/cdc_job.sql`).
- **Stage 2 (exactly-once via XA): done.** `XA START/END/PREPARE/COMMIT/ROLLBACK/
  RECOVER` on the MySQL wire; durable prepared branches survive a crash; proven
  end to end with the **real Connector/J `XAResource`** including kill-and-recover
  (`tests/flink/xa-java`) and a 1M-row `JdbcSink.exactlyOnceSink` run
  (`tests/flink/ingest-bench`). Orphan branches are GC'd (`--xa-timeout-secs`);
  failures map to real MySQL XA error codes.
- **Throughput (1M rows, parallelism 2):** ~140K rows/s both modes after the
  incremental-upsert-index fix (d390e2c); exactly-once ≈ at-least-once (2PC
  overhead negligible). Bottleneck is now off thinDB's insert path.

Recipe for the exactly-once DataStream sink is at the bottom of this doc.

---

## Delivery-guarantee model (why the stages exist)

Guarantees are only about **failure + replay**: Flink rewinds to its last
checkpoint and re-sends records. What that does to the sink:

- **at-least-once** — records applied ≥ once (possible duplicates).
- **exactly-once** — effect applied once, even across replays.
- **effectively-once** — at-least-once delivery + an **idempotent** apply.

thinDB fact (confirmed): **every table has an order key; `PRIMARY KEY` → unique →
INSERT upserts last-writer-wins; `ORDER BY` → non-unique → INSERT appends.**
So:

| Table model | Stream | Transport | Guarantee |
|---|---|---|---|
| PK/unique | CDC upsert+delete | JDBC | **effectively-once, FREE** |
| Keyless (`ORDER BY`) | append-only | JDBC | at-least-once (dupes possible) |
| Keyless | append-only, no dupes | Stream Load + 2PC | exactly-once |

Upsert is idempotent → replay overwrites the same row. Append is NOT → replay
double-counts. Only keyless append streams need true exactly-once.

---

## Stage 1 — JDBC (server-complete)

**Validated over the wire (mysql2, all green):**
- Plain INSERT upserts on unique tables (StarRocks last-writer-wins).
- `INSERT … ON DUPLICATE KEY UPDATE col=VALUES(col)` (441fadb) — single-row AND
  multi-row (`rewriteBatchedStatements=true`) forms; accumulation forms rejected.
- Prepared statements (COM_STMT_PREPARE/EXECUTE) with bound params: parameterized
  upsert batch + `DELETE WHERE pk=?` (CDC deletes).
- Connector/J session-init probes: `@@version_comment`, `@@session.auto_increment_increment`,
  `SET NAMES`, `SET character_set_results`, `@@sql_mode`, `SHOW VARIABLES LIKE`.

**Remaining to ship:** a live Flink run (real Connector/J), a recipe, docs.
Guarantee: **effectively-once for PK tables, at-least-once for keyless.** Flink's
own exactly-once JDBC sink is XA-based; thinDB has no XA, so true exactly-once is
NOT available over plain JDBC (see Stage 2).

### Flink recipe (JDBC)
```sql
-- thinDB target: CREATE TABLE dim (id INT, val INT, name STRING, PRIMARY KEY(id));
CREATE TABLE thindb_dim (
  id INT, val INT, name STRING,
  PRIMARY KEY (id) NOT ENFORCED           -- puts the connector in UPSERT mode
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://host.docker.internal:3306/main',
  'table-name' = 'dim',
  'username' = 'root', 'password' = '',
  'sink.buffer-flush.max-rows' = '1000',
  'sink.buffer-flush.interval' = '1s'
);
INSERT INTO thindb_dim SELECT ... ;         -- upsert stream → ON DUPLICATE KEY UPDATE
```
`host.docker.internal` = thinDB on the Windows host from Flink-in-Docker.

---

## Stage 2 — Exactly-once over JDBC via XA (DECISION 2026-07-07)

**We go JDBC-only. HTTP Stream Load is dropped** (it was only ever buying bulk
throughput + StarRocks-connector drop-in — neither is a functional need). Flink's
JDBC connector does exactly-once via **XA transactions**; Connector/J's XA path
issues `XA START/END/PREPARE/COMMIT/ROLLBACK/RECOVER` as **SQL statements on the
MySQL wire**. So thinDB implements those and Flink's `sink.semantic=exactly-once`
JDBC sink works — no HTTP, testable with mysql2 on Windows (final proof needs
real Flink kill-and-replay in Docker).

The hard part is the same either way: the **durable 2PC core**. XA is just a thin
SQL skin on it.

### XA flow (what Connector/J does per Flink checkpoint)
```
XA START 'xid'   → begin branch, bind to this connection
INSERT ...       → rows STAGED in the branch (not applied)
XA END 'xid'     → detach branch from connection
XA PREPARE 'xid' → make staged data durable + invisible (crash-safe)
XA COMMIT 'xid'  → atomically make visible + record xid (idempotent: re-commit = no-op)
XA ROLLBACK 'xid'→ discard
XA RECOVER       → list prepared-but-uncommitted xids (recovery after restart)
```
Crash between PREPARE and COMMIT: on restart Flink does `XA RECOVER`, finds the
xid, re-`COMMIT`s (idempotent). Crash before PREPARE: staging lost, Flink
reproduces under the next checkpoint. xid = opaque dedup key.

### Code seams (mapped)
- **Session** (api.zig:332) is copied by value; connection-persistent state lives
  behind a Connection-owned pointer (see `vars: ?*SessionVars`). Add
  `xa: ?*XaConnState` the same way → tracks this connection's active xid.
- **XaManager** (new module, Catalog- or Database-level): branches keyed by xid →
  { state ACTIVE|IDLE|PREPARED, staged rows per target table }. Prepared branches
  are global (survive connection close + restart). Single writer thread serializes
  commits → no lock manager needed.
- **Parser**: `XA <cmd> ['xid']` → new IR op (contextual like REFRESH; `XA` lexes
  as identifier). 
- **Dispatch** (local.zig compileOp @1830): handle the XA op → XaManager calls
  using the session's connection state.
- **Staging hook** (local.zig compileInsert @1855): if the connection has an
  active xid → `XaManager.stage(xid, table, rows)` instead of `insertBatch`.

### Build slices — ALL SHIPPED
As built, `src/net/xa.zig` holds the XaManager; the wire hooks live in
`src/net/mysql/server.zig` (`handleXaCommand` + the DML-staging checks on both
the COM_QUERY and COM_STMT_EXECUTE compile sites), not `compileInsert`.
- **S1 (a12ff13):** XA parse + `SessionState.xa_active` + XaManager
  (begin/stage/end/prepare/commit/rollback/recover). Staged DML buffered as
  encoded IR; idempotent commit.
- **S2 (0749ccd):** prepared branches persisted to `<root>/_xa/<hex-xid>.xa`
  (xid + db + staged IR), reloaded on open; COMMIT/ROLLBACK unlink. Crash between
  PREPARE and COMMIT survives.
- **RECOVER (b818dfa):** MySQL 4-column result (formatID, gtrid_length,
  bqual_length, data); `parseXid` decomposes Connector/J's `0x`-hex xid form.
- **GC (f28541a):** background sweep rolls back orphaned prepared branches older
  than `--xa-timeout-secs` (default 24h).
- **Error codes (f78e6a9):** XAER_DUPID / XAER_NOTA / XAER_RMFAIL.
- **Proof:** `tests/flink/xa-java` (real Connector/J XAResource, incl. prepare →
  kill → restart → recover → commit) + `tests/flink/ingest-bench` (1M-row
  `JdbcSink.exactlyOnceSink`, 1M exact).

### Deferred (not building): HTTP Stream Load
Kept here only as the future throughput/bulk option. Same 2PC core, HTTP skin
(`/api/transaction/{begin,prepare,commit,rollback}` + `_stream_load`). Revisit
only if JDBC row-batch ingest throughput becomes a bottleneck.

---

## (Deferred) HTTP Stream Load reference

### 2a. Stream Load endpoint (bulk, effectively-once)
New HTTP listener alongside the mysql/pg/native ones.
- **Seams:** `cmd/server.zig` `Listener` union + `WireSpec` loop (add
  `--stream-load-port`, a `serveStreamLoad(gpa, io, catalog, addr, limiter)`);
  model the accept loop on `net/tcp_server.zig` (`IpAddress.listen` →
  `listener.accept(io)` → per-conn thread; `stream.reader(io,&buf)` /
  `stream.writer(io,&buf)`; `readSliceAll` for the body).
- **Endpoint:** `PUT /api/{db}/{table}/_stream_load`, headers `label`, `format`
  (csv|json), `column_separator`, `columns`, `Content-Length`, `Expect:
  100-continue` (must reply `100 Continue` before reading body).
- **Body → rows:** parse CSV/JSON → typed columnar batch (per table schema) →
  `table.insertBatch` (the fast bulk path; a first slice can build an INSERT and
  run `compileWithSession` for correctness, then swap to insertBatch).
- **Response:** StarRocks JSON `{"Status":"Success"|"Label Already Exists"|"Fail",
  "Label":…, "NumberLoadedRows":N, "NumberFilteredRows":…, "Message":…}`.
- **Label dedup:** in-memory set first; then durable (2b).
- **Testable on Windows with `curl -X PUT`** — no Docker needed for the endpoint.

### 2b. Durable exactly-once (the 2PC core)
Only needed for keyless append that can't tolerate dupes. One core primitive,
two possible skins.

**Core (storage/writer layer):**
- `begin(label)` → staging buffer (spillable, per larger-than-RAM rule).
- `append(label, batch)` → buffer into staging (invisible to readers).
- `prepare(label)` → flush staging to durable **staging-segments** (reuse the
  segment writer; fsync) + record the prepared txn durably. After prepare, commit
  is guaranteed to succeed (crash-safe).
- `commit(label)` → **atomic manifest swap**: add staging-segments to the live
  manifest + mark label committed, in one step. Idempotent (re-commit = no-op).
- `rollback(label)` / timeout → discard staging.
- **Recovery on open:** prepared-but-uncommitted txns held for re-commit;
  committed labels dedup retries; orphaned staging GC'd.
- **Label store:** small — only the last 1–2 labels per (job, subtask) (Flink
  replays only from the last checkpoint). Persisted in a manifest sidecar.
- The **single writer thread serializes commits** → no concurrent-commit races.

**Skins (same core):**
- **XA on the MySQL wire** (`XA START/END/PREPARE/COMMIT/ROLLBACK/RECOVER`, xid =
  dedup key) → Flink's existing JDBC exactly-once sink works, stays 100% on JDBC.
- **Transaction Stream Load HTTP** (`/api/transaction/{begin,prepare,commit,rollback}`)
  → flink-connector-starrocks exactly-once + bulk throughput.

Recommendation: XA first (no HTTP, minimal surface, reuses the wire) → add the
Stream Load skin when throughput demands it.

---

## Build order
1. **Stage 1 sign-off:** Flink-in-Docker JDBC run + recipe/docs. *(needs Docker started)*
2. **Stage 2a:** HTTP listener + `_stream_load` + CSV/JSON→insertBatch + StarRocks
   JSON + in-memory label dedup. curl-tested on Windows.
3. **Stage 2b core:** begin/append/prepare/commit/abort + durable staging + label
   store + recovery. Zig unit tests + curl.
4. **Exactly-once skin:** XA commands (mysql2-testable) and/or transaction Stream
   Load; kill-and-replay proof with real Flink in Docker.

## Testing on Windows
- Endpoint mechanics: `curl` (Stream Load) / `mysql2` (JDBC, XA) — no Docker.
- Real Flink end-to-end (checkpoint/recovery, exactly-once kill test): **Flink in
  Docker Desktop** → thinDB on host via `host.docker.internal`. thinDB stays on
  Windows; only Flink needs the container.

---

## Recipe — Flink exactly-once JDBC sink → thinDB

thinDB speaks MySQL XA, so Flink's `JdbcSink.exactlyOnceSink` works unchanged
(DataStream API; the Table/SQL `jdbc` connector is at-least-once only). Full
runnable example: `tests/flink/ingest-bench/`.

```java
env.enableCheckpointing(5000); // exactly-once commits fire on checkpoint

stream.addSink(JdbcSink.exactlyOnceSink(
    "INSERT INTO events (id,a,b) VALUES (?,?,?)", // thinDB upserts on PK
    (ps, r) -> { ps.setInt(1, r.id); ps.setInt(2, r.a); ps.setLong(3, r.b); },
    JdbcExecutionOptions.builder().withBatchSize(1000).withMaxRetries(0).build(),
    JdbcExactlyOnceOptions.builder()
        .withTransactionPerConnection(true) // REQUIRED for MySQL XA
        .build(),
    () -> {                                  // XADataSource supplier
        MysqlXADataSource ds = new MysqlXADataSource();
        ds.setUrl("jdbc:mysql://<host>:3306/main?rewriteBatchedStatements=true");
        ds.setUser("root"); ds.setPassword("");
        return ds;
    }));
```

Gotchas, all load-bearing:
- **`withTransactionPerConnection(true)`** — MySQL XA disallows multiple branches
  per connection; without it the sink errors.
- **`withMaxRetries(0)`** — XA requires it (retries + 2PC don't mix).
- **`rewriteBatchedStatements=true`** — collapses each batch into one multi-row
  INSERT; without it every row is a separate round-trip (~30× slower).
- **`--xa-timeout-secs`** on the server must exceed your checkpoint interval + max
  tolerable downtime, or a slowly-recovering job's prepared branch gets GC'd.
- For at-least-once (effectively-once on PK tables) use plain `JdbcSink.sink(...)`
  with `JdbcConnectionOptions` — no XA, simpler, same `rewriteBatchedStatements`.
