# Flink → thinDB ingest (#132)

Two-stage plan. Stage 1 (JDBC) is **server-complete and wire-validated**; Stage 2
(HTTP Stream Load, then exactly-once) is designed with the code seams mapped.

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

## Stage 2 — HTTP Stream Load (bulk) + exactly-once

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
