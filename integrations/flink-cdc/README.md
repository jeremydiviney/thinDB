# thinDB Flink CDC sink

A single Flink DataStream job that streams MySQL change data (binlog CDC) into
thinDB over its MySQL wire protocol. One binlog connection carries every
configured table — replacing N per-table jobs, each of which would otherwise
pull the full binlog firehose independently.

Delivery is **at-least-once with primary-key upserts**, which on thinDB's
last-writer-wins PK tables is effectively-once: replayed rows dedup on key,
replayed deletes are no-ops.

## Why this exists (thinDB-specific tuning)

Generic JDBC sinks work against thinDB but replay multi-day binlog backlogs
at a crawl. Each of the following is load-bearing and was measured against a
30M+-row deployment; together they took a replay from ~220 rows/s to
10-25K rows/s (~84×):

1. **`maxAllowedPacket=67108864` in the sink JDBC URL — required.** thinDB
   returns no row for `SHOW VARIABLES LIKE 'max_allowed_packet'`, so MySQL
   Connector/J falls back to a 65,535-byte default and refuses to send larger
   packets client-side (`PacketTooBigException`). thinDB itself accepts
   multi-megabyte packets fine.

2. **Upserts are emitted as multi-VALUES `INSERT ... ON DUPLICATE KEY UPDATE`
   statements** (5,000 rows per statement). thinDB executes statements on a
   single writer with a per-statement floor of a few milliseconds, so
   per-row statements — including driver-batched multi-statement packets,
   which the server still executes one statement at a time — cap out at a few
   hundred rows/s. One statement carrying thousands of rows amortizes
   parse/plan/commit and runs orders of magnitude faster.

3. **Deletes are emitted as single-row point statements on the full primary
   key** (`WHERE pk1=? AND pk2=? ...`), driver-batched into multi-statement
   packets. Do NOT be tempted to batch deletes into `pk IN (...)` lists or
   OR-chains of key groups: those predicate shapes bypass thinDB's keyed-
   delete fast path (bloom-pruned point deletes with internal chain
   coalescing) and degrade to a full table scan per statement — on a
   33M-row table a single 5,000-key OR-chain DELETE ran for over 30 minutes.

4. **Changelog compaction per flush.** For last-writer-wins PK tables the
   correctness invariant is per-key order, not global binlog order: within a
   flush window only the last operation per (table, key) determines the row's
   final state. The buffer compacts to that final image before applying, so
   regeneration bursts that rewrite the same keys many times shrink to one
   write per key.

5. **Flush on checkpoint.** The sink flushes on size, interval, AND on the
   checkpoint barrier (`snapshotState`), so a completed Flink checkpoint
   implies everything before it is durably in thinDB.

## Build

Requires JDK 17 + Maven, or use Docker:

```
docker run --rm -v "$(pwd):/app" -v m2cache:/root/.m2 -w /app \
  maven:3.9-eclipse-temurin-17 mvn -q -DskipTests package
```

Produces `target/thindb-flink-cdc.jar` (Jackson shaded in; Flink, the CDC
connector, and the MySQL driver are `provided`).

## Run

The Flink cluster's `lib/` must contain (versions in `pom.xml`):

- `flink-sql-connector-mysql-cdc-3.6.0-1.20.jar`
- `mysql-connector-j-8.4.0.jar`

Then:

```
flink run -d -c dev.thindb.cdc.ConsolidatedCdcJob thindb-flink-cdc.jar /path/to/config.json
```

The config path can also be passed via the `CDC_CONFIG` environment variable.
See `example-config.json` for the shape.

## Config reference

**`source`** — the MySQL side. `serverId` is a range (one id per source
subtask; must not collide with other binlog readers on the same server).
`startupMode` is one of:

- `initial` — snapshot existing rows, then stream the binlog (default)
- `timestamp` — no snapshot; replay binlog from `startupTimestampMs`
  (UTC epoch ms). Idempotent over already-applied data, so this is also the
  recovery tool: replay a window to heal a gap.
- `latest-offset` / `earliest-offset` — stream from the binlog head / the
  earliest retained position.

**`sink`** — thinDB's MySQL wire endpoint. Keep both URL parameters from the
example. `flushRows`/`flushIntervalMs` bound the buffer; larger flushes widen
the compaction window (20,000 works well for bulk replays, 2,000 for at-head
trickle).

**`tables`** — every table to sink, with ordered columns and their Debezium
semantic types (`INT`, `TINYINT`, `BIGINT`, `DATE`, `DATETIME`, `DECIMAL`,
anything else is passed through as text) and the primary-key column list.
Composite keys are supported. Target tables must already exist in thinDB with
matching primary keys.

## Semantics and caveats

- Deletes are applied by primary key from the Debezium `before` image.
- `DECIMAL` uses `decimal.handling.mode=string` to avoid float drift.
- `DATETIME` values are interpreted as UTC (`server-time-zone=UTC`).
- Snapshot-phase backfill reconciliation is skipped
  (`skipSnapshotBackfill(true)`): safe for PK-upsert sinks, where re-delivered
  rows dedup on key — and it prevents unbounded state growth when
  snapshotting a table that is being written concurrently.
- A cancelled job finishes its in-flight statement before exiting; wait for
  `CANCELED` (not `CANCELLING`) before submitting a replacement, or the two
  will contend on thinDB's writer.
