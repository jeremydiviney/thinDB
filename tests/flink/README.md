# Flink → thinDB JDBC ingest validation (Phase 1)

Validates the JDBC ingest path with a **real Flink job + real Connector/J**
(what mysql2 smoke tests can't fully cover). thinDB runs natively on the host
(Windows/Linux); only Flink runs in Docker. See `../../INGEST_DESIGN.md`.

## Steps

1. **Start thinDB on the host** on the MySQL port, and create the target table:
   ```
   ./zig-out/bin/thindb-server.exe --data-dir .flink-db --mysql-port 3306 --max-dop 4
   # in a mysql client:  CREATE TABLE dim (id INT, val INT, name STRING, PRIMARY KEY(id));
   ```

2. **Build + start Flink** (Docker Desktop must be running):
   ```
   docker compose -f tests/flink/docker-compose.yml build
   docker compose -f tests/flink/docker-compose.yml up -d
   ```

3. **Submit the job:**
   ```
   docker compose -f tests/flink/docker-compose.yml exec jobmanager \
     ./bin/sql-client.sh -f /opt/sql/job.sql
   ```

4. **Verify** in thinDB: the job emits a 2000-row upsert stream over 20 keys, so
   the table should settle to **exactly 20 rows** (one per id, last-writer-wins):
   ```
   SELECT COUNT(*) FROM dim;         -- 20
   SELECT * FROM dim ORDER BY id;
   ```
   Flink web UI at http://localhost:8081.

## Notes
- `host.docker.internal` lets the Flink container reach thinDB on the host.
- `PRIMARY KEY (id) NOT ENFORCED` on the Flink table is what puts the JDBC
  connector in **upsert mode** → it emits `INSERT ... ON DUPLICATE KEY UPDATE`,
  which thinDB maps to its last-writer-wins upsert.
- Guarantee here is **effectively-once** (at-least-once delivery + idempotent
  upsert). True exactly-once for keyless/append tables is Stage 2 (Stream Load +
  2PC / XA) — see INGEST_DESIGN.md.
- Connector/jar versions are pinned in `Dockerfile.flink`; bump them together.
