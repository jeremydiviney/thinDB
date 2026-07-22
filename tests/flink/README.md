# Flink → thinDB ingest validation

Validates the JDBC ingest path with a **real Flink job + real Connector/J**
(what mysql2 smoke tests can't cover). thinDB runs natively on the host
(Windows/Linux) on **port 13306**; Flink (and a MySQL CDC source) run in Docker
and reach thinDB via `host.docker.internal`. See `../../docs/plans/INGEST_DESIGN.md`.

Two harnesses share this compose stack:
- **`job.sql`** — datagen → thinDB upsert (basic JDBC-sink sanity).
- **`cdc_job.sql`** — the real one: **MySQL binlog → Flink CDC → thinDB**.

## End-to-end CDC test (snapshot + streaming insert/update/delete)

Proves the production scenario: a MySQL source with existing data is snapshotted
into thinDB, then live inserts/updates/deletes tail the binlog into thinDB.

```
# 0. thinDB on the host + target table (mysql client on :13306):
./zig-out/bin/thindb-server.exe --data-dir .flink-db --mysql-port 13306 --max-dop 4
#    CREATE TABLE orders (id INT, customer_id INT, amount INT, status STRING, PRIMARY KEY(id));

# 1. Bring up MySQL (binlog source) + Flink:
docker compose -f tests/flink/docker-compose.yml up -d --build

# 2. Seed the MySQL source + point thinDB at it (from tests/bun, has mysql2):
node _cdc_driver.mjs seed 1000            # 1000 rows into src.orders (MySQL :13307)

# 3. Submit the CDC job (MSYS_NO_PATHCONV=1 needed in Git Bash for the -f path):
MSYS_NO_PATHCONV=1 docker compose -f tests/flink/docker-compose.yml \
  exec -T jobmanager ./bin/sql-client.sh -f /opt/sql/cdc_job.sql

# 4. Snapshot lands:            node _cdc_driver.mjs check   -> COUNT = 1000
# 5. Live insert:               node _cdc_driver.mjs live 1001 50   -> COUNT = 1050
# 6. Update + delete:           node _cdc_driver.mjs mutate
#      -> id=1 amount=999999, id=2 DELETED, COUNT = 1049
```

Result (verified 2026-07-07): snapshot 1000 → live +50 → update+delete all
propagate correctly through the JDBC upsert sink. `mutate` exercises the full
CDC changelog: `+U` → `ON DUPLICATE KEY UPDATE`, `-D` → `DELETE WHERE pk=?`.

## Basic JDBC-sink sanity (datagen)

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
  2PC / XA) — see docs/plans/INGEST_DESIGN.md.
- Connector/jar versions are pinned in `Dockerfile.flink`; bump them together.
