# CDC fault-injection rig

Reproduces the failure class where a truncated read on a long-haul link makes
the flink-cdc binlog reader silently skip an event, losing rows without any
error surfacing. Deployment-specific material (customer schemas, the
consolidated ingest job, repair scripts) is kept locally and gitignored.

## Rig
`docker compose up -d` — MySQL 8 (GTID/ROW) + truncation proxy + Flink 1.20.3
+ thinDB (linux binary, not committed — scp from the target host).
Jars (not committed): flink-sql-connector-mysql-cdc 3.3.0 + 3.6.0-1.20,
flink-connector-jdbc-3.3.0-1.20, mysql-connector-j-8.4.0 → ./jars/.
`CDC_JAR=<jarname> docker compose up -d --force-recreate jobmanager taskmanager`
switches connector versions. After any recreate: chmod -R 777 /ckpt (uid 9999).

Proxy modes (ctrl :8123): pass | storm (random cut, prob/stallFrac) |
poison (deterministic cut of every chunk >= minsize — wedges the reader at one
event, the Aug-10 signature) | stallpoison (partial event then silent socket =
black-hole WAN; spawns keepalive zombie readers).

Harness: `node harness/rig.mjs setup|seed|bigtxn|churn|smallwrite|verify|mode|stats`.
Rules learned the hard way: only verify after a marker row settles (recovery
can lag minutes and read as false loss), and churn values carry a per-run
nonce (deterministic re-writes make lost updates invisible).

Results 2026-08-15: cdc both connector versions tested survived the suite with zero loss; note they bundle the identical mysql-binlog-connector-java, so the low-level truncated-read handling is unchanged between them.

