-- Phase-1 JDBC ingest validation job. Run against thinDB on the host.
--
-- On thinDB first (mysql -h127.0.0.1 -P3306 -uroot main):
--   CREATE TABLE dim (id INT, val INT, name STRING, PRIMARY KEY (id));
--
-- Then submit this via the Flink SQL client (see docker-compose.yml header).
-- It generates a bounded upsert stream keyed on id in [0,20) — many updates
-- per key — so after it finishes, thinDB should hold exactly 20 rows (one per
-- id, last-writer-wins), proving the JDBC upsert path end-to-end.

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '3s';

CREATE TEMPORARY TABLE src (
  id INT,
  val INT,
  name STRING
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '2000',            -- bounded: finishes on its own
  'fields.id.min' = '0', 'fields.id.max' = '19',   -- 20 keys, heavy update rate
  'fields.val.min' = '0', 'fields.val.max' = '1000000',
  'fields.name.length' = '6'
);

CREATE TEMPORARY TABLE thindb_dim (
  id INT,
  val INT,
  name STRING,
  PRIMARY KEY (id) NOT ENFORCED         -- upsert mode → INSERT ... ON DUPLICATE KEY UPDATE
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://host.docker.internal:13306/main',
  'table-name' = 'dim',
  'username' = 'root',
  'password' = '',
  'sink.buffer-flush.max-rows' = '200',
  'sink.buffer-flush.interval' = '1s'
);

INSERT INTO thindb_dim SELECT id, val, name FROM src;
