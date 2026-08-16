-- Old-world per-table SQL jobs for small_a and small_b (one binlog
-- connection EACH — the topology being replaced by the consolidated job).
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '20';
SET 'parallelism.default' = '1';

CREATE TABLE src_small_a (
  id INT NOT NULL, name STRING, val INT, updatedAt TIMESTAMP(6),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc', 'hostname' = 'proxy', 'port' = '3307',
  'username' = 'cdcuser', 'password' = 'cdcpass',
  'database-name' = 'rigdb', 'table-name' = 'small_a',
  'server-id' = '9201-9204', 'server-time-zone' = 'UTC',
  'heartbeat.interval' = '15s'
);
CREATE TABLE sink_small_a (
  id INT NOT NULL, name STRING, val INT, updatedAt TIMESTAMP(6),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc', 'url' = 'jdbc:mysql://thindb:13310/rigdb__public',
  'table-name' = 'small_a', 'username' = 'root', 'password' = '',
  'sink.buffer-flush.interval' = '1s'
);
INSERT INTO sink_small_a SELECT * FROM src_small_a;

CREATE TABLE src_small_b (
  id INT NOT NULL, label STRING, qty BIGINT, updatedAt TIMESTAMP(6),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc', 'hostname' = 'proxy', 'port' = '3307',
  'username' = 'cdcuser', 'password' = 'cdcpass',
  'database-name' = 'rigdb', 'table-name' = 'small_b',
  'server-id' = '9301-9304', 'server-time-zone' = 'UTC',
  'heartbeat.interval' = '15s'
);
CREATE TABLE sink_small_b (
  id INT NOT NULL, label STRING, qty BIGINT, updatedAt TIMESTAMP(6),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc', 'url' = 'jdbc:mysql://thindb:13310/rigdb__public',
  'table-name' = 'small_b', 'username' = 'root', 'password' = '',
  'sink.buffer-flush.interval' = '1s'
);
INSERT INTO sink_small_b SELECT * FROM src_small_b;
