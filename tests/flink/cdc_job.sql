-- End-to-end CDC: MySQL binlog (snapshot + stream) -> thinDB JDBC upsert sink.
-- The source `mysql` container holds src.orders; thinDB (host:13306) holds
-- main.orders. Streaming/unbounded: snapshots existing rows, then tails the
-- binlog for live inserts/updates/deletes.

SET 'execution.checkpointing.interval' = '3s';

CREATE TEMPORARY TABLE src_orders (
  id INT,
  customer_id INT,
  amount INT,
  status STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = 'mysql',
  'port' = '3306',
  'username' = 'root',
  'password' = 'root',
  'database-name' = 'src',
  'table-name' = 'orders',
  'server-id' = '5400-5404'
);

CREATE TEMPORARY TABLE sink_orders (
  id INT,
  customer_id INT,
  amount INT,
  status STRING,
  PRIMARY KEY (id) NOT ENFORCED       -- upsert mode: +I/+U -> ON DUPLICATE KEY UPDATE, -D -> DELETE
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://host.docker.internal:13306/main',
  'table-name' = 'orders',
  'username' = 'root',
  'password' = '',
  'sink.buffer-flush.max-rows' = '100',
  'sink.buffer-flush.interval' = '1s'
);

INSERT INTO sink_orders SELECT id, customer_id, amount, status FROM src_orders;
