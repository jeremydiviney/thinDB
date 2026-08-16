-- Mirrors the prod-generated cdc_invoice_import_amortized.sql shape:
-- same checkpoint settings, same source options, JDBC upsert sink.
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.timeout' = '15min';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '20';
SET 'parallelism.default' = '1';

CREATE TABLE src_invoice_test (
  projectId INT NOT NULL,
  integrationConfigId INT NOT NULL,
  invoiceId STRING NOT NULL,
  invoiceItemId STRING NOT NULL,
  modelType STRING NOT NULL,
  `date` DATE NOT NULL,
  amount INT,
  customerName STRING,
  payload STRING,
  updatedAt TIMESTAMP(6),
  PRIMARY KEY (projectId, integrationConfigId, invoiceId, invoiceItemId, modelType, `date`) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = 'proxy',
  'port' = '3307',
  'username' = 'cdcuser',
  'password' = 'cdcpass',
  'database-name' = 'rigdb',
  'table-name' = 'invoice_test',
  'server-id' = '9001-9004',
  'server-time-zone' = 'UTC',
  'scan.incremental.snapshot.enabled' = 'true',
  'scan.incremental.snapshot.chunk.size' = '65536',
  'scan.snapshot.fetch.size' = '4096',
  'scan.incremental.snapshot.backfill.skip' = 'true',
  'jdbc.properties.tcpKeepAlive' = 'true',
  'jdbc.properties.socketTimeout' = '600000',
  'connect.timeout' = '60s',
  'heartbeat.interval' = '15s',
  'debezium.connect.keep.alive.interval.ms' = '30000'
);

CREATE TABLE sink_invoice_test (
  projectId INT NOT NULL,
  integrationConfigId INT NOT NULL,
  invoiceId STRING NOT NULL,
  invoiceItemId STRING NOT NULL,
  modelType STRING NOT NULL,
  `date` DATE NOT NULL,
  amount INT,
  customerName STRING,
  payload STRING,
  updatedAt TIMESTAMP(6),
  PRIMARY KEY (projectId, integrationConfigId, invoiceId, invoiceItemId, modelType, `date`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://thindb:13310/rigdb__public?rewriteBatchedStatements=true',
  'table-name' = 'invoice_test',
  'username' = 'root',
  'password' = '',
  'sink.buffer-flush.max-rows' = '2000',
  'sink.buffer-flush.interval' = '1s',
  'sink.max-retries' = '3'
);

INSERT INTO sink_invoice_test SELECT * FROM src_invoice_test;
