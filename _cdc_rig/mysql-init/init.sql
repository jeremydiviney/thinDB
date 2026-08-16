CREATE USER 'cdcuser'@'%' IDENTIFIED BY 'cdcpass';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdcuser'@'%';

CREATE DATABASE rigdb;
USE rigdb;

-- Shaped like a wide production invoice table: same 6-column PK, text payload
-- sized so multi-row transactions produce ~8KB binlog events.
CREATE TABLE invoice_test (
  projectId INT NOT NULL,
  integrationConfigId INT NOT NULL,
  invoiceId VARCHAR(64) NOT NULL,
  invoiceItemId VARCHAR(64) NOT NULL,
  modelType VARCHAR(16) NOT NULL,
  `date` DATE NOT NULL,
  amount INT,
  customerName VARCHAR(128),
  payload TEXT,
  updatedAt DATETIME(6) NOT NULL,
  PRIMARY KEY (projectId, integrationConfigId, invoiceId, invoiceItemId, modelType, `date`),
  KEY idx_updated (updatedAt)
);
