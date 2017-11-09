CREATE TABLE nasalogs (
  logid UUID,
  host TEXT,
  datetime TIMESTAMP,
  path TEXT,
  bytes INT,
  PRIMARY KEY (logid)
);