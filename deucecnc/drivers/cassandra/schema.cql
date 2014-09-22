CREATE KEYSPACE IF NOT EXISTS deucecnckeyspace WITH
  replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

use deucecnckeyspace;

CREATE TABLE vaults(
  projectid TEXT,
  vaultid TEXT,
  PRIMARY KEY(proectid, vaultid)
);
