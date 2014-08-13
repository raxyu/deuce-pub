"""mock_cassandra -- Cassandra mocking using Sqlite

This is a simple special-case CQL/Cassandra mocker using
sqlite to actually perform the queries. It is not intended
to successfully mock every CQL use-case, rather it supports
only the minimum required for Deuce
"""

import sqlite3
import uuid
import collections

MOCK_CASSANDRA_SCHEMA = ["""
CREATE TABLE blocks (
  projectid TEXT,
  vaultid TEXT,
  blockid TEXT,
  blocksize INT,
  PRIMARY KEY(projectid, vaultid, blockid)
);
""", """
CREATE TABLE files (
  projectid TEXT,
  vaultid TEXT,
  fileid TEXT,
  size INT,
  finalized BOOLEAN,
  PRIMARY KEY(projectid, vaultid, fileid)
);
""", """
CREATE INDEX finalized_index
    on files (finalized);
""", """
CREATE TABLE fileblocks (
  projectid TEXT,
  vaultid TEXT,
  fileid TEXT,
  blockid TEXT,
  blocksize INT,
  offset INT,
  PRIMARY KEY(projectid, vaultid, fileid, offset)
);
"""]


class Session(object):

    def __init__(self, conn):
        self.conn = conn

    def execute(self, query, queryargs):
        # Cassandra driver supports UUID type natively but sqlite3
        # does not, so convert every UUID to a string.
        queryargs = tuple([str(s) if isinstance(s, uuid.UUID)
                           else s for s in queryargs])

        query = query.replace('false', '0')
        query = query.replace('true', '1')
        query = query.replace('%s', '?')

        res = self.conn.execute(query, queryargs)
        res = list(res)

        return res


class Cluster(object):

    def __init__(self, hosts):
        # Create the mock driver in memory only
        self._sqliteconn = sqlite3.connect(':memory:')

        for stmt in MOCK_CASSANDRA_SCHEMA:
            self._sqliteconn.execute(stmt)

    def connect(self, keyspace):
        return Session(self._sqliteconn)
