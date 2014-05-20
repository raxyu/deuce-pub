from pecan import conf

# Monkey-patch the storage driver

import deuce.tests.mock_cassandra as cassandra

from deuce.tests import FunctionalTest
from deuce.drivers.storage.metadata import MetadataStorageDriver
from deuce.drivers.storage.metadata.cassandra import CassandraStorageDriver
from deuce.tests.test_sqlite_storage_driver import SqliteStorageDriverTest

import hashlib
import os
import six
import testtools
import uuid


# Explanation:
#   - The SqliteStorageDriver is the reference metadata driver. All
# other drivers should conform to the same interface, therefore
# we simply extend the SqliteStorageTest and run the sqlite driver tests
# against the Cassandra driver. The sqlite tests simply exercise the
# interface.
class CassandraStorageDriverTest(SqliteStorageDriverTest):

    def test_basic_construction(self):
        driver = CassandraStorageDriver()

    def test_geneology(self):
        driver = CassandraStorageDriver()
        assert isinstance(driver, MetadataStorageDriver)
        assert isinstance(driver, object)

    def create_driver(self):
        return CassandraStorageDriver()
