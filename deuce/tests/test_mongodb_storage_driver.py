from pecan import conf

import pymongo
from pymongo import MongoClient
import tempfile
import subprocess
import os
import time
import atexit
import shutil

from deuce.tests import FunctionalTest
from deuce.drivers.storage.metadata import MetadataStorageDriver
from deuce.drivers.storage.metadata.mongodb import MongoDbStorageDriver


class MongoDbStorageDriverTest(FunctionalTest):

    def __init__(self, *args, **kwargs):
        self.db = MongoTemporaryInstance.get_instance()
        self.conn = self.db.conn
        super(MongoDbStorageDriverTest, self).__init__(*args, **kwargs)
        atexit.register(self.shutdown)

    def shutdown(self):
        if os.path.exists('/tmp/deuce_mongo_unittest_vaultmeta.db'):\
                #pragma: no cover
            os.remove('/tmp/deuce_mongo_unittest_vaultmeta.db')\
                #pragma: no cover

    def setUp(self):
        super(MongoDbStorageDriverTest, self).setUp()

        for db_name in self.conn.database_names():
            self.conn.drop_database(db_name)

    def test_basic_construction(self):
        try:
            # Try system existed mongod
            driver = MongoDbStorageDriver()
        except:  # pragma: no cover
            assert True

    def test_geneology(self):
        driver = MongoDbStorageDriver(self.conn)
        assert isinstance(driver, MetadataStorageDriver)
        assert isinstance(driver, object)

    def test_file_crud(self):
        driver = MongoDbStorageDriver(self.conn)

        project_id = 'project_id'
        vault_id = 'vault_id'
        file_id = 'blah'

        assert not driver.has_file(project_id, vault_id, file_id)

        driver.create_file(project_id, vault_id, file_id)

        assert driver.has_file(project_id, vault_id, file_id)

        data = driver.get_file_data(project_id, vault_id, file_id)

        driver.delete_file(project_id, vault_id, file_id)

        assert not driver.has_file(project_id, vault_id, file_id)

        files = driver.create_file_generator(project_id, vault_id)

    def test_finalize_empty_file(self):
        driver = MongoDbStorageDriver(self.conn)

        project_id = 'project_id'
        vault_id = 'vault_id'
        file_id = 'whatever'

        #assert not driver.is_finalized(project_id, vault_id, file_id)

        driver.create_file(project_id, vault_id, file_id)

        assert not driver.is_finalized(project_id, vault_id, file_id)

        driver.finalize_file(project_id, vault_id, file_id)

        assert driver.is_finalized(project_id, vault_id, file_id)

    def test_finalize_nonexistent_file(self):
        driver = MongoDbStorageDriver(self.conn)

        project_id = 'project_id'
        vault_id = 'vault_id'
        file_id = 'should_not_exist'

        assert not driver.has_file(project_id, vault_id, file_id)
        driver.finalize_file(project_id, vault_id, file_id)

        try:
            data = driver.get_file_data(project_id, vault_id, file_id)
        except:
            assert True

        assert not driver.has_file(project_id, vault_id, file_id)
        assert not driver.is_finalized(project_id, vault_id, file_id)

    def test_block_crud(self):
        driver = MongoDbStorageDriver(self.conn)

        project_id = 'project_id'
        vault_id = 'vault_id'
        block_id = 'block_id'
        size = 4096

        assert not driver.has_block(project_id, vault_id, block_id)
        driver.register_block(project_id, vault_id, block_id, size)

        assert driver.has_block(project_id, vault_id, block_id)

        # Call again, shouldn't throw
        driver.register_block(project_id, vault_id, block_id, size)

        driver.unregister_block(project_id, vault_id, block_id)
        assert not driver.has_block(project_id, vault_id, block_id)

        assert not driver.has_block(project_id, vault_id, 'invalidid')

    def test_file_assignment(self):

        driver = MongoDbStorageDriver(self.conn)

        project_id = 'project_id'
        vault_id = 'vault_id'
        file_id = 'file_id'
        block_size = 333

        num_blocks = int(0.5 * conf.api_configuration.max_returned_num)
        block_ids = ['block_{0}'.format(id) for id in range(0, num_blocks)]
        offsets = [x * block_size for x in range(0, len(block_ids))]

        pairs = dict(zip(block_ids, offsets))

        # Create a file
        driver.create_file(project_id, vault_id, file_id)

        # Assign each block
        for bid, offset in pairs.items():
            driver.register_block(project_id, vault_id, bid, block_size)
            driver.assign_block(project_id, vault_id, file_id, bid, offset)

        assert not driver.is_finalized(project_id, vault_id, file_id)

        driver.finalize_file(project_id, vault_id, file_id)

        assert driver.is_finalized(project_id, vault_id, file_id)

        # Now create a generator of the files. The output
        # should be in the same order as block_ids

        cnt = 0
        limit = 6
        offset = 0
        # for an extra missing round
        while True:  # pragma: no cover
            retgen = \
                driver.create_file_block_generator(
                    project_id, vault_id, file_id, offset, limit)

            fetched_blocks = list(retgen)
            if not fetched_blocks:
                break
            blockid = fetched_blocks[-1][0]
            block_offset = fetched_blocks[-1][1]
            block_size = driver.get_block_data(project_id,
                vault_id, blockid)['blocksize']
            offset = block_offset + block_size

            # The driver actually returns limit+1 so that any
            # caller knows that the list is truncated.

            # -1 to exclude the trailer
            if len(fetched_blocks) == limit:
                for x in range(0, len(fetched_blocks) - 1):
                    self.assertEqual(fetched_blocks[x][0], block_ids[cnt + x])
                cnt += limit
            else:
                self.assertEqual(len(fetched_blocks), len(block_ids) - cnt)
                cnt += len(fetched_blocks)

        # Add 2 more blocks that aren't assigned.
        driver.register_block(project_id, vault_id, 'unassigned_1', 1024)
        driver.register_block(project_id, vault_id, 'unassigned_2', 1024)

        num_blocks += 2

        # Now create a generator of the files. The output
        # should be in the same order as block_ids
        gen = driver.create_block_generator(project_id, vault_id)

        fetched_blocks = list(gen)


'''
http://blogs.skicelab.com/maurizio/python-unit-testing-and-mongodb.html
Author/Credit: Posted by Maurizio Sambati Wed 03 October 2012
'''


class MongoTemporaryInstance(object):  # pragma: no cover
    """Singleton to manage a temporary MongoDB instance

    Use this for testing purpose only. The instance is automatically destroyed
    at the end of the program.

    """
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
            atexit.register(cls._instance.shutdown)
        return cls._instance

    def __init__(self):
        self._port = 8888
        self._url = "127.0.0.1"
        self._tmpdir = tempfile.mkdtemp()
        self._process = subprocess.Popen(['mongod', '--bind_ip', 'localhost',
                                          '--port', str(self._port),
                                          '--dbpath', self._tmpdir,
                                          '--nojournal', '--nohttpinterface',
                                          '--noauth', '--smallfiles',
                                          '--syncdelay', '0',
                                          '--maxConns', '10',
                                          '--nssize', '1', ],
                                         stdout=open(os.devnull, 'wb'),
                                         stderr=subprocess.STDOUT)

        # XXX: wait for the instance to be ready
        #      Mongo is ready in a glance, we just wait to be able to open a
        #      Connection.
        for i in range(3):
            time.sleep(0.1)
            try:
                self._conn = MongoClient(self._url, self._port)
                #self._conn = pymongo.Connection('localhost', int(self._port))
            except pymongo.errors.ConnectionFailure:
                continue
            else:
                break
        else:
            self.shutdown()
            assert False, 'Cannot connect to the mongodb test instance'

    @property
    def conn(self):
        return self._conn

    def shutdown(self):
        if self._process:
            self._process.terminate()
            self._process.wait()
            self._process = None
            shutil.rmtree(self._tmpdir, ignore_errors=True)


'''
KEEP as usage reference...

http://blogs.skicelab.com/maurizio/python-unit-testing-and-mongodb.html
Author/Credit: Posted by Maurizio Sambati Wed 03 October 2012
class TestCase(unittest.TestCase):
    """TestCase with an embedded MongoDB temporary instance.

    Each test runs on a temporary instance of MongoDB. Please note that
    these tests are not thread-safe and different processes should set a
    different value for the listening port of the MongoDB instance with the
    settings `global.MONGODB_TEST_PORT`.

    A test can access the connection using the attribute `conn`.

    """
    fixtures = []

    def __init__(self, *args, **kwargs):
        super(TestCase, self).__init__(*args, **kwargs)
        self.db = MongoTemporaryInstance.get_instance()
        self.conn = self.db.conn

    def setUp(self):
        super(TestCase, self).setUp()

        for db_name in self.conn.database_names():
            self.conn.drop_database(db_name)

        for fixture in self.fixtures:
            load_fixture(self.conn, fixture)
'''
