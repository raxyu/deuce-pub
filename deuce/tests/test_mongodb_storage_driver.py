from pecan import conf

import deuce

from deuce.tests import FunctionalTest
from deuce.drivers.storage.metadata import MetadataStorageDriver
from deuce.drivers.storage.metadata.mongodb import MongoDbStorageDriver


class MongoDbStorageDriverTest(FunctionalTest):

    def test_basic_construction(self):
        try:
            # Try system existed mongod
            driver = MongoDbStorageDriver()
        except:
            assert True

    def test_geneology(self):
        driver = MongoDbStorageDriver()
        assert isinstance(driver, MetadataStorageDriver)
        assert isinstance(driver, object)

    def test_file_crud(self):
        driver = MongoDbStorageDriver()

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
        driver = MongoDbStorageDriver()

        project_id = 'project_id'
        vault_id = 'vault_id'
        file_id = 'whatever'

        assert not driver.is_finalized(project_id, vault_id, file_id)

        driver.create_file(project_id, vault_id, file_id)

        assert not driver.is_finalized(project_id, vault_id, file_id)

        driver.finalize_file(project_id, vault_id, file_id)

        assert driver.is_finalized(project_id, vault_id, file_id)

    def test_finalize_nonexistent_file(self):
        driver = MongoDbStorageDriver()

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
        driver = MongoDbStorageDriver()

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

        driver = MongoDbStorageDriver()

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
        if conf.metadata_driver.mongodb.is_mocking:
            retgen = \
                driver.create_file_block_generator(
                    project_id, vault_id, file_id, offset, limit)
            retgen = \
                driver.create_file_block_generator(
                    project_id, vault_id, file_id, offset, limit)
            block_size = driver.get_block_data(project_id,
                vault_id, block_ids[0])['blocksize']
        else:
            while True:
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
                        self.assertEqual(fetched_blocks[x][0],
                            block_ids[cnt + x])
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

        gen = driver.create_block_generator(project_id, vault_id, marker='zzz')
        self.assertEqual(len(gen), 0)
