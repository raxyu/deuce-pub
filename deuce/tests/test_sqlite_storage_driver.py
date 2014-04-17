from pecan import conf

from deuce.tests import FunctionalTest
from deuce.drivers.storage.metadata import MetadataStorageDriver
from deuce.drivers.storage.metadata.sqlite import SqliteStorageDriver


class SqliteStorageDriverTest(FunctionalTest):

    def test_basic_construction(self):
        driver = SqliteStorageDriver()

    def test_geneology(self):
        driver = SqliteStorageDriver()
        assert isinstance(driver, MetadataStorageDriver)
        assert isinstance(driver, object)

    def test_file_crud(self):
        driver = SqliteStorageDriver()

        project_id = 'project_id'
        vault_id = 'vault_id'
        file_id = 'blah'

        assert not driver.has_file(project_id, vault_id, file_id)

        driver.create_file(project_id, vault_id, file_id)

        assert driver.has_file(project_id, vault_id, file_id)

        data = driver.get_file_data(project_id, vault_id, file_id)

        driver.delete_file(project_id, vault_id, file_id)

        assert not driver.has_file(project_id, vault_id, file_id)

    def test_finalize_empty_file(self):
        driver = SqliteStorageDriver()

        project_id = 'project_id'
        vault_id = 'vault_id'
        file_id = 'whatever'

        driver.create_file(project_id, vault_id, file_id)

        assert not driver.is_finalized(project_id, vault_id, file_id)

        driver.finalize_file(project_id, vault_id, file_id)

        assert driver.is_finalized(project_id, vault_id, file_id)

    def test_finalize_nonexistent_file(self):
        driver = SqliteStorageDriver()

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
        driver = SqliteStorageDriver()

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

        driver = SqliteStorageDriver()

        project_id = 'project_id'
        vault_id = 'vault_id'
        file_id = 'file_id'

        normal_block_size = 333
        gap_block_size = 222
        overlap_block_size = 444

        # GAP at front (miss the 1st block)
        num_blocks = int(0.5 * conf.api_configuration.max_returned_num)
        block_ids = ['block_{0}'.format(id) for id in range(1, num_blocks)]
        offsets = [x * normal_block_size for x in range(1, len(block_ids))]

        pairs = dict(zip(block_ids, offsets))

        # Create a file
        driver.create_file(project_id, vault_id, file_id)

        # Assign each block
        for bid, offset in pairs.items():
            driver.assign_block(project_id, vault_id, file_id, bid, offset)

        assert not driver.is_finalized(project_id, vault_id, file_id)

        # GAPs (gap at front)
        for bid, offset in pairs.items():
            driver.register_block(project_id, vault_id, bid, gap_block_size)
        res = driver.finalize_file(project_id, vault_id, file_id)
        assert not driver.is_finalized(project_id, vault_id, file_id)

        # OVERLAPs (gap at front)
        for bid, offset in pairs.items():
            driver.unregister_block(project_id, vault_id, bid)
            driver.register_block(project_id, vault_id,
            bid, overlap_block_size)
        res = driver.finalize_file(project_id, vault_id, file_id)
        assert not driver.is_finalized(project_id, vault_id, file_id)

        # put back the missed block at the front
        # Create a gap in the middle
        block_ids.insert(0, 'block_0')
        pairs['block_0'] = 0
        driver.assign_block(project_id, vault_id, file_id, 'block_0', 0)
        for bid, offset in pairs.items():
            driver.unregister_block(project_id, vault_id, bid)
            driver.register_block(project_id, vault_id, bid, gap_block_size)
        res = driver.finalize_file(project_id, vault_id, file_id)
        assert not driver.is_finalized(project_id, vault_id, file_id)

        # Create a overlap in the middle
        for bid, offset in pairs.items():
            driver.unregister_block(project_id, vault_id, bid)
            driver.register_block(project_id, vault_id,
                bid, overlap_block_size)
        res = driver.finalize_file(project_id, vault_id, file_id)
        assert not driver.is_finalized(project_id, vault_id, file_id)

        # Fix and back to normal
        for bid, offset in pairs.items():
            driver.unregister_block(project_id, vault_id, bid)
            driver.register_block(project_id, vault_id, bid, normal_block_size)

        # GAP at the eof.
        res = driver.finalize_file(project_id, vault_id,
            file_id, file_size=2000)
        assert not driver.is_finalized(project_id, vault_id, file_id)

        # OVERLAP at the eof.
        res = driver.finalize_file(project_id, vault_id,
            file_id, file_size=1000)
        assert not driver.is_finalized(project_id, vault_id, file_id)

        # Just perfect.
        res = driver.finalize_file(project_id, vault_id,
            file_id, file_size=1332)
        assert driver.is_finalized(project_id, vault_id, file_id)

        # Now create a generator of the files. The output
        # should be in the same order as block_ids
        offset = 0
        limit = 4

        retgen = \
            driver.create_file_block_generator(
                project_id, vault_id, file_id, offset, limit)

        fetched_blocks = list(retgen)

        # The driver actually returns limit+1 so that any
        # caller knows that the list is truncated.
        self.assertEqual(len(fetched_blocks) + 1, len(block_ids))

        # -1 to exclude the trailer
        for x in range(0, len(block_ids) - 2):
            self.assertEqual(fetched_blocks[x][0], block_ids[x])

        # Add 2 more blocks that aren't assigned.
        driver.register_block(project_id, vault_id, 'unassigned_1', 1024)
        driver.register_block(project_id, vault_id, 'unassigned_2', 1024)

        num_blocks += 2

        # Now create a generator of the files. The output
        # should be in the same order as block_ids
        gen = driver.create_block_generator(project_id, vault_id)

        fetched_blocks = list(gen)

        assert len(fetched_blocks) == num_blocks - 1
