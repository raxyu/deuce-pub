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

        block_ids = ['block_{0}'.format(id) for id in range(0, 10)]
        offsets = [x * 333 for x in range(0, len(block_ids))]

        pairs = dict(zip(block_ids, offsets))

        # Create a file
        driver.create_file(project_id, vault_id, file_id)

        # Assign each block
        for bid, offset in pairs.items():
            driver.register_block(project_id, vault_id, bid, 1024)
            driver.assign_block(project_id, vault_id, file_id, bid, offset)

        assert not driver.is_finalized(project_id, vault_id, file_id)

        driver.finalize_file(project_id, vault_id, file_id)

        assert driver.is_finalized(project_id, vault_id, file_id)

        # Now create a generator of the files. The output
        # should be in the same order as block_ids
        gen = []
        offset = 0
        limit = 4
        while True:
            retgen, offset = \
                driver.create_file_block_generator(
                    project_id, vault_id, file_id, offset, limit)
            retgen = list(retgen)
            gen.extend(retgen)
            if not offset:
                break

        fetched_blocks = list(gen)

        assert len(fetched_blocks) == len(block_ids)

        for x in range(0, len(fetched_blocks)):
            assert fetched_blocks[x] == block_ids[x]

        # Add 2 more blocks that aren't assigned.
        driver.register_block(project_id, vault_id, 'unassigned_1', 1024)
        driver.register_block(project_id, vault_id, 'unassigned_2', 1024)

        # Now create a generator of the files. The output
        # should be in the same order as block_ids
        gen, marker = driver.create_block_generator(project_id, vault_id)

        fetched_blocks = list(gen)

        assert len(fetched_blocks) == 12
