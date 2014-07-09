
from deuce.tests import FunctionalTest
from deuce.drivers.storage.blocks import BlockStorageDriver
from deuce.drivers.storage.blocks.disk import DiskStorageDriver
from deuce.tests.util import MockFile

from hashlib import md5

# TODO: Make this test generic -- it should not konw
# which particular driver it is testing.


class DiskStorageDriverTest(FunctionalTest):

    def create_driver(self):
        return DiskStorageDriver()

    def test_ancestry(self):
        driver = self.create_driver()

        assert isinstance(driver, BlockStorageDriver)
        assert isinstance(driver, object)

    def test_basic_construction(self):
        driver = self.create_driver()

    def test_vault_crud(self):

        driver = self.create_driver()

        projectid = 'test_project_id'
        vaultid = 'test_vault_id'

        driver.delete_vault(projectid, vaultid)
        assert not driver.vault_exists(projectid, vaultid)

        # delete a non-empty vault.
        driver.create_vault(projectid, vaultid)
        block_id = 'baab'
        driver.store_block(projectid, vaultid, block_id, b' ')
        assert driver.block_exists(projectid, vaultid, block_id)
        assert not driver.delete_vault(projectid, vaultid)
        assert driver.vault_exists(projectid, vaultid)
        # Cleanup and delete again.
        driver.delete_block(projectid, vaultid, block_id)
        assert driver.delete_vault(projectid, vaultid)
        assert not driver.vault_exists(projectid, vaultid)

        # To create an existed vault.
        driver.create_vault(projectid, vaultid)
        driver.create_vault(projectid, vaultid)

        assert driver.vault_exists(projectid, vaultid)

        driver.delete_vault(projectid, vaultid)

        assert not driver.vault_exists(projectid, vaultid)

    def test_block_crud(self):

        driver = self.create_driver()

        block_size = 3000
        vault_id = 'block_crud_vault_test'
        block_id = 'blah'
        projectid = 'test_project_id'

        driver.create_vault(projectid, vault_id)

        # Create a file-like object
        block_data = MockFile(block_size)

        driver.store_block(projectid, vault_id, block_id, block_data.read())

        block_data.seek(0)

        assert driver.block_exists(projectid, vault_id, block_id)

        # Read back the block data and compare
        file_obj = driver.get_block_obj(projectid, vault_id, block_id)

        returned_data = file_obj.read()

        # Returned data should be exatly the same

        assert len(returned_data) == block_size
        assert returned_data == block_data._content

        driver.delete_block(projectid, vault_id, block_id)
        driver.delete_block(projectid, vault_id, block_id)

        assert not driver.block_exists(projectid, vault_id, block_id)

        assert None == driver.get_block_obj(projectid,
            vault_id, 'invalid_block_id')

        assert driver.delete_vault(projectid, vault_id)

    def test_block_generator(self):
        driver = self.create_driver()

        block_size = 3000
        vault_id = 'generator_test'
        projectid = 'test_project_id'

        driver.create_vault(projectid, vault_id)

        blocks = [(x, MockFile(block_size)) for x in range(0, 10)]

        orig_hash = md5()

        for block_id, block_data in blocks:
            orig_hash.update(block_data._content)

        orig_hex = orig_hash.hexdigest()

        for block_id, block_data in blocks:
            retval = driver.store_block(projectid, vault_id,
                block_id, block_data.read())
            block_data.seek(0)

        # Now call the block generator.

        blockid_gen = (x[0] for x in blocks)

        gen = driver.create_blocks_generator(projectid, vault_id, blockid_gen)

        fetched_data = list(gen)

        assert len(fetched_data) == len(blocks) == 10

        for x in range(0, len(fetched_data)):
            blocks[x][1].seek(0)
            assert fetched_data[x].read() == blocks[x][1].read()

        # Clenaup.
        for block_id, block_data in blocks:
            driver.delete_block(projectid, vault_id,
                block_id)
        assert driver.delete_vault(projectid, vault_id)
