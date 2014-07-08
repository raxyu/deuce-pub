
import os
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

        driver.create_vault(projectid, vaultid)

        # To create an existed vault.
        driver.create_vault(projectid, vaultid)

        assert driver.vault_exists(projectid, vaultid)

        driver.delete_vault(projectid, vaultid)

        assert not driver.vault_exists(projectid, vaultid)

    def test_block_crud(self):

        driver = self.create_driver()

        block_size = 3000
        vault_id = 'block_crud_vault_test'
        projectid = 'test_project_id'

        driver.create_vault(projectid, vault_id)

        # Create a file-like object
        block_data = MockFile(block_size)

        # Test Invalid block_id, ie, wrong sha1 hash.
        try:
            driver.store_block(projectid, vault_id,
                "test_disk_trouble_file", os.urandom(10))
        except:
            assert True

        # Test valid block_id.
        block_id = block_data.sha1()
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

    def test_block_generator(self):
        driver = self.create_driver()

        block_size = 3000
        vault_id = 'generator_test'
        projectid = 'test_project_id'

        driver.create_vault(projectid, vault_id)

        # Test re-entrance
        driver.create_vault(projectid, vault_id)

        blocks = [MockFile(block_size) for x in range(0, 10)]

        orig_hash = md5()

        for block_data in blocks:
            orig_hash.update(block_data._content)

        orig_hex = orig_hash.hexdigest()

        for block_data in blocks:
            block_id = block_data.sha1()
            driver.store_block(projectid, vault_id, block_id, block_data.read())
            block_data.seek(0)

        # Now call the block generator.

        blockid_gen = (x.sha1() for x in blocks)

        gen = driver.create_blocks_generator(projectid, vault_id, blockid_gen)

        fetched_data = list(gen)

        assert len(fetched_data) == len(blocks) == 10

        for x in range(0, len(fetched_data)):
            blocks[x].seek(0)
            assert fetched_data[x].read() == blocks[x].read()
