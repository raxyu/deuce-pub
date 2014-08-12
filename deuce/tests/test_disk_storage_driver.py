import os
from deuce.tests import FunctionalTest
from deuce.drivers.blockstoragedriver import BlockStorageDriver
from deuce.drivers.disk import DiskStorageDriver
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

    def get_mock_hdrs(self):
        return self.create_mock_hdrs()

    def test_vault_crud(self):
        hdrs = self.get_mock_hdrs()
        driver = self.create_driver()

        vault_id = self.create_vault_id()

        driver.delete_vault(
            request_headers=hdrs,
            vault_id=vault_id)
        assert not driver.vault_exists(
            request_headers=hdrs,
            vault_id=vault_id)

        # delete a non-empty vault.
        driver.create_vault(
            request_headers=hdrs,
            vault_id=vault_id)
        block_id = self.create_block_id()
        driver.store_block(
            request_headers=hdrs,
            vault_id=vault_id,
            block_id=block_id,
            block_data=b' ')
        assert driver.block_exists(
            request_headers=hdrs,
            vault_id=vault_id,
            block_id=block_id)
        assert not driver.delete_vault(
            request_headers=hdrs,
            vault_id=vault_id)
        assert driver.vault_exists(
            request_headers=hdrs,
            vault_id=vault_id)
        # Cleanup and delete again.
        driver.delete_block(
            request_headers=hdrs,
            vault_id=vault_id,
            block_id=block_id)
        assert driver.delete_vault(
            request_headers=hdrs,
            vault_id=vault_id)
        assert not driver.vault_exists(
            request_headers=hdrs,
            vault_id=vault_id)

        # To create an existed vault.
        driver.create_vault(
            request_headers=hdrs,
            vault_id=vault_id)
        driver.create_vault(
            request_headers=hdrs,
            vault_id=vault_id)

        assert driver.vault_exists(
            request_headers=hdrs,
            vault_id=vault_id)

        driver.delete_vault(
            request_headers=hdrs,
            vault_id=vault_id)

        assert not driver.vault_exists(
            request_headers=hdrs,
            vault_id=vault_id)

    def test_block_crud(self):

        hdrs = self.get_mock_hdrs()
        driver = self.create_driver()

        block_size = 3000
        vault_id = self.create_vault_id()

        driver.create_vault(
            request_headers=hdrs,
            vault_id=vault_id)

        # Create a file-like object
        block_data = MockFile(block_size)

        # Test Invalid block_id, ie, wrong sha1 hash.
        try:
            driver.store_block(
                request_headers=hdrs,
                vault_id=vault_id,
                block_id="test_disk_trouble_file",
                block_data=os.urandom(10))
        except:
            assert True
        driver.delete_block(
            request_headers=hdrs,
            vault_id=vault_id,
            block_id="test_disk_trouble_file")

        # Test delete invalid block
        driver.delete_block(
            request_headers=hdrs,
            vault_id=vault_id,
            block_id="test_invalid_block_deletion")

        # Test valid block_id.
        block_id = block_data.sha1()
        driver.store_block(
            request_headers=hdrs,
            vault_id=vault_id,
            block_id=block_id,
            block_data=block_data.read())
        block_data.seek(0)

        assert driver.block_exists(
            request_headers=hdrs,
            vault_id=vault_id,
            block_id=block_id)

        # Read back the block data and compare
        file_obj = driver.get_block_obj(
            request_headers=hdrs,
            vault_id=vault_id,
            block_id=block_id)

        returned_data = file_obj.read()

        # Returned data should be exatly the same

        assert len(returned_data) == block_size
        assert returned_data == block_data._content

        driver.delete_block(
            request_headers=hdrs,
            vault_id=vault_id,
            block_id=block_id)

        assert not driver.block_exists(
            request_headers=hdrs,
            vault_id=vault_id,
            block_id=block_id)

        assert None == driver.get_block_obj(
            request_headers=hdrs,
            vault_id=vault_id,
            block_id='invalid_block_id')

        assert driver.delete_vault(
            request_headers=hdrs,
            vault_id=vault_id)

    def test_block_generator(self):
        hdrs = self.get_mock_hdrs()
        driver = self.create_driver()

        block_size = 3000
        vault_id = self.create_vault_id()

        driver.create_vault(
            request_headers=hdrs,
            vault_id=vault_id)

        # Test re-entrance
        driver.create_vault(
            request_headers=hdrs,
            vault_id=vault_id)

        blocks = [MockFile(block_size) for x in range(0, 10)]

        orig_hash = md5()

        for block_data in blocks:
            orig_hash.update(block_data._content)

        orig_hex = orig_hash.hexdigest()

        block_ids = []
        for block_data in blocks:
            block_id = block_data.sha1()
            block_ids.append(block_id)
            driver.store_block(
                request_headers=hdrs,
                vault_id=vault_id,
                block_id=block_id,
                block_data=block_data.read())
            block_data.seek(0)

        # Now call the block generator.

        blockid_gen = block_ids[:]

        gen = driver.create_blocks_generator(
            request_headers=hdrs,
            vault_id=vault_id,
            block_gen=blockid_gen)

        fetched_data = list(gen)

        assert len(fetched_data) == len(blocks) == 10

        for x in range(0, len(fetched_data)):
            blocks[x].seek(0)
            assert fetched_data[x].read() == blocks[x].read()

        # Clenaup.
        for block_id in block_ids[:]:
            driver.delete_block(
                request_headers=hdrs,
                vault_id=vault_id,
                block_id=block_id)
        assert driver.delete_vault(
            request_headers=hdrs,
            vault_id=vault_id)
