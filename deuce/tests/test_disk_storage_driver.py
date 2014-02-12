
from deuce.tests import FunctionalTest
from deuce.drivers.storage.blocks import BlockStorageDriver
from deuce.drivers.storage.blocks.disk import DiskStorageDriver
from deuce.tests.util import MockFile

from hashlib import md5

# TODO: Make this test generic -- it should not konw
# which particular driver it is testing.


class DiskStorageDriverTest(FunctionalTest):

    def __drivertype__(self):
        """The type of driver that should be created
        for this test"""

    def test_ancestry(self):
        d = DiskStorageDriver()

        assert isinstance(d, BlockStorageDriver)
        assert isinstance(d, object)

    def test_basic_construction(self):
        d = DiskStorageDriver()

    def test_vault_crud(self):

        d = DiskStorageDriver()

        vaultid = 'test_vault_id'

        assert not d.vault_exists(vaultid)

        d.create_vault(vaultid)

        assert d.vault_exists(vaultid)

        d.delete_vault(vaultid)

        assert not d.vault_exists(vaultid)

    def test_block_crud(self):

        d = DiskStorageDriver()

        block_size = 3000
        vault_id = 'block_crud_vault_test'
        block_id = 'blah'

        # set up test.
        if not d.vault_exists(vault_id):
            d.create_vault(vault_id)

        # Create a file-like object
        block_data = MockFile(block_size)

        d.store_block(vault_id, block_id, block_data.read())

        block_data.seek(0)

        assert d.block_exists(vault_id, block_id)

        # Read back the block data and compare
        file_obj = d.get_block_obj(vault_id, block_id)

        returned_data = file_obj.read()

        # Returned data should be exatly the same

        assert len(returned_data) == block_size
        assert returned_data == block_data._content

        d.delete_block(vault_id, block_id)
        d.delete_block(vault_id, block_id)

        assert not d.block_exists(vault_id, block_id)

        assert None == d.get_block_obj(vault_id, 'invalid_block_id')

    def test_block_generator(self):
        d = DiskStorageDriver()

        block_size = 3000
        vault_id = 'generator_test'

        d.create_vault(vault_id)

        # Test re-entrance
        d.create_vault(vault_id)

        blocks = [(x, MockFile(block_size)) for x in range(0, 10)]

        orig_hash = md5()

        for block_id, block_data in blocks:
            orig_hash.update(block_data._content)

        orig_hex = orig_hash.hexdigest()

        for block_id, block_data in blocks:
            d.store_block(vault_id, block_id, block_data.read())
            block_data.seek(0)

        # Now call the block generator.

        blockid_gen = (x[0] for x in blocks)

        gen = d.create_blocks_generator(vault_id, blockid_gen)

        fetched_data = list(gen)

        assert len(fetched_data) == len(blocks) == 10

        for x in range(0, len(fetched_data)):
            blocks[x][1].seek(0)
            assert fetched_data[x].read() == blocks[x][1].read()
