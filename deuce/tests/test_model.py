from unittest import TestCase
from webtest import TestApp
from deuce.tests import FunctionalTest

from deuce.model import Vault, Block, File


class TestModel(FunctionalTest):

    def test_get_nonexistent_block(self):
        v = Vault.get('should_not_exist')
        assert v is None

    def test_vault_crud(self):

        vault_id = 'my_vault_id'

        v = Vault.get(vault_id)
        assert v is None
        v = Vault.get(vault_id)
        assert v is None

        v = Vault.create(vault_id)
        assert v is not None

        v.delete()

        v = Vault.get(vault_id)
        assert v is None

    def test_file_crud(self):
        vault_id = 'test_file_vault'
        vault_id = 'my_vault_id'

        v = Vault.create(vault_id)

        f = v.create_file()

        assert isinstance(f, File)
        assert f.vault_id == vault_id

        file_id = f.file_id

        assert(len(file_id) > 0)

        file2 = v.get_file(file_id)

        assert isinstance(file2, File)
        assert file2.file_id == file_id

    def test_block_crud(block_id):
        vault_id = 'block_test_vault'

        v = Vault.create(vault_id)

        # Check for blocks, should be none
        blocks_gen = v.get_blocks()
        blocks_list = list(blocks_gen)

        assert len(blocks_list) == 0
