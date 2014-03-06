from unittest import TestCase
from webtest import TestApp
from deuce.tests import FunctionalTest

from deuce.model import Vault, Block, File


class TestModel(FunctionalTest):

    def setUp(self):
        super(TestModel, self).setUp()

        self.project_id = 'test_project_id'

    def test_get_nonexistent_block(self):
        v = Vault.get(self.project_id, 'should_not_exist')
        assert v is None

    def test_vault_crud(self):

        vault_id = 'my_vault_id'

        v = Vault.get(self.project_id, vault_id)
        assert v is None

        v = Vault.get(self.project_id, vault_id)
        assert v is None

        v = Vault.create(self.project_id, vault_id)
        assert v is not None

        v.delete()

        v = Vault.get(self.project_id, vault_id)
        assert v is None

    def test_file_crud(self):
        vault_id = 'test_file_vault'
        vault_id = 'my_vault_id'

        v = Vault.create(self.project_id, vault_id)

        f = v.create_file()

        assert isinstance(f, File)
        assert f.vault_id == vault_id

        file_id = f.file_id

        assert(len(file_id) > 0)

        file2 = v.get_file(file_id)

        assert isinstance(file2, File)
        assert file2.file_id == file_id
        assert file2.project_id == self.project_id

    def test_block_crud(self):
        vault_id = 'block_test_vault'

        v = Vault.create(self.project_id, vault_id)

        # Check for blocks, should be none
        blocks_gen, marker = v.get_blocks(0, 0)
        blocks_list = list(blocks_gen)

        assert len(blocks_list) == 0
