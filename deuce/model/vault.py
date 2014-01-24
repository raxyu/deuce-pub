
from pecan import conf, response
from block import Block

import deuce

class Vault(object):

    def __init__(self, vault_id):
        self.vault_id = vault_id

    @staticmethod
    def get(vault_id):
        """Fetches the vault for the specified vault ID.
        """

        # TODO: check the metadata driver to ensure that
        # we are working with a real vault

        return Vault(vault_id)

    def get_block(self, block_id):
        # Try to fetch the block data from the
        # storage driver.
        obj = deuce.storage_driver.get_block_obj(self.vault_id, block_id)

        if not obj:
            response.status_code = 404
            return

        return Block(self.vault_id, block_id, obj)

    def create_file(self):
        file_id = deuce.metadata_driver.create_file()
        return file_id
