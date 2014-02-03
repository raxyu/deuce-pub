
from pecan import conf, response
from block import Block
from file import File

import deuce

import uuid

class Vault(object):

    @staticmethod
    def get(vault_id):
        """Fetches the vault for the specified vault ID.
        """

        # TODO: Check metadata driver to ensure that we
        # are working with an actual vault.
        return Vault(vault_id)

    def __init__(self, vault_id):
        self.id = vault_id

    def put_block(self, block_id, blockdata, data_len):
        try:
          retval = deuce.storage_driver.insert_block_obj(self.id, block_id, blockdata)
          file_id = deuce.metadata_driver.register_block(self.id, block_id, data_len)
        except Exception as ex:
          return False
        return retval

    def get_block(self, block_id):
        obj = deuce.storage_driver.get_block_obj(self.id, block_id)
        return Block(self.id, block_id, obj)

    def create_file(self):
        file_id = str(uuid.uuid4())
        file_id = deuce.metadata_driver.create_file(self.id, file_id)
        return file_id

    def get_file(self, file_id):
        try:
            data = deuce.metadata_driver.get_file_data(self.id, file_id)
        except Exception as ex:
            return None

        return File(self.id, file_id, finalized=data[0])
