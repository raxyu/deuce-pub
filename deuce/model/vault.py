
from pecan import conf, response

from deuce.model.block import Block
from deuce.model.file import File

import deuce
import uuid


class Vault(object):

    @staticmethod
    def get(project_id, vault_id):

        if deuce.storage_driver.vault_exists(project_id, vault_id):
            return Vault(project_id, vault_id)

        return None

    @staticmethod
    def create(project_id, vault_id):
        """Creates the vault with the specified vault_id"""
        deuce.storage_driver.create_vault(project_id, vault_id)
        return Vault(project_id, vault_id)

    def __init__(self, project_id, vault_id):
        self.project_id = project_id
        self.id = vault_id

    def put_block(self, block_id, blockdata, data_len):
        retval = deuce.storage_driver.store_block(
            self.project_id, self.id, block_id, blockdata)

        file_id = deuce.metadata_driver.register_block(
            self.project_id, self.id, block_id, data_len)

        return retval

    def get_blocks(self, marker, limit):
        #TODO: ranges, etc.
        gen = deuce.metadata_driver.create_block_generator(self.project_id,
            self.id, marker=marker, limit=limit)

        return (Block(self.project_id, self.id, bid) for bid in gen)

    def get_block(self, block_id):
        obj = deuce.storage_driver.get_block_obj(self.project_id, self.id,
            block_id)

        return Block(self.project_id, self.id, block_id, obj) if obj else None

    def create_file(self):
        file_id = str(uuid.uuid4())
        file_id = deuce.metadata_driver.create_file(self.project_id,
            self.id, file_id)

        return File(self.project_id, self.id, file_id)

    def get_file(self, file_id):
        try:
            data = deuce.metadata_driver.get_file_data(self.project_id,
                self.id, file_id)

        except:
            # TODO: Improve this. This could be very
            # dangerous and cause a lot of head-scratching.
            return None

        return File(self.project_id, self.id, file_id, finalized=data[0])

    def delete(self):
        deuce.storage_driver.delete_vault(self.project_id, self.id)
