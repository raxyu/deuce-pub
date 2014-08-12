
from pecan import conf, response

from deuce.model.block import Block
from deuce.model.file import File

import deuce
import uuid
import hashlib


class Vault(object):

    @staticmethod
    def get(request_headers, vault_id):
        if deuce.storage_driver.vault_exists(
                request_headers=request_headers,
                vault_id=vault_id):
            return Vault(
                request_headers=request_headers,
                vault_id=vault_id)
        return None

    @staticmethod
    def create(request_headers, vault_id):
        """Creates the vault with the specified vault_id"""
        deuce.storage_driver.create_vault(
            request_headers=request_headers,
            vault_id=vault_id)
        return Vault(
            request_headers=request_headers,
            vault_id=vault_id)

    def __init__(self, request_headers, vault_id):
        self.request_headers = request_headers
        self.project_id = request_headers['x-project-id']
        self.vault_id = vault_id

    def put_block(self, block_id, blockdata, data_len):

        # Validate the hash of the block data against block_id
        if hashlib.sha1(blockdata).hexdigest() != block_id:
            raise ValueError('Invalid Hash Value in the block ID')

        retval = deuce.storage_driver.store_block(
            request_headers=self.request_headers,
            vault_id=self.vault_id,
            block_id=block_id,
            block_data=blockdata)

        file_id = deuce.metadata_driver.register_block(
            self.project_id, self.vault_id, block_id, data_len)

        return retval

    def get_blocks(self, marker, limit):
        gen = deuce.metadata_driver.create_block_generator(
            self.project_id,
            self.vault_id, marker=marker, limit=limit)

        return (Block(self.project_id, self.vault_id, bid) for bid in gen)

    def get_block(self, block_id):
        obj = deuce.storage_driver.get_block_obj(
            request_headers=self.request_headers,
            vault_id=self.vault_id,
            block_id=block_id)

        return Block(self.project_id, self.vault_id,
            block_id, obj) if obj else None

    def get_blocks_generator(self, block_ids):
        return deuce.storage_driver.create_blocks_generator(
            request_headers=self.request_headers,
            vault_id=self.vault_id,
            block_gen=block_ids)

    def create_file(self):
        file_id = str(uuid.uuid4())
        file_id = deuce.metadata_driver.create_file(self.project_id,
            self.vault_id, file_id)

        return File(self.project_id, self.vault_id, file_id)

    def get_files(self, marker, limit):
        gen = deuce.metadata_driver.create_file_generator(
            self.project_id, self.vault_id, marker=marker,
            limit=limit, finalized=True)

        return (File(self.project_id, self.vault_id, bid, finalized=True)
            for bid in gen)

    def get_file(self, file_id):
        try:
            data = deuce.metadata_driver.get_file_data(self.project_id,
                self.vault_id, file_id)

        except:
            # TODO: Improve this. This could be very
            # dangerous and cause a lot of head-scratching.
            return None

        return File(self.project_id, self.vault_id, file_id, finalized=data[0])

    def delete(self):
        return deuce.storage_driver.delete_vault(
            request_headers=self.request_headers,
            vault_id=self.vault_id)

    def delete_file(self, file_id):
        return deuce.metadata_driver.delete_file(
            self.project_id, self.vault_id, file_id)
