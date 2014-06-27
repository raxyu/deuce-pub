
from pecan import conf

from deuce.drivers.storage.blocks import BlockStorageDriver

import os
import io
import shutil

import importlib
import hashlib

import pycouchdb

from six import BytesIO


class CouchdbStorageDriver(BlockStorageDriver):

    def __init__(self, server_url):
        self.couch = pycouchdb.Server(server_url)

    # =========== VAULTS ===============================

    def get_vault(self, project_id, vault_id):
        try:
            db_path = "deuce_test/" + project_id + \
                "/" + vault_id + "/" + "blocks"
            return self.couch.database(db_path)
        except:
            return None

    def create_vault(self, project_id, vault_id):
        try:
            db_path = "deuce_test/" + project_id + \
                "/" + vault_id + "/" + "blocks"
            self.couch.create(db_path)
            return True
        except:
            return False

    def vault_exists(self, project_id, vault_id):
        vault = self.get_vault(project_id, vault_id)
        if vault is None:
            return False
        return True

    def delete_vault(self, project_id, vault_id):
        try:
            db_path = "deuce_test/" + project_id + \
                "/" + vault_id + "/" + "blocks"
            self.couch.delete(db_path)
            return True
        except:
            return False

    # =========== BLOCKS ===============================
    def store_block(self, project_id, vault_id, block_id, blockdata):
        try:
            doc = {"_id": block_id}
            vault = self.get_vault(project_id, vault_id)
            doc = vault.save(doc)
            ret = vault.put_attachment(doc, blockdata, filename=block_id)
            return True
        except:
            return False

    def block_exists(self, project_id, vault_id, block_id):
        vault = self.get_vault(project_id, vault_id)
        if vault and block_id in vault:
            return True
        return False

    def delete_block(self, project_id, vault_id, block_id):
        vault = self.get_vault(project_id, vault_id)
        if vault:
            vault.delete(block_id)
            return True
        return False

    def get_block_obj(self, project_id, vault_id, block_id):
        try:
            vault = self.get_vault(project_id, vault_id)
            if vault:
                doc = vault.get(block_id)
                return BytesIO(vault.get_attachment(doc, filename=block_id))
            return None
        except:
            return None

    def create_blocks_generator(self, project_id, vault_id, block_gen):
        """Returns a generator of file-like objects that are
        ready to read. These objects will get closed
        individually."""
        return (self.get_block_obj(project_id, vault_id, block_id)
            for block_id in block_gen)
