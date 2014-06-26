
from pecan import conf

from deuce.drivers.storage.blocks import BlockStorageDriver

import os
import io
import shutil

import importlib
import hashlib

import couchdb

from six import BytesIO


class CouchdbStorageDriver(BlockStorageDriver):

    def __init__(self, server_url):
        self.couch = couchdb.Server(server_url)
        self.vault = None


    # =========== VAULTS ===============================
    def create_vault(self, project_id, vault_id):
        db = "deuce_test/"+project_id+"/"+vault_id+"/"+"blocks"
        try:
            self.vault = self.couch.create(db)
            return True
        except Exception, e:
            self.vault = self.couch[db]
            return False


    def vault_exists(self, project_id, vault_id):
        db = 'deuce_test/'+project_id+'/'+vault_id+'/'+'blocks'
        try:
            if self.couch[db]:
                return True
            else:
                return False
        except Exception:
            return False

    def delete_vault(self, project_id, vault_id):
        db = 'deuce_test/'+project_id+'/'+vault_id+'/'+'blocks'
        try:
            self.couch.delete(db)
            return True
        except Exception:
            return False

    # =========== BLOCKS ===============================
    def store_block(self, project_id, vault_id, block_id, blockdata):
        try:
            doc = {
                "_id": block_id
                }
            self.vault.save(doc)
            self.vault.put_attachment(doc, blockdata, filename=block_id)
            return True
        except Exception:
            return False

    def block_exists(self, project_id, vault_id, block_id):

        if block_id in self.vault:
            return True
        else:
            return False

    def delete_block(self, project_id, vault_id, block_id):
        try:
            doc = self.vault[block_id]
            self.vault.delete(doc)
            return True
        except:
            return False
        

    def get_block_obj(self, project_id, vault_id, block_id):
        try:
            doc = self.vault[block_id]
            return self.vault.get_attachment(block_id, filename=block_id)
        except:
            return None

    def create_blocks_generator(self, project_id, vault_id, block_gen):
        """Returns a generator of file-like objects that are
        ready to read. These objects will get closed
        individually."""
        return (self.get_block_obj(project_id, vault_id, block_id)
            for block_id in block_gen)
