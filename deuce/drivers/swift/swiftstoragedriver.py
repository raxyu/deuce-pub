
from pecan import conf

from deuce.drivers.blockstoragedriver import BlockStorageDriver

import os
import io
import shutil

import importlib
import hashlib

from swiftclient.exceptions import ClientException

from six import BytesIO


class SwiftStorageDriver(BlockStorageDriver):

    def __init__(self, storage_url, auth_token, project_id):
        self._storage_url = storage_url
        self._token = auth_token
        self._project_id = project_id

        self.lib_pack = importlib.import_module(
            conf.block_storage_driver.swift.swift_module)
        self.Conn = getattr(self.lib_pack, 'client')

    # =========== VAULTS ===============================
    def create_vault(self, storage_url, project_id, vault_id, auth_token):
        response = dict()

        try:
            self.Conn.put_container(
                url=storage_url,
                token=auth_token,
                container=vault_id,
                response_dict=response)
            return response['status'] == 201
        except ClientException as e:
            return False

    def vault_exists(self, storage_url, project_id, vault_id, auth_token):
        try:
            ret = self.Conn.head_container(
                url=storage_url,
                token=auth_token,
                container=vault_id)
            return ret is not None
        except ClientException as e:
            return False

    def delete_vault(self, storage_url, project_id, vault_id, auth_token):
        response = dict()
        try:
            self.Conn.delete_container(
                url=storage_url,
                token=auth_token,
                container=vault_id,
                response_dict=response)
            return response['status'] >= 200 and response['status'] < 300
        except ClientException as e:
            return False

    # =========== BLOCKS ===============================
    def store_block(self, storage_url, project_id, vault_id, block_id,
            blockdata, auth_token):
        response = dict()
        try:
            mdhash = hashlib.md5()
            mdhash.update(blockdata)
            mdetag = mdhash.hexdigest()
            ret_etag = self.Conn.put_object(
                url=storage_url,
                token=auth_token,
                container=vault_id,
                name='blocks/' + str(block_id),
                contents=blockdata,
                content_length=len(blockdata),
                etag=mdetag,
                response_dict=response)
            return response['status'] == 201 and ret_etag == mdetag
        except ClientException as e:
            return False

    def block_exists(self, storage_url, project_id, vault_id, block_id,
            auth_token):
        try:
            ret = self.Conn.head_object(
                url=storage_url,
                token=auth_token,
                container=vault_id,
                name='blocks/' + str(block_id))
            return ret is not None
        except ClientException as e:
            return False

    def delete_block(self, storage_url, project_id, vault_id, block_id,
            auth_token):
        response = dict()
        try:
            self.Conn.delete_object(
                url=storage_url,
                token=auth_token,
                container=vault_id,
                name='blocks/' + str(block_id),
                response_dict=response)
            return response['status'] >= 200 and response['status'] < 300
        except ClientException as e:
            return False

    def get_block_obj(self, storage_url, project_id, vault_id, block_id,
            auth_token):
        response = dict()
        buff = BytesIO()
        try:
            ret_hdr, ret_obj_body = \
                self.Conn.get_object(
                    url=storage_url,
                    token=auth_token,
                    container=vault_id,
                    name='blocks/' + str(block_id),
                    response_dict=response)
            buff.write(ret_obj_body)
            buff.seek(0)
            return buff
        except ClientException as e:
            return None

    def create_blocks_generator(self, storage_url, project_id, vault_id,
            block_gen, auth_token):
        """Returns a generator of file-like objects that are
        ready to read. These objects will get closed
        individually."""
        return (self.get_block_obj(storage_url, project_id, vault_id, block_id,
            auth_token)
            for block_id in block_gen)
