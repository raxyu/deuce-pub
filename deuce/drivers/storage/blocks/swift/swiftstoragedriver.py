
from pecan import conf

from deuce.drivers.storage.blocks import BlockStorageDriver

import os
import io
import shutil

import importlib

from swiftclient.exceptions import ClientException

from six import StringIO


class SwiftStorageDriver(BlockStorageDriver):

    def __init__(self, storage_url, auth_token, project_id):
        self._storage_url = storage_url
        self._token = auth_token
        self._project_id = project_id

        self.lib_pack = importlib.import_module(
            conf.block_storage_driver.swift.swift_module)
        self.Conn = getattr(self.lib_pack, 'client')

    # =========== VAULTS ===============================
    def create_vault(self, project_id, vault_id):
        response = dict()

        try:
            self.Conn.put_container(
                url=self._storage_url,
                token=self._token,
                container=vault_id,
                response_dict=response)
            return response['status'] == 201
        except ClientException as e:
            return False

    def vault_exists(self, project_id, vault_id):
        try:
            ret = self.Conn.head_container(
                url=self._storage_url,
                token=self._token,
                container=vault_id)
            return ret is not None
        except ClientException as e:
            return False

    def delete_vault(self, project_id, vault_id):
        response = dict()
        try:
            self.Conn.delete_container(
                url=self._storage_url,
                token=self._token,
                container=vault_id,
                response_dict=response)
            return response['status'] >= 200 and response['status'] < 300
        except ClientException as e:
            return False

    # =========== BLOCKS ===============================
    def store_block(self, project_id, vault_id, block_id, blockdata):
        response = dict()
        try:
            ret_etag = self.Conn.put_object(
                url=self._storage_url,
                token=self._token,
                container=vault_id,
                name='blocks/' + str(block_id),
                contents=blockdata,
                content_length=len(blockdata),
                # etag=*TODO*,
                response_dict=response)
            return response['status'] == 201
        except ClientException as e:
            return False

    def block_exists(self, project_id, vault_id, block_id):
        try:
            ret = self.Conn.head_object(
                url=self._storage_url,
                token=self._token,
                container=vault_id,
                name='blocks/' + str(block_id))
            return ret is not None
        except ClientException as e:
            return False

    def delete_block(self, project_id, vault_id, block_id):
        response = dict()
        try:
            self.Conn.delete_object(
                url=self._storage_url,
                token=self._token,
                container=vault_id,
                name='blocks/' + str(block_id),
                response_dict=response)
            return response['status'] >= 200 and response['status'] < 300
        except ClientException as e:
            return False

    def get_block_obj(self, project_id, vault_id, block_id):
        response = dict()
        buff = StringIO()
        try:
            ret_hdr, ret_obj_body = \
                self.Conn.get_object(
                    url=self._storage_url,
                    token=self._token,
                    container=vault_id,
                    name='blocks/' + str(block_id),
                    response_dict=response)
            buff.write(ret_obj_body)
            buff.seek(0)
            return buff
        except ClientException as e:
            return None
