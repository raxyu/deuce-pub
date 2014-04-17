from pecan import conf
from deuce.drivers.storage.blocks import BlockStorageDriver

import os
import io
import shutil

#try:
from  swiftclient import client as Conn
#except ImportError:
#import swift.common.client as swift_client


class SwiftStorageDriver(BlockStorageDriver):

    """A driver for storing blocks onto local disk

    IMPORTANT: This driver should not be considered
    secure and therefore should not be ran in
    any production environment.
    """

    def __init__(self, storage_url, auth_token, project_id):
        # Load the pecan config
        self._storage_url = storage_url
        self._token = auth_token
        self._project_id = project_id

    def _get_vault_path(self, project_id, vault_id):
        return '/'.join([str(project_id), str(vault_id)])

    def _get_block_path(self, project_id, vault_id, block_id):
        vault_path = self._get_vault_path(project_id, vault_id)
        return '/'.join([vault_path, str(block_id)])

    def create_vault(self, project_id, vault_id):
        vault_path = self._get_vault_path(project_id, vault_id)

        ret = Conn.put_container(self._storage_url, self._token, project_id)
        print ('yude: create vault: ', ret)

    def vault_exists(self, project_id, vault_id):
        vault_path = self._get_vault_path(project_id, vault_id)
        
        try:
            ret = Conn.head_object(self._storage_url, self._token, project_id, vault_id)
            print ('yude: check container head ', ret)
            return ret.http_status == '200'
        except Conn.ClientException as e:
            print ('yude: check container head exception ', e.http_status)
            return e.http_status == '200'
            
    def delete_vault(self, project_id, vault_id):
        path = self._get_vault_path(project_id, vault_id)
        os.rmdir(path)

    def store_block(self, project_id, vault_id, block_id, blockdata):
        path = self._get_block_path(project_id, vault_id, block_id)

        with open(path, 'wb') as outfile:
            outfile.write(blockdata)

        return True

    def block_exists(self, project_id, vault_id, block_id):
        path = self._get_block_path(project_id, vault_id, block_id)
        return os.path.exists(path)

    def delete_block(self, project_id, vault_id, block_id):
        path = self._get_block_path(project_id, vault_id, block_id)

        if os.path.exists(path):
            os.remove(path)

    def get_block_obj(self, project_id, vault_id, block_id):
        """Returns a file-like object capable or streaming the
        block data. If the object cannot be retrieved, the list
        of objects should be returned
        """
        path = self._get_block_path(project_id, vault_id, block_id)

        if not os.path.exists(path):
            return None

        return open(path, 'rb')
