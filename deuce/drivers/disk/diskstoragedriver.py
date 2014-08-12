from pecan import conf
from deuce.drivers.blockstoragedriver import BlockStorageDriver

import os
import os.path
import io
import shutil


class DiskStorageDriver(BlockStorageDriver):

    """A driver for storing blocks onto local disk

    IMPORTANT: This driver should not be considered
    secure and therefore should not be ran in
    any production environment.
    """

    def __init__(self):
        # Load the pecan config
        self._path = conf.block_storage_driver.options.path

    def _get_vault_path(self, project_id, vault_id):
        return os.path.join(
            self._path, str(project_id), vault_id)

    def _get_block_path(self, project_id, vault_id, block_id):
        vault_path = self._get_vault_path(project_id, vault_id)
        return os.path.join(vault_path, str(block_id))

    def create_vault(self, request_headers, vault_id):
        path = self._get_vault_path(request_headers['x-project-id'], vault_id)

        if not os.path.exists(path):
            shutil.os.makedirs(path)

    def vault_exists(self, request_headers, vault_id):
        path = self._get_vault_path(request_headers['x-project-id'], vault_id)
        return os.path.exists(path)

    def delete_vault(self, request_headers, vault_id):
        path = self._get_vault_path(request_headers['x-project-id'], vault_id)
        try:
            os.rmdir(path)
            return True
        except:
            return False

    def store_block(self, request_headers, vault_id, block_id, block_data):
        path = self._get_block_path(request_headers['x-project-id'],
            vault_id, block_id)

        with open(path, 'wb') as outfile:
            outfile.write(block_data)

        return True

    def block_exists(self, request_headers, vault_id, block_id):
        path = self._get_block_path(request_headers['x-project-id'],
            vault_id, block_id)
        return os.path.exists(path)

    def delete_block(self, request_headers, vault_id, block_id):
        path = self._get_block_path(request_headers['x-project-id'],
            vault_id, block_id)

        if os.path.exists(path):
            os.remove(path)

    def get_block_obj(self, request_headers, vault_id, block_id):
        """Returns a file-like object capable or streaming the
        block data. If the object cannot be retrieved, the list
        of objects should be returned
        """
        path = self._get_block_path(request_headers['x-project-id'],
            vault_id, block_id)

        if not os.path.exists(path):
            return None

        return open(path, 'rb')
