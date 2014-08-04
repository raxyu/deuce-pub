from pecan import conf
from deuce.drivers.blockstoragedriver import BlockStorageDriver

import os
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

    def _get_vault_path(self, project_id, vault_id,
            auth_token=None):
        return os.path.join(self._path, str(project_id), vault_id)

    def _get_block_path(self, project_id, vault_id, block_id,
            auth_token=None):
        vault_path = self._get_vault_path(project_id, vault_id)
        return os.path.join(vault_path, str(block_id))

    def create_vaults_generator(self, project_id,
            marker=None, limit=None, auth_token=None):
        rootpath = os.path.join(self._path, str(project_id))
        if os.path.exists(rootpath):
            dirs = sorted([str(name) for
                name in os.listdir(rootpath)
                if os.path.isdir(os.path.join(rootpath, name))])
            if dirs:
                if marker:
                    if marker in dirs:
                        pos = dirs.index(marker)
                        retval = dirs[pos:]
                        if limit < len(retval):
                            retval = retval[:limit]
                        return retval
                    else:
                        return None
                else:
                    if limit < len(dirs):
                        dirs = dirs[:limit]
                    return dirs
        return None

    def create_vault(self, project_id, vault_id,
            auth_token=None):
        path = self._get_vault_path(project_id, vault_id)

        if not os.path.exists(path):
            shutil.os.makedirs(path)

    def vault_exists(self, project_id, vault_id,
            auth_token=None):
        path = self._get_vault_path(project_id, vault_id)
        return os.path.exists(path)

    def delete_vault(self, project_id, vault_id,
            auth_token=None):
        path = self._get_vault_path(project_id, vault_id)
        try:
            os.rmdir(path)
            return True
        except:
            return False

    def store_block(self, project_id, vault_id, block_id, blockdata,
            auth_token=None):
        path = self._get_block_path(project_id, vault_id, block_id)

        with open(path, 'wb') as outfile:
            outfile.write(blockdata)

        return True

    def block_exists(self, project_id, vault_id, block_id,
            auth_token=None):
        path = self._get_block_path(project_id, vault_id, block_id)
        return os.path.exists(path)

    def delete_block(self, project_id, vault_id, block_id,
            auth_token=None):
        path = self._get_block_path(project_id, vault_id, block_id)

        if os.path.exists(path):
            os.remove(path)

    def get_block_obj(self, project_id, vault_id, block_id,
            auth_token=None):
        """Returns a file-like object capable or streaming the
        block data. If the object cannot be retrieved, the list
        of objects should be returned
        """
        path = self._get_block_path(project_id, vault_id, block_id)

        if not os.path.exists(path):
            return None

        return open(path, 'rb')
