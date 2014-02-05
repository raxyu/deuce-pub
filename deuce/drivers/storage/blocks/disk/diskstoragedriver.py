
from pecan import conf

import os
import io


class DiskStorageDriver(object):

    """A driver for storing blocks onto local disk

    IMPORTANT: This driver should not be considered
    secure and therefore should not be ran in
    any production environment.
    """

    def __init__(self):
        # Load the pecan config
        self._path = conf.block_storage_driver.options.path

        if not os.path.exists(self._path):
            # TODO: Use a real exception
            raise Exception("Block path does not exist {0}"
                            .format(self._path))

    def block_exists(self, vault_id, block_id):
        path = os.path.join(self._path, block_id)
        return os.path.exists(path)

    def get_block_objs(self, vault_id, block_gen):
        """Returns a generator of file-like objects
        for the specified block IDs"""

        filenames = (os.path.join(self._path, bid) for bid in block_gen)
        generator = (open(fname, 'rb') for fname in filenames)

        return generator

    def insert_block_obj(self, vault_id, block_id, blockdata):
        """
        """
        path = os.path.join(self._path, block_id)

        if os.path.exists(path):
            # TODO: need compare both?
            return True

        fd = open(path, 'w+')
        fd.write(blockdata)
        fd.close()
        return True

    def get_block_obj(self, vault_id, block_id):
        """Returns a file-like object capable or streaming the
        block data. If the object cannot be retrieved, the list
        of objects should be returned
        """
        path = os.path.join(self._path, block_id)

        if not os.path.exists(path):
            return None

        return open(path, 'rb')

    def get_objects_list(self, vault_id):
        """Lists (and yields) a list of each object that is in
        a particular vault
        """
        pass
