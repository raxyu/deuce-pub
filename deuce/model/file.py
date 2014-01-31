
class File(object):

    def __init__(self, vault_id, file_id, finalized=False):
        self._vault_id = vault_id
        self._file_id = file_id
        self._finalized = finalized

    def get_blocks(self):
        """Returns a generator that can be used
        to retrieve all of the blocks contained in
        a this file"""
        pass

    def get_obj(self):
        pass

    def finalize(self):
        """Finalizes this file"""
        # deuce.metadata_driver.finalize(self.vault_id, self.id)
        self._finalized = True

    @property
    def file_id(self):
        return self._file_id

    @property
    def vault_id(self):
        return self._vault_id

    @property
    def finalized(self):
        return self._finalized
