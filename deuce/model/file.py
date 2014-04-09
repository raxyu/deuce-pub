
class File(object):

    def __init__(self, project_id, vault_id, file_id, finalized=False):
        self._project_id = project_id
        self._vault_id = vault_id
        self._file_id = file_id
        self._finalized = finalized

    @property
    def project_id(self):
        return self._project_id

    @property
    def file_id(self):
        return self._file_id

    @property
    def vault_id(self):
        return self._vault_id

    @property
    def finalized(self):
        return self._finalized

    # callback, for serializing to json
    def __json__(self):
        return self._file_id
