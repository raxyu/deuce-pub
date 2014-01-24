
from pecan import expose, response
from pecan.rest import RestController

from deuce.model import Vault, Block, File

class FilesController(RestController):

    @expose('json')
    def get_all(self, vault_id):
        """Returns a json-formatted list of all files that
        are stored in this vault"""
        return ['whatever']

    @expose()
    def get_one(self, vault_id, file_id):
        """Fetches, re-assembles and streams a single
        file out of Deuce"""
        vault = Vault.get(vault_id)

        if not vault:
            response.status_code = 404
            return

        f = vault.get_file(file_id)

        if not f:
            response.status_code = 404
            return

        response.body_file = f.get_obj()
        response.status_code = 200

    @expose()
    def post(self, vault_id):
        """Initializes a new file. The location of
        the new file is returned in the Location
        header
        """
        vault = Vault.get(vault_id)

        if not vault:
            response.status_code = 404
            return

        file_id = vault.create_file()
        response.headers["Location"] = "files/%s" % file_id

        response.status_code = 204 # No content
