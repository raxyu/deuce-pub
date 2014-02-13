
from pecan import expose, response
from pecan.rest import RestController
from deuce.controllers.blocks import BlocksController
from deuce.controllers.files import FilesController
from deuce.model import Vault


class VaultController(RestController):

    blocks = BlocksController()
    files = FilesController()

    @expose()
    def index(self):
        response.status_code = 404

    @expose()
    def post(self, vault_name):
        vault = Vault.create(vault_name)
        response.status_code = 201 if vault else 500

    @expose()
    def get_one(self, vault_id):
        """Returns the vault controller object"""

        if Vault.get(vault_id):
            response.status_code = 200
        else:
            response.status_code = 404

        return None

    @expose()
    def delete(self, vault_id):

        vault = Vault.get(vault_id)

        if vault:
            vault.delete()
            response.status_code = 200
        else:
            response.status_code = 404
