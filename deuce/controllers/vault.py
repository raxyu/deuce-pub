
from pecan import expose, response, request
from deuce.controllers import DeuceController
from deuce.controllers.blocks import BlocksController
from deuce.controllers.files import FilesController
from deuce.model import Vault


class VaultController(DeuceController):

    blocks = BlocksController()
    files = FilesController()

    @expose()
    def index(self):
        response.status_code = 404

    @expose()
    def post(self, vault_name):
        vault = Vault.create(request.project_id, vault_name)
        response.status_code = 201 if vault else 500

    @expose()
    def get_one(self, vault_id):
        """Returns the vault controller object"""

        if Vault.get(request.project_id, vault_id):
            response.status_code = 200
        else:
            response.status_code = 404

        return None

    @expose()
    def delete(self, vault_id):

        vault = Vault.get(request.project_id, vault_id)

        if vault:
            vault.delete()
            response.status_code = 200
        else:
            response.status_code = 404
