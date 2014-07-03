
from pecan import expose, response, request
from pecan.rest import RestController
from deuce.controllers.blocks import BlocksController
from deuce.controllers.files import FilesController
from deuce.controllers.validation import *
from deuce.model import Vault


class VaultController(RestController):

    blocks = BlocksController()
    files = FilesController()

    @expose()
    def index(self):
        response.status_code = 404

    @expose()
    @validate(vault_name=VaultPutRule)
    def post(self, vault_name):
        vault = Vault.create(request.project_id, vault_name)
        response.status_code = 201 if vault else 500

    @expose()
    @validate(vault_id=VaultGetRule)
    def get_one(self, vault_id):
        """Returns the vault controller object"""

        if Vault.get(request.project_id, vault_id):
            response.status_code = 204
        else:
            response.status_code = 404

        return None

    @expose()
    @validate(vault_id=VaultPutRule)
    def delete(self, vault_id):

        vault = Vault.get(request.project_id, vault_id)

        if vault:
            vault.delete()
            response.status_code = 204
        else:
            response.status_code = 404
