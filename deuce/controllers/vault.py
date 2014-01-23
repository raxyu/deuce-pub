
from pecan import expose, response
from pecan.rest import RestController
from blocks import BlocksController

class VaultController(RestController):

    blocks = BlocksController()

    @expose()
    def index(self):
        response.status_code = 404

    @expose()
    def get(self, vault_id):
        """Returns the vault controller object"""

        if vault_id != 'myvault':
            response.status_code = 404
            return

        print vault_id
