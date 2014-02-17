
from pecan import expose, request, response
from pecan.rest import RestController

from deuce.model import Vault, Block

BLOCK_ID_LENGTH = 40


class BlocksController(RestController):

    """The BlocksController is responsible for:

    a) Listing blocks stored in a particular vault
    b) Fetching an individual block from Deuce
    c) Uploading blocks
    d) Deleting blocks
    """
    @expose('json')
    def get_all(self, vault_id):
        vault = Vault.get(vault_id)

        if not vault:
            response.status_code = 404
            return

        blocks = vault.get_blocks()

        # Convert the block object to JSON and return.
        # TODO: figure out a way to stream this back
        resp = list(blocks)

        return resp

    @expose()
    def get_one(self, vault_id, block_id):
        """Returns a specific block"""

        if not block_id:
            response.status_code = 404
            return

        # Step 1: Is the block in our vault store?  If not, return 404
        # Step 2: Stream the block back to the user

        vault = Vault.get(vault_id)

        # Note: vault_id should have been validated in the
        # vault controller so we can assume that it's OK
        # here. If vault is None, it's going to throw and
        # cause a 500 error which is what should happen
        # in this scenario.
        block = vault.get_block(block_id)

        if not block:
            response.status_code = 404
            return

        response.body_file = block.get_obj()
        response.status_code = 200

    @expose()
    def put(self, vault_id, block_id=None):
        """Uploads a block into Deuce. The URL of the block
        is returned in the Location header
        """
        vault = Vault.get(vault_id)  # Validated in VaultController

        vault.put_block(
            block_id, request.body, request.headers['content-length'])
