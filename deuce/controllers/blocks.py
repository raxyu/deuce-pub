
from pecan import expose, response
from pecan.rest import RestController

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
        """Returns a json-formatted list of all blocks that
        are returned"""
        return ['whatever']

    @expose()
    def get_one(self, vault_id, block_id):
        """Returns a specific block data from Deuce
        """

        # Perform a fast sanity-check. We need not even
        # go to the datastore if the size is wrong
        if len(block_id) <> BLOCK_ID_LENGTH:
            response.status_code = 404
            return

        # Step 1: Is the block in our vault store?  If not, return 404
        # Step 2: Stream the block back to the user
        return vault_id + block_id

    @expose()
    def post(self):
        """Uploads a block into Deuce. The URL of the block
        is returned in the Location header
        """
