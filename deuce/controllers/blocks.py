
from pecan import conf, expose, request, response
from pecan.rest import RestController
from deuce.model import Vault, Block
from deuce.util import set_qs
from six.moves.urllib.parse import urlparse

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
        vault = Vault.get(request.project_id, vault_id)

        if not vault:
            response.status_code = 404
            return

        inmarker = request.params.get('marker')

        limit = int(request.params.get('limit',
           conf.api_configuration.max_returned_num))

        # We actually fetch the user's requested
        # limit +1 to detect if the list is being
        # truncated or not.
        blocks = vault.get_blocks(inmarker, limit + 1)

        # List the blocks into JSON and return.
        # TODO: figure out a way to stream this back(??)
        resp = list(blocks)

        # Was the list truncated? See note above about +1
        truncated = len(resp) > 0 and len(resp) == limit + 1

        outmarker = resp.pop().block_id if truncated else None

        if outmarker:
            query_args = {'marker': outmarker}

            query_args['limit'] = limit

            returl = set_qs(request.url, query_args)

            response.headers["X-Next-Batch"] = returl

        return resp

    @expose()
    def get_one(self, vault_id, block_id):
        """Returns a specific block"""

        if not block_id:
            response.status_code = 404
            return

        # Step 1: Is the block in our vault store?  If not, return 404
        # Step 2: Stream the block back to the user

        vault = Vault.get(request.project_id, vault_id)

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
        vault = Vault.get(request.project_id, vault_id)

        vault.put_block(
            block_id, request.body, request.headers['content-length'])
