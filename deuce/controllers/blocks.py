
from pecan import conf, expose, request, response
from pecan.rest import RestController
from deuce.model import Vault, Block
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

        marker = request.params.get('marker', 0)
        limit = int(request.params.get('limit', 0))

        blocks = vault.get_blocks(marker, limit)

        # List the blocks into JSON and return.
        # TODO: figure out a way to stream this back(??)
        resp = list(blocks)

        returl = ''
        resplen = int(len(resp))
        if resplen != 0 and \
                ((limit != 0 and
                resplen == limit) or
                (limit == 0 and
                resplen == conf.api_configuration.max_returned_num)):
            # Return a full list.
            parsedurl = urlparse(request.url)
            returl = parsedurl.scheme + '://' + \
                parsedurl.netloc + parsedurl.path
            returl = returl + '?marker=' + resp[-1].block_id
            if limit != 0:
                returl = returl + '&limit=' + str(limit)

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
