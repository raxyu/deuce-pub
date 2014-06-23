
from pecan import conf, expose, request, response, abort
from pecan.rest import RestController
from deuce.model import Vault, Block
from deuce.util import set_qs
from six.moves.urllib.parse import urlparse
from deuce.controllers.validation import *
import logging

logger = logging.getLogger(__name__)


BLOCK_ID_LENGTH = 40


class BlocksController(RestController):

    """The BlocksController is responsible for:

    a) Listing blocks stored in a particular vault
    b) Fetching an individual block from Deuce
    c) Uploading blocks
    d) Deleting blocks
    """
    @expose('json')
    @validate(vault_id=VaultGetRule, marker=BlockMarkerRule,
              limit=LimitRule)
    def get_all(self, vault_id):

        vault = Vault.get(request.project_id, vault_id)

        if not vault:
            logger.error('Vault [{0}] does not exist'.format(vault_id))
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
    @validate(vault_id=VaultGetRule, block_id=BlockGetRule)
    def get_one(self, vault_id, block_id):
        """Returns a specific block"""

        # Step 1: Is the block in our vault store?  If not, return 404
        # Step 2: Stream the block back to the user
        vault = Vault.get(request.project_id, vault_id)

        # Existence of the vault should have been confirmeds
        # in the vault controller
        assert vault is not None

        block = vault.get_block(block_id)

        if block is None:
            logger.error('block [{0}] does not exist'.format(block_id))
            abort(404)

        response.body_file = block.get_obj()
        response.status_code = 200

    @expose()
    @validate(vault_id=VaultPutRule, block_id=BlockPutRuleNoneOk)
    def put(self, vault_id, block_id=None):
        """Uploads a block into Deuce. The URL of the block
        is returned in the Location header
        """
        vault = Vault.get(request.project_id, vault_id)

        vault.put_block(
            block_id, request.body, request.headers['content-length'])
        logger.info('block [{0}] added'.format(block_id))
