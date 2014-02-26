
from pecan import conf, expose, request, response
from pecan.rest import RestController
from pecan.core import abort

import deuce
from deuce.model import Vault, File, Block
from six.moves.urllib.parse import urlparse

BLOCK_ID_LENGTH = 40

class FileBlocksController(RestController):

    """The FileBlocksController is responsible for:
    Listing blocks belong to a particular file
    """
    @expose('json')
    def get_all(self, vault_id, file_id):
        vault = Vault.get(request.project_id, vault_id)
        if not vault:
            abort(404)
        f = vault.get_file(file_id)
        if not f:
            abort(404)

        marker = int(request.params.get('marker', 0))
        limit = int(request.params.get('limit', 0))

        resp = []
        # Get the block generator from the metadata driver
        retblks, marker = \
            deuce.metadata_driver.create_file_block_generator(
                request.project_id, vault_id, file_id, marker, limit)
        resp = list(retblks)

        returl = ''
        resplen = int(len(resp))
        if (limit != 0 and resplen == limit) or \
                (limit == 0 and
                resplen == conf.api_configuration.max_returned_num):
            # Return a full list.
            parsedurl = urlparse(request.url)
            returl = parsedurl.scheme + '://' + \
                parsedurl.netloc + parsedurl.path
            returl = returl + '?marker=' + str(marker)
            if limit != 0:
                returl = returl + '&limit=' + str(limit)
        response.headers["X-Next-Batch"] = returl

        return resp
