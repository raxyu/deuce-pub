
from pecan import expose, request, response
from pecan.core import abort
from pecan.rest import RestController

import deuce
from deuce.model import Vault, Block, File
from deuce.util import FileCat


class FilesController(RestController):

    @expose()
    def get_one(self, vault_id, file_id):
        """Fetches, re-assembles and streams a single
        file out of Deuce"""

        vault = Vault.get(vault_id)

        if not vault:
            abort(404)

        f = vault.get_file(file_id)

        if not f:
            abort(404)

        # Get the block generator from the metadata driver
        blks = deuce.metadata_driver.create_block_generator(vault_id,
            file_id)

        objs = deuce.storage_driver.create_blocks_generator(vault_id, blks)

        response.body_file = FileCat(objs)
        response.status_code = 200

    @expose('json')
    def post(self, vault_id, file_id=None):
        """Initializes a new file. The location of
        the new file is returned in the Location
        header
        """

        if file_id == "":  # i.e .../files/
            abort(404)

        if file_id:
            return self._assign(vault_id, file_id)

        vault = Vault.get(vault_id)

        if not vault:
            abort(404)

        file_id = vault.create_file()

        response.headers["Location"] = "files/%s" % file_id
        response.status_code = 201  # Created

    def _assign(self, vault_id, file_id):

        vault = Vault.get(vault_id)

        if not vault:
            abort(404)

        f = vault.get_file(file_id)

        if not f:
            abort(404)

        if f.finalized:
            # A finalized file cannot be
            # modified
            # TODO: Determine a better, more precise
            #       status code
            abort(400)

        blocks = request.json_body['blocks']

        missing_blocks = list()

        for mapping in blocks:

            block_id = mapping['id']
            offset = mapping['offset']

            if not deuce.metadata_driver.has_block(vault_id, block_id):
                missing_blocks.append(block_id)

            deuce.metadata_driver.assign_block(vault_id,
               file_id, mapping['id'], mapping['offset'])

        return missing_blocks
