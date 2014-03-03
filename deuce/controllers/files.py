from pecan import conf, expose, request, response
from pecan.core import abort
from pecan.rest import RestController

import deuce
from deuce.controllers.fileblocks import FileBlocksController
from deuce.model import Vault, Block, File
from deuce.util import FileCat
from six.moves.urllib.parse import urlparse


class FilesController(RestController):

    blocks = FileBlocksController()

    @expose('json')
    def get_all(self, vault_id):
        vault = Vault.get(request.project_id, vault_id)
        if not vault:
            abort(404)

        marker = request.params.get('marker', 0)
        limit = int(request.params.get('limit', 0))

        files, marker = vault.get_files(marker, limit)

        # List the files to JSON and return.
        resp = list(files)

        if marker:
            parsedurl = urlparse(request.url)
            returl = '' + \
                parsedurl.scheme + '://' + \
                parsedurl.netloc + parsedurl.path + \
                '?marker=' + marker
            if limit != 0:
                returl = returl + '&limit=' + str(limit)
            response.headers["X-Next-Batch"] = returl
        else:
            response.headers["X-Next-Batch"] = ''
        return resp

    @expose()
    def get_one(self, vault_id, file_id):
        """Fetches, re-assembles and streams a single
        file out of Deuce"""

        vault = Vault.get(request.project_id, vault_id)
        if not vault:
            abort(404)

        f = vault.get_file(file_id)
        if not f:
            abort(404)

        # Get the block generator from the metadata driver
        blks = []
        offset = 0
        limit = conf.api_configuration.max_returned_num
        while True:
            retblks, offset = \
                deuce.metadata_driver.create_file_block_generator(
                    request.project_id, vault_id, file_id, offset, limit)
            retblks = list(retblks)
            blks.extend(retblks)
            if not offset:
                break

        objs = deuce.storage_driver.create_blocks_generator(request.project_id,
            vault_id, blks)

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

        vault = Vault.get(request.project_id, vault_id)
        if not vault:
            abort(404)

        if file_id:
            return self._assign(vault, vault_id, file_id)

        file = vault.create_file()

        response.headers["Location"] = "files/%s" % file.file_id
        response.status_code = 201  # Created

    def _assign(self, vault, vault_id, file_id):

        f = vault.get_file(file_id)

        if not f:
            abort(404)

        # Fileid with an empty body will finalize the file.
        if not request.body:
            deuce.metadata_driver.finalize_file(request.project_id, vault_id,
                file_id)

            return

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

            if not deuce.metadata_driver.has_block(request.project_id,
                    vault_id, block_id):

                missing_blocks.append(block_id)

            deuce.metadata_driver.assign_block(request.project_id, vault_id,
               file_id, mapping['id'], mapping['offset'])

        return missing_blocks
