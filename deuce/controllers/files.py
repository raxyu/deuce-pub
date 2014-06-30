from pecan import conf, expose, request, response
from pecan.core import abort
from pecan.rest import RestController

import deuce
from deuce.controllers.fileblocks import FileBlocksController
from deuce.controllers.validation import *

from deuce.model import Vault, Block, File
from deuce.util import FileCat
from deuce.util import set_qs

# Standard rule for marker-limit semantics
# for the listing files


class FilesController(RestController):

    blocks = FileBlocksController()

    @expose('json')
    @validate(vault_id=VaultGetRule, marker=FileMarkerRule, limit=LimitRule)
    def get_all(self, vault_id):

        vault = Vault.get(request.project_id, vault_id)

        if not vault:
            abort(404)

        inmarker = request.params.get('marker')
        limit = int(request.params.get('limit',
           conf.api_configuration.max_returned_num))

        # The +1 is to fetch one past the user's
        # requested limit so that we can determine
        # if the list was truncated or not
        files = vault.get_files(inmarker, limit + 1)

        resp = list(files)

        # Note: the list may not actually be truncated
        truncated = len(resp) == limit + 1

        outmarker = resp.pop().file_id if truncated else None

        if outmarker:
            query_args = {'marker': outmarker}
            query_args['limit'] = limit

            returl = set_qs(request.url, query_args)

            response.headers["X-Next-Batch"] = returl
            response.headers["Transaction-ID"] = request.context.request_id

        return resp

    @expose()
    @validate(vault_id=VaultGetRule, file_id=FileGetRule)
    def get_one(self, vault_id, file_id):
        """Fetches, re-assembles and streams a single
        file out of Deuce"""

        vault = Vault.get(request.project_id, vault_id)

        if not vault:
            abort(404)

        f = vault.get_file(file_id)

        if not f:
            abort(404)

        block_gen = deuce.metadata_driver.create_file_block_generator(
            request.project_id, vault_id, file_id)

        objs = deuce.storage_driver.create_blocks_generator(
            request.project_id, vault_id, block_gen)

        response.body_file = FileCat(objs)
        response.status_code = 204

    @expose('json')
    @validate(vault_id=VaultPutRule, file_id=FilePostRuleNoneOk)
    def post(self, vault_id, file_id=None):
        """Initializes a new file. The location of
        the new file is returned in the Location
        header
        """
        vault = Vault.get(request.project_id, vault_id)

        # caller tried to post to a vault that
        # does not exist
        if not vault:
            abort(400)

        if file_id is not None:
            return self._assign(vault, vault_id, file_id)

        file = vault.create_file()

        response.headers["Location"] = "files/%s" % file.file_id
        response.headers["Transaction-ID"] = request.context.request_id
        response.status_code = 201  # Created

    def _assign(self, vault, vault_id, file_id):

        f = vault.get_file(file_id)

        if not f:
            abort(404)

        if not request.body:
            try:
                # Fileid with an empty body will finalize the file.
                filesize = request.headers['Filesize'] if 'Filesize' \
                    in request.headers.keys() else 0
                res = deuce.metadata_driver.finalize_file(request.project_id,
                    vault_id, file_id, filesize)
                return res
            except Exception as e:
                # There are gaps or overlaps in blocks of the file
                # The list of errors returns
                # NEED RETURN 413
                details = str(e)
                response.status_code = 413
                return details

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
            offset = int(mapping['offset'])

            if not deuce.metadata_driver.has_block(request.project_id,
                    vault_id, block_id):

                missing_blocks.append(block_id)

            deuce.metadata_driver.assign_block(request.project_id, vault_id,
                file_id, mapping['id'], mapping['offset'])

        return missing_blocks
