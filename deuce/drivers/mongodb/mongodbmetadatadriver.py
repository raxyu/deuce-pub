
from pecan import conf

import deuce

import sys
import importlib
import atexit


import itertools
from deuce.drivers.metadatadriver import MetadataStorageDriver, \
    GapError, OverlapError


class MongoDbStorageDriver(MetadataStorageDriver):

    def __init__(self):

        self._dbfile = conf.metadata_driver.mongodb.path

        self.mongo_pack = importlib.import_module(
            conf.metadata_driver.mongodb.db_module)

        self.client = getattr(self.mongo_pack, 'MongoClient')(
            conf.metadata_driver.mongodb.url)

        self._db = self.client[self._dbfile]
        self._blocks = self._db.blocks
        self._files = self._db.files
        self._fileblocks = self._db.fileblocks
        # Maintain the document size less than the system maximun.
        self._docnum = int(conf.metadata_driver.mongodb.maxFileBlockSegNum)

    def _determine_limit(self, limit):
        """ Determines the limit based on user input """

        # Note: +1 is allowed here because it allows
        # the user to fetch one beyond to see if they
        # are at the end of the list
        if not limit:
            res = conf.api_configuration.max_returned_num + 1
        else:
            res = min(conf.api_configuration.max_returned_num + 1, limit)

        return res

    def create_file(self, project_id, vault_id, file_id):
        """Creates a new FILES with no blocks and no files"""
        args = {
            'projectid': project_id,
            'vaultid': vault_id,
            'fileid': file_id,
            'finalized': False,
            'seq': 0,
            'blocks': []
        }

        self._files.insert(args)

        return file_id

    def has_file(self, project_id, vault_id, file_id):
        self._files.ensure_index([('projectid', 1),
            ('vaultid', 1), ('fileid', 1)])
        args = {
            'projectid': project_id,
            'vaultid': vault_id,
            'fileid': file_id
        }

        res = self._files.find_one(args)

        if res is None:
            return False
        return True

    def is_finalized(self, project_id, vault_id, file_id):
        self._files.ensure_index([('projectid', 1),
            ('vaultid', 1), ('fileid', 1)])
        args = {
            'projectid': project_id,
            'vaultid': vault_id,
            'fileid': file_id
        }
        res = self._files.find_one(args)

        if res is not None:
            return res.get("finalized")
        return False

    def delete_file(self, project_id, vault_id, file_id):
        self._files.ensure_index([('projectid', 1),
            ('vaultid', 1), ('fileid', 1)])
        args = {
            'projectid': project_id,
            'vaultid': vault_id,
            'fileid': file_id
        }

        self._files.remove(args)

    def finalize_file(self, project_id, vault_id, file_id, file_size=None):
        """Updates FILES to set a file to finalized. This function
        makes no assumptions about whether or not the file record actually
        exists"""
        self._files.ensure_index([('projectid', 1),
            ('vaultid', 1), ('fileid', 1)])

        find_args = {
            "projectid": project_id,
            "vaultid": vault_id,
            "fileid": file_id
        }

        # There could be multiple document for the same file.
        # Need work on one single document a time.
        resfile = self._files.find(find_args)
        if resfile.count() < 1:
            return

        # Check for gap and overlap.
        fileblocks_list = list(self._fileblocks.
            find(find_args).sort('offset', 1))
        expected_offset = 0

        for item in fileblocks_list:
            offset = item['offset']
            blockid = item['blockid']

            if offset == expected_offset:
                blockdata = self.get_block_data(project_id, vault_id, blockid)
                expected_offset += int(blockdata['blocksize'])
            elif offset < expected_offset:  # Overlap scenario
                raise OverlapError(project_id, vault_id, file_id,
                    blockid, startpos=offset, endpos=expected_offset)
            else:
                raise GapError(project_id, vault_id, file_id,
                    startpos=expected_offset, endpos=offset)

        # Now we must check the very last block
        if file_size and file_size != expected_offset:
            if expected_offset < file_size:
                raise GapError(project_id, vault_id, file_id, expected_offset,
                    file_size)
            else:
                assert expected_offset > file_size

                raise OverlapError(project_id, vault_id, file_id, file_size,
                    startpos=file_size, endpos=expected_offset)

        filerec_id = list(resfile)[0].get('_id')

        # Chop up FILEBLOCKS list, and save to block chunks
        # in FILES.
        starts = 0
        pageseq = 0
        Finished = False
        self._files.update({'_id': filerec_id},
            {"$set": {"finalized": True}},
            upsert=False)

        # This number is an arbitrary reading segment size defined
        # in config.py.
        chunk_num = conf.metadata_driver.mongodb.FileBlockReadSegNum
        while True:

            # Cooking one chunk page
            docsize = 0
            blocks = []
            block_cnt = 0

            while True:
                # Read next segment from FILEBLOCKS
                blocks = [{'blockid': block['blockid'],
                    'offset': block['offset']}
                    for block in
                    self._fileblocks.find(find_args).
                        sort('offset', 1).
                        # Always fetch one more, if returns one more,
                        # there'll be more rounds of fetch.
                        limit(chunk_num + 1).
                        skip(starts)]
                if not blocks:
                    Finished = True
                    break

                blocks_len = len(blocks)

                if blocks_len < chunk_num + 1:
                    # End of the list.
                    block_cnt += blocks_len
                    Finished = True
                else:
                    # There is more than a full segment.
                    blocks = blocks[:-1]
                    starts += blocks_len - 1
                    block_cnt += blocks_len - 1

                # Add the segment to the embedded document.
                self._files.update({'_id': filerec_id}, {"$push":
                    {'blocks': {'$each': blocks}}},
                    upsert=False)

                # Monitor the size of the document.
                docsize += blocks_len
                if docsize > self._docnum or Finished:
                    break

            if Finished:
                break

            # Add another document for more blocks.
            pageseq += 1
            ins_args = {
                "projectid": project_id,
                "vaultid": vault_id,
                "fileid": file_id,
                "finalized": True,
                "seq": pageseq,
                "blocks": [],
            }
            filerec_id = self._files.insert(ins_args)

        # Clean up FILEBLOCKS, delete all old block records
        self._fileblocks.remove(find_args)

    def get_file_data(self, project_id, vault_id, file_id):
        """Returns a tuple representing data for this file"""
        self._files.ensure_index([('projectid', 1),
            ('vaultid', 1), ('fileid', 1)])
        args = {
            'projectid': project_id,
            'vaultid': vault_id,
            'fileid': file_id
        }

        res = self._files.find_one(args)

        if res is None:
            raise Exception("No such file: {0}".format(file_id))

        return [res.get("finalized")]

    def has_block(self, project_id, vault_id, block_id):
        # Query BLOCKS for the block
        self._blocks.ensure_index([('projectid', 1),
            ('vaultid', 1), ('blockid', 1)])
        retval = False

        args = {
            'projectid': project_id,
            'vaultid': vault_id,
            'blockid': str(block_id)
        }

        return self._blocks.find_one(args) is not None

    def get_block_data(self, project_id, vault_id, block_id):
        """Returns the blocksize for this block"""
        self._blocks.ensure_index([('projectid', 1),
            ('vaultid', 1), ('blockid', 1)])
        retval = False

        args = {
            'projectid': project_id,
            'vaultid': vault_id,
            'blockid': str(block_id)
        }

        return self._blocks.find_one(args)

    def create_block_generator(self, project_id,
            vault_id, marker=None, limit=None):
        self._blocks.ensure_index([('projectid', 1),
            ('vaultid', 1), ('blockid', 1)])
        args = {
            "projectid": project_id,
            "vaultid": vault_id
        }
        if marker is not None:
            args["blockid"] = {"$gte": str(marker)}

        limit = self._determine_limit(limit)

        return list(block['blockid'] for block in
            self._blocks.find(args).sort('blockid', 1).limit(limit))

    def create_file_generator(self, project_id, vault_id,
            marker=None, limit=None, finalized=True):
        self._files.ensure_index([('projectid', 1),
            ('vaultid', 1), ('fileid', 1)])
        limit = self._determine_limit(limit)

        args = dict()
        if marker:
            args = {'projectid': project_id, 'vaultid': vault_id,
                'fileid': {"$gte": marker}, 'finalized': finalized}
        else:
            args = {'projectid': project_id, 'vaultid': vault_id,
                'finalized': finalized}

        return list(retfile['fileid'] for retfile in
            self._files.find(args).sort("fileid", 1).limit(limit))

    def create_file_block_generator(self, project_id, vault_id, file_id,
            offset=None, limit=None):
        self._files.ensure_index([('projectid', 1),
            ('vaultid', 1), ('fileid', 1), ('seq', 1)])
        limit = self._determine_limit(limit)
        search_offset = offset if offset else 0
        blocks = []
        blockset = []

        args = {'projectid': project_id, 'vaultid': vault_id,
            'fileid': file_id}

        # This aggregation searches all embedded documents in FILES
        # cross different documents,
        # from the given start point,
        # for the limit number,
        # and sorted by the block offset.

        resblocks = self._files.aggregate(
            [{'$match': args},
            {'$sort': {"seq": 1}},
            {'$unwind': '$blocks'},
            {'$match': {'blocks.offset': {"$gte": search_offset}}},
            {'$limit': limit},
            {'$group': {"_id": "$_id",
                "blocks": {
                    "$push": {
                        "blockid": "$blocks.blockid",
                        "offset": "$blocks.offset"}}}},
            {'$sort': {"blocks.offset": 1}}])

        resblocks = resblocks.get('result')

        # The resblocks is a list of lists; Flat it.
        blockset = list(itertools.chain.from_iterable(
            (resset.get('blocks') for resset in resblocks)))

        if len(blockset) < 1:
            return blockset

        num_blocks = len(blockset)
        blocks = list((block.get('blockid'),
            block.get('offset')) for block in blockset)

        return blocks

    def assign_block(self, project_id, vault_id, file_id, block_id, offset):
        # TODO(jdp): tweak this to support multiple assignments
        # TODO(jdp): check for overlaps in metadata
        self._files.ensure_index([('projectid', 1),
            ('vaultid', 1), ('fileid', 1)])
        args = {
            'projectid': project_id,
            'vaultid': vault_id,
            'fileid': file_id,
            'blockid': block_id,
            'offset': offset
        }

        self._fileblocks.update(args, args, upsert=True)
        # Ordered in pymongo.ASCENDING.
        self._fileblocks.ensure_index([("projectid", 1),
            ("vaultid", 1),
            ("fileid", 1),
            ("blockid", 1)])

    def register_block(self, project_id, vault_id, block_id, blocksize):
        if not self.has_block(project_id, vault_id, block_id):
            args = {
                'projectid': project_id,
                'vaultid': vault_id,
                'blockid': str(block_id),
                'blocksize': blocksize
            }

            self._blocks.update(args, args, upsert=True)

    def unregister_block(self, project_id, vault_id, block_id):
        self._blocks.ensure_index([('projectid', 1),
            ('vaultid', 1), ('blockid', 1)])
        args = {
            'projectid': project_id,
            'vaultid': vault_id,
            'blockid': str(block_id)
        }
        self._blocks.remove(args)
