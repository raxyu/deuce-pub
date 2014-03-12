
from pecan import conf

import pymongo
from pymongo import MongoClient
from deuce.drivers.storage.metadata import MetadataStorageDriver


# SQL schemas. Note: the schema is versions
# in such a way that new instances always start
# with user version 1, then proceeed to upgrade
# through each version until we get to the latest.
#schemas = list()
#schemas.append()  # Version 1
#CURRENT_DB_VERSION = len(schemas)

class MongoDbStorageDriver(MetadataStorageDriver):

    def __init__(self):
        self._dbfile = conf.metadata_driver.mongodb.path
        self.client = MongoClient(conf.metadata_driver.mongodb.url)
        self._conn = self.client[self._dbfile]
        self._blocks = self._conn.blocks
        self._files = self._conn.files
        self._fileblocks = self._conn.fileblocks
        self._sys = self._conn.sys

        #self._do_migrate()

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

    '''
    def _get_user_version(self):
        #res = self._conn.execute('pragma user_version')
        #row = next(res)
        #return row[0]
        res = self._sys.find_one("userversion")
        if res is None:
            print ("YUDEBUG: mongodb get user version: ", 0)
            return 0
        ret = res.get("userversion")
        print ("YUDEBUG: mongodb get user version: ", ret)
        return ret

    def _set_user_version(self, version):
        print ("YUDEBUG: mongodb set user version: ", version)
        # NOTE: for whatever reason, pragma's don't seem to
        # work with the built-in query formatter so
        # we just use string formatting here. This should be
        # OK since version is internally generated.
        #self._conn.execute('pragma user_version=%d' % version)

        self._sys.insert({'userversion': version})

    def _do_migrate(self):
        db_ver = self._get_user_version()
        print ("YUDEBUG: mongodb do migrate: db_ver: ", db_ver)

        for ver in range(db_ver, CURRENT_DB_VERSION):
            print("YUDEBUG: got one ver: ", ver)
        #    schema = schemas[db_ver]

        #    for query in schema:
        #        res = self._conn.execute(query)

        #    db_ver = db_ver + 1
        #    self._set_user_version(db_ver)
    '''

    def create_file(self, project_id, vault_id, file_id):
        """Creates a new file with no blocks and no files"""
        args = {
            'projectid': project_id,
            'vaultid': vault_id,
            'fileid': file_id,
            'finalized': False
        }

        self._files.insert(args)

        return file_id

    def has_file(self, project_id, vault_id, file_id):
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
        args = {
            'projectid': project_id,
            'vaultid': vault_id,
            'fileid': file_id
        }

        self._files.remove(args)

    def finalize_file(self, project_id, vault_id, file_id):
        """Updates the files table to set a file to finalized. This function
        makes no assumptions about whether or not the file record actually
        exists"""

        args = {
            "projectid": project_id,
            "vaultid": vault_id,
            "fileid": file_id,
        }
        update_args = args.copy()
        update_args["finalized"] = True

        self._files.update(args, update_args, upsert=False)

    def get_file_data(self, project_id, vault_id, file_id):
        """Returns a tuple representing data for this file"""
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
        # Query the blocks table
        retval = False

        args = {
            'projectid': project_id,
            'vaultid': vault_id,
            'blockid': block_id
        }

        return self._blocks.find_one(args) is not None

    def create_block_generator(self, project_id, vault_id, marker=0, limit=0):
        args = {"projectid": project_id, "vaultid": vault_id,
            "blockid": {"$gte": marker}}

        limit = self._determine_limit(limit)

        return list(block['blockid'] for block in
            self._blocks.find(args).sort("blockid", 1).limit(limit))

    def create_file_generator(self, project_id, vault_id,
            marker=0, limit=0, finalized=True):
        limit = self._determine_limit(limit)

        args = {'projectid': project_id, 'vaultid': vault_id,
            'fileid': {"$gte": marker}, 'finalized': finalized}

        return list(retfile['fileid'] for retfile in
            self._files.find(args).sort("fileid", 1).limit(limit))

    def create_file_block_generator(self, project_id, vault_id, file_id,
            offset=0, limit=0):
        limit = self._determine_limit(limit)

        args = {'projectid': project_id, 'vaultid': vault_id,
            'fileid': file_id, 'offset': {"$gte": offset}}

        return [(retfile['blockid'], retfile['offset']) for retfile in
            self._fileblocks.find(args).sort("offset", 1).limit(limit)]

    def assign_block(self, project_id, vault_id, file_id, block_id, offset):
        # TODO(jdp): tweak this to support multiple assignments
        # TODO(jdp): check for overlaps in metadata
        args = {
            'projectid': project_id,
            'vaultid': vault_id,
            'fileid': file_id,
            'blockid': block_id,
            'offset': offset
        }
        self._fileblocks.update(args, args, upsert=True)

    def register_block(self, project_id, vault_id, block_id, blocksize):
        if not self.has_block(project_id, vault_id, block_id):
            args = {
                'projectid': project_id,
                'vaultid': vault_id,
                'blockid': block_id,
                'blocksize': blocksize
            }

            self._blocks.update(args, args, upsert=True)

    def unregister_block(self, project_id, vault_id, block_id):
        args = {
            'projectid': project_id,
            'vaultid': vault_id,
            'blockid': block_id
        }
        self._blocks.remove(args)
