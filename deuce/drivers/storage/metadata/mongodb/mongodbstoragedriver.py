
from pecan import conf

from sqlite3 import Connection
from deuce.drivers.storage.metadata import MetadataStorageDriver

# SQL schemas. Note: the schema is versions
# in such a way that new instances always start
# with user version 1, then proceeed to upgrade
# through each version until we get to the latest.

schemas = list()

schemas.append([
    """
    CREATE TABLE files
    (
        projectid TEXT NOT NULL,
        vaultid TEXT NOT NULL,
        fileid TEXT NOT NULL,
        finalized INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(projectid, vaultid, fileid)
    )
    """,
    """
    CREATE TABLE fileblocks
    (
        projectid TEXT NOT NULL,
        vaultid TEXT NOT NULL,
        fileid TEXT NOT NULL,
        blockid TEXT NOT NULL,
        offset INTEGER NOT NULL,
        UNIQUE (vaultid, fileid, blockid, offset)
    )
    """,
    """
    CREATE TABLE blocks
    (
        projectid TEXT NOT NULL,
        vaultid TEXT NOT NULL,
        blockid TEXT NOT NULL,
        size INTEGER NOT NULL,
        PRIMARY KEY(projectid, vaultid, blockid)
    )
    """
])  # Version 1

CURRENT_DB_VERSION = len(schemas)

SQL_CREATE_FILE = '''
    INSERT INTO files (projectid, vaultid, fileid)
    VALUES (:projectid, :vaultid, :fileid)
'''

SQL_GET_FILE = '''
    SELECT finalized
    FROM files
    WHERE projectid=:projectid AND vaultid=:vaultid AND fileid=:fileid
'''

SQL_DELETE_FILE = '''
    DELETE FROM files
    where projectid=:projectid
    AND vaultid=:vaultid
    AND fileid=:fileid
'''

SQL_GET_FILE_BLOCKS = '''
    SELECT blockid, offset
    FROM fileblocks
    WHERE projectid=:projectid
    AND vaultid=:vaultid
    AND fileid=:fileid
    AND offset>=:offset
    ORDER BY offset
    LIMIT :limit
'''

SQL_GET_ALL_BLOCKS = '''
    SELECT blockid
    FROM blocks
    WHERE projectid=:projectid
    AND vaultid=:vaultid
    AND blockid>=:marker
    order by blockid
    LIMIT :limit
'''

SQL_GET_ALL_FILES = '''
    SELECT fileid
    FROM files
    WHERE projectid=:projectid
    AND vaultid=:vaultid
    AND fileid>=:marker
    AND finalized=:finalized
    order by fileid
    LIMIT :limit
'''

SQL_FINALIZE_FILE = '''
    update files
    set finalized=1
    where projectid=:projectid
    AND fileid=:fileid
    AND vaultid=:vaultid
'''

SQL_ASSIGN_BLOCK_TO_FILE = '''
    INSERT OR REPLACE INTO fileblocks
    (projectid, vaultid, fileid, blockid, offset)
    VALUES (:projectid, :vaultid, :fileid, :blockid, :offset)
'''

SQL_REGISTER_BLOCK = '''
    INSERT INTO blocks
    (projectid, vaultid, blockid, size)
    values (:projectid, :vaultid, :blockid, :blocksize)
'''

SQL_UNREGISTER_BLOCK = '''
    DELETE FROM blocks
    where projectid=:projectid AND blockid=:blockid
'''

SQL_HAS_BLOCK = '''
    SELECT count(*)
    FROM blocks
    WHERE projectid=:projectid
    AND blockid = :blockid
    AND vaultid = :vaultid
'''


class MongoDbStorageDriver(MetadataStorageDriver):

    def __init__(self):
        #print ("YUDEBUG: mongodb init")
        self._dbfile = conf.metadata_driver.options.path
        self._conn = Connection(self._dbfile)

        self._do_migrate()

    def _get_user_version(self):
        #res = self._conn.execute('pragma user_version')
        #row = next(res)
        #return row[0]
        return 0

    def _set_user_version(self, version):
        # NOTE: for whatever reason, pragma's don't seem to
        # work with the built-in query formatter so
        # we just use string formatting here. This should be
        # OK since version is internally generated.
        #self._conn.execute('pragma user_version=%d' % version)
        pass

    def _do_migrate(self):
        db_ver = self._get_user_version()

        #for ver in range(db_ver, CURRENT_DB_VERSION):
        #    schema = schemas[db_ver]

        #    for query in schema:
        #        res = self._conn.execute(query)

        #    db_ver = db_ver + 1
        #    self._set_user_version(db_ver)

    def create_file(self, project_id, vault_id, file_id):
        """Creates a new file with no blocks and no files"""
        #args = {
        #    'projectid': project_id,
        #    'vaultid': vault_id,
        #    'fileid': file_id
        #}

        #res = self._conn.execute(SQL_CREATE_FILE, args)
        #self._conn.commit()

        # TODO: check that one row was inserted
        return file_id

    def has_file(self, project_id, vault_id, file_id):
        #args = {
        #    'projectid': project_id,
        #    'vaultid': vault_id,
        #    'fileid': file_id
        #}

        #res = self._conn.execute(SQL_GET_FILE, args)

        #try:
        #    row = next(res)
        #    return True
        #except StopIteration:
        #    return False
        return True

    def is_finalized(self, project_id, vault_id, file_id):
        #args = {
        #    'projectid': project_id,
        #    'vaultid': vault_id,
        #    'fileid': file_id
        #}

        #res = self._conn.execute(SQL_GET_FILE, args)

        #try:
        #    row = next(res)
        #    return row[0] == 1
        #except StopIteration:
        #    return False
        return True

    def delete_file(self, project_id, vault_id, file_id):
        #args = {
        #    'projectid': project_id,
        #    'vaultid': vault_id,
        #    'fileid': file_id
        #}

        #res = self._conn.execute(SQL_DELETE_FILE, args)
        #self._conn.commit()
        pass

    def finalize_file(self, project_id, vault_id, file_id):
        """Updates the files table to set a file to finalized. This function
        makes no assumptions about whether or not the file record actually
        exists"""

        #args = {
        #    'projectid': project_id,
        #    'vaultid': vault_id,
        #    'fileid': file_id
        #}

        #res = self._conn.execute(SQL_FINALIZE_FILE, args)
        #self._conn.commit()
        pass

    def get_file_data(self, project_id, vault_id, file_id):
        """Returns a tuple representing data for this file"""
        #args = {
        #    'projectid': project_id,
        #    'vaultid': vault_id,
        #    'fileid': file_id
        #}

        #res = self._conn.execute(SQL_GET_FILE, args)

        #try:
        #    row = next(res)
        #except StopIteration:
        #    raise Exception("No such file: {0}".format(file_id))

        #return row
        pass

    def has_block(self, project_id, vault_id, block_id):
        # Query the blocks table
        #retval = False

        #args = {
        #    'projectid': project_id,
        #    'vaultid': vault_id,
        #    'blockid': block_id
        #}

        #res = self._conn.execute(SQL_HAS_BLOCK, args)

        #cnt = next(res)
        #return cnt[0] > 0
        pass

    def create_block_generator(self, project_id, vault_id, marker=0, limit=0):
        #args = {'projectid': project_id, 'vaultid': vault_id}
        #args['limit'] = limit + 1 \
        #    if limit != 0 \
        #    and (int(limit) <= conf.api_configuration.max_returned_num) \
        #    else conf.api_configuration.max_returned_num + 1
        #args['marker'] = marker

        #query = SQL_GET_ALL_BLOCKS
        #res = self._conn.execute(query, args)
        #res = list(row[0] for row in res)

        #if len(res) == args['limit']:
        #    #Full length.
        #    rets = res[:-1]
        #    return rets, res[len(res) - 1]
        #else:
        #    #End of the records.
        #    return res, None
        pass

    def create_file_generator(self, project_id, vault_id,
            marker=0, limit=0, finalized=True):
        #args = {'projectid': project_id, 'vaultid': vault_id}
        #args['limit'] = limit + 1 \
        #    if limit != 0 \
        #    and (int(limit) <= conf.api_configuration.max_returned_num) \
        #    else conf.api_configuration.max_returned_num + 1
        #args['marker'] = marker
        #args['finalized'] = finalized

        #query = SQL_GET_ALL_FILES
        #res = self._conn.execute(query, args)
        #res = list(row[0] for row in res)

        #if len(res) == args['limit']:
        #    #Full length.
        #    rets = res[:-1]
        #    return rets, res[len(res) - 1]
        #else:
        #    #End of the records.
        #    return res, None
        pass

    def create_file_block_generator(self, project_id, vault_id, file_id,
            offset=0, limit=0):

        #args = {'projectid': project_id, 'vaultid': vault_id}
        #args['limit'] = limit + 1 \
        #    if limit != 0 \
        #    and (int(limit) <= conf.api_configuration.max_returned_num) \
        #    else conf.api_configuration.max_returned_num + 1
        #args['fileid'] = file_id
        #args['offset'] = offset

        #query = SQL_GET_FILE_BLOCKS
        #query_res = self._conn.execute(query, args)
        #res = list((row[0], row[1]) for row in query_res)

        #if len(res) == args['limit']:
        #    #Full length.
        #    rets = res[:-1]
        #    return (row[0] for row in rets), res[len(res) - 1][1]
        #else:
        #    #End of the records.
        #    return (row[0] for row in res), None
        pass

    def assign_block(self, project_id, vault_id, file_id, block_id, offset):
        # TODO(jdp): tweak this to support multiple assignments
        # TODO(jdp): check for overlaps in metadata
        #args = {
        #    'projectid': project_id,
        #    'vaultid': vault_id,
        #    'fileid': file_id,
        #    'blockid': block_id,
        #    'offset': offset
        #}

        #res = self._conn.execute(SQL_ASSIGN_BLOCK_TO_FILE, args)
        #self._conn.commit()
        pass

    def register_block(self, project_id, vault_id, block_id, blocksize):
        #if not self.has_block(project_id, vault_id, block_id):
        #    args = {
        #        'projectid': project_id,
        #        'vaultid': vault_id,
        #        'blockid': block_id,
        #        'blocksize': blocksize
        #    }

        #    res = self._conn.execute(SQL_REGISTER_BLOCK, args)
        #    self._conn.commit()
        pass

    def unregister_block(self, project_id, vault_id, block_id):
        #args = {
        #    'projectid': project_id,
        #    'vaultid': vault_id,
        #    'blockid': block_id
        #}

        #res = self._conn.execute(SQL_UNREGISTER_BLOCK, args)
        #self._conn.commit()
        pass
