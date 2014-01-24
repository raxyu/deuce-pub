
from pecan import conf

from sqlite3 import Connection
import uuid

# SQL schemas. Note: the schema is versions
# in such a way that new instances always start
# with user version 1, then proceeed to upgrade
# through each version until we get to the latest.

schemas = list()

schemas.append ([
    """
    CREATE TABLE files
    (
        fileid TEXT NOT NULL PRIMARY KEY,
        finalized INTEGER NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE TABLE fileblocks
    (
        fileid TEXT NOT NULL,
        blockid TEXT NOT NULL,
        offset INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE blocks
    (
        blockid TEXT NOT NULL PRIMARY KEY,
        size INTEGER NOT null
    )
    """
]) # Version 1

CURRENT_DB_VERSION = len(schemas)

class SqliteStorageDriver(object):

    def __init__(self):
        self._dbfile = conf.metadata_driver.options.path
        self._conn = Connection(self._dbfile)

        self._do_migrate()

    def _get_user_version(self):
        res = self._conn.execute('pragma user_version')
        row = res.next()
        return row[0]

    def _set_user_version(self, version):
        # NOTE: for whatever reason, pragma's don't seem to
        # work with the built-in query formatter so
        # we just use string formatting here. This should be
        # OK since version is internally generated.
        self._conn.execute('pragma user_version=%d' % version)

    def _do_migrate(self):
        db_ver = self._get_user_version()

        for ver in range(db_ver, CURRENT_DB_VERSION):
            schema = schemas[db_ver]

            for query in schema:
                res = self._conn.execute(query)

            db_ver = db_ver + 1
            self._set_user_version(db_ver)

    def create_file(self):
        """Creates a file in the storage driver, returns the
        unique ID for this file"""

        file_id = str(uuid.uuid4())

        args = (file_id,)
        res = self._conn.execute('insert into files (fileid) values (?)', args)

        return file_id

