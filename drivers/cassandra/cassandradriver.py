
import importlib
import six
import uuid


CQL_ADD_VAULT = '''
    INSERT INTO vaults (projectid, vaultid)
    VALUES (%(projectid)s, %(vaultid)s)
'''

CQL_DELETE_VAULT = '''
    DELETE FROM vaults
    where projectid = %(projectid)s
    AND vaultid = %(vaultid)s
'''

CQL_GET_ALL_VAULTS = '''
    SELECT vaultid
    FROM vaults
    WHERE projectid = %(projectid)s
    AND vaultid >= %(vaultid)s
    ORDER BY vaultid
    LIMIT %(limit)s
'''


class CassandraDriver():

    def __init__(self):
        # Import the driver module.
        self.cassandra = importlib.import_module('tests.mock_cassandra')

        # Import the cluster submodule
        cluster_module = importlib.import_module(
            '{0}.cluster'.format('tests.mock_cassandra'))

        self._cluster = cluster_module.Cluster('127.0.0.1')

        deuce_keyspace = 'deucecnckeyspace'
        self._session = self._cluster.connect(deuce_keyspace)

    def _determine_limit(self, limit):
        """ Determines the limit based on user input """

        # Note: +1 is allowed here because it allows
        # the user to fetch one beyond to see if they
        # are at the end of the list
        max_returned_num = 2000
        if not limit:
            res = max_returned_num + 1
        else:
            res = min(max_returned_num + 1, limit)

        return res

    def add_vault(self, project_id, vault_id):
        """Creates a vault"""
        args = dict(
            projectid=project_id,
            vaultid=vault_id
        )
        res = self._session.execute(CQL_ADD_VAULT, args)
        return

    def delete_vault(self, project_id, vault_id):
        args = dict(
            projectid=project_id,
            vaultid=vault_id
        )
        self._session.execute(CQL_DELETE_VAULT, args)
        return

    def create_vaults_generator(self, project_id, marker=None, limit=None):
        args = dict(
            projectid=project_id,
            vaultid=marker or '0',
            limit=self._determine_limit(limit)
        )
        res = self._session.execute(CQL_GET_ALL_VAULTS, args)
        return [row[0] for row in res]

    def get_vault_statistics(self, vault_id):
        """Return the statistics on the vault.

        "param vault_id: The ID of the vault to gather statistics for"""
        res = {}

        args = dict(
            projectid=project_id,
            vaultid=vault_id
        )

        def __stats_query(cql_statement, default_value):
            result = self._session.execute(cql_statement, args)
            try:
                return result[0][0]

            except IndexError:  # pragma: no cover
                return default_value

        def __stats_get_vault_file_count():
            return __stats_query(CQL_GET_COUNT_ALL_FILES, 0)

        def __stats_get_vault_block_count():
            return __stats_query(CQL_GET_COUNT_ALL_BLOCKS, 0)

        # Add any statistics regarding files
        res['files'] = {}
        res['files']['count'] = __stats_get_vault_file_count()

        # Add any statistics regarding blocks
        res['blocks'] = {}
        res['blocks']['count'] = __stats_get_vault_block_count()

        # Add any statistics specific to the Cassandra backend
        res['internal'] = {}

        return res
