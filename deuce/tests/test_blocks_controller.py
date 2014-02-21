import os
import hashlib
from random import randrange
import six
from unittest import TestCase
from deuce.tests import FunctionalTest


class TestBlocksController(FunctionalTest):

    def setUp(self):
        super(TestBlocksController, self).setUp()

        # Create a vault for us to work with
        vault_name = 'blocks_vault_test'
        self._vault_path = '/v1.0/{0}'.format(vault_name)
        self._blocks_path = '{0}/blocks'.format(self._vault_path)

        self._hdrs = {"X-Project-ID": "sample_project_id"}

        response = self.app.post(self._vault_path,
            headers=self._hdrs)

    def test_no_block_state(self):
        # Try listing the blocks. There should be none
        response = self.app.get(self._blocks_path, headers=self._hdrs)
        assert response.json_body == []

    def _calc_sha1(self, data):
        sha1 = hashlib.sha1()
        sha1.update(data)
        return sha1.hexdigest()

    def _get_block_path(self, blockid):
        return '{0}/{1}'.format(self._blocks_path, blockid)

    def test_get_all_with_trailing_slash(self):
        path = self._get_block_path('')

        response = self.app.get(path, headers=self._hdrs,
            expect_errors=True)

        assert response.status_int == 404

    def test_get_all_invalid_vault_id(self):
        path = '/v1.0/{0}/blocks'.format('bad_vault_id')
        response = self.app.get(path, headers=self._hdrs,
            expect_errors=True)
        assert response.status_int == 404

    def test_put_and_list(self):
        num_blocks = 5
        min_size = 1
        max_size = 2000

        block_sizes = [randrange(min_size, max_size) for x in
            range(0, num_blocks)]

        data = [os.urandom(x) for x in block_sizes]
        hashes = [self._calc_sha1(d) for d in data]

        block_data = zip(block_sizes, data, hashes)

        # Put each one of the generated blocks on the
        # size
        for size, data, sha1 in block_data:
            path = self._get_block_path(sha1)

            # NOTE: Very important to set the content-type
            # header. Otherwise pecan tries to do a UTF-8 test.
            headers = {
                "Content-Type": "application/binary",
                "Content-Length": str(size),
            }

            headers.update(self._hdrs)

            response = self.app.put(path, headers=headers,
                params=data)

        # Now list the contents
        response = self.app.get(self._blocks_path, headers=self._hdrs)
        res = response.json_body

        assert isinstance(res, list)
        assert len(res) == len(hashes)

        for h in hashes:
            assert h in res

        for h in res:
            assert h in hashes

        # ask for some blocks from the system
        params = {'marker': 0, 'limit': 4}
        response = self.app.get(self._blocks_path, params=params, headers=self._hdrs)
        result = response.json_body
        assert len(result) == 4

        # ask for the rest blocks from the system
        params = {'marker': result[-1], 'limit': 8}
        response = self.app.get(self._blocks_path, params=params, headers=self._hdrs)
        result = response.json_body
        assert len(result) == 1

        # TODO: blocks of a file


        # Try to get some blocks that don't exist. This should
        # result in 404s
        bad_block_ids = ['ajeoijwoefij23oj', '234234234223', '2342234']

        for bad_id in bad_block_ids:
            path = self._get_block_path(bad_id)

            response = self.app.get(path, headers=self._hdrs,
                expect_errors=True)

            assert response.status_int == 404

        # Now try to fetch each block, and compare against
        # the original block data
        for sha1 in res:
            path = self._get_block_path(sha1)
            response = self.app.get(path, headers=self._hdrs)
            assert response.status_int == 200

            bindata = response.body

            # Now re-hash the data, the data that
            # was returned should match the original
            # sha1
            z = hashlib.sha1()
            z.update(bindata)
            assert z.hexdigest() == sha1
