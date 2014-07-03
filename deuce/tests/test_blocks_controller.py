from pecan import conf
import os
import hashlib
from random import randrange
import six
from six.moves.urllib.parse import urlparse, parse_qs
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

        self.block_list = []
        self.total_block_num = 0

    def test_no_block_state(self):
        # Try listing the blocks. There should be none
        response = self.app.get(self._blocks_path, headers=self._hdrs)
        assert response.json_body == []

    def _create_block_id(data=None):
        """Creates a block ID for testing purpose"""
        sha1 = hashlib.sha1()
        sha1.update(data if data is None else os.urandom(2048))
        return sha1.hexdigest()

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

        self.assertEqual(response.status_int, 404)

    def test_invalid_block_id(self):
        path = self._blocks_path + '/invalid_block_id'

        response = self.app.put(path, headers=self._hdrs,
                                expect_errors=True)

        self.assertEqual(response.status_int, 400)

    def test_with_bad_marker_and_limit(self):
        block_list = self.helper_create_blocks(num_blocks=5)

        # Now try to get a list of blocks to ensure that they'e
        # there.
        resp = self.app.get(self._blocks_path, headers=self._hdrs)
        all_blocks = resp.json_body
        self.assertEqual(len(all_blocks), 5)
        self.assertEqual(resp.status_code, 200)

        # Now check the first one. We're going to send the marker
        # and limit and we should get just one

        args = dict(limit=1)

        resp = self.app.get(self._blocks_path, params=args,
                            headers=self._hdrs)

        self.assertEqual(len(resp.json_body), 1)
        self.assertEqual(resp.status_code, 200)

        # The block here should match the first block
        # in the list.

        self.assertEqual(all_blocks[0], resp.json_body[0])

        # Now try with a bad limit
        args = dict(limit='blah')

        resp = self.app.get(self._blocks_path, params=args,
                            headers=self._hdrs, expect_errors=True)

        self.assertEqual(resp.status_code, 404)

        # Now try a bad marker
        args = dict(marker='blah')
        resp = self.app.get(self._blocks_path, params=args,
                            headers=self._hdrs, expect_errors=True)

        self.assertEqual(resp.status_code, 404)

    def test_put_and_list(self):

        # Test None block_id
        path = '{0}/'.format(self._blocks_path)
        data = os.urandom(100)
        headers = {
            "Content-Type": "application/binary",
            "Content-Length": "100"
        }
        response = self.app.put(path, headers=headers,
            params=data, expect_errors=True)
        # Returns from BlockPutRuleNoneOk validation.
        self.assertEqual(response.status_code, 400)

        # Create 5 blocks
        block_list = self.helper_create_blocks(num_blocks=5)
        self.total_block_num = 5
        self.block_list += block_list

        # List all.
        next_batch_url = self.helper_get_blocks(self._blocks_path,
            0, 0, assert_ret_url=False, assert_data_len=5,
            repeat=False, exam_block_data=True)

        # List some blocks
        next_batch_url = self.helper_get_blocks(self._blocks_path,
                                                0, 4, True, 4, False)

        # List the rest blocks
        marker = parse_qs(urlparse(next_batch_url).query)['marker']
        next_batch_url = self.helper_get_blocks(self._blocks_path,
                                                marker, 8, False, 1, False)

        # Create more blocks.
        num_blocks = int(1.5 * conf.api_configuration.max_returned_num)
        block_list = self.helper_create_blocks(num_blocks=num_blocks)
        self.block_list += block_list
        self.total_block_num += num_blocks

        # List from 0; use conf limit
        next_batch_url = self.helper_get_blocks(self._blocks_path,
            0, 0, assert_ret_url=True,
            assert_data_len=conf.api_configuration.max_returned_num,
            repeat=False)

        # List from 0; Use conf limit, repeat to the end.
        next_batch_url = self.helper_get_blocks(self._blocks_path,
            0, 0, assert_ret_url=False,
            assert_data_len=self.total_block_num, repeat=True)

        # Try to get some blocks that don't exist. This should
        # result in 404s
        bad_block_ids = [self._create_block_id() for _ in range(0, 5)]

        for bad_id in bad_block_ids:
            path = self._get_block_path(bad_id)

            response = self.app.get(path, headers=self._hdrs,
                                    expect_errors=True)

            self.assertEqual(response.status_int, 404)

    def helper_create_blocks(self, num_blocks):
        min_size = 1
        max_size = 2000

        block_sizes = [randrange(min_size, max_size) for x in
                       range(0, num_blocks)]

        data = [os.urandom(x) for x in block_sizes]
        block_list = [self._calc_sha1(d) for d in data]

        block_data = zip(block_sizes, data, block_list)

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

        return block_list

    def helper_get_blocks(self, path, marker, limit, assert_ret_url,
              assert_data_len, repeat=False, exam_block_data=False):

        resp_block_list = []

        params = dict()

        if limit != 0:
            params['limit'] = limit

        if marker != 0:
            params['marker'] = marker

        while True:
            response = self.app.get(path,
                                    params=params, headers=self._hdrs)

            next_batch_url = response.headers.get("X-Next-Batch")

            resp_block_list += response.json_body
            assert isinstance(response.json_body, list)

            if not repeat:
                self.assertEqual(not next_batch_url, not assert_ret_url)
                self.assertEqual(len(resp_block_list), assert_data_len)
                for h in resp_block_list:
                    assert h in self.block_list
                if assert_data_len == -1 or \
                        assert_data_len == self.total_block_num:
                    for h in self.block_list:
                        assert h in resp_block_list
                if exam_block_data:
                    self.helper_exam_block_data(resp_block_list)
                return next_batch_url
            if not next_batch_url:
                break
            params['marker'] = \
                parse_qs(urlparse(next_batch_url).query)['marker']
        assert len(resp_block_list) == assert_data_len
        for h in resp_block_list:
            assert h in self.block_list
        for h in self.block_list:
            assert h in resp_block_list
        # By default exam blocks if fetching all blocks
        self.helper_exam_block_data(resp_block_list)

    def helper_exam_block_data(self, block_list):
        # Now try to fetch each block, and compare against
        # the original block data
        for sha1 in block_list:
            path = self._get_block_path(sha1)
            response = self.app.get(path, headers=self._hdrs)
            assert response.status_int == 204

            bindata = response.body

            # Now re-hash the data, the data that
            # was returned should match the original
            # sha1
            z = hashlib.sha1()
            z.update(bindata)
            assert z.hexdigest() == sha1
