from tests.api.utils import base

import ddt
import os
import sha


class TestListBlock(base.TestBase):

    def setUp(self):
        super(TestListBlock, self).setUp()
        self.createEmptyVault()

    def test_list_blocks_empty_vault(self):
        """List blocks for an empty vault"""

        resp = self.client.list_of_blocks(self.vaultname)
        self.assertEqual(200, resp.status_code,
                         'Status code for listing all blocks is'
                         ' {0}'.format(resp.status_code))
        self.validate_headers(resp.headers, json=True)
        self.assertListEqual([], resp.json(),
                             'Response to List Blocks for an empty vault '
                             'should be an empty list []')

    def tearDown(self):
        super(TestListBlock, self).tearDown()
        self.client.delete_vault(self.vaultname)


@ddt.ddt
class TestUploadBlocks(base.TestBase):

    def setUp(self):
        super(TestUploadBlocks, self).setUp()
        self.createEmptyVault()

    @ddt.data(1, 100, 10000, 30720, 61440)
    def test_upload_block(self, value):
        """Upload a block to a vault"""

        block_data = os.urandom(value)
        self.blockid = sha.new(block_data).hexdigest()
        resp = self.client.upload_block(self.vaultname, self.blockid,
                                        block_data)
        self.assertEqual(201, resp.status_code,
                         'Status code for uploading a block is '
                         '{0}'.format(resp.status_code))
        self.validate_headers(resp.headers)
        self.assertEqual(0, len(resp.content),
                         'Response Content was not empty. Content: '
                         '{0}'.format(resp.content))

    def tearDown(self):
        super(TestUploadBlocks, self).tearDown()
        self.client.delete_vault(self.vaultname)


class TestBlockUploaded(base.TestBase):

    def setUp(self):
        super(TestBlockUploaded, self).setUp()
        self.createEmptyVault()
        self.uploadBlock()

    def test_list_one_block(self):
        """List a single block"""

        resp = self.client.list_of_blocks(self.vaultname)
        self.assertEqual(200, resp.status_code,
                         'Status code for listing all blocks is '
                         '{0}'.format(resp.status_code))
        self.validate_headers(resp.headers, json=True)
        self.assertListEqual([self.blockid], resp.json(),
                             'Response for List Blocks should have 1 item')

    def test_get_one_block(self):
        """Get an individual block"""

        # TODO
        self.skipTest('Skipping. Currently fails because content-type '
                      'header returned is text/html')
        resp = self.client.get_block(self.vaultname, self.blockid)
        self.assertEqual(200, resp.status_code,
                         'Status code for getting data of a block is '
                         '{0}'.format(resp.status_code))
        self.validate_headers(resp.headers, binary=True)
        self.assertEqual(resp.content, self.block_data,
                         'Block data returned does not match block uploaded')

    def test_delete_block(self):
        """Delete one block"""

        # TODO
        self.skipTest('Skipping. Functionality not implemented')
        resp = self.client.delete_block(self.vaultname, self.blockid)
        self.assertEqual(204, resp.status_code,
                         'Status code for deleting a block is '
                         '{0}'.format(resp.status_code))
        self.assertEqual(0, len(resp.content))

    def tearDown(self):
        super(TestBlockUploaded, self).tearDown()
        self.client.delete_vault(self.vaultname)


@ddt.ddt
class TestListBlocks(base.TestBase):

    def setUp(self):
        super(TestListBlocks, self).setUp()
        self.createEmptyVault()
        for _ in range(20):
            self.uploadBlock()
        self.blockids = []
        for block in self.blocks:
            self.blockids.append(block.Id)

    def test_list_multiple_blocks(self):
        """List multiple blocks (20)"""

        resp = self.client.list_of_blocks(self.vaultname)
        self.assertEqual(200, resp.status_code,
                         'Status code for listing all blocks is '
                         '{0}'.format(resp.status_code))
        self.validate_headers(resp.headers, json=True)
        self.assertListEqual(sorted(self.blockids), sorted(resp.json()),
                             'Response for List Blocks'
                             ' {0} {1}'.format(self.blockids, resp.json()))

    @ddt.data(2, 4, 5, 10)
    def test_list_blocks_limit(self, value):
        """List multiple blocks, setting the limit to value"""

        url = None
        for i in range(20 / value):
            if not url:
                resp = self.client.list_of_blocks(self.vaultname, limit=value)
            else:
                resp = self.client.list_of_blocks(alternate_url=url)
            self.assertEqual(200, resp.status_code,
                             'Status code for listing all blocks is '
                             '{0}'.format(resp.status_code))
            self.validate_headers(resp.headers, json=True)
            if i < 20 / value - 1:
                self.assertIn('x-next-batch', resp.headers)
                url = resp.headers['x-next-batch']
                self.validate_url(url, nextblocklist=True)
            else:
                self.assertNotIn('x-next-batch', resp.headers)
            self.assertEqual(value, len(resp.json()),
                             'Number of block ids returned is not {0} . '
                             'Returned {1}'.format(value, len(resp.json())))
            for blockid in resp.json():
                self.assertIn(blockid, self.blockids)
                self.blockids.remove(blockid)
        self.assertEqual(0, len(self.blockids),
                         'Discrepancy between the list of blocks returned '
                         'and the blocks uploaded')

    @ddt.data(2, 4, 5, 10)
    def test_list_blocks_limit_marker(self, value):
        """List multiple blocks, setting the limit to value and using a
        marker"""

        markerid = sorted(self.blockids)[value]

        url = None
        for i in range(20 / value - 1):
            if not url:
                resp = self.client.list_of_blocks(self.vaultname,
                                                  marker=markerid, limit=value)
            else:
                resp = self.client.list_of_blocks(alternate_url=url)
            self.assertEqual(200, resp.status_code,
                             'Status code for listing all blocks is '
                             '{0}'.format(resp.status_code))
            self.validate_headers(resp.headers, json=True)
            if i < 20 / value - 2:
                self.assertIn('x-next-batch', resp.headers)
                url = resp.headers['x-next-batch']
                self.validate_url(url, nextblocklist=True)
            else:
                self.assertNotIn('x-next-batch', resp.headers)
            self.assertEqual(value, len(resp.json()),
                             'Number of block ids returned is not {0} . '
                             'Returned {1}'.format(value, len(resp.json())))
            for blockid in resp.json():
                self.assertIn(blockid, self.blockids)
                self.blockids.remove(blockid)
        self.assertEqual(value, len(self.blockids),
                         'Discrepancy between the list of blocks returned '
                         'and the blocks uploaded')

    def tearDown(self):
        super(TestListBlocks, self).tearDown()
        self.client.delete_vault(self.vaultname)
