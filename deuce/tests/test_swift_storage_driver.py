
from deuce.tests import FunctionalTest
from deuce.drivers.storage.blocks import BlockStorageDriver
from deuce.drivers.storage.blocks.swift import SwiftStorageDriver
from deuce.tests.util import MockFile

from hashlib import md5

# Users need take care of authenticate themselves and
# have the token ready for each query.

from swiftclient import client as Conn
from swiftclient.exceptions import ClientException

from pecan import conf

import sys
import traceback


# TODO: Make this test generic -- it should not konw
# which particular driver it is testing.


class SwiftStorageDriverTest(FunctionalTest):

    def setUp(self):  # pragma: no cover
        super(SwiftStorageDriverTest, self).setUp()

        auth_url = conf.block_storage_driver.swift.auth_url
        username = conf.block_storage_driver.swift.username
        password = conf.block_storage_driver.swift.password

        os_options = dict()

        self.mocking = False
        try:
            if conf.block_storage_driver.swift.is_mocking:
                self.mocking = True
        except:
            self.mocking = False

        if not self.mocking:
            try:
                self.storage_url, self.token = \
                    Conn.get_keystoneclient_2_0(
                        auth_url=auth_url,
                        user=username,
                        key=password,
                        os_options=os_options)
            except ClientException as e:
                sys.exit(str(e))

        else:
            self.storage_url = 'mocking_url'
            self.token = 'mocking_token'
        self.project_id = self.storage_url[self.storage_url.rfind("/") + 1:]

    def test_ancestry(self):

        d = SwiftStorageDriver(self.storage_url, self.token, self.project_id)
        assert isinstance(d, SwiftStorageDriver)
        assert isinstance(d, object)

        # Test all exceptions
        d = SwiftStorageDriver(
            self.storage_url,
            self.token + '1',
            self.project_id)
        projectid = 'notmatter'
        vaultid = 'notmatter'
        blockid = 'notmatter'
        d.create_vault(projectid, vaultid)
        d.vault_exists(projectid, vaultid)
        d.delete_vault(projectid, vaultid)
        d.store_block(projectid, vaultid, blockid, '')
        d.block_exists(projectid, vaultid, blockid)
        d.delete_block(projectid, vaultid, blockid)
        d.get_block_obj(projectid, vaultid, blockid)

    def test_vault_crud(self):

        d = SwiftStorageDriver(self.storage_url, self.token, self.project_id)

        projectid = 'test_project_id'
        vaultid = 'test_vault_id'

        if not self.mocking:
            assert not d.vault_exists(projectid, vaultid)
        else:
            d.vault_exists(projectid, vaultid)

        if not self.mocking:
            assert d.create_vault(projectid, vaultid)

            assert d.vault_exists(projectid, vaultid)

        assert d.delete_vault(projectid, vaultid)

        if not self.mocking:
            assert not d.vault_exists(projectid, vaultid)
        else:
            d.vault_exists(projectid, vaultid)

    def test_block_crud(self):

        d = SwiftStorageDriver(self.storage_url, self.token, self.project_id)

        block_size = 3000
        vault_id = 'block_crud_vault_test'
        block_id = 'blah'
        projectid = 'test_project_id'

        if not d.vault_exists(projectid, vault_id):  # pragma: no cover
            d.create_vault(projectid, vault_id)

        # Create a file-like object
        block_data = MockFile(block_size)

        d.store_block(projectid, vault_id, block_id, block_data.read())

        block_data.seek(0)

        assert d.block_exists(projectid, vault_id, block_id)

        # Read back the block data and compare
        file_obj = d.get_block_obj(projectid, vault_id, block_id)

        if not self.mocking:
            returned_data = file_obj.read()

            # Returned data should be exatly the same

            assert len(returned_data) == block_size
            assert returned_data == block_data._content

        d.delete_block(projectid, vault_id, block_id)

        if not self.mocking:
            assert not d.block_exists(projectid, vault_id, block_id)
            assert None == d.get_block_obj(projectid,
                vault_id, 'invalid_block_id')

    def test_block_generator(self):
        d = SwiftStorageDriver(self.storage_url, self.token, self.project_id)

        block_size = 3000
        vault_id = 'generator_test'
        projectid = 'test_project_id'

        d.create_vault(projectid, vault_id)

        # Test re-entrance
        d.create_vault(projectid, vault_id)

        blocks = [(x, MockFile(block_size)) for x in range(0, 10)]

        orig_hash = md5()

        for block_id, block_data in blocks:
            orig_hash.update(block_data._content)

        orig_hex = orig_hash.hexdigest()

        for block_id, block_data in blocks:
            d.store_block(projectid, vault_id, block_id, block_data.read())
            block_data.seek(0)

        # Now call the block generator.

        blockid_gen = (x[0] for x in blocks)

        gen = d.create_blocks_generator(projectid, vault_id, blockid_gen)

        fetched_data = list(gen)

        assert len(fetched_data) == len(blocks) == 10

        for x in range(0, len(fetched_data)):
            blocks[x][1].seek(0)
            if not self.mocking:
                assert fetched_data[x].read() == blocks[x][1].read()

        # Clean up.
        for block_id in range(0, 10):
            d.delete_block(projectid, vault_id, block_id)
