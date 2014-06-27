
from deuce.drivers.storage.blocks.couchdb import CouchdbStorageDriver
from deuce.tests.test_disk_storage_driver import DiskStorageDriverTest

# Users need take care of authenticate themselves and
# have the token ready for each query.

from pecan import conf

import sys
import shutil

# TODO: Make this test generic -- it should not konw
# which particular driver it is testing.


class CouchdbStorageDriverTest(DiskStorageDriverTest):

    def setUp(self):
        super(CouchdbStorageDriverTest, self).setUp()

        os_options = dict()

        self.storage_url = 'http://127.0.0.1:5984'

    def create_driver(self):
        return CouchdbStorageDriver(self.storage_url)

    def test_basic_construction(self):
        driver = self.create_driver()

    def test_ancestry(self):

        driver = self.create_driver()
        assert isinstance(driver, CouchdbStorageDriver)
        assert isinstance(driver, object)

        # Test all exceptions
        projectid = 'couchprojid'
        vaultid = 'couchvaultid'
        blockid = '0123456789012345678901234567890123456789'
        wrong_vaultid = 'wrong_couchvaultid'
        wrong_blockid = 'wrong_blockid'
        driver.create_vault(projectid, vaultid)
        driver.vault_exists(projectid, vaultid)
        driver.store_block(projectid, wrong_vaultid, blockid,
            str('123456').encode('utf-8'))
        driver.store_block(projectid, vaultid, blockid,
            str('123456').encode('utf-8'))
        assert driver.block_exists(projectid, vaultid, blockid)
        driver.get_block_obj(projectid, vaultid, wrong_blockid)
        driver.get_block_obj(projectid, vaultid, blockid)
        driver.delete_block(projectid, vaultid, blockid)
        driver.delete_vault(projectid, vaultid)
        assert not driver.vault_exists(projectid, vaultid)
