import os
import hashlib
import string
from random import randrange
import six
from unittest import TestCase
from deuce.tests import FunctionalTest
from deuce.drivers.storage.metadata.sqlite import SqliteStorageDriver


class TestFilesController(FunctionalTest):

    def setUp(self):
        super(TestFilesController, self).setUp()

        # Create a vault and a file for us to work with
        self.vault_id = 'files_vault_test'
        self._vault_path = '/v1.0/' + self.vault_id
        self._files_path = self._vault_path + '/files'
        # Create Vault
        response = self.app.post(self._vault_path)
        # Create File
        response = self.app.post(self._files_path)
        self._file_id = response.headers["Location"]
        self._file_id = self._file_id.replace('http://localhost', '')
        # Now, _file_id is '/v1.0/files_vault_test/files/SOME_FILE_ID'

        self._NOT_EXIST_files_path = '/v1.0/not_exists/files'

    def test_get_one(self):
        # vault does not exists
        response = self.app.get(self._NOT_EXIST_files_path, expect_errors=True)
        assert response.status_int == 404
        response = self.app.get(self._NOT_EXIST_files_path + '/',
                                expect_errors=True)
        assert response.status_int == 404
        response = self.app.get(self._NOT_EXIST_files_path + '/not_matter',
                                expect_errors=True)
        assert response.status_int == 404

        # fileid is not privded
        response = self.app.get(self._files_path + '/', expect_errors=True)
        assert response.status_int == 404
        # fileid does not exists
        response = self.app.get(self._files_path + '/not_exists',
                                expect_errors=True)
        assert response.status_int == 404

    def test_post_one(self):
        # vault does not exists
        response = self.app.post(self._NOT_EXIST_files_path,
                                 expect_errors=True)
        assert response.status_int == 404
        response = self.app.post(self._NOT_EXIST_files_path + '/',
                                 expect_errors=True)
        assert response.status_int == 404
        response = self.app.post(self._NOT_EXIST_files_path + '/not_matter',
                                 expect_errors=True)
        assert response.status_int == 404

        # fileid is not provided
        response = self.app.post(self._files_path + '/', expect_errors=True)
        assert response.status_int == 404
        # fileid does not exists
        response = self.app.post(self._files_path + '/not_exists',
                                 expect_errors=True)
        assert response.status_int == 404

        # Register blocks to fileid
        hdrs = {'content-type': 'application/x-deuce-block-list'}
        blockid1 = "1"
        blockid2 = "2"
        data = "{\"blocks\":[{\"id\": \"" + blockid1 + "\", \"size\": 100, \
                 \"offset\": 0}, {\"id\": \"" + blockid2 + "\", \
                 \"size\": 100, \"offset\": 100}]}"
        response = self.app.post(self._file_id, params=data, headers=hdrs)

        driver = SqliteStorageDriver()
        driver.register_block(self.vault_id, blockid1, 100)
        driver.register_block(self.vault_id, blockid2, 100)
        response = self.app.post(self._file_id, params=data, headers=hdrs)

        # Get file.
        response = self.app.get(self._file_id, expect_errors=True)

        # Finalize file
        params = {}
        response = self.app.post(self._file_id, params=params, headers=hdrs)

        # Error on trying to change Finalized file.
        response = self.app.post(self._file_id, params=data, headers=hdrs,
                                 expect_errors=True)
        assert response.status_int == 400
