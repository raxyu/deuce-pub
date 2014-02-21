from pecan import conf
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

        self._hdrs = {"X-Project-ID": "test_project_id"}

        # Create a vault and a file for us to work with
        self.vault_id = 'files_vault_test'
        self._vault_path = '/v1.0/' + self.vault_id
        self._files_path = self._vault_path + '/files'

        # Create Vault
        response = self.app.post(self._vault_path, headers=self._hdrs)
        # Create File
        response = self.app.post(self._files_path, headers=self._hdrs)
        self._file_id = response.headers["Location"]
        self._file_id = self._file_id.replace('http://localhost', '')
        # Now, _file_id is '/v1.0/files_vault_test/files/SOME_FILE_ID'

        self._NOT_EXIST_files_path = '/v1.0/not_exists/files'

    def test_tenancy_requirement(self):
        # vault does not exists
        # If we pass in no headers, we should get a 400 back
        response = self.app.get(self._NOT_EXIST_files_path,
            expect_errors=True)

        assert response.status_int == 400

        # vault does not exists
        response = self.app.get(self._NOT_EXIST_files_path,
            headers=self._hdrs, expect_errors=True)

        assert response.status_int == 404  # Not found

    def test_get_one(self):
        # vault does not exists
        response = self.app.get(self._NOT_EXIST_files_path,
            headers=self._hdrs, expect_errors=True)

        assert response.status_int == 404

        response = self.app.get(self._NOT_EXIST_files_path + '/',
            headers=self._hdrs, expect_errors=True)

        assert response.status_int == 404

        response = self.app.get(self._NOT_EXIST_files_path + '/not_matter',
            headers=self._hdrs, expect_errors=True)

        assert response.status_int == 404

        # fileid is not privded
        response = self.app.get(self._files_path + '/', headers=self._hdrs,
            expect_errors=True)

        assert response.status_int == 404

        # fileid does not exists
        response = self.app.get(self._files_path + '/not_exists',
            headers=self._hdrs, expect_errors=True)

        assert response.status_int == 404

    def test_post_one(self):
        # vault does not exists
        response = self.app.post(self._NOT_EXIST_files_path,
            headers=self._hdrs, expect_errors=True)

        assert response.status_int == 404
        response = self.app.post(self._NOT_EXIST_files_path + '/',
            headers=self._hdrs, expect_errors=True)

        assert response.status_int == 404
        response = self.app.post(self._NOT_EXIST_files_path + '/not_matter',
            headers=self._hdrs, expect_errors=True)

        assert response.status_int == 404

        # fileid is not provided
        response = self.app.post(self._files_path + '/',
            headers=self._hdrs, expect_errors=True)

        assert response.status_int == 404

        # fileid does not exists
        response = self.app.post(self._files_path + '/not_exists',
            headers=self._hdrs, expect_errors=True)

        assert response.status_int == 404

        driver = SqliteStorageDriver()
        hdrs = {'content-type': 'application/x-deuce-block-list'}
        hdrs.update(self._hdrs)
        data = "{\"blocks\":["
        enough_num = int(1.5 * conf.api_configuration.max_returned_num)
        for cnt in range(0, enough_num):
            data = data + '{' + '\"id\": {0}, \"size\": 100, \"offset\": {1}'.format(cnt, cnt*100) + '}'
            if cnt < enough_num-1:
                data = data + ','
        data = data + ']}'
        # Add blocks to files, resp has a list of missing blocks.
        response = self.app.post(self._file_id, params=data, headers=hdrs)
        assert len(response.body) == 640

        driver = SqliteStorageDriver()

        '''
        data = "{\"blocks\":["
        enough_num = int(1.5 * conf.api_configuration.max_returned_num)
        for cnt in range(0, enough_num):
            data = data + '{' + '\"id\": {0}, \"size\": 100, \"offset\": {1}'.format(cnt, cnt*100) + '}'
            if cnt < enough_num-1:
                data = data + ','
        data = data + ']}'
        '''
        # Register 150 blocks into system.
        for cnt in range(0, enough_num):
            driver.register_block(self.vault_id, cnt, 100)
        # Then add blocks to files again. resp is empty.
        response = self.app.post(self._file_id, params=data, headers=hdrs)
        assert len(response.body) == 2

        # Get file.
        response = self.app.get(self._file_id, expect_errors=True)

        # Register 200 blocks into system.
        data = "{\"blocks\":["
        enough_num = int(2 * conf.api_configuration.max_returned_num)
        for cnt in range(0, enough_num):
            data = data + '{' + '\"id\": {0}, \"size\": 100, \"offset\": {1}'.format(cnt, cnt*100) + '}'
            if cnt < enough_num-1:
                data = data + ','
            driver.register_block(self.vault_id, cnt, 100)
        data = data + ']}'
        # Then add blocks to files again. resp is empty.
        response = self.app.post(self._file_id, params=data, headers=hdrs)
        assert len(response.body) == 2

        # Get the file.
        response = self.app.get(self._file_id, headers=hdrs,
            expect_errors=True)

        # Finalize file
        params = {}
        response = self.app.post(self._file_id, params=params, headers=hdrs)

        # Error on trying to change Finalized file.
        response = self.app.post(self._file_id, params=data, headers=hdrs,
                                 expect_errors=True)
        assert response.status_int == 400
