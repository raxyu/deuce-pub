import os
import hashlib
from random import randrange
import six
from unittest import TestCase
from deuce.tests import FunctionalTest


class TestFilesController(FunctionalTest):

    def setUp(self):
        super(TestFilesController, self).setUp()

        # Create a vault and a file for us to work with
        vault_name = 'files_vault_test'
        self._vault_path = '/v1.0/{0}'.format(vault_name)
        self._files_path = '{0}/files'.format(self._vault_path)
        response = self.app.post(self._vault_path)
        response = self.app.post(self._files_path)
        self._file_id = response.headers["Location"]
        self._file_id = self._file_id.replace('http://localhost', '')
        hdrs = {'content-type': 'application/x-deuce-block-list'}
        params = {}
        response = self.app.post(self._files_path, params=params,  headers=hdrs)

    def test_get_all(self):
        response = self.app.get(self._files_path, expect_errors=True)

    def test_get_one(self):
        response = self.app.get('/v1.0/not_exists/files/not_matter', expect_errors=True)
        assert response.status_int == 404
        wrong_file_path = '{0}/not_exists', format(self._files_path)
        response = self.app.get('/v1.0/files_vault_test/files/not_exists', expect_errors=True)
        assert response.status_int == 404

    def test_post_one(self):
        response = self.app.post('/v1.0/not_exists/files', expect_errors=True)
        assert response.status_int == 404

        response = self.app.post('/v1.0/not_exists/files/not_matter', expect_errors=True)
        assert response.status_int == 404

        response = self.app.post('/v1.0/files_vault_test/files/', expect_errors=True)
        assert response.status_int == 404

        response = self.app.post('/v1.0/files_vault_test/files')


        response = self.app.post('/v1.0/files_vault_test/files/not_exists', expect_errors=True)
        assert response.status_int == 404

        hdrs = {'content-type': 'application/x-deuce-block-list'}
        data = "{\"blocks\":[{\"id\": \"1\", \"size\": 100, \"offset\": 0}, {\"id\": \"2\", \"size\": 100, \"offset\": 100}]}"
        response = self.app.post(self._file_id, params=data, headers=hdrs)


        response = self.app.get(self._file_id, expect_errors=True)

        response = self.app.post('/v1.0/files_vault_test/files/not_exists', expect_errors=True)
        assert response.status_int == 404

        hdrs = {'content-type': 'application/x-deuce-block-list'}
        data = "{\"blocks\":[{\"id\": \"1\", \"size\": 100, \"offset\": 0}, {\"id\": \"2\", \"size\": 100, \"offset\": 100}]}"
        response = self.app.post(self._file_id, params=data, headers=hdrs)


        response = self.app.get(self._file_id, expect_errors=True)

