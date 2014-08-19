from unittest import TestCase
from webtest import TestApp
from deuce.tests import FunctionalTest
import os
import hashlib
import random
import string
from random import randrange

import json


class TestVaultController(FunctionalTest):

    def setUp(self):
        super(TestVaultController, self).setUp()
        self._hdrs = {"x-project-id": self.create_project_id(),
            "x-auth-token": ''}

    def test_vault_leaf(self):
        response = self.app.get('/v1.0/', headers=self._hdrs,
            expect_errors=True)
        assert response.status_int == 404

        # ping end point does not require token or projectid headers!
        response = self.app.get('/v1.0/ping', headers={})

        assert response.status_int == 200
        assert response.body == b'["ok"]'

    def test_invalid_vault_id(self):
        vault_name = '@#$@#$@$'
        vault_path = '/v1.0/{0}'.format(vault_name)

        # regex validation.
        response = self.app.put(vault_path, headers=self._hdrs,
                expect_errors=True)
        self.assertEqual(response.status_int, 400)

        response = self.app.head(vault_path, headers=self._hdrs,
                expect_errors=True)
        self.assertEqual(response.status_int, 404)

        # vault name 'health' and 'ping' are not allowed.
        vault_name = 'health'
        vault_path = '/v1.0/{0}'.format(vault_name)
        response = self.app.put(vault_path, headers=self._hdrs,
                expect_errors=True)
        self.assertEqual(response.status_int, 400)

        # vault name cannot be more than 128 chars.
        vault_name = ''.join(random.choice(string.ascii_uppercase)
            for i in range(200))
        vault_path = '/v1.0/{0}'.format(vault_name)
        response = self.app.put(vault_path, headers=self._hdrs,
                expect_errors=True)
        self.assertEqual(response.status_int, 400)

    def test_vault_deletion(self):

        # 1. Delete non-existent vault
        vault_name = self.create_vault_id()
        vault_path = '/v1.0/{0}'.format(vault_name)
        response = self.app.delete(vault_path,
            headers=self._hdrs, expect_errors=True)
        self.assertEqual(response.status_code, 404)

        # 2. Create Vault and Delete it (Empty Vault)
        vault_name = self.create_vault_id()
        vault_path = '/v1.0/{0}'.format(vault_name)
        response = self.app.put(vault_path,
            headers=self._hdrs)
        self.assertEqual(response.status_code, 201)

        response = self.app.delete(vault_path,
            headers=self._hdrs)
        self.assertEqual(response.status_code, 204)

        # 3. Create Vault, Add a Block, and Delete It (Non-Empty Vault)
        vault_name = self.create_vault_id()
        vault_path = '/v1.0/{0}'.format(vault_name)
        response = self.app.put(vault_path,
            headers=self._hdrs)
        self.assertEqual(response.status_code, 201)

        # Build a dummy block
        block_data = os.urandom(randrange(1, 2000))
        block_hash = hashlib.sha1()
        block_hash.update(block_data)
        block_id = block_hash.hexdigest()
        block_path = '{0:}/blocks/{1:}'.format(vault_path, block_id)

        # Upload a dummy block
        headers = {}
        headers.update(self._hdrs)

        headers['content-type'] = 'application/binary'
        headers['content-length'] = str(len(block_data))

        response = self.app.put(block_path, headers=headers,
            params=block_data)
        self.assertEqual(response.status_code, 201)

        # Delete the vault
        response = self.app.delete(vault_path,
            headers=self._hdrs, expect_errors=True)
        self.assertEqual(response.status_code, 412)

        # Delete the dummy block
        # TODO:
        # response = self.app.delete(block_path, headers=self._hdrs)
        # self.assertEqual(response.status_code, 204)
        #
        # # Delete the vault
        # response = self.app.delete(vault_path,
        #    headers = self._hdrs)
        # self.assertEqual(response.status_code, 204)

    def test_vault_crud(self):
        vault_name = self.create_vault_id()
        vault_path = '/v1.0/{0}'.format(vault_name)

        # If we try to head the vault before it exists, it should
        # return a 404
        response = self.app.head(vault_path,
            headers=self._hdrs, expect_errors=True)

        assert response.status_code == 404

        # If we try to get the statistics on the vault before it
        # exists, it should return a 404
        response = self.app.get(vault_path,
            headers=self._hdrs, expect_errors=True)

        assert response.status_code == 404

        # Now we create the vault, which should return a 201 (created)
        response = self.app.put(vault_path, headers=self._hdrs)
        assert response.status_code == 201

        # Now verify the vault exists
        response = self.app.head(vault_path, headers=self._hdrs)

        assert response.status_code == 204

        # Now get the statistics, what do we get?
        # Base statistics:
        #   metadata (file count = 0, file-block count = 0, blocks = 0)
        #   storage (size = 0,...)
        # For now, just enforce we get a 200
        response = self.app.get(vault_path, headers=self._hdrs)

        assert response.status_code == 200

        # Now delete the vault (this should be OK since it
        # contains nothing in it.
        response = self.app.delete(vault_path, headers=self._hdrs)

        assert response.status_code == 204

        # Now we should get a 404 when trying to head the vault
        response = self.app.head(vault_path, headers=self._hdrs,
            expect_errors=True)

        assert response.status_code == 404

        # Try to delete again, this time it should be a 404
        response = self.app.delete(vault_path, headers=self._hdrs,
            expect_errors=True)

        assert response.status_code == 404
