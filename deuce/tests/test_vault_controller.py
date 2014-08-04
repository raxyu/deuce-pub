from unittest import TestCase
from webtest import TestApp
from deuce.tests import FunctionalTest
import os

import json


class TestVaultController(FunctionalTest):

    def setUp(self):
        super(TestVaultController, self).setUp()
        self._hdrs = {"x-project-id": 'testvaultctrl',
            "x-auth-token": ''}

    def helper_create_vault(self, vault_name, hdrs):
        vault_path = '/v1.0/{0}'.format(vault_name)
        response = self.app.put(vault_path, headers=hdrs)

    def helper_delete_vault(self, vault_name, hdrs):
        vault_path = '/v1.0/{0}'.format(vault_name)
        response = self.app.delete(vault_path, headers=hdrs)

    def test_vault_leaf(self):
        hdrs = {"x-project-id": 'test_list_vaults',
            "x-auth-token": ''}
        params = {}

        # Create an empty root path in the storage.
        self.helper_create_vault('vault_0', hdrs)
        self.helper_delete_vault('vault_0', hdrs)

        response = self.app.get('/v1.0/',
            params=params,
            headers=hdrs)
        assert str(response.body) == str(b'[]')

        # Prepare several vaults in the storage.
        for cnt in range(5):
            self.helper_create_vault('vault_{0}'.format(cnt), hdrs)

        params['limit'] = 99
        response = self.app.get('/v1.0/',
            params=params,
            headers=hdrs)
        assert str(response.body) == str(
            b'["vault_0", "vault_1", "vault_2", "vault_3", "vault_4"]')

        params['limit'] = 1
        response = self.app.get('/v1.0/',
            params=params,
            headers=hdrs)
        assert str(response.body) == str(b'["vault_0"]')
        assert response.headers["X-Next-Batch"] == 'vault_1'

        params['marker'] = response.headers["X-Next-Batch"]
        params['limit'] = 99  # More than what left in the list.
        response = self.app.get('/v1.0/',
            params=params,
            headers=hdrs)
        assert str(response.body) == str(
            b'["vault_1", "vault_2", "vault_3", "vault_4"]')

        params['limit'] = 1
        response = self.app.get('/v1.0/',
            params=params,
            headers=hdrs)
        assert str(response.body) == str(b'["vault_1"]')

        params['marker'] = 'vault_not_exists'
        params['limit'] = 99
        response = self.app.get('/v1.0/',
            params=params,
            headers=hdrs,
            expect_errors=True)
        assert str(response.body) == str(b'[]')

        # Cleanup
        for cnt in range(5):
            self.helper_delete_vault('vault_{0}'.format(cnt), hdrs)

    def test_invalid_vault_id(self):
        vault_name = '@#$@#$@$'
        vault_path = '/v1.0/{0}'.format(vault_name)

        response = self.app.put(vault_path, headers=self._hdrs,
                expect_errors=True)
        self.assertEqual(response.status_int, 400)

        response = self.app.get(vault_path, headers=self._hdrs,
                expect_errors=True)
        self.assertEqual(response.status_int, 404)

    def test_vault_crud(self):
        vault_name = 'my_new_vault'
        vault_path = '/v1.0/{0}'.format(vault_name)

        # If we try to get the vault before it exists, it should
        # return a 404
        response = self.app.get(vault_path,
            headers=self._hdrs, expect_errors=True)

        assert response.status_code == 404

        # Now we create the vault, which should return a 201 (created)
        response = self.app.put(vault_path, headers=self._hdrs)
        assert response.status_code == 201

        # Now if we get the vault, what do we get? For now,
        # let's enforce that we get a 204 (No Content)
        response = self.app.get(vault_path, headers=self._hdrs)

        assert response.status_code == 204

        # Now delete the vault (this should be OK since it
        # contains nothing in it.
        response = self.app.delete(vault_path, headers=self._hdrs,
            expect_errors=True)

        assert response.status_code == 204

        # Now we should get a 404 when trying to get the vault
        response = self.app.get(vault_path, headers=self._hdrs,
            expect_errors=True)

        assert response.status_code == 404

        # Try to delete again, this time it should be a 404
        response = self.app.delete(vault_path, headers=self._hdrs,
            expect_errors=True)

        assert response.status_code == 404
