from unittest import TestCase
from webtest import TestApp
from deuce.tests import FunctionalTest


class TestVaultController(FunctionalTest):

    def test_vault_leaf(self):
        response = self.app.get('/v1.0/', expect_errors=True)
        assert response.status_int == 404

    def test_vault_crud(self):
        vault_name = 'my_new_vault'
        vault_path = '/v1.0/{0}'.format(vault_name)

        # If we try to get the vault before it exists, it should
        # return a 404
        response = self.app.get(vault_path, expect_errors=True)
        assert response.status_code == 404

        # Now we create the vault, which should return a 201 (created)
        response = self.app.post(vault_path)
        assert response.status_code == 201

        # Now if we get the vault, what do we get? For now,
        # let's enforce that we get a 204 (No Content)
        response = self.app.get(vault_path, expect_errors=True)
        assert response.status_code == 200

        # Now delete the vault (this should be OK since it
        # contains nothing in it.
        response = self.app.delete(vault_path, expect_errors=True)
        assert response.status_code == 200

        # Now we should get a 404 when trying to get the vault
        response = self.app.get(vault_path, expect_errors=True)
        assert response.status_code == 404

        # Try to delete again, this time it should be a 404
        response = self.app.delete(vault_path, expect_errors=True)
        assert response.status_code == 404

    """
    def test_get(self):
        response = self.app.get('/')
        assert response.status_int == 200

    def test_search(self):
        response = self.app.post('/', params={'q': 'RestController'})
        assert response.status_int == 302
        assert response.headers['Location'] == (
            'http://pecan.readthedocs.org/en/latest/search.html'
            '?q=RestController'
        )

    def test_get_not_found(self):
        response = self.app.get('/a/bogus/url', expect_errors=True)
        assert response.status_int == 404
    """
