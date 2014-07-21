from tests.api.utils import base
import ddt


@ddt.ddt
class TestCreateVaults(base.TestBase):

    def setUp(self):
        super(TestCreateVaults, self).setUp()

    @ddt.data(1, 10, 100)
    def test_create_vaults(self, size):
        """Create a Vault"""

        self.vaultname = self.id_generator(size)
        resp = self.client.create_vault(self.vaultname)
        self.assertEqual(201, resp.status_code,
                         'Status code returned for Create Vault: {0} . '
                         'Expected 201'.format(resp.status_code))
        self.validate_headers(resp.headers)
        self.assertEqual(0, len(resp.content),
                         'Response Content was not empty. Content: '
                         '{0}'.format(resp.content))

    def tearDown(self):
        super(TestCreateVaults, self).tearDown()
        self.client.delete_vault(self.vaultname)


class TestDeleteVaults(base.TestBase):

    def setUp(self):
        super(TestDeleteVaults, self).setUp()
        self.createEmptyVault()

    def test_delete_vault(self):
        """Delete a Vault"""

        resp = self.client.delete_vault(self.vaultname)
        self.assertEqual(204, resp.status_code,
                         'Status code returned for Delete Vault: {0} . '
                         'Expected 204'.format(resp.status_code))
        self.validate_headers(resp.headers)
        self.assertEqual(0, len(resp.content),
                         'Response Content was not empty. Content: '
                         '{0}'.format(resp.content))

    def tearDown(self):
        super(TestDeleteVaults, self).tearDown()


class TestGetVault(base.TestBase):

    def setUp(self):
        super(TestGetVault, self).setUp()
        self.createEmptyVault()

    def test_get_vault(self):
        """Get an individual vault"""

        resp = self.client.get_vault(self.vaultname)
        self.assertEqual(204, resp.status_code,
                         'Status code returned for Get Vault: {0} . '
                         'Expected 204'.format(resp.status_code))
        self.validate_headers(resp.headers)
        self.assertEqual(0, len(resp.content),
                         'Response Content was not empty. Content: '
                         '{0}'.format(resp.content))

    def test_vault_head(self):
        """Head of an individual vault. Get the statistics for a vault"""

        # TODO
        self.skipTest('HEAD Vault functionality not implemented')
        resp = self.client.vault_head(self.vaultname)
        self.assertEqual(200, resp.status_code,
                         'Status code returned for Vault HEAD: {0} . '
                         'Expected 200'.format(resp.status_code))
        self.validate_headers(resp.headers)
        self.assertIn('X-File-Count', resp.headers)
        self.assertEqual(0, resp.headers['X-File-Count'])
        self.assertIn('X-Unfinalized-Count', resp.headers)
        self.assertEqual(0, resp.headers['X-Unfinalized-Count'])
        self.assertIn('X-Block-Count', resp.headers)
        self.assertEqual(0, resp.headers['X-Block-Count'])
        self.assertIn('X-Byte-Count', resp.headers)
        self.assertEqual(0, resp.headers['X-Byte-Count'])

    def tearDown(self):
        super(TestGetVault, self).tearDown()
        self.client.delete_vault(self.vaultname)
