from tests.api import base
import ddt


class TestDiagnostics(base.TestBase):

    def setUp(self):
        super(TestDiagnostics, self).setUp()

    def test_ping(self):
        """Create a Vault"""

        resp = self.client.ping()
        self.assertEqual(resp.status_code, 204,
                         'Status code returned for Create Vault: {0} . '
                         'Expected 204'.format(resp.status_code))
        self.assertHeaders(resp.headers)
        self.assertEqual(len(resp.content), 0,
                         'Response Content was not empty. Content: '
                         '{0}'.format(resp.content))

    def tearDown(self):
        super(TestDiagnostics, self).tearDown()
