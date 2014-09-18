import json

import falcon

from tests import V1Base


class TestAuth(V1Base):

    def test_auth(self):
        response = self.simulate_get('/auth/testproj')
        self.assertEqual(self.srmock.status, falcon.HTTP_200)
