from unittest import TestCase
from webtest import TestApp
from deuce.tests import FunctionalTest

import json


class TestVaultController(FunctionalTest):

    def setUp(self):
        super(TestVaultController, self).setUp()
        # health end point does not require token or projectid headers!
        self._hdrs = {}

    def test_health(self):
        response = self.app.get('/v1.0/health', headers=self._hdrs)
        assert response.status_int == 200
