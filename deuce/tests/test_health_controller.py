from unittest import TestCase
from webtest import TestApp
from deuce.tests import FunctionalTest

import json


class TestVaultController(FunctionalTest):

    def setUp(self):
        super(TestVaultController, self).setUp()
        self._hdrs = {"x-project-id": self.create_project_id(),
            "x-auth-token": ''}

    def test_health(self):
        response = self.app.get('/v1.0/health', headers=self._hdrs)
        assert response.status_int == 200
