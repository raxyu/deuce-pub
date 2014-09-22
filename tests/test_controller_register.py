import json

import falcon

from tests import V1Base


class TestRegister(V1Base):
    def setUp(self):
        super(TestRegister, self).setUp()
        self._hdrs = {"X-Auth-Token": self.create_auth_token()}
        self._projid = self.create_project_id()
        self._vault_num = 2
        self._vaultlist=sorted([self.create_vault_id() for i in range(0, self._vault_num)])

    def test_register(self):
        # Empty list
        response = self.simulate_get('/list/{0}'.format(self._projid), headers=self._hdrs)
        
        # Add vaults
        for i in range(0, self._vault_num):
            response = self.simulate_post('/register/{0}/{1}'.format(self._projid, self._vaultlist[i]), headers=self._hdrs)
            self.assertEqual(self.srmock.status, falcon.HTTP_201)

        # Filled list
        response = self.simulate_get('/list/{0}'.format(self._projid), headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_200)
        self.assertEqual(response[0],
            str(self._vaultlist).replace('\'', '"').encode(encoding='utf-8', errors='strict'))

        # Remove one vault
        response = self.simulate_delete('/register/{0}/{1}'.format(self._projid, self._vaultlist[-1]), headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_204)

        # A shorter list
        response = self.simulate_get('/list/{0}'.format(self._projid), headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_200)
        self.assertEqual(response[0],
            str(self._vaultlist[:-1]).replace('\'', '"').encode(encoding='utf-8', errors='strict'))
