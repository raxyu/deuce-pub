from tests import V1Base
from deucecnc.drivers.cassandra import CassandraDriver
import random

import deucecnc
from deucecnc import conf
from mock import MagicMock


class CassandraDriverTest(V1Base):
    def create_driver(self):
        return CassandraDriver(conf.cnc_driver.testing.db_module)

    def test_vault_statistics(self):
        driver = self.create_driver()

        vault_id = self.create_vault_id()
        project_id = self.create_project_id()

        # empty vault stats
        # TODO ** Create Vault Here **
        statistics = driver.get_vault_statistics(project_id, vault_id)

        vault_num = 2
        vaultlist = sorted([self.create_vault_id()
            for i in range(0, vault_num)])
        hdrs = {"X-Auth-Token": self.create_auth_token()}

        # Add vaults
        for i in range(0, vault_num):
            driver.add_vault(project_id, vaultlist[i])
        reslist = driver.create_vaults_generator(project_id, limit=1)
        self.assertEqual(len(reslist), 1)

        # TODO: Add files and check that founds match as expected
