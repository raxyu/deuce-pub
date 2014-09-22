import deucecnc.model
from deucecnc.drivers.cassandra import cassandradriver


class Tasks(object):

    @staticmethod
    def register(project_id, vault_id):
        Tasks.driver.add_vault(project_id, vault_id)

    @staticmethod
    def unregister(project_id, vault_id):
        Tasks.driver.delete_vault(project_id, vault_id)

    """
        DEBUG ONLY
    """
    @staticmethod
    def listing(project_id):
        return Tasks.driver.create_vaults_generator(project_id)
