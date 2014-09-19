import model
from drivers.cassandra import cassandradriver


class Tasks(object):

    @staticmethod
    def register(project_id, vault_id):
        print('yud : Tasks: POS2.   ', Tasks.driver)
        Tasks.driver.add_vault(project_id, vault_id)

    @staticmethod
    def unregister(project_id, vault_id):
        Tasks.driver.delete_vault(project_id, vault_id)

    """
        DEBUG ONLY
    """
    @staticmethod
    def debuglist(project_id):
        return Tasks.driver.create_vaults_generator(project_id)
