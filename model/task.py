import model


class Tasks(object):

    @staticmethod
    def register(driver, project_id, vault_id):
        driver.add_vault(project_id, vault_id)

    @staticmethod
    def unregister(driver, project_id, vault_id):
        driver.delete_vault(project_id, vault_id)

    """
        DEBUG ONLY
    """
    @staticmethod
    def debuglist(driver, project_id):
        return driver.create_vaults_generator(project_id)
