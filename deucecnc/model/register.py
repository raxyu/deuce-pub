import deucecnc


class Register(object):
    ###############################################################
    # End point

    @staticmethod
    def register(project_id, vault_id):
        deucecnc.db_driver.add_vault(project_id, vault_id)

    @staticmethod
    def unregister(project_id, vault_id):
        deucecnc.db_driver.delete_vault(project_id, vault_id)
