import deucecnc


class List(object):
    ###############################################################
    # End point

    @staticmethod
    def listing(project_id):
        return deucecnc.db_driver.create_vaults_generator(project_id)
