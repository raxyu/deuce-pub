
class Authentications(object):

    def __init__(self):
        pass

    @staticmethod
    def validate(token, project_id):
        if token is 'wrong':
            return False
        return True
