import logging
import falcon
from model.task import Tasks
import json


class ItemResource(object):

    # def __init__(self, driver):
    #    self.logger = logging.getLogger('testlogging.' + __name__)
    #    self.driver = driver

    '''
    def on_get(self, req, resp, project_id):
        resp.status = falcon.HTTP_200  # This is the default status
        resp.location = '/register/%s' % (project_id)
        resp.body = ('\n\nSystem is registering {0}...\n'.format(project_id))
    '''

    def on_post(self, req, resp, project_id, vault_id):
        try:
            pass
        except ValueError:
            raise falcon.HTTPError(falcon.HTTP_753, 'unknow errors')

        resp.status = falcon.HTTP_201
        resp.location = '/register/%s/%s' % (project_id, vault_id)
        resp.body = ('\n\nSystem is registering {0}/{1}...\n'.format(
            project_id, vault_id))
        Tasks.register(project_id, vault_id)
