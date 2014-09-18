import logging
import falcon
from model.task import Tasks
import json


class ItemResource(object):

    # def __init__(self, driver):
    #    self.logger = logging.getLogger('testlogging.' + __name__)
    #    self.driver = driver

    def on_put(self, req, resp, project_id, vault_id):
        resp.status = falcon.HTTP_200  # This is the default status
        resp.location = '/unregister/%s/%s' % (project_id, vault_id)
        resp.body = ('\n\nSystem is unregistering {0}/{1}...\n'.format(
            project_id, vault_id))
        Tasks.unregister(self.driver, project_id, vault_id)
