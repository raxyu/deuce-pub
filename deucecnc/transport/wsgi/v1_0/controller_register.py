import logging
import falcon
from deucecnc.model.tasks import Tasks
import json
import deucecnc.util.log as logging
logger = logging.getLogger(__name__)


class ItemResource(object):

    '''
    def on_get(self, req, resp, project_id):
        resp.status = falcon.HTTP_200  # This is the default status
        resp.location = '/register/%s' % (project_id)
        resp.body = ('\n\nSystem is registering {0}...\n'.format(project_id))
    '''

    def on_post(self, req, resp, project_id, vault_id):
        resp.status = falcon.HTTP_201
        resp.location = '/register/%s/%s' % (project_id, vault_id)
        resp.body = ('\n\nSystem is registering {0}/{1}...\n'.format(
            project_id, vault_id))
        Tasks.register(project_id, vault_id)

    def on_delete(self, req, resp, project_id, vault_id):
        resp.status = falcon.HTTP_204  # This is the default status
        resp.location = '/unregister/%s/%s' % (project_id, vault_id)
        resp.body = ('\n\nSystem is unregistering {0}/{1}...\n'.format(
            project_id, vault_id))
        Tasks.unregister(project_id, vault_id)
