import logging
import falcon
from deucecnc.model.tasks import Tasks
import json
import deucecnc.util.log as logging
logger = logging.getLogger(__name__)


class ItemResource(object):

    def on_get(self, req, resp, project_id):
        resp.status = falcon.HTTP_200  # This is the default status
        resp.location = '/list/'

        resp.body = json.dumps(Tasks.listing(project_id))
