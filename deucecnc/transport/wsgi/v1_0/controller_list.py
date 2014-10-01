import logging
import falcon
from deucecnc.model.list import List
import json
import deucecnc.util.log as logging
logger = logging.getLogger(__name__)


class ItemResource(object):

    def on_get(self, req, resp, project_id):
        resp.status = falcon.HTTP_200  # This is the default status
        resp.location = '/list/'

        resp.body = json.dumps(List.listing(project_id))
