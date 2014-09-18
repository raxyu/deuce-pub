import logging
import falcon
from model.task import Tasks
import json


class ItemResource(object):

    def on_get(self, req, resp, project_id):
        resp.status = falcon.HTTP_200  # This is the default status
        resp.location = '/list/'

        resp.body = json.dumps(Tasks.debuglist(self.driver, project_id))
