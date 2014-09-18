import logging
import falcon


class ItemResource(object):

    def on_get(self, req, resp, project_id):
        resp.status = falcon.HTTP_200  # This is the default status
        resp.location = '/auth/%s' % (project_id)
        resp.body = ('\n\nSystem is authing {0}...\n'.format(project_id))
