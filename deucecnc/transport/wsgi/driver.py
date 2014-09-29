
from wsgiref import simple_server

import falcon

from deucecnc.transport.wsgi import v1_0
from deucecnc.transport.wsgi import hooks

import deucecnc.model
import deucecnc.util.log as logging
from deucecnc import conf


class Driver(object):

    def __init__(self):
        self.app = None
        self._init_routes()
        deucecnc.model.init_model()
        # To schedule tasks of validation, cleanup, etc.
        tasks = deucecnc.model.tasks.Tasks()
        tasks.tasks_scheduler()

    def before_hooks(self, req, resp, params):

        # Disk + Sqlite

        return [
            hooks.DeucecncContextHook(req, resp, params),
            hooks.TransactionidHook(req, resp, params),
            hooks.AuthHook(req, resp, params)
        ]

    def _init_routes(self):
        """Initialize hooks and URI routes to resources."""

        endpoints = [
            ('', v1_0.public_endpoints()),

        ]

        self.app = falcon.API(before=self.before_hooks)

        for version_path, endpoints in endpoints:
            for route, resource in endpoints:
                self.app.add_route(version_path + route, resource)

    def listen(self):
        """Self-host using 'bind' and 'port' from conf"""
        msgtmpl = (u'Serving on host %(bind)s:%(port)s')
        logger = logging.getLogger(__name__)
        logger.info(msgtmpl,
                    {'bind': conf.server.host, 'port': conf.server.port})

        httpd = simple_server.make_server(conf.server.host,
                                          conf.server.port,
                                          self.app)
        httpd.serve_forever()
