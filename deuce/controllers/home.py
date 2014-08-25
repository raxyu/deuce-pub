from deuce.util import log as logging

from pecan import expose, response, request
from pecan.rest import RestController

import deuce

from deuce.controllers.validation import *
from deuce.controllers.vault import VaultController
from deuce.controllers.health import HealthController

logger = logging.getLogger(__name__)


class HomeController(RestController):

    vaults = VaultController()

    @expose()
    def index(self):

        #  TODO: This is just a space holder. Remove me after impl.
        #      : Need return a meaningful home document.
        response.status_code = 200
        return b'{}'

    @expose('json')
    def get_one(self, cmd):
        if cmd == 'health':
            return HealthController.get_health()
        if cmd == 'ping':
            response.status_code = 204
            return
        else:
            response.status_code = 404
