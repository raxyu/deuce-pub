from deuce.util import log as logging

from pecan import expose, response, request
from pecan.rest import RestController

import deuce

from deuce.controllers.validation import *
from deuce.controllers.vault import VaultController

logger = logging.getLogger(__name__)


class HomeController(RestController):

    vaults = VaultController()
    @expose()
    def index(self):
        logger.warning('Invalid controller index request')
        response.status_code = 404
