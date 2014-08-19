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

        #  TODO: This is just a space holder. Remove me after impl.
        #      : Need return a meaningful home document.
        response.status_code = 200
        return b'{}'
