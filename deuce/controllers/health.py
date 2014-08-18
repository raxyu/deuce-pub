from deuce.util import log as logging

from pecan import conf, expose, request, response
from pecan.core import abort
from pecan.rest import RestController

import deuce
from deuce.controllers.validation import *

from deuce.model import Health, Vault, Block, File
from deuce.util import set_qs

logger = logging.getLogger(__name__)


class HealthController(object):

    @staticmethod
    def get_health():
        return Health.health()
