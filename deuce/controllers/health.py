from deuce.util import log as logging

from pecan import conf, expose, request, response
from pecan.core import abort
from pecan.rest import RestController

import deuce
from deuce.controllers.validation import *

from deuce.model import Health

logger = logging.getLogger(__name__)


class HealthController(object):

    @staticmethod
    def get_health():
        return Health.health()
