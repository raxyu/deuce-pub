import deucecnc.model
import sched
import threading
from deucecnc import conf

import time


class Worker(object):
    pass


class WorkerValidation(Worker):
    """
        Validation Task
    """
    @staticmethod
    def do_validation():
        pass


class WorkerCleanup(Worker):
    """
        Cleanup Task
    """
    @staticmethod
    def do_cleanup():
        pass
