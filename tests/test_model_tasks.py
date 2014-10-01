from hashlib import md5
from random import randrange
from unittest import TestCase
from deucecnc.model.tasks import Tasks

import time


class TestTasks(TestCase):

    def test_tasks(self):
        task = Tasks()

        task.tasks_schedule_cnc()
        time.sleep(10)
        task.tasks_stop()
