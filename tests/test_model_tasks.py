from hashlib import md5
from random import randrange
from unittest import TestCase
from deucecnc.model.tasks import Tasks


class TestTasks(TestCase):

    def test_tasks(self):
        task = Tasks()

        task.tasks_scheduler()
        task.tasks_stop()
