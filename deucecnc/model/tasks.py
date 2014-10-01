import deucecnc.model
import sched
import threading
from deucecnc import conf

import time


class Tasks(object):
    def __init__(self):
        self._scheduler = sched.scheduler(time.time, time.sleep)
        self._thd = None

    """
        Validation Task
    """
    @staticmethod
    def task_validation():
        pass

    """
        Cleanup Task
    """
    @staticmethod
    def task_cleanup():
        pass

    def tasks_periodic(self, scheduler, interval, action, actionargs=()):
        scheduler.enter(interval, 1, self.tasks_periodic,
                      (scheduler, interval, action, actionargs))
        action(*actionargs)

    def tasks_start(self, interval, task):
        event = self.tasks_periodic(self._scheduler, interval, task)
        thd = threading.Thread(target=self._scheduler.run)
        thd.start()
        return thd

    def tasks_stop(self):
        for event in self._scheduler.queue:
            self._scheduler.cancel(event)

        self._thd.join()

    """
        Tasks Scheduler
    """
    def tasks_schedule_cnc(self):
        self._thd = self.tasks_start(
            interval=int(conf.cnc_tasks.tick_interval),
            task=deucecnc.model.tasks.Tasks.task_validation)
        self._thd = self.tasks_start(
            interval=int(conf.cnc_tasks.tick_interval),
            task=deucecnc.model.tasks.Tasks.task_cleanup)

    ###############################################################
    # End point

    @staticmethod
    def register(project_id, vault_id):
        Tasks.driver.add_vault(project_id, vault_id)

    @staticmethod
    def unregister(project_id, vault_id):
        Tasks.driver.delete_vault(project_id, vault_id)

    @staticmethod
    def listing(project_id):
        return Tasks.driver.create_vaults_generator(project_id)
