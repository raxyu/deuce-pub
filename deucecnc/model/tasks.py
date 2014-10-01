import deucecnc.model
import sched
import datetime
import threading

#............DEBUG ONLY...............
import datetime
import time


class Tasks(object):
    def __init__(self):
        self._scheduler = sched.scheduler(time.time, time.sleep)
        self._events = []
        self._thd = None

    def tasks_periodic(self, scheduler, interval, action, actionargs=()):
        print ('yud : tesks_periodic : POS1 ', datetime.datetime.now())
        event = scheduler.enter(interval, 1, self.tasks_periodic,
                      (scheduler, interval, action, actionargs))
        action(*actionargs)
        return event

    def tasks_start(self, interval, task):
        print ('yud : task_start: POS1 : ', datetime.datetime.now())
        event = self.tasks_periodic(self._scheduler, interval, task)
        print ('yud : task_start: POS2 : ', datetime.datetime.now())
        self._events.append(event)
        thd = threading.Thread(target=self._scheduler.run)
        thd.start()
        print ('yud : task_start: POS3 : ', datetime.datetime.now())
        return thd

    def tasks_stop(self):
        #if self._scheduler:
        #    for event in self._events:
        #        print ('yud: cancelling  POS1..... ', event)
        #        self._scheduler.cancel(event)
        for event in self._scheduler.queue:
            print ('yud event 1 : ', event)
            print ('yud event 2 : size[', len(self._events), '], ', self._events[0])
            self._scheduler.cancel(event)
        print ('yud: cancelled.  ', datetime.datetime.now())

        self._thd.join()
        print ('yud tasks_stop... EXIT.  ', datetime.datetime.now())

    """
        Tasks Scheduler
    """
    def tasks_scheduler(self):
        print ('\n\nyud tasks_scheduler POS1...', datetime.datetime.now())

        #schedule.every(1).seconds.do(deucecnc.model.tasks.Tasks.task_validation)
        #for i in range(2):
        #    schedule.run_pending()
        #    time.sleep(5)

        self._thd = self.tasks_start(2, deucecnc.model.tasks.Tasks.task_validation)
        print ('yud tasks_scheduler POS2...', datetime.datetime.now())

        #time.sleep(5)
        #print ('yud tasks_scheduler POS3...', datetime.datetime.now())

        #self.tasks_stop()
        #print ('yud tasks_scheduler POS4... EXIT.  ', datetime.datetime.now())




    @staticmethod
    def register(project_id, vault_id):
        Tasks.driver.add_vault(project_id, vault_id)

    @staticmethod
    def unregister(project_id, vault_id):
        Tasks.driver.delete_vault(project_id, vault_id)

    """
        DEBUG ONLY
    """
    @staticmethod
    def listing(project_id):
        return Tasks.driver.create_vaults_generator(project_id)

    """
        Validation Task
    """
    @staticmethod
    def task_validation():
        #print ('yud : {0}: {1}: Validation periodic task is running... '.format(threading.current_thread(), datetime.datetime.now()))
        print ('yud : task_validation POS1... {0}'.format(datetime.datetime.now()))
        time.sleep(1)
        print ('yud : task_validation POS2... {0}'.format(datetime.datetime.now()))
