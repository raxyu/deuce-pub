
# Either use real Python MongoDB module...
#
# import pymongo
# from pymongo import MongoClient as mongoclient
#
# Or from a mocking package...
from mongomock.connection import Connection
from mongomock.collection import Collection


# Mongomock does not have aggregate impl.
# Patch a quick impl.
def patch_aggregate(self, list):
    if self._aggregate_calling_cnt is 0:
        retval = dict()
        retval['result'] = [{'blocks': [{'blockid': 'full mocking'}]}]
        self._aggregate_calling_cnt += 1
        return retval
    else:
        retval = dict()
        retval['result'] = [{'blocks': []}]
        self._aggregate_calling_cnt += 1
        return retval

old_init = Collection.__init__


def new_init(self, db, name):
    old_init(self, db, name)
    self._aggregate_calling_cnt = 0

Collection.aggregate = patch_aggregate
Collection.__init__ = new_init


def MongoClient(url):
    # Either use real Python MongoDB module...
    #
    # return mongoclient(url)
    #
    # Or from a mocking package...
    return Connection()
