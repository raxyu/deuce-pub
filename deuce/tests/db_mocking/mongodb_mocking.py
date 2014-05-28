
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
def patch_aggregate(self, arglist):
    num_blocks = 0
    for item in arglist:
        if any(key == '$limit' for key in item):
            num_blocks = item['$limit']

    if num_blocks == -1:
        retval = dict()
        retval['result'] = [{'blocks': []}]
        return retval

    if num_blocks != 4:
        num_blocks = 40

    retval = dict()

    blocks = [{'blockid': 'block_{0}'.format(x),
        'offset': x * 333} for x in range(0, num_blocks)]
    retval['result'] = [{'blocks': blocks}]

    return retval

old_init = Collection.__init__


def new_init(self, db, name):
    old_init(self, db, name)

Collection.aggregate = patch_aggregate
Collection.__init__ = new_init


def MongoClient(url):
    # Either use real Python MongoDB module...
    #
    # return mongoclient(url)
    #
    # Or from a mocking package...
    return Connection()
