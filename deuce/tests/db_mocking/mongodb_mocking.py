
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
    limit = 0
    retval = dict()
    retval['result'] = []
    blocks = []
    blocksdict = dict()

    for item in list:
        if '$match' in item and item['$match'] == {
                'blocks.offset': {"$gte": 999999999}}:
            # tester asks an empty return.
            retval['result'].append({'blocks': []})
            return retval

        if '$limit' in item:
            limit = item['$limit']
    if limit > 40:
        limit = 40

    for cnt in range(0, limit):
        block = {}
        block['blockid'] = 'block_{0}'.format(cnt)
        block['offset'] = cnt * 333
        blocks.append(block.copy())

    blocksdict['blocks'] = blocks
    retval['result'].append(blocksdict.copy())

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
