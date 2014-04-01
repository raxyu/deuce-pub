
import sqlite3


class Connection(object):
    def __init__(self, dbfile):
        self._conn = sqlite3.Connection(dbfile)
        self._files = {}
        self._filescnt = 0
        self._blocks = {}
        self._blockscnt = 0

    def execute(self, cmd, args={}):
        # propagate table
        cmd = ' '.join(cmd.split())
        #print ("cmd: ", cmd)
        if cmd.startswith('INSERT INTO files'):
            self._files[self._filescnt] = args
            self._filescnt += 1

        if cmd.startswith('INSERT INTO blocks'):
            self._blocks[self._blockscnt] = args
            self._blockscnt += 1
        #if cmd.startswith('SELECT size FROM blocks'):
        #    print ("YUDEBUG: here.", self._blocks)
        #    retval = []
        #    for cnt in range (self._blockscnt):
        #        if self._blocks[cnt]['projectid'] ==
        #            args['projectid'] and
        #            self._blocks[cnt]['vaultid'] ==
        #            args['vaultid'] and
        #            self._blocks[cnt]['blockid'] ==
        #            args['blockid']:
        #            retval.extend([self._blocks[cnt]['blocksize']])
        #    return ((ret for ret in retval))

        return self._conn.execute(cmd, args)

    def commit(self):
        pass
