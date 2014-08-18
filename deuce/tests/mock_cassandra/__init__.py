
import uuid


class Session(object):

    def __init__(self, conn):
        self.conn = conn

    def execute(self, query, queryargs):

        query = query.replace('false', '0')
        query = query.replace('true', '1')

        if isinstance(queryargs, tuple):

            # convert UUID to string
            queryargs = tuple([str(s) if isinstance(s, uuid.UUID)
                           else s for s in queryargs])

            # sqlite prefers ? over %s for positional args
            query = query.replace('%s', '?')

        elif isinstance(queryargs, dict):

            # If the user passed dictionary arguments, assume that they
            # used that cassandra %(fieldname)s and convert to sqlite's
            # :fieldname

            for k,v in queryargs.items():
                cass_style_arg = "%({0})s".format(k)
                sqlite_style_arg = ":{0}".format(k)
                query = query.replace(cass_style_arg, sqlite_style_arg)

                # Convert UUID parameters to strings
                if isinstance(v, uuid.UUID):
                    queryargs[k] = str(v)

        try:
            res = self.conn.execute(query, queryargs)
        except sqlite3.OperationalError as ex:
            import pdb; pdb.set_trace()

        res = list(res)

        return res

