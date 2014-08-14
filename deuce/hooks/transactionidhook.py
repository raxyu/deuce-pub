from pecan.hooks import PecanHook

from deuce.common import local
from deuce.common import context

import deuce


class TransactionIDHook(PecanHook):
    def on_route(self, state):

        transaction = context.RequestContext()
        setattr(local.store, 'context', transaction)
        deuce.context.request_id = transaction

        from pecan import response
        response.headers['X-Transaction-ID'] = deuce.context.request_id
