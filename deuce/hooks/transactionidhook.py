from pecan.hooks import PecanHook

from deuce.common import local
from deuce.common import context


class TransactionIDHook(PecanHook):
    def on_route(self, state):

        transaction = context.RequestContext()
        setattr(local.store, 'context', transaction)
        state.request.context = transaction
