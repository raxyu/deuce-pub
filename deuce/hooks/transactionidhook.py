from pecan.hooks import PecanHook

from deuce.openstack.common import local
from deuce.openstack.common import context


class TransactionIDHook(PecanHook):
    def on_route(self, state):

        transaction = context.RequestContext()
        setattr(local.store, 'context', transaction)
        state.request.context = transaction
