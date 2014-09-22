import deucecnc

from deucecnc.common import context
from deucecnc.common import local


def TransactionidHook(req, resp, params):
    transaction = context.RequestContext()
    setattr(local.store, 'context', transaction)
    deucecnc.context.transaction = transaction
    resp.set_header('Transaction-ID', deucecnc.context.transaction.request_id)
