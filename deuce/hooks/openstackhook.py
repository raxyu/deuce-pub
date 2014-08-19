
from pecan.hooks import PecanHook
from pecan.core import abort

import deuce


class OpenStackHook(PecanHook):
    """Every request that hits Deuce must have a header specifying the
    auth_token for the user the request is for.

    If a request does not provide the header the request should fail
    with a 401"""

    def on_route(self, state):

        class OpenStackObject(object):
            pass

        deuce.context.openstack = OpenStackObject()

        # Enforce the existence of the x-auth-token header and assign
        # the value to the deuce context's open stack context
        try:

            # auth_token is assumed validated by an outside source (e.g Repose
            # or WSGI middleware
            if hasattr(state.request, 'path') and \
                    (state.request.path.endswith('v1.0/health') or
                    state.request.path.endswith('v1.0/ping')):
                return
            deuce.context.openstack.auth_token = \
                state.request.headers['x-auth-token']
        except KeyError:
            # Invalid request
            abort(401, comment="Missing Header : X-Auth-Token",
                headers={
                    'Transaction-ID': deuce.context.transaction.request_id
                })
