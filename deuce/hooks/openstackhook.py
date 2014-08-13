
from pecan.hooks import PecanHook
from pecan.core import abort


class OpenstackHook(PecanHook):
    """Every request that hits Deuce must have a header specifying the
    auth_token for the user the request is for.

    If a request does not provide the header the request should fail
    with a 401"""

    def on_route(self, state):

        # Enforce the existence of the x-auth-token header and assign
        # the value to the request auth_token.
        try:
            state.request.storage_hdrs = dict()
            state.request.storage_hdrs['x-project-id'] = \
                state.request.project_id

            # auth_token is assumed validated by an outside source (e.g Repose
            # or WSGI middleware
            state.request.storage_hdrs['x-auth-token'] = \
                state.request.headers['x-auth-token']

            if 'x-storage-url' in state.request.headers:
                state.request.storage_hdrs['x-storage-url'] = \
                    state.request.headers['x-storage-url']

        except KeyError:
            # Invalid request
            abort(401, comment="Missing Header : X-Auth-Token",
                  headers={'Transaction-ID': state.request.context.request_id})
