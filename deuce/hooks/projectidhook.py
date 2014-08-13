
from pecan.hooks import PecanHook
from pecan.core import abort


class ProjectIDHook(PecanHook):
    """Every request that hits Deuce must have a header specifying the
    project id that the request is for. The Project ID is synonymous with
    Account ID, Mosso ID, etc. in the Rackspace world.

    If a request does not provide the header the request should fail
    with a 400"""

    def on_route(self, state):

        # Enforce the existence of the x-project-id header and assign
        # the value to the request project id.
        try:
            state.request.project_id = state.request.headers['x-project-id']
            state.request.storage_hdrs = dict()
            state.request.storage_hdrs['x-project-id'] = \
                state.request.project_id
            if 'x-auth-token' in state.request.headers:
                state.request.storage_hdrs['x-auth-token'] = \
                    state.request.headers['x-auth-token']
            if 'x-storage-url' in state.request.headers:
                state.request.storage_hdrs['x-storage-url'] = \
                    state.request.headers['x-storage-url']
            # TODO: validate the project_id
        except KeyError:
            # Invalid request
            abort(400, comment="Missing Header : X-Project-ID",
                  headers={'Transaction-ID': state.request.context.request_id})
