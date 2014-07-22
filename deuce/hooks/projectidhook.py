
from pecan.hooks import PecanHook
from pecan.core import abort
from pecan import conf
import importlib
from swiftclient.exceptions import ClientException


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

            self.lib_pack = importlib.import_module(
                conf.block_storage_driver.swift.swift_module)
            self.Conn = getattr(self.lib_pack, 'client')

            # TODO: before hook up with patched Repose:
            if 'x-username' in state.request.headers \
                    and 'x-password' in state.request.headers:
                auth_url = conf.block_storage_driver.swift.auth_url
                os_options = dict()
                storage_url, token = \
                    self.Conn.get_keystoneclient_2_0(
                        auth_url=auth_url,
                        user=state.request.headers['x-username'],
                        key=state.request.headers['x-password'],
                        os_options=os_options)

                state.request.storage_url = storage_url
                state.request.auth_token = token
                state.request.project_id = storage_url.rsplit('/')[-1]
                return

            hdr_name = 'x-project-id'
            state.request.project_id = state.request.headers[hdr_name]

            # TODO: hook up patched Repose.

            # TODO: validate the project_id

        except (KeyError, ClientException):
            # Invalid request
            abort(400, comment="Missing Header : X-Project-ID",
                  headers={'Transaction-ID': state.request.context.request_id})
