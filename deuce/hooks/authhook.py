
from pecan.hooks import PecanHook
from pecan.core import abort
from pecan import conf
import importlib
from swiftclient.exceptions import ClientException


class AuthHook(PecanHook):

    def on_route(self, state):

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
                state.request.headers['x-project-id'] = \
                    storage_url.rsplit('/')[-1]
                return

            # TODO: hook up patched Repose.

            # TODO: validate the project_id

        except (KeyError, ClientException):
            # Invalid request
            abort(403, comment="Auth failed",
                  headers={'Transaction-ID': state.request.context.request_id})
