import six
from pecan.hooks import PecanHook
from deuce.hooks import HealthHook
from pecan.core import abort

import deuce


class OpenstackSwiftHook(HealthHook):
    """Every request that hits Deuce must have a header specifying the
    x-storage-url if running the swift storage driver

    If a request does not provide the header the request should fail
    with a 401"""

    def on_route(self, state):
        if super(OpenstackSwiftHook, self).health(state):
            return

        class OpenStackObject(object):
            pass

        def check_storage_url():

            try:  # pragma: no cover
                # x-storage-url is assumed validated by an outside source
                # (e.g Repose or WSGI middleware
                deuce.context.openstack.swift = OpenStackObject()
                deuce.context.openstack.swift.storage_url = \
                    state.request.headers['x-storage-url']

            except KeyError:  # pragma: no cover
                # Invalid request
                abort(401, comment="Missing Headers : "
                                   "X-Storage-URL",
                    headers={
                        'Transaction-ID': deuce.context.transaction.request_id
                    })

        # Enforce the existence of the x-storage-url header and assign
        # the value to the deuce context's open stack context, if the
        # current storage driver is swift

        if six.PY2:  # pragma: no cover
            if isinstance(deuce.storage_driver,
                          deuce.drivers.swift.py2.SwiftStorageDriver):
                check_storage_url()
            else:
                pass
        elif six.PY3:  # pragma: no cover
            if isinstance(deuce.storage_driver,
                          deuce.drivers.swift.py3.SwiftStorageDriver):
                check_storage_url()
            else:
                pass
