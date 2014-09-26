import falcon


from deucecnc.transport.wsgi import hooks
from tests import HookTest


def before_hooks(req, resp, params):
    # Openstack Hook included
    return [
        hooks.DeucecncContextHook(req, resp, params),
        hooks.TransactionidHook(req, resp, params),
        hooks.AuthHook(req, resp, params)
    ]


class TestHooks(HookTest):

    def test_auth_hook(self):

        self.app_setup(before_hooks)

        response = self.simulate_get('/list/anyproj',
            headers={'X-Auth-Token': 'blah'})
        self.assertEqual(self.srmock.status, falcon.HTTP_200)

        # Wrong Auth Token
        response = self.simulate_get('/list/anyproj',
            headers={'X-Auth-Token': 'wrong'})
        self.assertEqual(self.srmock.status, falcon.HTTP_401)

        response = self.simulate_get('/list/anyproj')
        self.assertEqual(self.srmock.status, falcon.HTTP_401)
