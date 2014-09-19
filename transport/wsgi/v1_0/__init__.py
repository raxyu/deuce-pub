from transport.wsgi.v1_0 import controller_auth
from transport.wsgi.v1_0 import controller_register
from transport.wsgi.v1_0 import controller_list


def public_endpoints():

    return [

        ('/auth/{project_id}',
         controller_auth.ItemResource()),

        ('/register/{project_id}/{vault_id}',
         controller_register.ItemResource()),

        ('/list/{project_id}',
         controller_list.ItemResource()),

    ]
