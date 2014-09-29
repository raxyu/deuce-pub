import falcon
from functools import wraps
from deucecnc import auth_driver


def authbypass(func):
    @wraps(func)
    def wrap(*args, **kwargs):
        if args[0].relative_uri.startswith('/auth/'):
            return
        else:
            func(*args, **kwargs)
    return wrap


def token_is_valid(token, project_id):
    # TODO 1. Credentials validating.
    # TODO 2. Update credentials with Auth.
    # TODO 3. Cache credentials so don't talk to Auth eveyrtime.

    return auth_driver.validate(token, project_id)


@authbypass
def AuthHook(req, resp, params):
    # Alternatively, use Talons or do this in WSGI middleware...
    token = req.get_header('X-Auth-Token')

    if token is None:
        description = ('Please provide an auth token '
                       'as part of the request.')

        raise falcon.HTTPUnauthorized('Auth token required',
                                      description,
                                      href='http://docs.example.com/auth')

    if 'project_id' in params and \
            not token_is_valid(token, params['project_id']):
        description = ('The provided auth token is not valid. '
                       'Please request a new token and try again.')

        raise falcon.HTTPUnauthorized('Authentication required',
                                      description,
                                      href='http://docs.example.com/auth',
                                      scheme='Token; UUID')