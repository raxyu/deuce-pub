
from pecan.rest import RestController
from pecan.core import abort
from pecan import expose, request, response


class DeuceController(RestController):
    """DeuceController allows us to enforce
    policies one each routed request (such as requiring
    an X-Project-ID header, without having to
    explicitly decorate each exposed endpoint.
    """
    def __init__(self):
        super(RestController, self).__init__()

    @expose()
    def _route(self, args):
        try:
            project_id = request.headers['X-PROJECT-ID']
        except KeyError:
            abort(400)  # Bad Request

        return RestController._route(self, args)
