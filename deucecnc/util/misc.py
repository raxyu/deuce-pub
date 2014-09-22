
# Find the

try:  # pragma: no cover
    import six.moves.urllib.parse as parse
except ImportError:  # pragma: no cover
    import urllib.parse as parse


def set_qs(url, args={}):
    """ Sets the query string on a URL using a dictionary """

    parts = list(parse.urlparse(url))
    parts[4] = parse.urlencode(args)
    return parse.urlunparse(parts)
