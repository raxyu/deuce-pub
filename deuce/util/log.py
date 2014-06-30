import logging
import six

from deuce.openstack.common import local

_loggers = {}


class ContextAdapter(logging.LoggerAdapter):

    def process(self, msg, kwargs):
        if not isinstance(msg, six.string_types):
            msg = six.text_type(msg)
        context = getattr(local.store, 'context', None)
        if context:
            return '[Transaction-ID:%s] %s' % (context.request_id, msg), kwargs
        else:
            return ('[No Transaction-ID present] %s' % msg), kwargs


def getLogger(name):
    if name not in _loggers:
        _loggers[name] = ContextAdapter(logging.getLogger(name), extra=None)
    return _loggers[name]
