import logging
import six

from deuce.openstack.common import local

_loggers = {}


class ContextAdapter(logging.LoggerAdapter):

    def process(self, msg, kwargs):

        if not isinstance(msg, six.string_types):
            msg = six.text_type(msg)

        context = getattr(local.store, 'context', None)
        kwargs['extra'] = {'request_id': context.request_id}
        self.extra = kwargs['extra']
        return msg, kwargs


def getLogger(name):
    if name not in _loggers:
        _loggers[name] = ContextAdapter(logging.getLogger(name), extra={})
    return _loggers[name]
