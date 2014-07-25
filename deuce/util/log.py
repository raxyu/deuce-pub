import logging

from deuce.common import local

_loggers = {}


class ContextAdapter(logging.LoggerAdapter):

    def process(self, msg, kwargs):
        context = getattr(local.store, 'context', None)
        if context is not None:
            kwargs['extra'] = {'request_id': context.request_id}
            self.extra = kwargs['extra']
        return msg, kwargs


def getLogger(name):
    if name not in _loggers:
        _loggers[name] = ContextAdapter(logging.getLogger(name), extra={})
    return _loggers[name]
