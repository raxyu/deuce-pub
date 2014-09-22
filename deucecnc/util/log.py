import logging
from logging.config import dictConfig
from deucecnc.common import local
from config import deuceconfig
_loggers = {}


def setup():
    log_config = deuceconfig.dict()
    log_config.update({'version': 1})
    dictConfig(log_config)


class ContextAdapter(logging.LoggerAdapter):

    def process(self, msg, kwargs):
        context = getattr(local.store, 'context', None)
        if context:
            kwargs['extra'] = {'request_id': context.request_id}
        else:
            kwargs['extra'] = {'request_id': 'Not in wsgi __call__'}
        self.extra = kwargs['extra']
        return msg, kwargs


def getLogger(name):
    if name not in _loggers:
        _loggers[name] = ContextAdapter(logging.getLogger(name), extra={})
    return _loggers[name]
