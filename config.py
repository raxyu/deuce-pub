# Server Specific Configurations
server = {
    'port': '8080',
    'host': '0.0.0.0'
}


def get_hooks():
    from deuce.hooks import ProjectIDHook
    return [ProjectIDHook()]

# Pecan Application Configurations
app = {
    'root': 'deuce.controllers.root.RootController',
    'modules': ['deuce'],
    'static_root': '%(confdir)s/public',
    'template_path': '%(confdir)s/deuce/templates',
    'debug': True,
    'hooks': get_hooks(),
    'errors': {
        404: '/error/404',
        '__force_dict__': True
    }
}

logging = {
    'loggers': {
        'root': {'level': 'INFO', 'handlers': ['console']},
        'deuce': {'level': 'DEBUG', 'handlers': ['console']},
        'py.warnings': {'handlers': ['console']},
        '__force_dict__': True
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple'
        }
    },
    'formatters': {
        'simple': {
            'format': ('%(asctime)s %(levelname)-5.5s [%(name)s]'
                       '[%(threadName)s] %(message)s')
        }
    }
}

block_storage_driver = {
    'driver': 'deuce.drivers.storage.blocks.disk.DiskStorageDriver',
    'options': {
        'path': '/tmp/block_storage'
    }
}

metadata_driver = {
    'driver-path': 'deuce.drivers.storage.metadata.sqlite.sqlitestoragedriver',
    'module': 'SqliteStorageDriver',
    'sqlite': {
        'path': '/tmp/vaultmeta.db'
    },
    'mongodb': {
        'path': 'vaultmeta',
        'url': 'mongodb://127.0.0.1'
    }
}

api_configuration = {
    'max_returned_num': 100
}
