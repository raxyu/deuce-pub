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
        'db_file': '/tmp/vaultmeta.db',
        'db_module': 'sqlite3'
    },
    'mongodb': {
        'db_file': 'vaultmeta',
        'url': 'mongodb://127.0.0.1',
        'db_module': 'pymongo',
        # An arbitary segment number for blocks fetching and
        # transferring from fileblocks collection to file collection
        'FileBlockReadSegNum': 1000,

        # pymongo block number in each File document
        'maxFileBlockSegNum': 100000
    }
}

api_configuration = {
    # Define system limitation on page size
    'max_returned_num': 100
}
