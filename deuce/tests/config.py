import deuce

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
    'static_root': '%(confdir)s/../../public',
    'template_path': '%(confdir)s/../../deuce/templates',
    'debug': True,
    'hooks': get_hooks(),
    'errors': {
        404: '/error/404',
        '__force_dict__': True
    }
}

logging = {
    'loggers': {
        'root': {'level': 'INFO', 'handlers': ['logfile']},
        'deuce': {'level': 'DEBUG', 'handlers': ['logfile']},
        'py.warnings': {'handlers': ['logfile']},
        '__force_dict__': True
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple'
        },
        'logfile': {
            'class': 'logging.FileHandler',
            'filename': '/var/log/deuce.log',
            'level': 'INFO',
            'formatter': 'simple'
        }
    },
    'formatters': {
        'simple': {
            'format': ('%(asctime)s %(levelname)-5.5s [%(name)s/%(lineno)d]'
                       '[%(threadName)s] : %(message)s')
        }
    }
}

import os

if not os.path.exists('/tmp/block_storage'):
    os.mkdir('/tmp/block_storage')

block_storage_driver = {
    'driver': 'deuce.drivers.storage.blocks.disk.DiskStorageDriver',
    'options': {
        'path': '/tmp/block_storage'
    }
}


metadata_driver = {
    'driver': ('deuce.drivers.storage.metadata.sqlite.sqlitestoragedriver.'
        'SqliteStorageDriver'),
    'sqlite': {
        'path': '/tmp/deuce_sqlite_unittest_vaultmeta.db',
        'db_module': 'sqlite3'
    },
    'mongodb': {
        'path': 'deuce_mongo_unittest_vaultmeta',
        'url': 'mongodb://127.0.0.1',
        'db_file': '/tmp/deuce_mongo_unittest_vaultmeta.db',

        # Production DB module or unittest
        # With real mongodb daemon.
        # 'is_mocking': False,
        # 'db_module': 'pymongo',
        #
        # Mocking DB module.
        'is_mocking': True,
        'db_module': 'deuce.tests.db_mocking.mongodb_mocking',

        # An arbitary segment number for blocks fetching and
        # transferring from fileblocks collection to file collection
        #    'FileBlockReadSegNum': 1000
        'FileBlockReadSegNum': 10,

        # pymongo block number in each File document
        # 'maxFileBlockSegNum': 100000
        'maxFileBlockSegNum': 30
    }
}

api_configuration = {
    # Define system limitation on page size
    'max_returned_num': 80,
}

# Always remove the database so that we can start over on
# test execution
# Drop sqlite DB
if os.path.exists('/tmp/deuce_sqlite_unittest_vaultmeta.db'):
    os.remove('/tmp/deuce_sqlite_unittest_vaultmeta.db')
