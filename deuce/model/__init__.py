from pecan import conf  # noqa

# Hoist up stuff into the model namespace
from vault import Vault
from block import Block
from file import File

from deuce.drivers.storage.blocks.disk import DiskStorageDriver
from deuce.drivers.storage.metadata.sqlite import SqliteStorageDriver

import deuce
deuce.storage_driver = None
deuce.metadata_driver = None

def init_model():
    # TODO: Use stevedore for loading drivers
    deuce.storage_driver = DiskStorageDriver()
    deuce.metadata_driver = SqliteStorageDriver()
