# Hoist up stuff into the model namespace
from deuce.model.vault import Vault
from deuce.model.block import Block
from deuce.model.file import File

# Load the storage drivers manually into the model. Note:
# This should change significantly.
from deuce.drivers.storage.blocks.disk import DiskStorageDriver
from deuce.drivers.storage.metadata.sqlite import SqliteStorageDriver

import deuce
deuce.storage_driver = None
deuce.metadata_driver = None

def init_model():
    # TODO: Use stevedore for loading drivers
    deuce.storage_driver = DiskStorageDriver()
    deuce.metadata_driver = SqliteStorageDriver()
