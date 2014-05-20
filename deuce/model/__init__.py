from pecan import conf

# Hoist up stuff into the model namespace
from deuce.model.vault import Vault
from deuce.model.block import Block
from deuce.model.file import File

# Load the storage drivers manually into the model. Note:
# This should change significantly.
from deuce.drivers.storage.blocks.disk import DiskStorageDriver

import deuce
import importlib

deuce.storage_driver = None
deuce.metadata_driver = None


def init_model():
    # Load metadata driver
    db_class = conf.metadata_driver.module
    db_path = conf.metadata_driver.driver_path
    assert db_class
    assert db_path

    mod = importlib.import_module(db_path)
    deuce.metadata_driver = getattr(mod, db_class)()

    # TODO: Use stevedore for loading drivers
    deuce.storage_driver = DiskStorageDriver()
