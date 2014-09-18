import os
import hashlib
import uuid
from unittest import TestCase

__all__ = ['FunctionalTest']

import shutil


prod_conf = None
conf_dict = {}


class DummyContextObject(object):
    pass


def setUp():
    """
        Unit tests environment setup.
        Called only once at the beginning.
    """
    global prod_conf
    global conf_dict
    if not os.path.exists('/tmp/block_storage'):
        os.mkdir('/tmp/block_storage')


def tearDown():
    """
        Unit tests environment cleanup.
        Called only once at the end.
    """
    shutil.rmtree('/tmp/block_storage')

    # Always remove the database so that we can start over on
    # test execution
    # Drop sqlite DB
    if os.path.exists('/tmp/deuce_sqlite_unittest_vaultmeta.db'):
        os.remove('/tmp/deuce_sqlite_unittest_vaultmeta.db')


class FunctionalTest(TestCase):

    """
    Used for functional tests where you need to test your
    literal application and its integration with the framework.
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass
