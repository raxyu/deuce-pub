import os
from unittest import TestCase
from pecan import set_config
import pecan
from pecan.testing import load_test_app

__all__ = ['FunctionalTest']

import os
import shutil


def setUp():
    if not os.path.exists('/tmp/block_storage'):
        os.mkdir('/tmp/block_storage')

    # Copy the production configs to the test path.
    if not os.path.exists('tests/config.py'):
        os.symlink('../../config.py', 'tests/config.py')
    if not os.path.exists('tests/public'):
        os.symlink('../../public', 'tests/public')
    if not os.path.exists('tests/deuce/templates'):
        if not os.path.exists('tests/deuce'):
            os.mkdir('tests/deuce')
        os.symlink('../../../deuce/templates', 'tests/deuce/templates')


def tearDown():
    shutil.rmtree('/tmp/block_storage')

    # Always remove the database so that we can start over on
    # test execution
    # Drop sqlite DB
    if os.path.exists('/tmp/deuce_sqlite_unittest_vaultmeta.db'):
        os.remove('/tmp/deuce_sqlite_unittest_vaultmeta.db')

    # Cleanup the test configs.
    if os.path.exists('tests/config.py'):
        os.remove('tests/config.py')
    if os.path.exists('tests/public'):
        os.remove('tests/public')
    if os.path.exists('tests/deuce/templates'):
        shutil.rmtree('tests/deuce')


class FunctionalTest(TestCase):
    """
    Used for functional tests where you need to test your
    literal application and its integration with the framework.
    """
    def setUp(self):
        self.app = load_test_app(os.path.join(
            os.path.dirname(__file__),
            'config.py'
        ))

        self._storagedir = pecan.conf.block_storage_driver.options.path

    def tearDown(self):
        set_config({}, overwrite=True)
