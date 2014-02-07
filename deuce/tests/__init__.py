import os
from unittest import TestCase
from pecan import set_config
import pecan
from pecan.testing import load_test_app

__all__ = ['FunctionalTest']

import os
import shutil


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

        if not os.path.exists(self._storagedir):
            os.mkdir(self._storagedir)

    def tearDown(self):
        set_config({}, overwrite=True)
        shutil.rmtree(self._storagedir)
