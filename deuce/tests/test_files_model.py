import os
import hashlib
from random import randrange
import six
from unittest import TestCase
from deuce.tests import FunctionalTest
from deuce.model import File


class TestFilesModel(FunctionalTest):

    def setUp(self):
        super(TestFilesModel, self).setUp()

        # Create a vault and a file for us to work with
        vault_name = 'files_vault_test'
        fileid = 'My_Testing_File_Id'
        self._file = File(vault_name, fileid)

    def tests(self):
        retval = self._file.file_id
        retval = self._file.vault_id
        retval = self._file.finalized
