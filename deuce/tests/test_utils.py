from unittest import TestCase
from deuce.util import FileCat
from deuce.tests.util import MockFile
import os
from random import randrange
from hashlib import md5

# TODO: We probably want to move this to a
# test helpers library


class TestFileCat(TestCase):

    def test_full_read(self):
        num_files = 9
        min_file_size = 1
        max_file_size = 5

        file_sizes = [randrange(min_file_size, max_file_size)
                      for i in range(0, num_files)]

        files = [MockFile(size) for size in file_sizes]

        # Calculate an md5 of all of our files.
        z = md5()
        for f in files:
            z.update(f._content)

        expected_size = sum(file_sizes)
        expected_md5 = z.hexdigest()

        # FileCat only takes generators
        fc = FileCat((f for f in files))

        data = fc.read()  # read it all

        z = md5()
        z.update(data)
        computed_md5 = z.hexdigest()

        assert len(data) == sum(file_sizes)
        assert computed_md5 == expected_md5

    def test_small_read(self):
        num_files = 7
        min_file_size = 0
        max_file_size = 10000

        file_sizes = [randrange(min_file_size, max_file_size)
                      for i in range(0, num_files)]

        files = [MockFile(size) for size in file_sizes]

        # Calculate an md5 of all of our files.
        z = md5()
        for f in files:
            z.update(f._content)

        expected_size = sum(file_sizes)
        expected_md5 = z.hexdigest()

        # FileCat only takes generators
        fc = FileCat((f for f in files))

        z = md5()
        bytes_read = 0

        while True:
            buff = fc.read(99)
            assert len(buff) <= 99

            if len(buff) == 0:
                break  # DONE

            bytes_read += len(buff)
            z.update(buff)

        computed_md5 = z.hexdigest()

        assert bytes_read == sum(file_sizes)
        assert computed_md5 == expected_md5
