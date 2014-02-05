#!/usr/bin/env python


class FileCat(object):

    """FileCat: Allows multiple files to be handled
    as a single file-like-object for read-only
    operations. Seek is not supported"""

    def __init__(self, fileobjs):
        """Constructs a new FileCat object.
        :param fileobjs: Any iterable capable of
            returning file-like objects. The objects
            must be ready-to-read
        """
        self._objs = fileobjs
        self._current_file = next(fileobjs)

    def read(self, count=None):
        """Reads data and returns it"""

        while True:
            res = self._current_file.read(count)

            if len(res) > 0:
                return res
            else:
                try:
                    self._current_file.close()
                    self._current_file = next(self._objs)
                except StopIteration:
                    return bytes()

if __name__ == '__main__':
    filenames = ["file1.dat", "file2.dat", "file3.dat"]
    file_generator = (open(f, 'rb') for f in filenames)

    with open('outfile.dat', 'wb') as outfile:
        obj = FileCat(file_generator)

        while True:
            content = obj.read(4096)

            outfile.write(content)

            if len(content) == 0:
                break
