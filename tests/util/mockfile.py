
import os
import hashlib


class MockFile(object):

    def __init__(self, size):
        self._pos = 0
        self._content = os.urandom(size)
        self._sha1 = hashlib.sha1()
        self._sha1.update(self._content)

    def seek(self, pos):
        self._pos = pos

    def close(self):
        pass

    def read(self, cnt=None):

        if cnt is None:
            cnt = len(self._content)

        bytes_remain = len(self._content) - self._pos
        bytes_to_read = min(cnt, bytes_remain)

        res = self._content[self._pos:self._pos + bytes_to_read]
        self._pos += bytes_to_read

        return res

    def sha1(self):
        return self._sha1.hexdigest()
