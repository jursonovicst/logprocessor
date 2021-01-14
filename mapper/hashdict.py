from os import urandom


class HashDict(dict):

    def __init__(self, hashlen: int, prefix: str = ''):
        super().__init__()
        self._hashlen = hashlen
        self._prefix = prefix

    def __missing__(self, key):
        if self._prefix is None:
            return urandom(self._hashlen).hex()
        else:
            return self._prefix + "-" + urandom(self._hashlen).hex()
