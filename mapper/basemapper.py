import operator
from cachetools import LRUCache, cachedmethod
import os
import pandas as pd
import numpy as np


class BaseMapper(object):
    """
    Base mapper client, maps key values to randomly generated hex strings.
    """

    def __init__(self, hashlen: int, prefix: str = '', store: dict = None, maxcachesize: int = 10000):
        """
        :param hashlen: length of hash generated
        :param prefix: prefix returned values
        :param store: use the specified store for storing the objects
        :param maxcachesize: size of the accelerator cache
        """
        self._hashlen = hashlen
        self._prefix = prefix.lower()
        self._store = {} if store is None else store
        self.cache = LRUCache(maxsize=maxcachesize)

    @cachedmethod(operator.attrgetter('cache'))
    def get(self, key):
        # do not map NaN/None values
        if key is None or key == np.nan:
            return key

        if self._prefix is None:
            self._store[key] = os.urandom(self._hashlen).hex()
        else:
            self._store[key] = self._prefix + "-" + os.urandom(self._hashlen).hex()

        return f"{self._store[key]}"

    def save(self, filename: str):
        pd.DataFrame.from_dict(data=self._store, orient='index').to_csv(filename, header=False, quotechar='"')

    def load(self, filename: str):
        try:
            self._store = pd.read_csv(filename, index_col=0, header=None, quotechar='"').to_dict()[1]
        except FileNotFoundError:
            pass
