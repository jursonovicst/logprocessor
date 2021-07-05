import logging
import operator
from cachetools import LRUCache, cachedmethod
import os
import pandas as pd
import numpy as np
from multiprocessing import Lock

from multiprocessing.managers import BaseManager


class MyManager(BaseManager):
    pass


class HashClass:
    def __init__(self, hashlen: int, prefix: str = '', filename: str = None, maxcachesize: int = 10000):
        """
        :param hashlen: length of hash generated
        :param prefix: prefix returned values
        :param filename: autoload secrets
        :param maxcachesize: size of the accelerator cache
        """
        self._hashlen = hashlen
        self._prefix = prefix.lower()
        self._store = {}
        self._lock = Lock()
        self.cache = LRUCache(maxsize=maxcachesize)

        if filename is not None:
            self.load(filename)

    @cachedmethod(operator.attrgetter('cache'))
    def get(self, key):
        # do not map NaN/None values
        if key is None or key == np.nan:
            return key

        if self._prefix is None:
            self._lock.acquire(block=True, timeout=10)
            if key not in self._store:
                self._store[key] = os.urandom(self._hashlen).hex()
            self._lock.release()
        else:
            self._lock.acquire(block=True, timeout=10)
            if key not in self._store:
                self._store[key] = self._prefix + "-" + os.urandom(self._hashlen).hex()
            self._lock.release()

        return f"{self._store[key]}"

    def save(self, filename: str):
        print(self._store)
        pd.DataFrame.from_dict(data=self._store, orient='index', dtype=object).to_csv(filename, header=False,
                                                                                      na_rep='-')

    def load(self, filename: str):
        try:
            self._store = pd.read_csv(filename, header=None, na_values='-').astype(str).set_index(0).to_dict()[1]
            print(self._store)
        except FileNotFoundError:
            logging.warning(f"Secret file {filename} not found, using empty dict.")
        except pd.errors.EmptyDataError:
            logging.warning(f"Secret file {filename} is empty, using empty dict.")
