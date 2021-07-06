import logging
import os
import pandas as pd
import numpy as np
from multiprocessing import Lock


class MapperClass:
    def __init__(self, hashlen: int, filename: str = None):
        """
        :param hashlen: length of random value generated
        :param filename: autoload secrets
        """
        self._hashlen = hashlen
        self._store = {}
        self._lock = Lock()

        if filename is not None:
            self.load(filename)

    def map(self, key):
        # do not map NaN/None values
        if key is None or key == np.nan:
            return np.nan

        self._lock.acquire(block=True, timeout=10)
        ret = self._store.setdefault(key, os.urandom(self._hashlen).hex())
        self._lock.release()

        return ret

    def save(self, filename: str):
        pd.DataFrame.from_dict(data=self._store, orient='index', dtype=object).to_csv(filename, header=False,
                                                                                      na_rep='-')

    def load(self, filename: str):
        try:
            self._store = pd.read_csv(filename, header=None, na_values='-').astype(str).set_index(0).to_dict()[1]
        except FileNotFoundError:
            logging.warning(f"Secret file {filename} not found, using empty dict.")
        except pd.errors.EmptyDataError:
            logging.warning(f"Secret file {filename} is empty, using empty dict.")
