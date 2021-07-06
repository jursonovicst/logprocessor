from multiprocessing import Lock
import pandas as pd
import logging
import os


class MyDict(object):
    def __init__(self, hashlen: int = 8):
        self._lock = Lock()
        self._dict = dict()
        self._hashlen = hashlen

    def map(self, key):
        if not self._lock.acquire(timeout=5):
            raise TimeoutError("Timeout acquiring lock for map!")
        ret = self._dict.setdefault(key, os.urandom(self._hashlen).hex())
        self._lock.release()
        return ret

    def save(self, filename: str):
        if not self._lock.acquire(timeout=5):
            raise TimeoutError("Timeout acquiring lock for save!")
        pd.DataFrame.from_dict(data=self._dict, orient='index', dtype=object).to_csv(filename, header=False, na_rep='-')
        self._lock.release()


    def load(self, filename: str):
        try:
            if not self._lock.acquire(timeout=5):
                raise TimeoutError("Timeout acquiring lock for load!")
            self._dict = pd.read_csv(filename, header=None, na_values='-').astype(str).set_index(0).to_dict()[1]
        except FileNotFoundError:
            logging.warning(f"Secret file {filename} not found, using empty dict.")
        except pd.errors.EmptyDataError:
            logging.warning(f"Secret file {filename} is empty, using empty dict.")
        finally:
            self._lock.release()
