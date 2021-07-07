import numpy as np
import pandas as pd
import logging


class MyDict(object):
    def __init__(self):
        self._dict = dict()
        self._counter = 0

    def map(self, key):
        if key is None:
            return np.nan

        if key in self._dict:
            return self._dict[key]
        else:
            self._dict[key] = str(self._counter)
            self._counter += 1
            return self._counter - 1

    def save(self, filename: str):
        pd.DataFrame.from_dict(data=self._dict, orient='index', dtype=object).to_csv(filename, header=False, na_rep='-')

    def load(self, filename: str):
        try:
            self._dict = pd.read_csv(filename, header=None, na_values='-').astype(str).set_index(0).to_dict()[1]
            self._counter = len(self._dict)
        except FileNotFoundError:
            logging.warning(f"Secret file {filename} not found, using empty dict.")
        except pd.errors.EmptyDataError:
            logging.warning(f"Secret file {filename} is empty, using empty dict.")
