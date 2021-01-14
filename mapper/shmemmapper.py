from mapper import BaseMapper
from multiprocessing import Manager


class SHMemMapper(BaseMapper):

    def __init__(self, hashlen: int, prefix: str = '', maxcachesize: int = 10000):
        super().__init__(hashlen, prefix, Manager().dict(), maxcachesize)
