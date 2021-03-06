from multiprocessing import Process, Queue
from queue import Full
from tqdm.auto import tqdm
import bz2
import os
from itertools import islice
import logging
import platform


class Reader(Process):
    def __init__(self, filename: str, batchsize: int, maxlines: int, queuelen: int):
        super().__init__(name=f"Reader-{filename}")
        self._filename = filename
        self._batchsize = batchsize
        self._maxlines = maxlines

        self._queue = Queue(maxsize=queuelen)
        self._logger = logging.getLogger(self.name)

    def run(self):
        try:
            # open logfile for reading
            # attach decompressor
            # create progress bars
            with open(self._filename, 'rb') as logfile, \
                    bz2.BZ2File(logfile) as logreader, \
                    tqdm(total=os.path.getsize(self._filename), position=0, desc=self._filename, unit='B',
                         unit_scale=True) as pbar_filepos, \
                    tqdm(position=1, unit='line', desc=self._filename, unit_scale=True) as pbar_lines, \
                    tqdm(position=2) as pbar_queue:

                # for progress bar
                lastpos = 0

                # slice lines in batches from logreader
                maxitems = self._maxlines
                it = iter(logreader)
                batch = b"".join(islice(it, self._batchsize))
                while batch:
                    # update progress bar
                    if logfile.tell() > lastpos:
                        pbar_filepos.update(logfile.tell() - lastpos)
                        lastpos = logfile.tell()
                    pbar_lines.update(self._batchsize)
                    if platform.system() != 'Darwin':
                        pbar_queue.display(f"read queue: {self._queue.qsize()}")

                    # send them for the workers, this may block for backpressure
                    while True:
                        try:
                            self._queue.put(batch, block=True, timeout=0.1)
                        except Full:
                            continue
                        else:
                            break

                    # check limit (-1 means, no limit)
                    if maxitems != -1:
                        maxitems = max(0, maxitems - self._batchsize)
                        if maxitems == 0:
                            # reached maxitems
                            break

                    # get the next one
                    batch = b"".join(islice(it, self._batchsize))

        except KeyboardInterrupt:
            self._logger.info("Interrupt")
        except Exception:
            self._logger.exception("Error")
        finally:
            self._queue.close()

    @property
    def queue(self):
        return self._queue
