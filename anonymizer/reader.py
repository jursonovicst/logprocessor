from multiprocessing import Process, Queue
from tqdm.auto import tqdm
import bz2
import os
from itertools import islice
import logging


class Reader(Process):
    def __init__(self, logfilename: str, batchsize: int, maxlines: int, queuelen: int):
        super().__init__(name="Reader")
        self._logfilename = logfilename
        self._batchsize = batchsize
        self._maxlines = maxlines

        self._queue = Queue(maxsize=queuelen)
        self._logger = logging.getLogger(self.name)

    def run(self):
        try:
            # open logfile for reading
            # attach decompressor
            # create progress bars
            with open(self._logfilename, 'rb') as logfile, \
                    bz2.BZ2File(logfile) as logreader, \
                    tqdm(total=os.path.getsize(self._logfilename), position=0, desc=self._logfilename, unit='B',
                         unit_scale=True) as pbar_filepos, \
                    tqdm(position=1, unit='line', desc=self._logfilename, unit_scale=True) as pbar_lines:

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

                    # send them for the workers, this may block for backpressure
                    self._queue.put(batch)

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
            self._logger.exception()
        finally:
            self._queue.close()

    @property
    def queue(self):
        return self._queue
