from multiprocessing import Process, Queue
import bz2
import logging
from tqdm.auto import tqdm
import platform


class Writer(Process):
    def __init__(self, logfilename: str, batchsize: int, queuelen: int):
        super().__init__(name="Writer")
        self._logfilename = logfilename
        self._batchsize = batchsize

        self._queue = Queue(maxsize=queuelen)
        self._logger = logging.getLogger(self.name)

    def run(self):
        try:
            self._logger.info("Start")

            # open logfile for writing
            # attach compressor
            # create progress bars
            with open(self._logfilename, 'wb') as logfile, \
                    bz2.BZ2File(logfile, mode='w') as logwriter, \
                    tqdm(position=3, desc=self._logfilename, unit='B',
                         unit_scale=True) as pbar_filepos, \
                    tqdm(position=4, unit='line', desc=self._logfilename, unit_scale=True) as pbar_lines, \
                    tqdm(position=5) as pbar_queue:

                # for progress bar
                lastpos = 0

                while True:
                    # get finished batches
                    batch = self._queue.get()

                    # check EOF
                    if batch is None:
                        break

                    # write batch
                    logwriter.write(batch)

                    # update progress bar
                    if logfile.tell() > lastpos:
                        pbar_filepos.update(logfile.tell() - lastpos)
                        lastpos = logfile.tell()
                    pbar_lines.update(self._batchsize)
                    if platform.system() != 'Darwin':
                        pbar_queue.display(f"read queue: {self._queue.qsize()}")

        except KeyboardInterrupt:
            self._logger.info("Interrupt")
        except Exception:
            self._logger.exception("Error")
        finally:
            self._queue.close()

    @property
    def queue(self):
        return self._queue
