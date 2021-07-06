#!/usr/bun/env python3
import argparse
from multiprocessing import cpu_count
import configparser
from anonymizer import Reader, Worker, MyDict
import logging
from multiprocessing.managers import BaseManager

parser = argparse.ArgumentParser()
parser.add_argument('logfile', type=str)
parser.add_argument('cachename', type=str)
parser.add_argument('popname', type=str)
parser.add_argument('--nproc', type=int, default=max(2, cpu_count() - 2),
                    help="Number of worker processes to start (default: %(default)s)")
parser.add_argument('--cachesize', type=int, default=10000,
                    help="Per process local cache size (default: %(default)s)")
parser.add_argument('--maxlines', type=int, default=-1,
                    help="Number of rows of file to read (default: %(default)s)")
parser.add_argument('--chunksize', type=int, default=10000,
                    help="Chunk (lines processed together) size (default: %(default)s)")
parser.add_argument('--queuelen', type=int, default=5,
                    help="Length of the inter process queue (default: %(default)s). Use it to control read-ahead...")
parser.add_argument('--encoding', type=str, default='utf8',
                    help="Encoding to use when reading/writing (default: %(default)s)")
parser.add_argument('--delimiter', type=str, default=' ',
                    help="Delimiter to use. If sep is None, the C engine cannot automatically detect the separator, but the Python parsing engine can, meaning the latter will be used and automatically detect the separator by Python’s builtin sniffer tool, csv.Sniffer. In addition, separators longer than 1 character and different from '\\s+' will be interpreted as regular expressions and will also force the use of the Python parsing engine. Note that regex delimiters are prone to ignoring quoted data. Regex example: '\\r\\t'. (default: %(default)s)")
parser.add_argument('--quotechar', type=str, default='"',
                    help="The character used to denote the start and end of a quoted item. Quoted items can include the delimiter and it will be ignored. (default: %(default)s)")
parser.add_argument('--navalues', type=str, default='-',
                    help="Additional strings to recognize as NA/NaN. (default: %(default)s)")
parser.add_argument('--escapechar', type=str, default='\\',
                    help="One-character string used to escape other characters (default: %(default)s)")
parser.add_argument('--configfile', type=str, default='config.ini', help='etc...')

config = configparser.ConfigParser()

if __name__ == "__main__":
    try:
        # arguments
        args = parser.parse_args()

        # logging
        logging.basicConfig(level=logging.INFO)

        # config
        config.read(args.configfile)

        # params
        params = [('cachename', 4), ('popname', 4), ('host', 8), ('coordinates', 8),
                  ('devicebrand', 4), ('devicefamily', 4), ('devicemodel', 4), ('osfamily', 4), ('uafamily', 4),
                  ('uamajor', 4), ('path', 16), ('livechannel', 4), ('contentpackage', 8), ('assetnumber', 8),
                  ('uid', 12), ('sid', 12)]

        # prefixes
        prefixes = ['cachename', 'popname', 'host', 'coordinates', 'devicebrand', 'devicefamily', 'devicemodel',
                    'osfamily', 'uafamily', 'uamajor', 'path', 'livechannel', 'contentpackage', 'assetnumber',
                    'uid', 'sid']
        hashlens = [4, 4, 8, 8, 4, 4, 4,
                    4, 4, 4, 16, 4, 8, 8,
                    12, 12]

        # register
        BaseManager.register('MyDict', MyDict)

        # managers
        managers = [BaseManager() for prefix in prefixes]

        # start
        list(map(lambda manager: manager.start(), managers))

        # shared dicts
        mydicts = {prefix: manager.MyDict(hashlen) for prefix, manager, hashlen in zip(prefixes, managers, hashlens)}

        # load from disk
        list(map(lambda mydict, prefix: mydict.load(f"secrets/secrets_{prefix}.csv"), mydicts.values(), prefixes))

        # create reader and writer processes
        reader = Reader(args.logfile, args.chunksize, args.maxlines, args.queuelen)

        # start worker processes with initializer (worker parameters and secrets)
        # open raw logfile
        # create progress bar for file position
        # create progress bar for processed lines
        workers = [
            Worker(i, f"{args.logfile}.ano-{i}.bz2", reader.queue, mydicts, args.cachename,
                   args.popname, config['secrets'].getint('timeshiftdays'), config['secrets'].getfloat('xyte'),
                   args.cachesize,
                   encoding=args.encoding,
                   delimiter=args.delimiter,
                   quotechar=args.quotechar,
                   na_values=args.navalues,
                   escapechar=args.escapechar,
                   header=None,
                   on_bad_lines='skip',
                   # X             X                            X                                                                              X   X     X                         X        X           X         X                                                                X                                                            X
                   # 0         1 2 3                     4      5                                                                              6   7 8   9              10         11       12  13      14  15 16 17                 18      19                                    20                                                 21 22     23
                   # 127.0.0.1 - - [22/Feb/2222:22:22:22 +0100] "GET http://xyz.cdn.de/this/is/the/path?and_this_is_the_query_string HTTP/1.1" 304 0 "-" "okhttp/4.9.0" xyz.cdn.de 0.000130 215 upstrea hit - 614 "application/json" 6596557 "session=-,INT-4178154,-,-; HttpOnly" "2222:22:2222:2222:2222:2222:2222:2222, 127.0.0.1" - TLSv1.2 c

                   # 0         1 2 3                     4      5                                         6   7   8   9              10          11       12  13       14  15 16  17                18        19                                      20                                 21                                      22                         23 24     25
                   # 127.0.0.1 - - [30/Jun/2021:07:05:20 +0200] "GET http://xyz.cdn.de/blablabl HTTP/1.1" 200 950 "-" "okhttp/4.9.0" xyz.cdn.com 0.000125 180 upstream hit - 1627 "application/zip" 978608424 "session=-,INT-969498284,-,-; HttpOnly" "Cache-Control:public,max-age=300" "ETag:18ad26753cb3db1be3cf097badf6df5d" "89.204.153.53, 127.0.0.1" - TLSv1.2 c
                   usecols=[0, 3, 5, 6, 7, 9, 10, 11, 12, 14, 17, 19, 20, 22, 25],
                   names=['ip', '#timestamp', 'request', 'statuscode', 'contentlength', 'useragent', 'host',
                          'timefirstbyte',
                          'timetoserv', 'hit', 'contenttype', 'sessioncookie', 'cachecontrol', 'xforwardedfor',
                          'side'],
                   parse_dates=['#timestamp'],
                   #           [22/Feb/2222:22:22:22s
                   dateformat='[%d/%b/%Y:%H:%M:%S'
                   ) for i in range(0, args.nproc)]

        # good to go
        list(map(lambda worker: worker.start(), workers))
        reader.start()

        ######

        # wait for EOF input file
        reader.join()
        logging.info(f"{reader.name} finished.")

        # signal workers the end and wait for termination
        for worker in workers:
            worker.eof()
        for worker in workers:
            worker.join(timeout=10)
            logging.info(f"{worker.name} {'timed out' if worker.exitcode is None else 'finished'}.")

        # save mapper secrets
        list(map(lambda mydict, prefix: mydict.save(f"secrets/secrets_{prefix}.csv"), mydicts.values(), prefixes))

        logging.info(f"logfile {args.logfile} anonymization complete")

    except KeyboardInterrupt:
        pass
    except Exception:
        logging.exception("Error in processing")
    finally:
        list(map(lambda manager: manager.shutdown(), managers))
