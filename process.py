import argparse
from anonymizer import Loader
import multiprocessing
from datetime import datetime
import configparser

parser = argparse.ArgumentParser()
parser.add_argument('logfile', type=str)
parser.add_argument('cachename', type=str)
parser.add_argument('popname', type=str)
parser.add_argument('--exporttype', type=str, choices=['csv', 'hdf5'], default='csv',
                    help="Export file format (default: %(default)s)")
parser.add_argument('--nproc', type=int, default=max(2, multiprocessing.cpu_count() - 2),
                    help="Number of worker processes to start (default: %(default)s)")
parser.add_argument('--cachesize', type=int, default=1000, help="Per process local cache size (default: %(default)s)")

parser.add_argument('--nrows', type=int, default=None,
                    help="Number of rows of file to read. Useful for reading pieces of large files (default: %(default)s)")
parser.add_argument('--chunksize', type=int, default=10000,
                    help="Chunk (lines processed together) size (default: %(default)s)")
parser.add_argument('--delimiter', type=str, default=' ',
                    help="Delimiter to use. If sep is None, the C engine cannot automatically detect the separator, but the Python parsing engine can, meaning the latter will be used and automatically detect the separator by Python’s builtin sniffer tool, csv.Sniffer. In addition, separators longer than 1 character and different from '\s+' will be interpreted as regular expressions and will also force the use of the Python parsing engine. Note that regex delimiters are prone to ignoring quoted data. Regex example: '\r\t'. (default: %(default)s)")
parser.add_argument('--quotechar', type=str, default='"',
                    help="The character used to denote the start and end of a quoted item. Quoted items can include the delimiter and it will be ignored. (default: %(default)s)")
parser.add_argument('--navalues', type=str, default='-',
                    help="Additional strings to recognize as NA/NaN. (default: %(default)s)")
parser.add_argument('--escapechar', type=str, default='\\',
                    help="One-character string used to escape other characters (default: %(default)s)")
parser.add_argument('--configfile', type=str, default='config.ini', help='etc...')

config = configparser.ConfigParser()

if __name__ == "__main__":
    # arguments
    args = parser.parse_args()

    # config
    config.read(args.configfile)

    # create logfile reader
    logreader = Loader(args.nproc, args.cachesize, config['secrets'].getint('timeshiftdays'),
                       config['secrets'].getfloat('xyte'))

    #                                           [22/Feb/2222:22:22:22
    dateparse = lambda x: datetime.strptime(x, '[%d/%b/%Y:%H:%M:%S')

    # load and process raw logfile, kwargs passed to df.read_csv
    logreader.load(args.logfile, args.cachename, args.popname,
                   exportcsv=(args.exporttype == 'csv'),
                   chunksize=args.chunksize,
                   delimiter=args.delimiter,
                   quotechar=args.quotechar,
                   na_values=args.navalues,
                   escapechar=args.escapechar,
                   nrows=args.nrows,
                   header=None,
                   # X             X                            X                                                                              X   X     X                         X        X           X         X                                                                X                                                            X
                   # 0         1 2 3                     4      5                                                                              6   7 8   9              10         11       12  13      14  15 16 17                 18      19                                    20                                                 21 22     23
                   # 127.0.0.1 - - [22/Feb/2222:22:22:22 +0100] "GET http://xyz.cdn.de/this/is/the/path?and_this_is_the_query_string HTTP/1.1" 304 0 "-" "okhttp/4.9.0" xyz.cdn.de 0.000130 215 upstrea hit - 614 "application/json" 6596557 "session=-,INT-4178154,-,-; HttpOnly" "2222:22:2222:2222:2222:2222:2222:2222, 127.0.0.1" - TLSv1.2 c
                   usecols=[0, 3, 5, 6, 7, 9, 11, 12, 14, 17, 20, 23],
                   names=['ip', 'timestamp', 'request', 'statuscode', 'contentlength', 'useragent', 'timefirstbyte',
                          'timetoserv', 'hit', 'contenttype', 'xforwardedfor', 'side'],
                   parse_dates=['timestamp'],
                   date_parser=dateparse
                   )

    print(f"logfile {args.logfile} anonymization complete")
