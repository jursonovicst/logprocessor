import pandas as pd
from multiprocessing import Pool, Manager
from tqdm.auto import tqdm
from mapper import SHMemMapper
from bz2 import BZ2File
import numpy as np
from cachetools import cached
from ua_parser import user_agent_parser
from pandas import HDFStore
import os
from contextlib import nullcontext
from datetime import timedelta
from geolite2 import geolite2
from urllib.parse import urlsplit


class Loader:
    # constant values
    _cachename = ''
    _popname = ''

    # secrets
    timeshiftdays = 0
    xyte = 0

    # local caches for acceleration
    _cachesize = 0
    _geocache = None
    _uacache = None

    # mappers
    mappers = {}

    @classmethod
    def initializer(cls, mappers: dict, cachename: str, popname: str, cachesize: int, timeshiftdays: int, xyte: float):
        cls.mappers = mappers

        assert len(cachename) > 0, f"invalid cachename: '{cachename}'"
        assert len(popname) > 0, f"invalid cachename: '{popname}'"
        cls._cachename = cachename
        cls._popname = popname

        assert cachesize >= 0, f"Wrong cachesize: {cachesize}"
        cls._cachesize = int(cachesize)
        cls._geocache = {}
        cls._uacache = {}

        assert timeshiftdays > 0, f"invalid timeshiftdays: '{timeshiftdays}'"
        cls.timeshiftdays = int(timeshiftdays)
        cls.xyte = float(xyte)

    @classmethod
    def process(cls, chunk: pd.DataFrame):

        _debug = False

        if _debug:
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_colwidth', -1)

        #        try:
        #########################
        # check TODO: add missing
        if 'ip' not in chunk.columns \
                or 'xforwardedfor' not in chunk.columns \
                or 'timestamp' not in chunk.columns \
                or 'contenttype' not in chunk.columns \
                or 'ip' not in chunk.columns \
                or 'request' not in chunk.columns \
                or 'side' not in chunk.columns \
                or 'statuscode' not in chunk.columns \
                or 'timetoserv' not in chunk.columns \
                :
            raise SyntaxError(f"Required column(s) not found: {chunk.columns}")

        if chunk.ip.dtype != object or \
                chunk.xforwardedfor.dtype != object or \
                chunk.timetoserv.dtype != float or \
                chunk.statuscode.dtype != np.int64:
            raise SyntaxError(f"dtype(s) incorrect: {chunk.dtypes}")

        if _debug: print(chunk.head(5))

        #########################
        # filter

        # drop non downstream lines
        chunk.drop(chunk.loc[chunk['side'] != 'c'].index, inplace=True)
        chunk.drop(['side'], axis=1, inplace=True)

        # add constant values
        chunk['cachename'] = cls._cachename
        chunk['popname'] = cls._popname

        #########################
        # parse

        # split xforwarded for, keep the first IP
        chunk.xforwardedfor = chunk.xforwardedfor.str.split(",", n=1, expand=True)[0]

        # check if all public
        assert True  # TODO: implement
        if _debug: print(chunk.head(5))

        # overwrite ip with xforwardedfor if it is 127.0.0.1 (TLS termination is from localhost)
        mask = chunk['ip'] == '127.0.0.1'
        chunk.loc[mask, 'ip'] = chunk.loc[mask, 'xforwardedfor']
        if _debug: print(chunk.head(5))

        # drop xforwardedfor
        chunk.drop(['xforwardedfor'], axis=1, inplace=True)
        if _debug: print(chunk.head(5))

        # convert timetoserv unit from ms to sec
        chunk['timetoserv'] /= 1000000

        # split request line
        chunk['method'], chunk['url'], chunk['protocol'] = zip(*chunk['request'].str.split(' ', n=2))
        chunk.drop(['request'], axis=1, inplace=True)

        # parse url, skip schema, fragment
        dummy, chunk['host'], chunk['path'], chunk['query'], dummy2 = zip(*chunk['url'].map(urlsplit))
        chunk.drop(['url'], axis=1, inplace=True)

        #########################
        # enrich - geoip, use local cache for performance
        geo = geolite2.reader()

        @cached(cache=cls._geocache)
        def coord(ip: str) -> str:
            geodata = geo.get(ip)
            return f"{geodata['location']['longitude']}:{geodata['location']['latitude']}" if geodata is not None else np.nan

        chunk['coordinates'] = chunk['ip'].map(coord, na_action='ignore')
        chunk.drop(['ip'], axis=1, inplace=True)

        if _debug: print(chunk.head(5))

        #########################
        # enrich - user agent, use local cache for performance

        @cached(cache=cls._uacache)
        def uaparser(ua_string: str) -> pd.Series:
            ps = user_agent_parser.Parse(ua_string)
            return pd.Series(
                [np.nan if x is None else x for x in
                 [ps['device']['brand'], ps['device']['family'], ps['device']['model'], ps['os']['family'],
                  ps['user_agent']['family'], ps['user_agent']['major']]]
            )

        chunk[["devicebrand", "devicefamily", "devicemodel", "osfamily", "uafamily", "uamajor"]] = chunk.loc[
            chunk['useragent'].notna(), 'useragent'].apply(uaparser)
        chunk.drop(['useragent'], axis=1, inplace=True)

        if _debug: print(chunk.head(5))

        #########################
        # anonymize

        # substitute: map values to random hashes
        for prefix in ['contenttype', 'cachename', 'popname', 'host', 'coordinates', 'devicebrand',
                       'devicefamily', 'devicemodel', 'osfamily', 'uafamily', 'uamajor', 'path', 'query']:
            assert prefix in Loader.mappers, f"Mapper prefix issue: '{prefix}' not found in '{Loader.mappers}'"
            chunk[prefix] = chunk[prefix].map(Loader.mappers[prefix].get, na_action='ignore')

        if _debug: print(chunk.head(5))

        # shift time
        chunk.timestamp += timedelta(days=cls.timeshiftdays)

        # convert byte to xyte
        chunk.contentlength = chunk.contentlength.divide(cls.xyte)

        #########################
        # set index
        chunk.set_index(['timestamp', 'statuscode', 'method', 'protocol',
                         'hit', 'contenttype', 'cachename', 'popname', 'host', 'coordinates', 'devicebrand',
                         'devicefamily', 'devicemodel', 'osfamily', 'uafamily', 'uamajor', 'path', 'query'],
                        inplace=True)

        assert set(chunk.columns) == set(
            list(['contentlength', 'timefirstbyte', 'timetoserv'])), f"Wrong column names: {chunk.columns}"
        # returns
        return chunk

    #        except Exception as e:
    #            return e

    def __init__(self, nproc: int, cachesize: int, timeshiftdays: int, xyte: float):
        # worker parameters
        self._nproc = nproc
        self._cachesize = cachesize

        # secrets
        self.timeshiftdays = timeshiftdays
        self.xyte = xyte

    def load(self, logfilename: str, cachename: str, popname: str, exportcsv=True, **read_csv_args):

        # storage
        if exportcsv:
            storecm = nullcontext()
        else:
            storecm = HDFStore(f"{logfilename}.hd5", mode='w')

        # create shared memory mappers
        mappers = {prefix: SHMemMapper(prefix=prefix, hashlen=hashlen) for prefix, hashlen in
                   [('contenttype', 8), ('cachename', 4), ('popname', 4), ('host', 8), ('coordinates', 8),
                    ('devicebrand', 4), ('devicefamily', 4), ('devicemodel', 4), ('osfamily', 4), ('uafamily', 4),
                    ('uamajor', 4), ('path', 16), ('query', 16)]}

        # load mapper secrets
        for prefix, mapper in mappers.items():
            mapper.load(f"secrets/secrets_{prefix}.csv")

        # start worker processes with initializer (worker parameters and secrets)
        # open raw logfile
        # create progress bar for file position
        # create progress bar for processed lines
        # create decompressor
        # create store (if needed)
        with Pool(self._nproc, Loader.initializer,
                  (mappers, cachename, popname, self._cachesize, self.timeshiftdays, self.xyte,)) as pool, \
                open(logfilename, 'rb') as logfile, \
                tqdm(total=os.path.getsize(logfilename), position=0, desc=logfilename, unit='B',
                     unit_scale=True) as pbar_filepos, \
                tqdm(position=1, unit='line', desc='lines', unit_scale=True) as pbar_lines, \
                BZ2File(logfile) as reader, \
                storecm as store:

            # for progress bar
            lastpos = 0

            # open logfile with pandas, use chunks to distribute the load among workers. Force dtypes
            chunk_reader = pd.read_csv(reader, **read_csv_args,
                                       dtype={
                                           'ip': object,
                                           'timefirstbyte': float,
                                           'timetoserv': float
                                       }
                                       , iterator=True)  # TODO: add dtypes

            # map the logprocessor function
            for result in pool.imap_unordered(Loader.process, chunk_reader):

                # update progress bar
                if logfile.tell() > lastpos:
                    pbar_filepos.update(logfile.tell() - lastpos)
                    lastpos = logfile.tell()

                # process result
                if isinstance(result, pd.DataFrame):
                    if exportcsv:
                        result.to_csv(f"{logfilename}.csv", mode='a', header=True)
                    else:
                        store.append('logs', result)

                    # update progress bar
                    pbar_lines.update(result.shape[0])

                elif isinstance(result, KeyboardInterrupt):
                    # exit for loop
                    break

                elif isinstance(result, Exception):
                    pbar_filepos.display(f"{type(result)} processing chunk: '{result}'")

                elif isinstance(result, str):
                    pbar_filepos.display(f"Message: {result}")

                else:
                    pbar_filepos.display(f"Unknown result type: '{type(result)}'")

        # save mapper secrets
        for prefix, mapper in mappers.items():
            mapper.save(f"secrets/secrets_{prefix}.csv")
