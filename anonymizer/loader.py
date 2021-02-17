import pandas as pd
from multiprocessing import Pool, Manager
from tqdm.auto import tqdm
from bz2 import BZ2File
import numpy as np
from cachetools import cached
from ua_parser import user_agent_parser
import os
from datetime import timedelta
from geolite2 import geolite2
from urllib.parse import urlsplit
from mapper import BaseMapper
from io import StringIO
from itertools import islice
from datetime import datetime
import logging


class Loader:
    # constant values
    _cachename = ''
    _popname = ''

    # secrets
    timeshiftdays = 0
    xyte = 1

    # csv_reader kwargs
    _read_csv_args = {}

    # local caches for acceleration
    _cachesize = 0
    _geocache = None
    _uacache = None

    # mappers
    mappers = {}

    @classmethod
    def initializer(cls, mappers: dict, cachename: str, popname: str, cachesize: int, timeshiftdays: int, xyte: float,
                    read_csv_args: dict):
        cls.mappers = mappers

        assert len(cachename) > 0, f"invalid cachename: '{cachename}'"
        assert len(popname) > 0, f"invalid cachename: '{popname}'"
        cls._cachename = cachename.lower()
        cls._popname = popname.lower()

        assert cachesize >= 0, f"Wrong cachesize: {cachesize}"
        cls._cachesize = int(cachesize)
        cls._geocache = {}
        cls._uacache = {}

        assert timeshiftdays > 0, f"invalid timeshiftdays: '{timeshiftdays}'"
        cls.timeshiftdays = int(timeshiftdays)
        cls.xyte = float(xyte)

        assert isinstance(read_csv_args, dict), f"wrong type for read_csv_args: '{type(read_csv_args)}'"
        dateformat = read_csv_args.pop('dateformat')
        cls._read_csv_args = read_csv_args
        cls._read_csv_args['date_parser'] = lambda x: datetime.strptime(x, dateformat)

    @classmethod
    def process(cls, data: bytes):
        # open logfile with pandas, use chunks to distribute the load among workers. Force dtypes
        chunk = pd.read_csv(StringIO(data.decode(encoding='utf8')), **cls._read_csv_args)

        _debug = False

        if _debug:
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_colwidth', -1)

        try:
            #########################
            # check TODO: add missing
            if 'ip' not in chunk.columns \
                    or 'xforwardedfor' not in chunk.columns \
                    or '#timestamp' not in chunk.columns \
                    or 'contenttype' not in chunk.columns \
                    or 'ip' not in chunk.columns \
                    or 'request' not in chunk.columns \
                    or 'side' not in chunk.columns \
                    or 'statuscode' not in chunk.columns \
                    or 'timetoserv' not in chunk.columns \
                    or 'sessioncookie' not in chunk.columns \
                    :
                raise SyntaxError(f"Required column(s) not found: {chunk.columns}")

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
            dummy_schema, chunk['host'], chunk['path'], dummy_query, dummy_fragment = zip(*chunk['url'].map(urlsplit))
            chunk.drop(['url'], axis=1, inplace=True)

            # session cookie
            dummy = chunk['sessioncookie'].str.extract(
                r"session=(?:-|([^,]+)),(?:-|([^,]+)),(?:-|([^,]+)),(?:-|([^,;]+))", expand=True)
            chunk['uid'] = dummy[0]
            chunk['sid'] = dummy[1]
            chunk.drop(['sessioncookie'], axis=1, inplace=True)

            # channel number (.fillna().sum() takes care of the OR case in the regexp)
            chunk['livechannel'] = chunk['path'].str.extract(r'PLTV/88888888/\d+/(\d+)/|([^/]+)\.isml',
                                                             expand=False).fillna('').sum(axis=1)

            # contentpackage, assetid
            dummy = chunk['path'].str.extract(r"/(\d{18,})/(\d{16,})/")
            chunk['contentpackage'] = dummy[0]
            chunk['assetnumber'] = dummy[1]

            #########################
            # enrich - geoip, use local cache for performance
            geo = geolite2.reader()

            @cached(cache=cls._geocache)
            def coord(ip: str) -> str:
                geodata = geo.get(ip)
                return f"{geodata['location']['longitude']}:{geodata['location']['latitude']}" if geodata is not None and 'location' in geodata else np.nan

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
            # enrich - streaming protocol

            chunk['manifest'] = chunk['path'].str.match(r'(?:\.isml?/Manifest|\.mpd|\.m3u8)$', case=False)
            chunk['fragment'] = chunk['path'].str.match(
                r'(?:\.m4[avi]|\.ts|\.ism[av]|\.mp[4a]|/(?:Fragments|KeyFrames)\(.*\))$', case=False)

            #########################
            # anonymize

            # substitute: map values to random hashes
            for prefix in ['cachename', 'popname', 'host', 'coordinates', 'devicebrand',
                           'devicefamily', 'devicemodel', 'osfamily', 'uafamily', 'uamajor', 'path',
                           'livechannel', 'contentpackage', 'assetnumber', 'uid', 'sid']:
                assert prefix in Loader.mappers, f"Mapper prefix issue: '{prefix}' not found in '{Loader.mappers}'"
                chunk[prefix] = chunk[prefix].map(Loader.mappers[prefix].get, na_action='ignore')

            if _debug: print(chunk.head(5))

            # shift time
            chunk['#timestamp'] += timedelta(days=cls.timeshiftdays)

            # convert byte to xyte
            chunk.contentlength = chunk.contentlength.divide(cls.xyte)

            #########################
            # set index
            chunk.set_index(['#timestamp', 'statuscode', 'method', 'protocol',
                             'hit', 'contenttype', 'cachename', 'popname', 'host', 'coordinates', 'devicebrand',
                             'devicefamily', 'devicemodel', 'osfamily', 'uafamily', 'uamajor', 'path', 'manifest',
                             'fragment', 'livechannel', 'contentpackage', 'assetnumber', 'uid', 'sid'],
                            inplace=True)

            assert set(chunk.columns) == set(
                list(['contentlength', 'timefirstbyte',
                      'timetoserv'])), f"Somethink went wrong, column name mismatch: {chunk.columns}"

            # return
            return chunk

        except Exception as e:
            logging.exception(e)

    def __init__(self, nproc: int, cachesize: int, timeshiftdays: int, xyte: float):
        # worker parameters
        self._nproc = nproc
        self._cachesize = cachesize

        # secrets
        self.timeshiftdays = timeshiftdays
        self.xyte = xyte

    def load(self, logfilename: str, cachename: str, popname: str, chunksize: int, **read_csv_args):
        # create shared memory mappers
        with Manager() as manager:
            mappers = {prefix: BaseMapper(prefix=prefix, hashlen=hashlen, store=manager.dict()) for prefix, hashlen in
                       [('cachename', 4), ('popname', 4), ('host', 8), ('coordinates', 8),
                        ('devicebrand', 4), ('devicefamily', 4), ('devicemodel', 4), ('osfamily', 4), ('uafamily', 4),
                        ('uamajor', 4), ('path', 16), ('livechannel', 4), ('contentpackage', 8), ('assetnumber', 8),
                        ('uid', 12), ('sid', 12)]}

            # load mapper secrets
            for prefix, mapper in mappers.items():
                mapper.load(f"secrets/secrets_{prefix}.csv")

            # start worker processes with initializer (worker parameters and secrets)
            # open raw logfile
            # create progress bar for file position
            # create progress bar for processed lines
            with open(logfilename, 'rb') as logfile, \
                    BZ2File(logfile) as logreader, \
                    tqdm(total=os.path.getsize(logfilename), position=0, desc=logfilename, unit='B',
                         unit_scale=True) as pbar_filepos, \
                    tqdm(position=1, unit='line', desc='lines', unit_scale=True) as pbar_lines, \
                    Pool(self._nproc, Loader.initializer,
                         (mappers, cachename, popname, self._cachesize, self.timeshiftdays, self.xyte,
                          read_csv_args,)) as pool:

                # for progress bar
                lastpos = 0

                # slice elements from an iterable
                def slicer(n, iterable):
                    it = iter(iterable)
                    while True:
                        chunk = b"".join(islice(it, n))
                        if not chunk:
                            return
                        yield chunk

                # map chunks (group of lines to the workers)
                for result in pool.imap(Loader.process, slicer(chunksize, logreader)):

                    # update progress bar
                    if logfile.tell() > lastpos:
                        pbar_filepos.update(logfile.tell() - lastpos)
                        lastpos = logfile.tell()

                    # process result
                    if isinstance(result, pd.DataFrame):
                        # export
                        result.to_csv(f"{logfilename}.csv", mode='a', header=True)

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
