from multiprocessing import Process, Queue, Event
from queue import Empty
from io import StringIO
import pandas as pd
import logging
import numpy as np
from cachetools import cached
from ua_parser import user_agent_parser
from datetime import timedelta
from geolite2 import geolite2
from urllib.parse import urlsplit
from datetime import datetime
import bz2


class Worker(Process):
    def __init__(self, name: str, logfilename: str, input: Queue, mappers: dict, cachename: str,
                 popname: str, timeshiftdays: int, xyte: float, cachesize: int, **read_csv_args):
        super().__init__()
        self._logfilename = logfilename
        self._input = input
        self._read_csv_args = read_csv_args
        self._logger = logging.getLogger(name)
        self._eof = Event()

        self._mappers = mappers

        assert len(cachename) > 0, f"invalid cachename: '{cachename}'"
        assert len(popname) > 0, f"invalid cachename: '{popname}'"
        self._cachename = cachename
        self._popname = popname

        # local caches for acceleration
        assert cachesize >= 0, f"Wrong cachesize: {cachesize}"
        self._cachesize = cachesize
        self._geocache = {}
        self._uacache = {}

        assert timeshiftdays > 0, f"invalid timeshiftdays: '{timeshiftdays}'"
        self._timeshiftdays = timeshiftdays
        self._xyte = xyte

        assert isinstance(read_csv_args, dict), f"wrong type for read_csv_args: '{type(read_csv_args)}'"
        self._read_csv_args = read_csv_args

    def run(self):

        try:
            dateformat = self._read_csv_args.pop('dateformat')
            self._read_csv_args['date_parser'] = lambda x: datetime.strptime(x, dateformat)

            with bz2.BZ2File(self._logfilename, mode='w') as logwriter:

                while True:

                    # wait for a task
                    try:
                        batch = self._input.get(block=True, timeout=0.25)
                    except Empty:
                        if self._eof.is_set():
                            # no job, and the EOF reached, no hope to get a task, exit
                            break
                        else:
                            # no job, but EOF not set, reade is behind, expect new tasks
                            continue

                    # read csv
                    chunk = pd.read_csv(StringIO(batch.decode(encoding='utf8')), **self._read_csv_args)

                    if self._logger.level == logging.DEBUG:
                        pd.set_option('display.max_columns', None)
                        pd.set_option('display.max_colwidth', -1)

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

                    self._logger.debug(chunk.head(5))

                    #########################
                    # filter

                    # drop non downstream lines
                    chunk.drop(chunk.loc[chunk['side'] != 'c'].index, inplace=True)
                    chunk.drop(['side'], axis=1, inplace=True)

                    # add constant values
                    chunk['cachename'] = self._cachename
                    chunk['popname'] = self._popname

                    #########################
                    # parse

                    # split xforwarded for, keep the first IP
                    chunk.xforwardedfor = chunk.xforwardedfor.str.split(",", n=1, expand=True)[0]

                    # check if all public
                    assert True  # TODO: implement
                    self._logger.debug(chunk.head(5))

                    # overwrite ip with xforwardedfor if it is 127.0.0.1 (TLS termination is from localhost)
                    mask = chunk['ip'] == '127.0.0.1'
                    chunk.loc[mask, 'ip'] = chunk.loc[mask, 'xforwardedfor']
                    self._logger.debug(chunk.head(5))

                    # drop xforwardedfor
                    chunk.drop(['xforwardedfor'], axis=1, inplace=True)
                    self._logger.debug(chunk.head(5))

                    # convert timetoserv unit from ms to sec
                    chunk['timetoserv'] /= 1000000

                    # split request line
                    chunk['method'], chunk['url'], chunk['protocol'] = zip(*chunk['request'].str.split(' ', n=2))
                    chunk.drop(['request'], axis=1, inplace=True)

                    # parse url, skip schema, fragment
                    dummy_schema, chunk['host'], chunk['path'], dummy_query, dummy_fragment = zip(
                        *chunk['url'].map(urlsplit))
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

                    @cached(cache=self._geocache)
                    def coord(ip: str) -> str:
                        geodata = geo.get(ip)

                        # round up to 2 digits (~1km precision, see https://wiki.openstreetmap.org/wiki/Precision_of_coordinates)
                        return f"{round(geodata['location']['longitude'], 2)}:{round(geodata['location']['latitude'], 2)}" if geodata is not None and 'location' in geodata else np.nan

                    chunk['coordinates'] = chunk['ip'].map(coord, na_action='ignore')
                    chunk.drop(['ip'], axis=1, inplace=True)

                    self._logger.debug(chunk.head(5))

                    #########################
                    # enrich - user agent, use local cache for performance

                    @cached(cache=self._uacache)
                    def uaparser(ua_string: str) -> pd.Series:
                        ps = user_agent_parser.Parse(ua_string)
                        return pd.Series(
                            [np.nan if x is None else x for x in
                             [ps['device']['brand'], ps['device']['family'], ps['device']['model'], ps['os']['family'],
                              ps['user_agent']['family'], ps['user_agent']['major']]]
                        )

                    chunk[["devicebrand", "devicefamily", "devicemodel", "osfamily", "uafamily", "uamajor"]] = \
                        chunk.loc[
                            chunk['useragent'].notna(), 'useragent'].apply(uaparser)
                    chunk.drop(['useragent'], axis=1, inplace=True)

                    self._logger.debug(chunk.head(5))

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
                        assert prefix in self._mappers, f"Mapper prefix issue: '{prefix}' not found in '{self._mappers}'"
                        chunk[prefix] = chunk[prefix].map(self._mappers[prefix].get, na_action='ignore')

                    self._logger.debug(chunk.head(5))

                    # shift time
                    chunk['#timestamp'] += timedelta(days=self._timeshiftdays)

                    # convert byte to xyte
                    chunk.contentlength = chunk.contentlength.divide(self._xyte)

                    #########################
                    # set index
                    chunk.set_index(['#timestamp', 'statuscode', 'method', 'protocol',
                                     'hit', 'contenttype', 'cachename', 'popname', 'host', 'coordinates', 'devicebrand',
                                     'devicefamily', 'devicemodel', 'osfamily', 'uafamily', 'uamajor', 'path',
                                     'manifest',
                                     'fragment', 'livechannel', 'contentpackage', 'assetnumber', 'uid', 'sid'],
                                    inplace=True)

                    assert set(chunk.columns) == set(
                        list(['contentlength', 'timefirstbyte',
                              'timetoserv'])), f"Somethink went wrong, column name mismatch: {chunk.columns}"

                    # write
                    buff = StringIO()
                    chunk.to_csv(buff, header=True)
                    logwriter.write(buff.getvalue().encode('utf-8'))

        except KeyboardInterrupt:
            self._logger.info("interrupt")

        except Exception:
            self._logger.exception("Error")

    def eof(self):
        self._eof.set()
