#!/usr/bin/env python3
import logging
import argparse
import configparser
import bz2
from tqdm.auto import tqdm
from datetime import datetime
import pandas as pd
import os
from ua_parser import user_agent_parser
from geolite2 import geolite2
from urllib.parse import urlsplit
from cachetools import cached, LRUCache
import numpy as np
from datetime import timedelta
from anonymizer import MyDict
from io import StringIO

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('logfile', type=str)
        parser.add_argument('cachename', type=str)
        parser.add_argument('popname', type=str)
        parser.add_argument('--cachesize', type=int, default=10000,
                            help="Per process local cache size (default: %(default)s)")
        parser.add_argument('--maxlines', type=int, default=-1,
                            help="Number of rows of file to read (default: %(default)s)")
        parser.add_argument('--chunksize', type=int, default=10000,
                            help="Chunk (lines processed together) size (default: %(default)s)")
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

        # arguments
        args = parser.parse_args()

        # logging
        logging.basicConfig(level=logging.INFO)

        # config
        config = configparser.ConfigParser()
        config.read(args.configfile)

        with open(args.logfile, 'rb') as logfile, \
                bz2.BZ2File(logfile) as logreader, \
                bz2.BZ2File(f"{args.logfile}.ano.bz2", mode='w', compresslevel=1) as logwriter, \
                tqdm(total=os.path.getsize(args.logfile), position=0, desc=args.logfile, unit='B',
                     unit_scale=True) as pbar_filepos, \
                tqdm(position=1, unit='line', desc=args.logfile, unit_scale=True) as pbar_lines:

            # for progress bar
            lastpos = 0

            maxitems = args.maxlines

            prefixes = ['cachename', 'popname', 'host', 'coordinates', 'devicebrand', 'devicefamily', 'devicemodel',
                        'osfamily', 'uafamily', 'uamajor', 'path', 'livechannel', 'contentpackage', 'assetnumber',
                        'uid', 'sid']

            mydicts = {prefix: MyDict() for prefix in prefixes}
            list(map(lambda mydict, prefix: mydict.load(f"secrets/{prefix}.csv"), mydicts.values(), prefixes))

            # maxmind
            geo = geolite2.reader()


            @cached(LRUCache(maxsize=1000))
            def coord(ip: str) -> str:
                geodata = geo.get(ip)

                # round up to 2 digits (~1km precision, see https://wiki.openstreetmap.org/wiki/Precision_of_coordinates)
                return f"{round(geodata['location']['longitude'], 2)}:{round(geodata['location']['latitude'], 2)}" if geodata is not None and 'location' in geodata else np.nan


            @cached(LRUCache(maxsize=1000))
            def uaparser(ua_string: str) -> pd.Series:
                ps = user_agent_parser.Parse(ua_string)
                return pd.Series(
                    [np.nan if x is None else x for x in
                     [ps['device']['brand'], ps['device']['family'], ps['device']['model'], ps['os']['family'],
                      ps['user_agent']['family'], ps['user_agent']['major']]]
                )


            for chunk in pd.read_csv(logreader,
                                     chunksize=args.chunksize,
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
                                     names=['ip', '#timestamp', 'request', 'statuscode', 'contentlength',
                                            'useragent', 'host',
                                            'timefirstbyte',
                                            'timetoserv', 'hit', 'contenttype', 'sessioncookie', 'cachecontrol',
                                            'xforwardedfor',
                                            'side'],
                                     parse_dates=['#timestamp'],
                                     #                                           [22/Feb/2222:22:22:22s
                                     date_parser=lambda x: datetime.strptime(x, '[%d/%b/%Y:%H:%M:%S')
                                     ):

                # if logging.level == logging.DEBUG:
                # pd.set_option('display.max_columns', None)
                # pd.set_option('display.max_colwidth', -1)

                #########################
                # check TODO: add missing
                if 'ip' not in chunk.columns \
                        or 'xforwardedfor' not in chunk.columns \
                        or '#timestamp' not in chunk.columns \
                        or 'contenttype' not in chunk.columns \
                        or 'ip' not in chunk.columns \
                        or 'host' not in chunk.columns \
                        or 'request' not in chunk.columns \
                        or 'side' not in chunk.columns \
                        or 'statuscode' not in chunk.columns \
                        or 'timetoserv' not in chunk.columns \
                        or 'sessioncookie' not in chunk.columns \
                        or 'cachecontrol' not in chunk.columns:
                    raise SyntaxError(f"Required column(s) not found: {chunk.columns}")

                logging.debug(chunk.head(5))

                #########################
                # filter

                # drop non downstream lines
                chunk.drop(chunk.loc[chunk['side'] != 'c'].index, inplace=True)
                chunk.drop(['side'], axis=1, inplace=True)

                # add constant values
                chunk['cachename'] = args.cachename
                chunk['popname'] = args.popname

                #########################
                # parse

                # split xforwarded for, keep the first IP
                chunk.xforwardedfor = chunk.xforwardedfor.str.split(",", n=1, expand=True)[0]

                # remove cache name, if present in host (http redirect)
                # chunk['host'].replace(r"^[a-zA-Z0-9-]+--", '', inplace=True)
                chunk.host = chunk.host.str.split("--", n=1, expand=True)[0]

                # check if all public
                assert True  # TODO: implement
                logging.debug(chunk.head(5))

                # overwrite ip with xforwardedfor if it is 127.0.0.1 (TLS termination is from localhost)
                mask = chunk['ip'] == '127.0.0.1'
                chunk.loc[mask, 'ip'] = chunk.loc[mask, 'xforwardedfor']
                logging.debug(chunk.head(5))

                # drop xforwardedfor
                chunk.drop(['xforwardedfor'], axis=1, inplace=True)
                logging.debug(chunk.head(5))

                # convert timetoserv unit from ms to sec
                chunk['timetoserv'] /= 1000000

                # split request line
                chunk['method'], chunk['url'], chunk['protocol'] = zip(*chunk['request'].str.split(' ', n=2))
                chunk.drop(['request'], axis=1, inplace=True)

                # parse url, skip schema, fragment
                dummy_schema, dummy_host, chunk['path'], dummy_query, dummy_fragment = zip(
                    *chunk['url'].map(urlsplit))
                chunk.drop(['url'], axis=1, inplace=True)

                # session cookie
                dummy = chunk['sessioncookie'].str.extract(
                    r"session=(?:-|([^,]+)),(?:-|([^,]+)),", expand=True)
                chunk['uid'] = dummy[0]
                chunk['sid'] = dummy[1]
                chunk.drop(['sessioncookie'], axis=1, inplace=True)

                # channel number (.fillna().sum() takes care of the OR case in the regexp)
                chunk['livechannel'] = chunk['path'].str.extract(r'PLTV/88888888/\d+/(\d+)/|([^/]+)\.isml',
                                                                 expand=False).fillna('').sum(axis=1).replace('',
                                                                                                              np.NaN)

                # contentpackage, assetid
                dummy = chunk['path'].str.extract(r"/(\d{18,})/(\d{16,})/")
                chunk['contentpackage'] = dummy[0]
                chunk['assetnumber'] = dummy[1]

                # cache control
                chunk['maxage'] = chunk['cachecontrol'].str.extract(r'max-age=(\d+)', expand=False)

                #########################
                # enrich - geoip, use local cache for performance
                chunk['coordinates'] = chunk['ip'].map(coord, na_action='ignore')
                chunk.drop(['ip'], axis=1, inplace=True)

                logging.debug(chunk.head(5))

                #########################
                # enrich - user agent, use local cache for performance

                chunk[["devicebrand", "devicefamily", "devicemodel", "osfamily", "uafamily", "uamajor"]] = \
                    chunk.loc[
                        chunk['useragent'].notna(), 'useragent'].apply(uaparser)
                chunk.drop(['useragent'], axis=1, inplace=True)

                logging.debug(chunk.head(5))

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
                    assert prefix in mydicts, f"Mapper prefix issue: '{prefix}' not found in '{mydicts}'"
                    chunk[prefix] = chunk[prefix].map(mydicts[prefix].map, na_action='ignore').astype(object)

                logging.debug(chunk.head(5))

                # shift time
                chunk['#timestamp'] += timedelta(days=config['secrets'].getint('timeshiftdays'))

                # convert byte to xyte
                chunk.contentlength = chunk.contentlength.divide(config['secrets'].getfloat('xyte'))

                # #########################
                # # set index
                # chunk.set_index(['#timestamp', 'statuscode', 'method', 'protocol',
                #                  'hit', 'contenttype', 'cachename', 'popname', 'host', 'coordinates', 'devicebrand',
                #                  'devicefamily', 'devicemodel', 'osfamily', 'uafamily', 'uamajor', 'path',
                #                  'manifest',
                #                  'fragment', 'livechannel', 'contentpackage', 'assetnumber', 'uid', 'sid',
                #                  'cachecontrol'],
                #                 inplace=True)
                #
                # assert set(chunk.columns) == set(
                #     list(['contentlength', 'timefirstbyte',
                #           'timetoserv'])), f"Somethink went wrong, column name mismatch: {chunk.columns}"
                print(chunk.dtypes)

                # write
                buff = StringIO()
                chunk.to_csv(buff, header=True, index=False)
                logwriter.write(buff.getvalue().encode('utf-8'))

                # update progress bar
                if logfile.tell() > lastpos:
                    pbar_filepos.update(logfile.tell() - lastpos)
                    lastpos = logfile.tell()
                pbar_lines.update(args.chunksize)

                # check limit (-1 means, no limit)
                if maxitems != -1:
                    maxitems = max(0, maxitems - args.chunksize)
                    if maxitems == 0:
                        # reached maxitems
                        break

            list(map(lambda mydict, prefix: mydict.save(f"secrets/{prefix}.csv"), mydicts.values(), prefixes))


    except Exception:
        logging.exception(f"tt")
