import pandas as pd
import argparse
import numpy as np
from math import sin, cos, sqrt, atan2, radians

parser = argparse.ArgumentParser()
parser.add_argument('coordinates', type=str)
parser.add_argument('output', type=str)
parser.add_argument('--resolution', type=int, default=5,
                    help="in km, distance values rounded up to this (default: %(default)s)")
parser.add_argument('--cap', type=int, default=50,
                    help="in km, distances over this value dropped (default: %(default)s)")


def dist(a_lat, a_lon, b_lat, b_lon, resolution, cap):
    # approximate radius of earth in km
    R = 6373.0

    lat1 = radians(a_lat)
    lon1 = radians(a_lon)
    lat2 = radians(b_lat)
    lon2 = radians(b_lon)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    # round up to resolution km
    distance = R * c
    return np.nan if distance > cap else resolution * round(distance / resolution)


if __name__ == "__main__":
    # arguments
    args = parser.parse_args()

    # read coordinate secret
    df = pd.read_csv(args.coordinates, delimiter=',', names=['coord', 'hash'])

    # split, parse, set index
    dummy = df['coord'].str.split(':', n=1, expand=True)
    df['lat'] = dummy[0].astype(float)
    df['lon'] = dummy[1].astype(float)
    df.drop(['coord'], axis=1, inplace=True)
    print(df.values)

    # create a matrix with nan
    distances = pd.DataFrame(
        [[
            dist(lat_a, lon_a, lat_b, lon_b, args.resolution, args.cap)
            for hash_a, lat_a, lon_a in df.values]
            for hash_b, lat_b, lon_b in df.values
        ], index=df['hash'], columns=df['hash'])
    print(distances)

    distances.to_csv(args.output, sep=',', header=True)
