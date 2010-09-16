'''
Common utilities that NetCDF handlers need.
'''


import datetime

try:
    from netCDF3 import Dataset
except ImportError, e:
    raise ImportError('%s\n%s' % (e,
        ("You should get netcdf4-python from http://code.google.com/p/"
         "netcdf4-python and install the NetCDF 3 module as directed by the "
         "README.")))


QC_SUFFIX = '_QC'
FILE_EXTENSION = 'nc'
EPOCH = datetime.datetime(1980, 1, 1, 0, 0, 0)


def _pad_station_cast(x):
    if type(x) is float: 
        x = int(x)
    return str(x).rjust(5, '0')


def get_filename(expocode, station, cast):
    station = _pad_station_cast(station)
    cast = _pad_station_cast(cast)
    return '%s.%s' % ('_'.join((expocode, station, cast, 'hy1')),
                      FILE_EXTENSION, )


def minutes_since_epoch(dtime, default=-9):
    delta = dtime - EPOCH
    minutes_in_day = 60 * 24
    minutes_in_seconds = 1.0 / 60
    minutes_in_microseconds = minutes_in_seconds / 1.0e6
    if dtime:
        return (delta.days * minutes_in_day + \
                delta.seconds * minutes_in_seconds + \
                delta.microseconds * minutes_in_microseconds)
    else:
        return default


def simplest_str(s):
    if type(s) is float:
        if libcchdo.fns.equal_with_epsilon(s, int(s)):
            s = int(s)
    return str(s)


