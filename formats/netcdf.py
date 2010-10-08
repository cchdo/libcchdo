"""Common utilities that NetCDF handlers need."""


import datetime

try:
    from netCDF3 import Dataset
except ImportError, e:
    raise ImportError('%s\n%s' % (e,
        ("You should get netcdf4-python from http://code.google.com/p/"
         "netcdf4-python and install the NetCDF 3 module as directed by the "
         "README.")))

from .. import fns


QC_SUFFIX = '_QC'
FILE_EXTENSION = 'nc'
EPOCH = datetime.datetime(1980, 1, 1, 0, 0, 0)


def simplest_str(s):
    """Give the simplest string representation.
       If a float is almost equivalent to an integer, swap out for the
       integer.
    """
    if type(s) is float:
        if fns.equal_with_epsilon(s, int(s)):
            s = int(s)
    return str(s)


def _pad_station_cast(x):
    """Pad a station or cast identifier out to 5 characters. This is usually
       for use in a file name.
       Args:
            x - a string to be padded
    """
    return simplest_str(x).rjust(5, '0')


def get_filename(expocode, station, cast, extension='hy1'):
    station = _pad_station_cast(station)
    cast = _pad_station_cast(cast)
    return '%s.%s' % ('_'.join((expocode, station, cast, extension)),
                      FILE_EXTENSION, )


def minutes_since_epoch(dt, error=-9):
    if not dt:
        return error
    if type(dt) is datetime.date:
    	dt = datetime.datetime(dt.year, dt.month, dt.day)
    delta = dt - EPOCH
    minutes_in_day = 60 * 24
    minutes_in_seconds = 1.0 / 60
    minutes_in_microseconds = minutes_in_seconds / 1.0e6
    return (delta.days * minutes_in_day + \
            delta.seconds * minutes_in_seconds + \
            delta.microseconds * minutes_in_microseconds)


