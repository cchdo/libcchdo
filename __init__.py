"""libcchdo Python

Internal Data Specification
---------------------------
Any unreported values must be represented as None. This includes -9, -999.000,
unspecified dates, times, etc.

Known unknown parameters have mnemonics that start with '_'. e.g. MAX PRESSURE
exists in certain files but there is no parameter defined for it. By prefixing
MAX_PRESSURE with a '_', the library will not retrive the parameter definition
from the database (there is none anyway).
"""

import os
import logging

try:
    from math import isnan
except ImportError:
    # Define if python < 2.6
    def isnan(n):
        return n != n


class memoize(object):

    def __init__(self, callable):
        self._cache = {}
        self._callable = callable

    def __call__(self, *args, **kwargs):
        cache = self._cache
        key = kwargs and (args, hash(tuple(kwargs.items()))) or args
        try:
            return cache[key]
        except KeyError:
            value = cache[key] = self._callable(*args, **kwargs)
            return value


def get_library_abspath():
    import inspect
    return os.path.split(os.path.abspath(inspect.getfile(
                       inspect.currentframe())))[0]


def set_list(L, i, value, fill=None):
    ''' Set a cell in a list. If the list is not long enough, extend it first.
        Args:
            L - the list
            i - the index
            value - the value to put at L[i]
            fill - the value to fill if the list is to be extended
    '''
    try:
        L[i] = value
    except IndexError:
        L.extend([fill] * (i - len(L) + 1))
        L[i] = value


import db
import formats


# Logging

_LIBLOG_HANDLER = logging.StreamHandler()
_LIBLOG_HANDLER.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

LOG = logging.getLogger('libcchdo')
LOG.setLevel(logging.DEBUG)
LOG.addHandler(_LIBLOG_HANDLER)


# Nice constants

RADIUS_EARTH = 6371.01 #km


LIBVER = 'SIOCCHDLIB'


COLOR_ESCAPE = '\x1b\x5b'
COLORS = {
    'RED': COLOR_ESCAPE + '1;31m',
    'YELLOW': COLOR_ESCAPE + '1;33m',
    'CYAN': COLOR_ESCAPE + '1;36m',
    'CLEAR': COLOR_ESCAPE + '0m',
}


LIBRARY_DB_FILE_PATH = os.path.join(get_library_abspath(), 
    'db', db.connect._DB_LIBRARY_FILE)


# Alias some model.datafile classes for legacy purposes
# TODO reference these classes directly rather than aliasing them here

import model.datafile

Column = model.datafile.Column
File = model.datafile.File
DataFile = model.datafile.DataFile
SummaryFile = model.datafile.SummaryFile
DataFileCollection = model.datafile.DataFileCollection


# Initialize the database file, if it is not present.


if not os.path.isfile(LIBRARY_DB_FILE_PATH):
    LOG.info(
        "The library's missing database file (%s) was auto-generated." % \
        LIBRARY_DB_FILE_PATH)
    import db.model.std
    import db.model.convert as convert
    db.model.std.create_all()

    std_session = db.model.std.session()
    std_session.add_all(convert.all_parameters())
    std_session.commit()
    std_session.close()
