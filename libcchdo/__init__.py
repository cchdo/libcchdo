"""
This library provides a format-less data model for the CCHDO and a centralized
place to put ways to read and write from it. Said format-less data model is
based on DataFiles which have Columns that are associated with Parameters. From
the data model, the user may write out to a database model of the data or read
in more data and mash it together or write it out in a different format. When
it is said the data is format-less, it is actually in a neutral format that
lets it be manipulated easily into other formats.

Reading/writing data to files
=============================
Data is represented internally as described in the model package. Currently
the main model used is datafile. Refer to the formats package for more with 
regard to file formats.

Internal Data Specification
===========================
Any unreported values must be represented as None. This includes -9, -999.000,
unspecified dates, times, etc.

Known unknown parameters have mnemonics that start with '_'. e.g. MAX PRESSURE
exists in certain files but there is no parameter defined for it. By prefixing
MAX_PRESSURE with a '_', the library will not retrive the parameter definition
from the database (there is none anyway).
"""

import datetime
import logging
import os
import __builtin__
import functools

from StringIO import StringIO as pyStringIO
try:
    from cStringIO import StringIO
except ImportError:
    StringIO = pyStringIO


__version__ = "0.7.1"


# Database cache for parameters will be ensured
check_cache = True


class memoize(object):
    '''Decorator. Caches a function's return value each time it is called.
    If called later with the same arguments, the cached value is returned 
    (not reevaluated).

    Grabbed from http://wiki.python.org/moin/PythonDecoratorLibrary#Memoize 
    2012-04-20

    '''
    def __init__(self, func):
       self.func = func
       self.cache = {}

    def __call__(self, *args):
       try:
          return self.cache[args]
       except KeyError:
          value = self.func(*args)
          self.cache[args] = value
          return value
       except TypeError:
          # uncachable -- for instance, passing a list as an argument.
          # Better to not cache than to blow up entirely.
          return self.func(*args)

    def __repr__(self):
       '''Return the function's docstring.'''
       return self.func.__doc__

    def __get__(self, obj, objtype):
       '''Support instance methods.'''
       return functools.partial(self.__call__, obj)


@memoize
def get_library_abspath():
    """Give the absolute path of the directory that is the root of the 
       package, i.e. it contains this file.
    """
    return os.path.split(os.path.realpath(__file__))[0]


import formats


# Nice constants


RADIUS_EARTH = 6371.01 #km


COLOR_ESCAPE = '\x1b\x5b'


COLORS = {
    'WHITE': COLOR_ESCAPE + '1;39m',
    'BOLDRED': COLOR_ESCAPE + '1;31m',
    'BOLDYELLOW': COLOR_ESCAPE + '1;33m',
    'RED': COLOR_ESCAPE + '0;31m',
    'GREEN': COLOR_ESCAPE + '0;32m',
    'YELLOW': COLOR_ESCAPE + '0;33m',
    'BLUE': COLOR_ESCAPE + '1;34m',
    'CYAN': COLOR_ESCAPE + '0;36m',
    'CLEAR': COLOR_ESCAPE + '0m',
}


# Logging


class _LibLogFormatter(logging.Formatter):

    _level_to_color = {
        logging.DEBUG: 'CYAN',
        logging.INFO: 'GREEN',
        logging.WARNING: 'BOLDYELLOW',
        logging.ERROR: 'RED',
        logging.CRITICAL: 'BOLDRED',
    }

    def __init__(self, fmt=None, datefmt=None):
        logging.Formatter.__init__(self, fmt, datefmt)
        self.library_abspath = get_library_abspath()

    def _get_color(self, level):
        try:
            return self._level_to_color[level]
        except KeyError:
            return 'GREEN'

    def format(self, record):
        d = record.__dict__
        d['asctime'] = self.formatTime(record, self.datefmt)
        d['message'] = record.getMessage()
        d['color_path'] = COLORS['BLUE']
        d['color_level'] = COLORS[self._get_color(record.levelno)]
        d['pathname'] = d['pathname'].replace(self.library_abspath, '')
        return self._fmt % d

    def formatTime(self, record, fmt):
        if not fmt:
        	fmt = self.datefmt
        now = datetime.datetime.utcnow()
        return '%s,%d' % (now.strftime(fmt), now.microsecond / 1000.0)


_LIBLOG_HANDLER = logging.StreamHandler()
_LIBLOG_HANDLER.setFormatter(_LibLogFormatter(
    ''.join((
        '%(asctime)-21s %(color_level)s%(levelname)s', COLORS['CLEAR'],
        ': %(message)s %(color_path)s%(name)s%(pathname)s:%(lineno)d',
        COLORS['CLEAR'])), "%Y-%j %H:%M:%S"))

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
LOG.addHandler(_LIBLOG_HANDLER)
