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

from datetime import datetime
import logging
import os
import __builtin__
import functools

from StringIO import StringIO as pyStringIO
try:
    from cStringIO import StringIO
except ImportError:
    StringIO = pyStringIO

from libcchdo.ui import TERMCOLOR


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
        d['color_path'] = TERMCOLOR['BLUE']
        d['color_level'] = TERMCOLOR[self._get_color(record.levelno)]
        d['levelname'] = record.levelname[0]
        path = d['pathname']
        path = path.replace(self.library_abspath + '/', '')
        d['pathname'] = path
        return self._fmt % d

    def formatTime(self, record, fmt):
        if not fmt:
        	fmt = self.datefmt
        now = datetime.utcnow()
        return '%s,%d' % (now.strftime(fmt), now.microsecond / 1000.0)


_LIBLOG_HANDLER = logging.StreamHandler()
_LIBLOG_HANDLER.setFormatter(_LibLogFormatter(
    ''.join((
        '%(asctime)-11s %(color_level)s%(levelname)s ',
        '%(color_path)s%(pathname)s:%(lineno)d', TERMCOLOR['CLEAR'],
        '\t%(message)s', TERMCOLOR['CLEAR'])), "%H%M:%S"))

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
LOG.addHandler(_LIBLOG_HANDLER)
