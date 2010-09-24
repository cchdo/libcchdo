"""libcchdo

Internal Data Specification
---------------------------
Any unreported values must be represented as None. This includes -9, -999.000,
unspecified dates, times, etc.

Known unknown parameters have mnemonics that start with '_'. e.g. MAX PRESSURE
exists in certain files but there is no parameter defined for it. By prefixing
MAX_PRESSURE with a '_', the library will not retrive the parameter definition
from the database (there is none anyway).
"""

import logging
import __builtin__


class memoize(object):
    """Memoization decorator class"""

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


# XXX EVIL


def _import_decorator(old_import, post_processor):
    """
       Args:
         old_import - The import function to decorate, most likely
                      ``__builtin__.__import__``.
         post_processor - Function of the form
                          `post_processor(module, __import__) -> module`.
       Returns: A new import function, most likely to be assigned to
                ``__builtin__.__import__``.
    """
    assert all([callable(fun) for fun in (old_import, post_processor)])

    def new_import(*args, **kwargs):
        module = old_import(*args, **kwargs)

        __builtin__.__import__ = old_import
        module = post_processor(module)
        return module

    return new_import


def post_import(fn):
    """Evil post-import hook for modules.
       The import function is decorated and then undecorated after
       post-processing. See _import_decorator for processor specification.
    """
    assert callable(fn)

    __builtin__.__import__ = _import_decorator(__builtin__.__import__, fn)


# XXX END EVIL


def get_library_abspath():
    """Give the absolute path of the directory that is the root of the 
       package, i.e. it contains this file.
    """
    import os
    import inspect
    return os.path.split(os.path.abspath(inspect.stack()[0][1]))[0]


import formats


# Nice constants

RADIUS_EARTH = 6371.01 #km


COLOR_ESCAPE = '\x1b\x5b'
COLORS = {
    'BOLDRED': COLOR_ESCAPE + '1;31m',
    'BOLDYELLOW': COLOR_ESCAPE + '1;33m',
    'RED': COLOR_ESCAPE + '0;31m',
    'GREEN': COLOR_ESCAPE + '0;32m',
    'YELLOW': COLOR_ESCAPE + '0;33m',
    'CYAN': COLOR_ESCAPE + '0;36m',
    'CLEAR': COLOR_ESCAPE + '0m',
}


# Logging

import datetime
import os

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

    def _get_color(self, level):
        try:
            return self._level_to_color[level]
        except KeyError:
            return 'GREEN'

    def format(self, record):
        record.__dict__['asctime'] = self.formatTime(record, self.datefmt)
        record.__dict__['message'] = record.getMessage()
        record.__dict__['color'] = COLORS[self._get_color(record.levelno)]
        return self._fmt % record.__dict__

    def formatTime(self, record, fmt):
        if not fmt:
        	fmt = self.datefmt
        now = datetime.datetime.utcnow()
        return '%s,%d' % (now.strftime(fmt), now.microsecond / 1000.0)


_LIBLOG_HANDLER = logging.StreamHandler()
_LIBLOG_HANDLER.setFormatter(_LibLogFormatter(
    ''.join(('%(color)s%(asctime)s_%(pathname)s:%(lineno)d_%(name)s %(levelname)s: ',
        COLORS['CLEAR'], '%(message)s')), "%Y-%j %H%M:%S"))

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
LOG.addHandler(_LIBLOG_HANDLER)
