"""Logging."""
from datetime import datetime
from contextlib import contextmanager
from re import compile as re_compile
from logging import (
    Formatter, StreamHandler, Filter, getLogger,
    DEBUG, INFO, WARNING, ERROR, CRITICAL
    )

from libcchdo.util import get_library_abspath
from libcchdo.ui import TERMCOLOR


class LibLogFormatter(Formatter):

    _level_to_color = {
        DEBUG: 'CYAN',
        INFO: 'GREEN',
        WARNING: 'BOLDYELLOW',
        ERROR: 'RED',
        CRITICAL: 'BOLDRED',
    }

    def __init__(self, fmt=None, datefmt=None):
        Formatter.__init__(self, fmt, datefmt)
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


class RebuildOceanSITESFilter(Filter):
    """Filter logs for rebuilding OceanSITES timeserieses."""

    os_ignore_re = re_compile(
        '|'.join(['({0})'.format(xxx) for xxx in 
            ['No unit converter', 'Mismatched units for',
             'Netcdf name for param', ' too small for ',
            'Falling back from depth integration', 'Stripped blank',
            'This may be caused by', 'too large for',
            'Looking through aliases for ']]))

    def filter(self, record):
        """Return zero to keep record out of log."""
        if self.os_ignore_re.match(record.msg):
            return 0
        return 1


LIBLOG_HANDLER = StreamHandler()
LIBLOG_HANDLER.setFormatter(LibLogFormatter(
    u''.join((
        '%(asctime)-11s %(color_level)s%(levelname)s ',
        '%(color_path)s%(pathname)s:%(lineno)d %(funcName)s', TERMCOLOR['CLEAR'],
        '\t%(message)s', TERMCOLOR['CLEAR'])), "%H%M:%S"))


def setup():
    log = getLogger(__name__.split('.')[0])
    log.setLevel(DEBUG)
    log.addHandler(LIBLOG_HANDLER)


@contextmanager
def log_above(level=ERROR, log=getLogger(__name__)):
    current_level = log.getEffectiveLevel()
    log.setLevel(level)
    try:
        yield
    finally:
        log.setLevel(current_level)
