import os
from unittest import TestCase
from logging import StreamHandler
from tempfile import TemporaryFile
from logging import getLogger

from libcchdo.db import connect
from libcchdo.log import LIBLOG_HANDLER


log = getLogger(__name__)


def setUpModule():
    getLogger('libcchdo').removeHandler(LIBLOG_HANDLER)


def tearDownModule():
    getLogger('libcchdo').addHandler(LIBLOG_HANDLER)


def sample_file(*args):
    return os.path.join(os.path.dirname(__file__), 'samples', *args)


# use in-memory database for testing
engine_std = connect._connect('sqlite://')
engine_legacy = connect._connect('sqlite://')


class BaseTestCase(TestCase):
    """Base test case that silences logging."""

    def _unload_handlers(self):
        self.saved_handlers = []
        for handler in log.handlers:
            log.removeHandler(handler)
            self.saved_handlers.append(handler)

    def _reload_handlers(self):
        for handler in self.saved_handlers:
            log.addHandler(handler)
        self.saved_handlers = []
        
    def setUp(self):
        self._unload_handlers()
        self.logstream = TemporaryFile()
        self.loghandler = StreamHandler(self.logstream)
        getLogger('libcchdo').addHandler(self.loghandler)

    def tearDown(self):
        log.removeHandler(self.loghandler)
        self._reload_handlers()

    def ensure_lines(self, lines):
        self.logstream.seek(0)
        for line in self.logstream:
            if not lines:
                break
            for query in lines:
                if type(query) is list:
                    found = 0
                    for qqq in query:
                        if qqq in line:
                            found += 1
                    if found == len(query):
                        lines.remove(query)
                else:
                    if query in line:
                        lines.remove(query)
        if not lines:
            return True
        print 'Missing log lines', repr(lines)
        self.logstream.seek(0)
        print repr(self.logstream.read())
        return False
