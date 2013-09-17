import os
from unittest import TestCase
from logging import StreamHandler
from tempfile import TemporaryFile

from libcchdo.log import LOG


def sample_file(*args):
    return os.path.join(os.path.dirname(__file__), 'samples', *args)


class BaseTestCase(TestCase):
    """Base test case that silences logging."""

    def _unload_handlers(self):
        self.saved_handlers = []
        for handler in LOG.handlers:
            LOG.removeHandler(handler)
            self.saved_handlers.append(handler)

    def _reload_handlers(self):
        for handler in self.saved_handlers:
            LOG.addHandler(handler)
        self.saved_handlers = []
        
    def setUp(self):
        self._unload_handlers()
        self.logstream = TemporaryFile()
        self.loghandler = StreamHandler(self.logstream)
        LOG.addHandler(self.loghandler)

    def tearDown(self):
        LOG.removeHandler(self.loghandler)
        self._reload_handlers()
