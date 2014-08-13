from unittest import TestCase
import os.path
from contextlib import closing

from libcchdo.datadir.store import PycchdoDatastore


class TestPycchdo(TestCase):
    def test_queuefile_info(self):
        with self.assertRaises(ValueError):
            ppp = PycchdoDatastore()
            ppp._queuefile_info({'id': '1234'})
