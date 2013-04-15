import unittest

from libcchdo.util import get_library_abspath, memoize
from libcchdo.log import LOG

class TestUtil(unittest.TestCase):

    def test_memoize(self):
        @memoize
        def fib(n):
            if n <= 1:
                return n
            return fib(n - 1) + fib(n - 2)
        self.assertTrue(isinstance(fib, memoize), 'memoize did not decorate the function')
        self.assertEqual(fib(2), 1)
        self.assertEqual(fib(2), 1)

    def test_get_library_abspath(self):
        import os
        s = os.path.split
        path = s(s(__file__)[0])[0]
        self.assertEqual(get_library_abspath(), path)

    def test_log_unknown_level(self):
        LOG.log(11, 'test unknown log level')