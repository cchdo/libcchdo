""" Test case for libcchdo initialization """


from unittest import TestCase

import libcchdo

class TestInit(TestCase):

    def test_memoize(self):
        @libcchdo.memoize
        def fib(n):
            if n <= 1:
                return n
            return fib(n - 1) + fib(n - 2)
        self.assertTrue(isinstance(fib, libcchdo.memoize), 'memoize did not decorate the function')
        self.assertEqual(fib(2), 1)
        self.assertEqual(fib(2), 1)

    def test_post_import(self):
        def nothing():
            pass

        libcchdo.post_import(nothing)

    def test_get_library_abspath(self):
        import os
        import inspect
        path = os.path.split(os.path.split(os.path.abspath(
                  inspect.stack()[0][1]))[0])[0]
        self.assertEqual(libcchdo.get_library_abspath(), path)


    def test_log_unknown_level(self):
        l = libcchdo.LOG
        l.log(11, 'test unknown log level')

