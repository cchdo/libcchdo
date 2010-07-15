""" Test case for libcchdo global functions """

from unittest import TestCase
from math import pi
import tempfile
import os
import datetime

import libcchdo


class TestFunctions(TestCase):

    def test_uniquify(self):
        xs = ['a', 'b', 'a', 'a', 'b']
        self.assertEqual(['a', 'b'], libcchdo.fns.uniquify(xs))

    def test_strip_all(self):
        xs = ['a', 'b   ', '   c', '  d   ']
        self.assertEqual(['a', 'b', 'c', 'd'], libcchdo.fns.strip_all(xs))

    def test_read_arbitrary(self):
        td = tempfile.mkstemp('notacchdofile.txt')
        self.assertRaises(ValueError, libcchdo.fns.read_arbitrary, os.fdopen(td[0]))
        td = tempfile.mkstemp('test_functions.py')
        self.assertRaises(ValueError, libcchdo.fns.read_arbitrary, os.fdopen(td[0]))
        # TODO check more

    def test_great_circle_distance(self):
        self.assertEqual(14095.0562929323, libcchdo.fns.great_circle_distance(0, 0, 0, 180))
        # TODO check this value

    def test_strftime_iso(self):
        self.assertEqual(
            '2010-05-04T13:42:39Z',
            libcchdo.fns.strftime_iso(datetime.datetime(2010, 5, 4, 13, 42, 39)))

    def test_equal_with_epsilon(self):
        self.assertTrue(libcchdo.fns.equal_with_epsilon(1, 1 + 1e-7))
        self.assertFalse(libcchdo.fns.equal_with_epsilon(1, 1 + 1e-5))
        self.assertFalse(libcchdo.fns.equal_with_epsilon(1, 1 + 1e-7, 1e-7))
        self.assertTrue(libcchdo.fns.equal_with_epsilon(1, 1 + 1e-7, 1e-6))

    def test_out_of_band(self):
        self.assertTrue(libcchdo.fns.out_of_band(-999))

        self.assertFalse(libcchdo.fns.out_of_band(-1000))
        self.assertFalse(libcchdo.fns.out_of_band(-998))
        self.assertFalse(libcchdo.fns.out_of_band(-998.9))

        self.assertTrue(libcchdo.fns.out_of_band(-998.91))
        self.assertTrue(libcchdo.fns.out_of_band(-999.09))

        self.assertFalse(libcchdo.fns.out_of_band(9))

        self.assertFalse(libcchdo.fns.out_of_band(''))
        self.assertTrue(libcchdo.fns.out_of_band(None))

    def test_in_band_or_none(self):
        self.assertTrue(libcchdo.fns.in_band_or_none(0) == 0)
        self.assertTrue(libcchdo.fns.in_band_or_none(-999) is None)

        self.assertTrue(libcchdo.fns.in_band_or_none(-9, -9) is None)
        self.assertFalse(libcchdo.fns.in_band_or_none(-9, -10) is None)
        self.assertFalse(libcchdo.fns.in_band_or_none(-9, -10, 1) is None)
        self.assertTrue(libcchdo.fns.in_band_or_none(-9, -10, 1.1) is None)

    def test_identity_or_oob(self):
        self.assertEqual(1, libcchdo.fns.identity_or_oob(1))
        self.assertEqual(-999, libcchdo.fns.identity_or_oob(None))

    def test_polynomial(self):
        self.assertEqual(0, libcchdo.fns.polynomial(5, []))
        self.assertEqual(1, libcchdo.fns.polynomial(5, [1]))
        self.assertEqual(5, libcchdo.fns.polynomial(5, [0, 1]))
        self.assertEqual(30, libcchdo.fns.polynomial(5, [0, 1, 1]))
        self.assertEqual(50, libcchdo.fns.polynomial(5, [0, 5, 1]))
