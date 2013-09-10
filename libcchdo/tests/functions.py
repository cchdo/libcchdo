import math
from tempfile import mkstemp, NamedTemporaryFile
from os import fdopen
from datetime import datetime
from zipfile import BadZipfile
import unittest

from libcchdo import fns
from libcchdo.formats.formats import read_arbitrary


class TestFunctions(unittest.TestCase):

    def test_python_less_than_26(self):
        isnan = math.isnan
        math.isnan = None
        reload(fns)
        self.assertTrue(fns.isnan == None)
        math.isnan = isnan
        reload(fns)
        self.assertFalse(fns.isnan(1))
        self.assertTrue(fns.isnan(float('nan')))

    def test_isnan(self):
        self.assertFalse(fns.isnan(1))
        self.assertFalse(fns.isnan(1.0))
        self.assertTrue(fns.isnan(float('nan')))

    def test_uniquify(self):
        xs = ['a', 'b', 'a', 'a', 'b']
        self.assertEqual(['a', 'b'], fns.uniquify(xs))

    def test_strip_all(self):
        xs = ['a', 'b   ', '   c', '  d   ']
        self.assertEqual(['a', 'b', 'c', 'd'], fns.strip_all(xs))

    def test_read_arbitrary(self):
        td = mkstemp('notacchdofile.txt')
        self.assertRaises(ValueError, read_arbitrary, fdopen(td[0]))
        td = mkstemp('test_functions.py')
        self.assertRaises(ValueError, read_arbitrary, fdopen(td[0]))
        # TODO check more

    def test_great_circle_distance(self):
        self.assertEqual(14095.0562929323, fns.great_circle_distance(0, 0, 0, 180))
        # TODO check this value

    def test_strftime_iso(self):
        self.assertEqual(
            '2010-05-04T13:42:39Z',
            fns.strftime_iso(datetime(2010, 5, 4, 13, 42, 39)))

    def test_equal_with_epsilon(self):
        self.assertTrue(fns.equal_with_epsilon(1, 1 + 1e-7))
        self.assertFalse(fns.equal_with_epsilon(1, 1 + 1e-5))
        self.assertFalse(fns.equal_with_epsilon(1, 1 + 1e-7, 1e-7))
        print '>>>>>>>>>>>>>>>>>>>>>>>>', fns.equal_with_epsilon(1, 1 + 1e-7, 1e-6)
        self.assertTrue(fns.equal_with_epsilon(1, 1 + 1e-7, 1e-6))

    def test_ordinal_datetime_to_datetime(self):
        self.assertEqual(
            fns.ordinal_datetime_to_datetime(728647), datetime(1994, 12, 19))
        self.assertEqual(
            fns.ordinal_datetime_to_datetime(728647.75),
            datetime(1994, 12, 19, 18, 0, 0))

    def test_out_of_band(self):
        self.assertTrue(fns.out_of_band(-999))

        self.assertFalse(fns.out_of_band(-1000))
        self.assertFalse(fns.out_of_band(-998))
        self.assertFalse(fns.out_of_band(-998.9))

        self.assertTrue(fns.out_of_band(-998.91))
        self.assertTrue(fns.out_of_band(-999.09))

        self.assertFalse(fns.out_of_band(9))

        self.assertFalse(fns.out_of_band(''))
        self.assertTrue(fns.out_of_band(None))

    def test_in_band_or_none(self):
        self.assertTrue(fns.in_band_or_none(0) == 0)
        self.assertTrue(fns.in_band_or_none(-999) is None)

        self.assertTrue(fns.in_band_or_none(-9, -9) is None)
        self.assertFalse(fns.in_band_or_none(-9, -10) is None)
        self.assertFalse(fns.in_band_or_none(-9, -10, 1) is None)
        self.assertTrue(fns.in_band_or_none(-9, -10, 1.1) is None)

    def test_identity_or_oob(self):
        self.assertEqual(1, fns.identity_or_oob(1))
        self.assertEqual(-999, fns.identity_or_oob(None))

    def test_polynomial(self):
        self.assertEqual(0, fns.polynomial(5, []))
        self.assertEqual(1, fns.polynomial(5, [1]))
        self.assertEqual(5, fns.polynomial(5, [0, 1]))
        self.assertEqual(30, fns.polynomial(5, [0, 1, 1]))
        self.assertEqual(50, fns.polynomial(5, [0, 5, 1]))

    def test_read_arbitrary(self):
        # TODO
        t = NamedTemporaryFile(suffix='su.txt')
        read_arbitrary(t)

        t = NamedTemporaryFile(suffix='.hot.su.txt')
        read_arbitrary(t)

        t = NamedTemporaryFile(suffix='hy.txt')
        self.assertRaises(ValueError, read_arbitrary, t)

        t = NamedTemporaryFile(suffix='hy1.csv')
        self.assertRaises(ValueError, read_arbitrary, t)

        t = NamedTemporaryFile(suffix='hy1.nc')
        self.assertRaises(RuntimeError, read_arbitrary, t)

        t = NamedTemporaryFile(suffix='nc_hyd.zip')
        self.assertRaises(BadZipfile, read_arbitrary, t)

        t = NamedTemporaryFile(suffix='ct1.csv')
        # TODO

        t = NamedTemporaryFile(suffix='ct1.zip')
        self.assertRaises(BadZipfile, read_arbitrary, t)

        t = NamedTemporaryFile(suffix='ctd.nc')
        self.assertRaises(RuntimeError, read_arbitrary, t)

        t = NamedTemporaryFile(suffix='nc_ctd.zip')
        self.assertRaises(BadZipfile, read_arbitrary, t)

        t = NamedTemporaryFile(suffix='unk.unk')
        self.assertRaises(ValueError, read_arbitrary, t)
