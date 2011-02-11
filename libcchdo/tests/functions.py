import math
import tempfile
import os
import datetime
import zipfile
import unittest

from .. import fns


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
        td = tempfile.mkstemp('notacchdofile.txt')
        self.assertRaises(ValueError, fns.read_arbitrary, os.fdopen(td[0]))
        td = tempfile.mkstemp('test_functions.py')
        self.assertRaises(ValueError, fns.read_arbitrary, os.fdopen(td[0]))
        # TODO check more

    def test_great_circle_distance(self):
        self.assertEqual(14095.0562929323, fns.great_circle_distance(0, 0, 0, 180))
        # TODO check this value

    def test_strftime_iso(self):
        self.assertEqual(
            '2010-05-04T13:42:39Z',
            fns.strftime_iso(datetime.datetime(2010, 5, 4, 13, 42, 39)))

    def test_equal_with_epsilon(self):
        self.assertTrue(fns.equal_with_epsilon(1, 1 + 1e-7))
        self.assertFalse(fns.equal_with_epsilon(1, 1 + 1e-5))
        self.assertFalse(fns.equal_with_epsilon(1, 1 + 1e-7, 1e-7))
        self.assertTrue(fns.equal_with_epsilon(1, 1 + 1e-7, 1e-6))

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
        t = tempfile.NamedTemporaryFile(suffix='su.txt')
        fns.read_arbitrary(t)

        t = tempfile.NamedTemporaryFile(suffix='.hot.su.txt')
        fns.read_arbitrary(t)

        t = tempfile.NamedTemporaryFile(suffix='hy.txt')
        self.assertRaises(ValueError, fns.read_arbitrary, t)

        t = tempfile.NamedTemporaryFile(suffix='hy1.csv')
        self.assertRaises(ValueError, fns.read_arbitrary, t)

        t = tempfile.NamedTemporaryFile(suffix='hy1.nc')
        self.assertRaises(RuntimeError, fns.read_arbitrary, t)

        t = tempfile.NamedTemporaryFile(suffix='nc_hyd.zip')
        self.assertRaises(zipfile.BadZipfile, fns.read_arbitrary, t)

        t = tempfile.NamedTemporaryFile(suffix='ct1.csv')
        self.assertRaises(ValueError, fns.read_arbitrary, t)

        t = tempfile.NamedTemporaryFile(suffix='ct1.zip')
        self.assertRaises(zipfile.BadZipfile, fns.read_arbitrary, t)

        t = tempfile.NamedTemporaryFile(suffix='ctd.nc')
        self.assertRaises(RuntimeError, fns.read_arbitrary, t)

        t = tempfile.NamedTemporaryFile(suffix='nc_ctd.zip')
        self.assertRaises(zipfile.BadZipfile, fns.read_arbitrary, t)

        t = tempfile.NamedTemporaryFile(suffix='unk.unk')
        self.assertRaises(ValueError, fns.read_arbitrary, t)
