""" Test case for libcchdo.formats.bottle.netcdf """

# Unit test loading considers the file a module and tries to load it if using from module import.
import StringIO
import sys
import os
from unittest import TestCase

import libcchdo
import libcchdo.formats.bottle.netcdf as netcdfbot

class TestBottleNetCDF(TestCase):

  def setUp(self):
      self.infile = open(
          'tests/samples/nc_hyd/i08s_33RR20070204_00001_00001_hy1.nc', 'r')

  def assertAlmostEqualOrNones(self, x, y):
      if x is None:
          self.assert_(y is None)
      else:
          self.assertAlmostEqual(x, y)

  def test_read(self):
      self.file = libcchdo.DataFile()
      netcdfbot.read(self.file, self.infile)
      self.infile.close()

      nitrite_values = (0.11, None, 0.08, 0.08, 0.08, 0.08, 0.06, 0.03, 0.06,
                        0.04, 0.03, None, 0.03, None, 0.03, None)
      map(lambda x: self.assertAlmostEqualOrNones(*x),
          zip(nitrite_values, self.file.columns['NITRIT'].values))

      freon11_values = (6.063, 6.055, 5.795, 5.619, 5.486, 5.508, 5.487,
                        5.683, 5.422, 5.190, 5.222, None, 5.289, None,
                        5.250, 5.254)
      map(lambda x: self.assertAlmostEqualOrNones(*x),
          zip(freon11_values, self.file.columns['CFC-11'].values))

      freon113_values = (None, ) * 16
      map(lambda x: self.assertAlmostEqualOrNones(*x),
          zip(freon113_values, self.file.columns['CFC113'].values))

      expocodes = ['33RR20070204'] * 16
      self.assertEqual(expocodes, self.file.columns['EXPOCODE'].values)

  def test_read_multiple(self):
      self.file = libcchdo.DataFile()
      netcdfbot.read(self.file, self.infile)
      self.infile.close()

      nitrite_values = (0.11, None, 0.08, 0.08, 0.08, 0.08, 0.06, 0.03, 0.06,
                        0.04, 0.03, None, 0.03, None, 0.03, None)
      map(lambda x: self.assertAlmostEqualOrNones(*x),
          zip(nitrite_values, self.file.columns['NITRIT'].values))

      freon11_values = (6.063, 6.055, 5.795, 5.619, 5.486, 5.508, 5.487,
                        5.683, 5.422, 5.190, 5.222, None, 5.289, None,
                        5.250, 5.254)
      map(lambda x: self.assertAlmostEqualOrNones(*x),
          zip(freon11_values, self.file.columns['CFC-11'].values))

      freon113_values = (None, ) * 16
      map(lambda x: self.assertAlmostEqualOrNones(*x),
          zip(freon113_values, self.file.columns['CFC113'].values))

      expocodes = ['33RR20070204'] * 16
      self.assertEqual(expocodes, self.file.columns['EXPOCODE'].values)

      # Read second file
      infile2 = open('tests/samples/nc_hyd/p03a_00199_00001_hy1.nc', 'r')
      netcdfbot.read(self.file, infile2)

      # Make sure all columns have the same length
      length = None
      for c in self.file.columns.values():
          if not length:
              length = len(c.values)
          else:
              self.assertEquals(len(c.values), length)
              if c.is_flagged_woce():
                  self.assertEquals(len(c.flags_woce), length)
              if c.is_flagged_igoss():
                  self.assertEquals(len(c.flags_igoss), length)
      

      infile2.close()

  def test_write(self):
      self.file = libcchdo.DataFile()
      # TODO
      self.infile.close()
