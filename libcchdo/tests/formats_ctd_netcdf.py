import unittest
import StringIO
import os

from ..model import datafile
from ..formats.ctd import netcdf as ctdnc


class TestCTDNetCDF (unittest.TestCase):

    def setUp (self):
        self.infile = open(os.path.join(
            os.path.dirname(__file__),
            'samples/i08s_33RR20070204_00101_ctd.nc'), 'r')

    def tearDown (self):
        self.infile.close()

    def test_read (self):
        self.datafile = datafile.DataFile()
        ctdnc.read(self.datafile, self.infile)
        self.assertTrue(True)

    def test_read_write (self):
        self.datafile = datafile.DataFile()
        ctdnc.read(self.datafile, self.infile)
        self.output_buffer = StringIO.StringIO()
        ctdnc.write(self.datafile, self.output_buffer)
        self.output_buffer.close()
        self.assertTrue(True)

