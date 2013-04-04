import unittest
from StringIO import StringIO

from libcchdo.model.datafile import DataFile
from libcchdo.formats.ctd import netcdf as ctdnc

from libcchdo.tests import sample_file


class TestCTDNetCDF (unittest.TestCase):

    def setUp (self):
        self.infile = open(sample_file('i08s_33RR20070204_00101_ctd.nc'))

    def tearDown (self):
        self.infile.close()

    def test_read (self):
        self.datafile = DataFile()
        ctdnc.read(self.datafile, self.infile)
        self.assertTrue(True)

    def test_read_write (self):
        self.datafile = DataFile()
        ctdnc.read(self.datafile, self.infile)
        self.output_buffer = StringIO()
        ctdnc.write(self.datafile, self.output_buffer)
        self.output_buffer.close()
        self.assertTrue(True)

