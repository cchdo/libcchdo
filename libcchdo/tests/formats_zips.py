import unittest

from libcchdo.model.datafile import DataFileCollection
from libcchdo.formats.ctd.zip import exchange as ctdzipex
from libcchdo.formats.ctd.zip import netcdf as ctdzipnc
from libcchdo.formats.ctd.zip import woce as ctdzipwoce
from libcchdo.formats.bottle.zip import netcdf as botzipnc

from libcchdo.tests import sample_file

class TestCTDZipExchange(unittest.TestCase):
    def setUp(self):
        self.infile = open(sample_file('i08s_33RR20070204_ct1.zip'))

    def tearDown(self):
        self.infile.close()

    def test_read(self):
        self.datafile = DataFileCollection()
        ctdzipex.read(self.datafile, self.infile)
        self.assertTrue(True)


class TestCTDZipNetCDF(unittest.TestCase):
    def setUp(self):
        self.infile = open(sample_file('i08s_33RR20070204_nc_ctd.zip'))

    def tearDown(self):
        self.infile.close()

    def test_read(self):
        self.datafile = DataFileCollection()
        ctdzipnc.read(self.datafile, self.infile)
        self.assertTrue(True)


class TestCTDZipWoce(unittest.TestCase):
    def setUp(self):
        self.infile = open(sample_file('i08s_33RR20070204ct.zip'))

    def tearDown(self):
        self.infile.close()

    def test_read(self):
        self.datafile = DataFileCollection()
        ctdzipwoce.read(self.datafile, self.infile)
        self.assertTrue(True)


class TestBotZipNetCDF(unittest.TestCase):
    def setUp(self):
        self.infile = open(sample_file('nc_hyd', 'i08s_33RR20070204_nc_hyd.zip'))

    def tearDown(self):
        self.infile.close()

    def test_read(self):
        self.datafile = DataFileCollection()
        botzipnc.read(self.datafile, self.infile)
        self.assertTrue(True)
