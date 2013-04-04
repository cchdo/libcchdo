import os
import unittest
from tempfile import NamedTemporaryFile

from libcchdo.model.datafile import DataFile, Column
from libcchdo.formats.ctd import netcdf_oceansites as ctdncos
from libcchdo.formats.woce import fuse_datetime


class TestCTDNetCDFOceansites (unittest.TestCase):

    def setUp (self):
        self._infile = open(os.path.join(
            os.path.dirname(__file__),
            'samples/nc_hyd/i08s_33RR20070204_00001_00001_hy1.nc'), 'r')
        self.datafile = DataFile()
        self._outfile = NamedTemporaryFile()

        g = self.datafile.globals
        g['DATE'] = '12341231'
        g['TIME'] = '2359'
        g['LATITUDE'] = 90
        g['LONGITUDE'] = 180
        g['DEPTH'] = -1
        g['EXPOCODE'] = 'test'
        g['STNNBR'] = '20'
        g['CASTNO'] = '5'
        g['_OS_ID'] = 'OS1'
        fuse_datetime(self.datafile)

    def tearDown (self):
        self._infile.close()

    def _setupData(self):
        self.datafile['CTDPRS'] = Column('CTDPRS')
        self.datafile['CTDPRS'].append(1, 2)
        self.datafile['CTDOXY'] = Column('CTDOXY')
        self.datafile['CTDOXY'].append(1, 2)
        self.datafile.check_and_replace_parameters()
        p = self.datafile['CTDOXY'].parameter
        p.description = 'ctd oxygen'
        p.bound_lower = 0
        p.bound_upper = 200

    def test_write (self):
        self.assertRaises(AttributeError, ctdncos.write, self.datafile, self._outfile)

        self._setupData()
        ctdncos.write(self.datafile, self._outfile)

    def test_write_timeseries (self):
        self._setupData()
        ctdncos.write(self.datafile, self._outfile, timeseries='BATS')
