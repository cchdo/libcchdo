import datetime
import sys
import os
import unittest
import tempfile

from ..model import datafile
from ..formats.ctd import netcdf_oceansites as ctdncos


class TestCTDNetCDFOceansites (unittest.TestCase):

    def setUp (self):
        self._infile = open('tests/samples/nc_hyd/i08s_33RR20070204_00001_00001_hy1.nc', 'r')
        self.datafile = datafile.DataFile()
        self._outfile = tempfile.NamedTemporaryFile()

        g = self.datafile.globals
        g['DATE'] = '12341231'
        g['TIME'] = '2359'
        g['LATITUDE'] = 90
        g['LONGITUDE'] = 180
        g['DEPTH'] = -1
        g['EXPOCODE'] = 'test'
        g['STNNBR'] = '20'
        g['CASTNO'] = '5'

    def tearDown (self):
        self._infile.close()

    def _setupData(self):
        self.datafile['CTDOXY'] = datafile.Column('CTDOXY')
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
