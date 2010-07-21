#!/usr/bin/env python

import StringIO
import sys
import os
import unittest

import libcchdo
import libcchdo.formats.ctd.netcdf_oceansites as osncdf


class TestCTDNetCDFOceansites (unittest.TestCase):

    def setUp (self):
        self._infile = open("tests/samples/nc_hyd/i08s_33RR20070204_00001_00001_hy1.nc", "r")

    def tearDown (self):
        self._infile.close()

    def test_write (self):
        self.datafile = libcchdo.DataFile()
