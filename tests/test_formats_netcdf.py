""" Test case for libcchdo.formats.netcdf """

from unittest import TestCase
import sys

import libcchdo.formats.netcdf as fnc

class TestFormatsNetCDF(TestCase):

    def test_import(self):
        # clobber path
        saved_path = sys.path
        sys.path = []

        try:
            del sys.modules['netCDF3']
            reload(fnc)
            self.assertTrue(False)
        except ImportError:
            pass
        finally:
            sys.path = saved_path
