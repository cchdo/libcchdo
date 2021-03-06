from datetime import timedelta
import sys
import unittest

from libcchdo.formats import netcdf as fnc


class TestFormatsNetCDF(unittest.TestCase):

    def test_import(self):
        # clobber path
        saved_path = sys.path
        sys.path = []

        try:
            del sys.modules['netCDF4']
        except KeyError:
            self.fail('Module netCDF4 should already be imported')
        try:
            reload(fnc)
            self.assertTrue(False)
        except ImportError:
            pass
        finally:
            sys.path = saved_path

    def test_simplest_str(self):
        self.assertEqual('1', fnc.simplest_str(1))
        self.assertEqual('1', fnc.simplest_str(1.0))
        self.assertEqual('1.1', fnc.simplest_str(1.1))
        self.assertEqual('1', fnc.simplest_str(1.0000000001))

    def test_pad_station_cast(self):
        self.assertEqual('00001', fnc._pad_station_cast(1))
        self.assertEqual('00012', fnc._pad_station_cast(12))
        self.assertEqual('00012', fnc._pad_station_cast(12.0))
        self.assertEqual('012.1', fnc._pad_station_cast(12.1))

    def test_get_filename(self):
        self.assertEqual('TESTEXPO_00001_00002_hy1.nc',
                         fnc.get_filename('TESTEXPO', 1, 2, 'hy1'))
        self.assertEqual('TESTEXPO_001.2_00002_hy1.nc',
                         fnc.get_filename('TESTEXPO', 1.2, 2, 'hy1'))

    def test_minutes_since_epoch(self):
        dtime = fnc.EPOCH + timedelta(minutes=1234)
        self.assertEqual(1234, fnc.minutes_since_epoch(dtime))

        dtime = fnc.EPOCH - timedelta(minutes=10)
        self.assertEqual(-10, fnc.minutes_since_epoch(dtime))

        dtime = None
        self.assertEqual(-9, fnc.minutes_since_epoch(dtime))
