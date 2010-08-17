""" Test case for libcchdo.formats.woce """

from unittest import TestCase
import datetime

import libcchdo
import libcchdo.formats.woce as fw


class TestFormatsWoce(TestCase):

    def test_woce_lat_to_dec_lat(self):
        toks = ['12', '34.567', 'N']
        self.assertAlmostEqual(12.57611666667,
            fw.woce_lat_to_dec_lat(toks))
        toks = ['12', '34.567', 'S']
        self.assertAlmostEqual(-12.57611666667,
            fw.woce_lat_to_dec_lat(toks))
  
    def test_woce_lng_to_dec_lng(self):
        toks = ['12', '34.567', 'E']
        self.assertAlmostEqual(12.57611666667,
            fw.woce_lng_to_dec_lng(toks))
        toks = ['12', '34.567', 'W']
        self.assertAlmostEqual(-12.57611666667,
            fw.woce_lng_to_dec_lng(toks))
  
    def test_dec_lat_to_woce_lat(self):
        self.assertEqual('12 34.57 N',
            fw.dec_lat_to_woce_lat(12.576116666667))
        self.assertEqual('12 34.57 S',
            fw.dec_lat_to_woce_lat(-12.576116666667))
  
    def test_dec_lng_to_woce_lng(self):
        self.assertEqual(' 12 34.57 E',
            fw.dec_lng_to_woce_lng(12.576116666667))
        self.assertEqual(' 12 34.57 W',
            fw.dec_lng_to_woce_lng(-12.576116666667))

    def test_strptime_woce_date_time(self):
        self.assertEqual(None, fw.strptime_woce_date_time(None, None))
        self.assertEqual(None, fw.strptime_woce_date_time(1234, None))
        self.assertEqual(None, fw.strptime_woce_date_time(None, 5432))

        self.assertRaises(ValueError, fw.strptime_woce_date_time, 'a', 12345)
        self.assertRaises(ValueError, fw.strptime_woce_date_time, 12345, 'a')

        self.assertEqual(
            datetime.datetime(2010, 03, 31, 16, 59),
            fw.strptime_woce_date_time(20100331, 1659))
