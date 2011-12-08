import unittest
import datetime

from ..formats import woce as fw
from ..fns import Decimal


class TestFormatsWoce(unittest.TestCase):

    def test_woce_lat_to_dec_lat(self):
        toks = ['12', '34.567', 'N']
        self.assertAlmostEqual(Decimal('12.576116'),
            fw.woce_lat_to_dec_lat(toks), 5)
        toks = ['12', '34.567', 'S']
        self.assertAlmostEqual(Decimal('-12.576116'),
            fw.woce_lat_to_dec_lat(toks), 5)
  
    def test_woce_lng_to_dec_lng(self):
        toks = ['12', '34.567', 'E']
        self.assertAlmostEqual(Decimal('12.57611666667'),
            fw.woce_lng_to_dec_lng(toks))
        toks = ['12', '34.567', 'W']
        self.assertAlmostEqual(Decimal('-12.57611666667'),
            fw.woce_lng_to_dec_lng(toks))
  
    def test_dec_lat_to_woce_lat(self):
        self.assertEqual('12 34.57 N',
            fw.dec_lat_to_woce_lat(Decimal('12.576116666667')))
        self.assertEqual('12 34.57 S',
            fw.dec_lat_to_woce_lat(Decimal('-12.576116666667')))
  
    def test_dec_lng_to_woce_lng(self):
        self.assertEqual(' 12 34.57 E',
            fw.dec_lng_to_woce_lng(Decimal('12.576116666667')))
        self.assertEqual(' 12 34.57 W',
            fw.dec_lng_to_woce_lng(Decimal('-12.576116666667')))

    def test_strptime_woce_date_time(self):
        self.assertEqual(None, fw.strptime_woce_date_time(None, None))
        self.assertEqual(None, fw.strptime_woce_date_time(None, 5432))

        today = datetime.date.today()
        self.assertEqual(
            today,
            fw.strptime_woce_date_time(today.strftime('%Y%m%d'), None))

        self.assertTrue(fw.strptime_woce_date_time('a', 12345) is None)
        self.assertTrue(fw.strptime_woce_date_time(12345, 'a') is None)

        self.assertEqual(
            datetime.datetime(2010, 03, 31, 16, 59),
            fw.strptime_woce_date_time(20100331, 1659))
