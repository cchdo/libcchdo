import unittest
import datetime

from libcchdo.fns import Decimal
from libcchdo.tests import sample_file
from libcchdo.formats import woce
from libcchdo.formats.bottle import woce as botwoce
from libcchdo.formats.summary import woce as sumwoce
from libcchdo.model import datafile as df


class TestFormatsWoce(unittest.TestCase):

    def test_woce_lat_to_dec_lat(self):
        toks = ['12', '34.567', 'N']
        self.assertAlmostEqual(Decimal('12.576116'),
            woce.woce_lat_to_dec_lat(toks), 5)
        toks = ['12', '34.567', 'S']
        self.assertAlmostEqual(Decimal('-12.576116'),
            woce.woce_lat_to_dec_lat(toks), 5)
  
    def test_woce_lng_to_dec_lng(self):
        toks = ['12', '34.567', 'E']
        self.assertAlmostEqual(Decimal('12.57611666667'),
            woce.woce_lng_to_dec_lng(toks))
        toks = ['12', '34.567', 'W']
        self.assertAlmostEqual(Decimal('-12.57611666667'),
            woce.woce_lng_to_dec_lng(toks))
  
    def test_dec_lat_to_woce_lat(self):
        self.assertEqual('12 34.57 N',
            woce.dec_lat_to_woce_lat(Decimal('12.576116666667')))
        self.assertEqual('12 34.57 S',
            woce.dec_lat_to_woce_lat(Decimal('-12.576116666667')))
  
    def test_dec_lng_to_woce_lng(self):
        self.assertEqual(' 12 34.57 E',
            woce.dec_lng_to_woce_lng(Decimal('12.576116666667')))
        self.assertEqual(' 12 34.57 W',
            woce.dec_lng_to_woce_lng(Decimal('-12.576116666667')))

    def test_strptime_woce_date_time(self):
        self.assertEqual(None, woce.strptime_woce_date_time(None, None))
        self.assertEqual(None, woce.strptime_woce_date_time(None, 5432))

        today = datetime.datetime.combine(
            datetime.date.today(), datetime.time(0, 0))
        self.assertEqual(
            today,
            woce.strptime_woce_date_time(today.strftime('%Y%m%d'), None))

        self.assertTrue(woce.strptime_woce_date_time('a', 12345) is None)
        self.assertTrue(woce.strptime_woce_date_time(12345, 'a') is None)

        # Bad time gives back the date as a datetime with time set to 0000
        self.assertEqual(
            datetime.datetime(2010, 03, 31, 0, 0),
            woce.strptime_woce_date_time(20100331, 81))

        # Bad time above 2400 returns a datetime with time as 0000
        self.assertEqual(
            datetime.datetime(2010, 03, 31, 0, 0),
            woce.strptime_woce_date_time(20100331, 2513))

        self.assertEqual(
            datetime.datetime(2010, 03, 31, 16, 59),
            woce.strptime_woce_date_time(20100331, 1659))

    def test_combine(self):
        botin = open(sample_file('bottle_woce', 'p01w_1999ahy.txt'), 'r')
        sumin = open(sample_file('summary_woce', 'p01w_1999asu.txt'), 'r')

        botfile = df.DataFile()
        sumfile = df.SummaryFile()

        botwoce.read(botfile, botin)
        sumwoce.read(sumfile, sumin)

        woce.combine(botfile, sumfile)
        self.assertTrue('X13' in botfile['STNNBR'].values)
