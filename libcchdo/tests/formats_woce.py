from datetime import datetime, date, time
from StringIO import StringIO
from contextlib import closing

from libcchdo.tests import BaseTestCase, sample_file
from libcchdo.log import LOG
from libcchdo.fns import Decimal
from libcchdo.formats import woce
from libcchdo.formats.bottle import woce as botwoce
from libcchdo.formats.summary import woce as sumwoce
from libcchdo.model.datafile import DataFile, SummaryFile, Column


class TestFormatsWoce(BaseTestCase):

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
        toks = ['000', '0.00', 'E']
        self.assertAlmostEqual(Decimal('0'),
            woce.woce_lng_to_dec_lng(toks))
        lng = woce.woce_lng_to_dec_lng(toks)
  
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

        today = datetime.combine(date.today(), time(0, 0))
        self.assertEqual(
            today,
            woce.strptime_woce_date_time(today.strftime('%Y%m%d'), None))

        self.assertTrue(woce.strptime_woce_date_time('a', 12345) is None)
        self.assertTrue(woce.strptime_woce_date_time(12345, 'a') is None)

        # Bad time gives back the date as a datetime with time set to 0000
        self.assertEqual(
            datetime(2010, 03, 31, 0, 0),
            woce.strptime_woce_date_time(20100331, 81))

        # Bad time above 2400 returns a datetime with time as 0000
        self.assertEqual(
            datetime(2010, 03, 31, 0, 0),
            woce.strptime_woce_date_time(20100331, 2513))

        self.assertEqual(
            datetime(2010, 03, 31, 16, 59),
            woce.strptime_woce_date_time(20100331, 1659))

    def test_combine(self):
        botin = open(sample_file('bottle_woce', 'p01w_1999ahy.txt'), 'r')
        sumin = open(sample_file('summary_woce', 'p01w_1999asu.txt'), 'r')

        botfile = DataFile()
        sumfile = SummaryFile()

        botwoce.read(botfile, botin)
        sumwoce.read(sumfile, sumin)

        woce.combine(botfile, sumfile)
        self.assertTrue('X13' in botfile['STNNBR'].values)

    def test_split_datetime_no_extras(self):
        """Splitting non-existant date time column should not create DATE and 
        TIME columns.

        """
        dfile = DataFile()
        woce.split_datetime(dfile)
        with self.assertRaises(KeyError):
            dfile['DATE']
        with self.assertRaises(KeyError):
            dfile['TIME']

    def test_write_data_qualt1_fill(self):
        """Length of quality word must be at least QUALT1?"""
        dfile = DataFile()
        cols = []
        for cname in ['AAA', 'BBB', 'CCC']:
            col = dfile[cname] = Column(cname)
            col.values = [None]
            col.flags_woce = [9]
            cols.append(col)

        # Short QUALT1 word must be left padded to at least the length of
        # 'QUALT1'
        with closing(StringIO()) as output:
            cols, base_format = \
                woce.columns_and_base_format(dfile)
            woce.write_data(dfile, output, cols, base_format)
            result = output.getvalue().split('\n')
            self.assertEqual(' ' * 6 + '*', result[2][8 * len(cols):])

        # Long QUALT1 word results in header being left padded.
        for cname in ['DDD', 'EEE', 'FFF', 'GGG']:
            col = dfile[cname] = Column(cname)
            col.values = [None]
            col.flags_woce = [9]
            cols.append(col)
        with closing(StringIO()) as output:
            cols, base_format = \
                woce.columns_and_base_format(dfile)
            woce.write_data(dfile, output, cols, base_format)
            result = output.getvalue().split('\n')
            self.assertEqual(' ' * 2 + 'QUALT1', result[0][8 * len(cols):])
            self.assertEqual(' ' * 7 + '*', result[2][8 * len(cols):])
            self.assertEqual(' ' + '9' * len(cols), result[3][8 * len(cols):])

    def test_write_data_fill_value(self):
        dfile = DataFile()
        cols = []
        col = dfile['AAA'] = Column('AAA')
        col.values = [None]
        col.flags_woce = [9]
        cols.append(col)
        with closing(StringIO()) as output:
            woce.write_data(dfile, output, cols, '{0:>8} {1:>1}\n')
            result = output.getvalue().split('\n')
            self.assertEqual('      -9', result[3][:8])
